use std::collections::{BTreeSet, HashMap};

use itertools::Itertools;

use crate::{execution::plan::{PhysicalQueryPlanOperator, self}, planner::{BoundTableRef, BoundAttribute, BoundAttributeRef}};

use super::query_graph::{QueryGraph};

// Quite a few todos still here. This is more like a quick and dirty first sketch of a DPccp optimizer.
// Also since we have neither the representations for the operators nor the representations for the
// Query yet, we have our own operators here and a placeholder for the predicate representation. 
// It's not unlikely the operator will keep this primitive Operator interface since the optimizer
// basically only optimizes join order.

pub struct OptimizerResult {
    pub plan_root: PhysicalQueryPlanOperator,
    pub cost: f64,
    pub inputs_order: Vec<BoundTableRef>
}

enum DpJoinRepresentation {
    Join(BTreeSet<BoundTableRef>, BTreeSet<BoundTableRef>, Vec<(BoundAttributeRef, BoundAttributeRef)>, f64, f64),
    Relation(BoundTableRef, f64, PhysicalQueryPlanOperator),
}

impl DpJoinRepresentation {
    fn get_cost(&self) -> f64 {
        match self {
            DpJoinRepresentation::Relation(input_id, _, _) => {
                0.0
            }
            DpJoinRepresentation::Join(_, _, _, cost, _) => {
                *cost
            }
        }
    }

    fn get_size(&self) -> f64 {
        match self {
            DpJoinRepresentation::Relation(_, size, _) => {
                *size
            }
            DpJoinRepresentation::Join(_, _, _, _, size) => {
                *size
            }
        }
    }
}

/*
    TODO: Refactor. Implement iterators for connected subraphs and 
          complements instead of returing a vector.
          Also implement DPhyp to support non-inner joins.
 */

// For now hardcoded the C_out cost function 
// (number of intermediate tuples, good heuristic for how simple it is)
pub fn run_dp_ccp(query_graph: &QueryGraph) -> OptimizerResult {
    let mut dp_table = HashMap::new();

    let mut all_relations = BTreeSet::new();

    for relation in query_graph.node_iterator() {
        let pred = query_graph.get_edge(&relation.input.to_ref(), &relation.input.to_ref())
            .map(|e| e.predicate);
        dp_table.insert(
            BTreeSet::from_iter(vec![relation.input.to_ref()]),
            DpJoinRepresentation::Relation(relation.input.to_ref(), relation.result_cardinality as f64, relation.source_plan.clone()),
        );
        all_relations.insert(relation.input.to_ref());
        let set = BTreeSet::from_iter(vec![relation.input.to_ref()]);
        let ban_set = query_graph.node_iterator().filter(|x| x.input.to_ref() <= relation.input.to_ref()).map(|x| x.input.to_ref()).collect();
        for csg in enumerate_csg_rec(query_graph, &set, ban_set) {
            for cmp in enumerate_cmp(query_graph, &csg) {
                let mut output_card = 0.0;
                for node in csg.iter().chain(cmp.iter()){
                    output_card *= query_graph.get_node(node).result_cardinality as f64;
                }
                for node1 in &csg {
                    for node2 in &cmp {
                        output_card *= query_graph.get_edge(node1, node2)
                                            .map(|e| e.selectivity)
                                            .unwrap_or(1.0) as f64;
                    }
                }
                let cost = dp_table[&csg].get_cost() + 
                            dp_table[&cmp].get_cost() + 
                            output_card;
                let total_subset = csg.union(&cmp).cloned().collect::<BTreeSet<BoundTableRef>>();
                let previous_cost = dp_table.get(&total_subset)
                                .map(|n| n.get_cost())
                                .unwrap_or(f64::MAX);
                if cost < previous_cost {
                    let predicates = csg.iter().cartesian_product(cmp.iter())
                        .map(|(n1, n2)| query_graph.get_edge(n1, n2).unwrap().predicate)
                        .collect();
                    dp_table.insert(total_subset, DpJoinRepresentation::Join(csg.clone(), cmp, predicates, cost, output_card));
                }
            }
        }
    }
    
    let cost = dp_table[&all_relations].get_cost();
    let (plan_root, inputs_order, _) = dp_get_operator_tree(query_graph, &dp_table, &all_relations);
    OptimizerResult { plan_root, cost, inputs_order: inputs_order.iter().map(|(id, _)| id.clone()).collect() }
}


fn enumerate_csg_rec(query_graph: &QueryGraph, subgraph: &BTreeSet<BoundTableRef>, 
                        ban_set: BTreeSet<BoundTableRef>) -> Vec<BTreeSet<BoundTableRef>> {
    let mut neighbors = BTreeSet::new();
    for node in subgraph {
        query_graph.neighbors(node)
            .for_each(|n| {neighbors.insert(n.input.to_ref());});
    }

    neighbors.retain(|n| !subgraph.contains(n) && !ban_set.contains(n));

    let mut result = Vec::new();
    for i in 1..neighbors.len() {
        for subset in neighbors.iter().cloned().combinations(i) {
            result.push(BTreeSet::from_iter(subset).union(&subgraph).cloned().collect());
        }
    }

    for c in neighbors.iter().cloned().combinations(neighbors.len()) {
        let s = BTreeSet::from_iter(c);
        let s_new = subgraph.union(&s).cloned().collect::<BTreeSet<BoundTableRef>>();
        let new_ban_set = ban_set.union(&neighbors).cloned().collect::<BTreeSet<BoundTableRef>>();
        result.append(&mut enumerate_csg_rec(query_graph, &s_new, new_ban_set));
    }
    result
}

fn enumerate_cmp(query_graph: &QueryGraph, subgraph: &BTreeSet<BoundTableRef>) -> Vec<BTreeSet<BoundTableRef>> {
    let min_subgraph = subgraph.iter().min().unwrap();
    let ban_set = BTreeSet::from_iter(subgraph.iter().filter(|&n| n < min_subgraph).cloned());
    let mut neighbors = BTreeSet::new();
    for node in subgraph {
        query_graph.neighbors(node)
            .for_each(|n| {neighbors.insert(n.input.to_ref());});
    }
    neighbors.retain(|n| !ban_set.contains(n));
    let mut result = Vec::new();
    for vi in neighbors.iter().rev() {
        result.push(BTreeSet::from_iter(vec![vi.clone()]));
        let mut bi = BTreeSet::from_iter(query_graph.node_iterator()
            .filter(|n| n.input.to_ref() <= *vi)
            .map(|q| q.input.to_ref()));
        bi.retain(|n| ban_set.contains(n));
        let new_ban_set = bi.union(&neighbors).cloned().collect::<BTreeSet<BoundTableRef>>();
        result.append(&mut enumerate_csg_rec(query_graph, &BTreeSet::from_iter(vec![vi.clone()]), new_ban_set));
    }
    result
}
    

fn dp_get_operator_tree(query_graph: &QueryGraph, dp_table: &HashMap<BTreeSet<BoundTableRef>, DpJoinRepresentation>, subset: &BTreeSet<BoundTableRef>) -> (plan::PhysicalQueryPlanOperator, Vec<(BoundTableRef, usize)>, f64) {
    match &dp_table[subset] {
        DpJoinRepresentation::Relation(input_id, size, operator) => {
            let num_attributes = query_graph.get_node(input_id).input.table.attributes.len();
            (operator.clone(), vec![(input_id.clone(), num_attributes)], *size)
        },
        DpJoinRepresentation::Join(left, right, predicates, cost, output_card) => {
            let (mut left_tree, mut left_order, left_size) = dp_get_operator_tree(query_graph, dp_table, left);
            let (mut right_tree, mut right_order, right_size) = dp_get_operator_tree(query_graph, dp_table, right);
            if left_size > right_size {
                std::mem::swap(&mut left_tree, &mut right_tree);
                std::mem::swap(&mut left_order, &mut right_order); // Make left always smaller
            }
            left_order.append(&mut right_order);
            let mut operator_predicates = Vec::with_capacity(predicates.len());
            // TODO: This is quite inefficient and ugly. Refactor.
            for (left, right) in predicates {
                let left_table = left.get_table_ref();
                let right_table = right.get_table_ref();
                let left_index = left_order.iter().position(|(id, _)| id == &left_table).unwrap();
                let left_attr_index = query_graph.get_node(&left_table).input.table.get_attribute_index_by_id(left.attribute_ref).unwrap()
                    + left_order.iter().take(left_index).map(|(_, num)| num).sum::<usize>();
                let right_index = left_order.iter().position(|(id, _)| id == &right_table).unwrap();
                let right_attr_index = query_graph.get_node(&right_table).input.table.get_attribute_index_by_id(right.attribute_ref).unwrap()
                    + left_order.iter().take(right_index).map(|(_, num)| num).sum::<usize>();
                operator_predicates.push((left_index, right_index));
            }
            (plan::PhysicalQueryPlanOperator::HashJoin {
                left: Box::new(left_tree),
                right: Box::new(right_tree),
                on: operator_predicates
            }, left_order, *output_card)
        }
    }
}

#[cfg(test)]
mod test {

    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }

}