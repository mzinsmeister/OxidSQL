use std::collections::{BTreeSet, HashMap};

use itertools::Itertools;

use super::query_graph::{QueryGraph};

// Quite a few todos still here. This is more like a quick and dirty first sketch of a DPccp optimizer.
// Also since we have neither the representations for the operators nor the representations for the
// Query yet, we have our own operators here and a placeholder for the predicate representation. 
// It's not unlikely the operator will keep this primitive Operator interface since the optimizer
// basically only optimizes join order.

pub enum Operator {
    TableScan{relation_id: usize},
    Selection{input: Box<Operator>, predicate: ()}, // TODO Placeholder: Predicate representation
    Join{left: Box<Operator>, right: Box<Operator>, predicate: ()}, // TODO Placeholder: Predicate representation
}

pub struct OptimizerResult {
    pub plan_root: Operator,
    pub cost: f64,
}

enum DpJoinRepresentation {
    Join(BTreeSet<usize>, BTreeSet<usize>, f64),
    Relation(usize, Option<()>), //Optional = predicate placeholder
}

impl DpJoinRepresentation {
    fn get_cost(&self, query_graph: &QueryGraph) -> f64 {
        match self {
            DpJoinRepresentation::Relation(relation_id, _) => {
                0.0
            }
            DpJoinRepresentation::Join(_, _, cost) => {
                *cost
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
// (number of intermediate tuples, good heuristic fo how simple it is)
pub(super) fn run_dp_ccp(query_graph: &QueryGraph) -> OptimizerResult {
    let mut dp_table = HashMap::new();

    let mut all_relations = BTreeSet::new();

    for relation in query_graph.node_iterator() {
        let pred = query_graph.get_edge(relation.relation_id, relation.relation_id)
            .map(|e| e.predicate);
        dp_table.insert(
            BTreeSet::from_iter(vec![relation.relation_id]),
            DpJoinRepresentation::Relation(relation.relation_id, pred),
        );
        all_relations.insert(relation.relation_id);
        let set = BTreeSet::from_iter(vec![relation.relation_id]);
        let ban_set = query_graph.node_iterator().filter(|x| x.relation_id <= relation.relation_id).map(|x| x.relation_id).collect();
        for csg in enumerate_csg_rec(query_graph, &set, ban_set) {
            for cmp in enumerate_cmp(query_graph, &csg) {
                let mut output_card = 0.0;
                for node in csg.iter().chain(cmp.iter()){
                    output_card *= query_graph.get_node(*node).result_cardinality as f64;
                }
                for &node1 in &csg {
                    for &node2 in &cmp {
                        output_card *= query_graph.get_edge(node1, node2)
                                            .map(|e| e.selectivity)
                                            .unwrap_or(1.0) as f64;
                    }
                }
                let cost = dp_table[&csg].get_cost(query_graph) + 
                            dp_table[&cmp].get_cost(query_graph) + 
                            output_card;
                let total_subset = csg.union(&cmp).copied().collect::<BTreeSet<usize>>();
                let previous_cost = dp_table.get(&total_subset)
                                .map(|n| n.get_cost(query_graph))
                                .unwrap_or(f64::MAX);
                if cost < previous_cost {
                    dp_table.insert(total_subset, DpJoinRepresentation::Join(csg.clone(), cmp, cost));
                }
            }
        }
    }
    
    let cost = dp_table[&all_relations].get_cost(query_graph);
    let plan_root = dp_get_operator_tree(&dp_table, &all_relations);
    OptimizerResult { plan_root, cost }
}


fn enumerate_csg_rec(query_graph: &QueryGraph, subgraph: &BTreeSet<usize>, 
                        ban_set: BTreeSet<usize>) -> Vec<BTreeSet<usize>> {
    let mut neighbors = BTreeSet::new();
    for &node in subgraph {
        query_graph.neighbors(node)
            .for_each(|n| {neighbors.insert(n.relation_id);});
    }

    neighbors.retain(|n| !subgraph.contains(n) && !ban_set.contains(n));

    let mut result = Vec::new();
    for i in 1..neighbors.len() {
        for subset in neighbors.iter().copied().combinations(i) {
            result.push(BTreeSet::from_iter(subset).union(&subgraph).copied().collect());
        }
    }

    for c in neighbors.iter().copied().combinations(neighbors.len()) {
        let s = BTreeSet::from_iter(c);
        let s_new = subgraph.union(&s).copied().collect::<BTreeSet<usize>>();
        let new_ban_set = ban_set.union(&neighbors).copied().collect::<BTreeSet<usize>>();
        result.append(&mut enumerate_csg_rec(query_graph, &s_new, new_ban_set));
    }
    result
}

fn enumerate_cmp(query_graph: &QueryGraph, subgraph: &BTreeSet<usize>) -> Vec<BTreeSet<usize>> {
    let min_subgraph = *subgraph.iter().min().unwrap();
    let mut ban_set = BTreeSet::from_iter(subgraph.iter().filter(|&&n| n < min_subgraph).copied());
    let mut neighbors = BTreeSet::new();
    for &node in subgraph {
        query_graph.neighbors(node)
            .for_each(|n| {neighbors.insert(n.relation_id);});
    }
    neighbors.retain(|n| !ban_set.contains(n));
    let mut result = Vec::new();
    for &vi in neighbors.iter().rev() {
        result.push(BTreeSet::from_iter(vec![vi]));
        let mut bi = BTreeSet::from_iter(query_graph.node_iterator()
            .filter(|&&n| n.relation_id <= vi)
            .map(|q| q.relation_id));
        bi.retain(|n| ban_set.contains(n));
        let new_ban_set = bi.union(&neighbors).copied().collect::<BTreeSet<usize>>();
        result.append(&mut enumerate_csg_rec(query_graph, &BTreeSet::from_iter(vec![vi]), new_ban_set));
    }
    result
}
    

fn dp_get_operator_tree(dp_table: &HashMap<BTreeSet<usize>, DpJoinRepresentation>, subset: &BTreeSet<usize>) -> Operator {
    match &dp_table[subset] {
        DpJoinRepresentation::Relation(relation_id, predicate) => {
            let scan = Operator::TableScan{relation_id: *relation_id};
            if let Some(predicate) = predicate {
                Operator::Selection{input: Box::new(scan), predicate: predicate.clone()}
            } else {
                scan
            }
        },
        DpJoinRepresentation::Join(left, right, cost) => {
            let left_tree = dp_get_operator_tree(dp_table, left);
            let right_tree = dp_get_operator_tree(dp_table, right);
            Operator::Join{left: Box::new(left_tree), right: Box::new(right_tree), predicate: ()}
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