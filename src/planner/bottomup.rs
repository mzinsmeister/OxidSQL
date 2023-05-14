
// The default planner will be what Andy Pavlo calls a "German Style" bottom-up planner/optimizer. 
// It will obviously not be as sophisticated as the one in HyPer/Umbra, but it will follow
// roughly the same principles. It will use the DPccp algorithm implemented in the optimizer module.

use std::{collections::BTreeMap};

use crate::{execution::{plan::{PhysicalQueryPlan, self, PhysicalQueryPlanOperator}}, optimizer::{query_graph::QueryGraph, optimizer::{run_dp_ccp, OptimizerResult}}, types::TupleValue, planner::BoundTableRef};

use super::{Planner, Query, PlannerError};

struct BottomUpPlanner {}

impl Planner for BottomUpPlanner {
    fn plan(&self, query: &Query) -> Result<PhysicalQueryPlan, PlannerError> {
        let query_graph = prepare_query_graph(query);

        let optimizer_result = run_dp_ccp(&query_graph);
        
        // collect the positions of all attributes to add the projection
        let cost = optimizer_result.cost;
        let root_operator = get_projection(query, optimizer_result);

        Ok(PhysicalQueryPlan {
            root_operator,
            cost
        })
    }
}

fn get_projection(query: &Query, optimizer_result: OptimizerResult) -> PhysicalQueryPlanOperator {
    let plan = optimizer_result.plan_root;

    let mut projection_attribute_indexes = Vec::new();

    for select_attribute in &query.select {
        let table_ref = select_attribute.get_table_ref();
        let table_index = optimizer_result.inputs_order.iter().position(|t| t == &table_ref).unwrap();
        let attribute_index = query.from[&table_ref].table.get_attribute_index_by_id(select_attribute.attribute.id).unwrap()
            + optimizer_result.inputs_order.iter().take(table_index).map(|t| query.from[t].table.attributes.len()).sum::<usize>();
        projection_attribute_indexes.push(attribute_index);
    }

    plan::PhysicalQueryPlanOperator::Projection {
        projection_ius: projection_attribute_indexes,
        input: Box::new(plan),
    }

}

fn prepare_query_graph(query: &Query) -> QueryGraph {
    let (relation_cardinalities, relation_predicates) = get_cardinality_estimates(query);
    let mut query_graph = QueryGraph::new();
    for (relation_id, relation) in &query.from {
        let table_scan = plan::PhysicalQueryPlanOperator::Tablescan {
            table: relation.table.clone()
        };
        let mut predicate = None;
        for (attribute_id, value) in relation_predicates.get(relation_id).unwrap_or(&Vec::new()) {
            let column_index = relation.table.get_attribute_index_by_id(*attribute_id).unwrap();
            let eq = plan::BooleanExpression::Eq(
                plan::ArithmeticExpression::Column(column_index), 
                plan::ArithmeticExpression::Literal(value.clone().unwrap())
            );
            predicate = match predicate {
                None => Some(eq),
                Some(p) => Some(plan::BooleanExpression::And(Box::new(p), Box::new(eq)))
            }
        }
        let plan = if let Some(predicate) = predicate {
            plan::PhysicalQueryPlanOperator::Selection {
                predicate,
                input: Box::new(table_scan),
            }
        } else {
            table_scan
        };
        query_graph.add_node(relation.clone(), relation_cardinalities[relation_id], plan);
    }
    for (from_attribute, to_attribute) in &query.join_predicates {
        // We again assume uniqueness (very stupid assumption)
        let cross_cardinality = relation_cardinalities[&from_attribute.get_table_ref()] * relation_cardinalities[&to_attribute.get_table_ref()];
        let result_cardinality = relation_cardinalities[&from_attribute.get_table_ref()].min(relation_cardinalities[&to_attribute.get_table_ref()]);
        let selectivity = result_cardinality as f64 / cross_cardinality as f64;
        query_graph.add_edge(&from_attribute.get_table_ref(), &to_attribute.get_table_ref(), selectivity, (from_attribute.to_ref(), to_attribute.to_ref()));
    }
    query_graph
}

fn get_cardinality_estimates(query: &Query) -> (BTreeMap<BoundTableRef, u64>, BTreeMap<BoundTableRef, Vec<(u32, Option<TupleValue>)>>) {
    let mut relation_cardinalities = BTreeMap::new();
    let mut relation_predicates: BTreeMap<BoundTableRef, Vec<(u32, Option<TupleValue>)>> = BTreeMap::new();
    for (_, relation) in &query.from {
        relation_cardinalities.insert(relation.to_ref(), relation.table.cardinality);
    }
    for (attribute, value) in &query.selections {
        relation_cardinalities.insert(attribute.get_table_ref(), 1); // Assume uniqueness for now
        relation_predicates.entry(attribute.get_table_ref())
            .or_insert_with(Vec::new)
            .push((attribute.attribute.id, Some(value.clone())));
    }
    (relation_cardinalities, relation_predicates)
}

