
// The default planner will be what Andy Pavlo calls a "German Style" bottom-up planner/optimizer. 
// It will obviously not be as sophisticated as the one in HyPer/Umbra, but it will follow
// roughly the same principles. It will use the DPccp algorithm implemented in the optimizer module.

use std::{collections::BTreeMap};

use crate::{execution::{plan::{PhysicalQueryPlan, self, PhysicalQueryPlanOperator, StdOutTupleWriter}}, optimizer::{query_graph::QueryGraph, optimizer::{run_dp_ccp, OptimizerResult}}, types::TupleValue, planner::BoundTableRef};

use super::{Planner, Query, PlannerError, BoundAttribute};

pub struct BottomUpPlanner {}

impl BottomUpPlanner{
    pub fn new() -> Self {
        Self {}
    }
}

impl Planner for BottomUpPlanner {
    fn plan(&self, query: &Query) -> Result<PhysicalQueryPlan, PlannerError> {
        let query_graph = prepare_query_graph(query);

        let optimizer_result = run_dp_ccp(&query_graph);
        
        // collect the positions of all attributes to add the projection
        let cost = optimizer_result.cost;
        let projection = get_projection(query, optimizer_result);
        let attribute_names = query.select.iter().map(BoundAttribute::get_qualified_name).collect();
        let root_operator = PhysicalQueryPlanOperator::Print { 
            input: Box::new(projection), 
            tuple_writer: Box::new(StdOutTupleWriter::new(attribute_names)) 
        };

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

#[cfg(test)]
mod test {
    // TODO: Add tests

    use super::*;
    use crate::{catalog::{TableDesc, AttributeDesc}, types::TupleValueType, planner::{BoundAttribute, BoundTable}};

    #[test]
    fn test_plan_single_relation_query() {
        let query = Query {
            select: vec![
                BoundAttribute {
                    attribute: AttributeDesc {
                        id: 1,
                        name: "id".to_string(),
                        table_ref: 0,
                        data_type: TupleValueType::Int,
                        nullable: false,
                    },
                    binding: None
                }
            ],
            from: BTreeMap::from_iter(vec![(BoundTableRef { 
                table_ref: 0,
                binding: None
             }, BoundTable {
                binding: None,
                table: TableDesc {
                id: 0,
                segment_id: 1000,
                name: "test".to_string(),
                attributes: vec![
                    AttributeDesc {
                        id: 1,
                        name: "id".to_string(),
                        table_ref: 1,
                        data_type: TupleValueType::Int,
                        nullable: false,
                    },
                    AttributeDesc {
                        id: 2,
                        name: "name".to_string(),
                        table_ref: 1,
                        data_type: TupleValueType::VarChar(500),
                        nullable: false,
                    },
                ],
                cardinality: 100,
            }})].iter().cloned()),
            selections: Vec::new(),
            join_predicates: Vec::new(),
        };
        let planner = BottomUpPlanner {};
        let plan = planner.plan(&query).unwrap();
        assert_eq!(plan.cost, 0.0);
        match plan.root_operator {
            PhysicalQueryPlanOperator::Print { input, tuple_writer } => {
                match *input {
                    PhysicalQueryPlanOperator::Projection { projection_ius, input } => {
                        assert_eq!(projection_ius, vec![0]);
                        match *input {
                            PhysicalQueryPlanOperator::Tablescan { table } => {
                                assert_eq!(table, TableDesc {
                                    id: 0,
                                    segment_id: 1000,
                                    name: "test".to_string(),
                                    attributes: vec![
                                        AttributeDesc {
                                            id: 1,
                                            name: "id".to_string(),
                                            table_ref: 1,
                                            data_type: TupleValueType::Int,
                                            nullable: false,
                                        },
                                        AttributeDesc {
                                            id: 2,
                                            name: "name".to_string(),
                                            table_ref: 1,
                                            data_type: TupleValueType::VarChar(500),
                                            nullable: false,
                                        },
                                    ],
                                    cardinality: 100,
                                });
                            },
                            _ => unreachable!()
                        }
                    },
                    _ => unreachable!()
                }
            }
            _ => unreachable!() 
        }
    }
}