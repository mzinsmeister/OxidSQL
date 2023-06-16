
// The default planner will be what Andy Pavlo calls a "German Style" bottom-up planner/optimizer. 
// It will obviously not be as sophisticated as the one in HyPer/Umbra, but it will follow
// roughly the same principles. It will use the DPccp algorithm implemented in the optimizer module.

use std::{collections::BTreeMap};

use atomic::Ordering;

use crate::{execution::{plan::{PhysicalQueryPlan, self, PhysicalQueryPlanOperator, StdOutTupleWriter}}, optimizer::{query_graph::QueryGraph, optimizer::{run_dp_ccp, OptimizerResult}}, types::{TupleValue, TupleValueType}, planner::BoundTableRef, catalog::{Catalog, AttributeDesc, SAMPLE_SIZE}, storage::buffer_manager::BufferManager, access::{SlottedPageHeapStorage, SlottedPageSegment, HeapStorage}};

use super::{Planner, Query, PlannerError, BoundAttribute, SelectionOperator, SelectQuery, InsertQuery, CreateTableQuery};

pub struct BottomUpPlanner<B: BufferManager> {
    buffer_manager: B,
    catalog: Catalog<B>,
}

impl<B: BufferManager> BottomUpPlanner<B> {
    pub fn new(buffer_manager: B, catalog: Catalog<B>) -> Self {
        Self { buffer_manager, catalog }
    }

    fn plan_select(&self, query: &SelectQuery) -> Result<PhysicalQueryPlan, PlannerError> {
        let query_graph = match self.prepare_query_graph(query) {
            Ok(query_graph) => query_graph,
            Err(e) => return Err(PlannerError::Other(Box::new(e)))
        };

        let optimizer_result = run_dp_ccp(&query_graph);
        
        // collect the positions of all attributes to add the projection
        let cost = optimizer_result.cost;
        let projection = self.get_projection(query, optimizer_result);
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

    fn get_projection(&self, query: &SelectQuery, optimizer_result: OptimizerResult) -> PhysicalQueryPlanOperator {
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
    
    fn prepare_query_graph(&self, query: &SelectQuery) -> Result<QueryGraph, B::BError> {
        let (relation_cardinalities, relation_predicates) = self.get_cardinality_estimates(query)?;
        let mut query_graph = QueryGraph::new();
        for (relation_id, relation) in &query.from {
            let table_scan = plan::PhysicalQueryPlanOperator::Tablescan {
                table: relation.table.clone()
            };
            let mut predicate = None;
            for (attribute_id, value, operator) in relation_predicates.get(relation_id).unwrap_or(&Vec::new()) {
                let column_index = relation.table.get_attribute_index_by_id(*attribute_id).unwrap();
                let o1 = plan::ArithmeticExpression::Column(column_index);
                let o2 = plan::ArithmeticExpression::Literal(value.clone().unwrap());
                let cmp = match operator {
                    SelectionOperator::Eq => plan::BooleanExpression::Eq(o1, o2),
                    SelectionOperator::LessThan => plan::BooleanExpression::LessThan(o1, o2),
                    SelectionOperator::LessThanOrEq => plan::BooleanExpression::LessThanOrEq(o1, o2),
                    SelectionOperator::GreaterThan => plan::BooleanExpression::LessThan(o2, o1),
                    SelectionOperator::GreaterThanOrEq => plan::BooleanExpression::LessThanOrEq(o2, o1),
                };
                predicate = match predicate {
                    None => Some(cmp),
                    Some(p) => Some(plan::BooleanExpression::And(Box::new(p), Box::new(cmp)))
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
            let selectivity = if cross_cardinality == 0 { 1.0 } else { result_cardinality as f64 / cross_cardinality as f64 };
            query_graph.add_edge(&from_attribute.get_table_ref(), &to_attribute.get_table_ref(), selectivity, (from_attribute.to_ref(), to_attribute.to_ref()));
        }
        Ok(query_graph)
    }
    
    fn get_cardinality_estimates(&self, query: &SelectQuery) -> Result<(BTreeMap<BoundTableRef, u64>, BTreeMap<BoundTableRef, Vec<(u32, Option<TupleValue>, SelectionOperator)>>), B::BError> {
        let mut relation_predicates: BTreeMap<BoundTableRef, Vec<(u32, Option<TupleValue>, SelectionOperator)>> = BTreeMap::new();
        for selection in &query.selections {
            relation_predicates.entry(selection.attribute.get_table_ref())
                .or_insert_with(Vec::new)
                .push((selection.attribute.attribute.id, Some(selection.value.clone()), selection.operator));
        }
        let mut relation_cardinalities = BTreeMap::new();
        for (relation_id, relation) in &query.from {
            let statistics = self.catalog.get_statistics(relation_id.table_ref)?.expect("Every table should have statistics");
            let cardinality = if let Some(selections) = relation_predicates.get(relation_id) {
                // Execute the selections on the sample
                // TODO: Test whether this actually works as intended
                let mut sample_table = relation.table.clone();
                sample_table.segment_id = sample_table.sample_segment_id;
                sample_table.fsi_segment_id = sample_table.sample_fsi_segment_id;
                sample_table.attributes.insert(0, AttributeDesc { id: u32::MAX, table_ref: relation_id.table_ref, name: "__TID__".to_string(), data_type: TupleValueType::BigInt, nullable: false });
                sample_table.attributes.insert(0, AttributeDesc { id: u32::MAX, table_ref: relation_id.table_ref, name: "__SKIP_INDEX__".to_string(), data_type: TupleValueType::BigInt, nullable: false });
                sample_table.attributes.insert(0, AttributeDesc { id: u32::MAX, table_ref: relation_id.table_ref, name: "__INDEX__".to_string(), data_type: TupleValueType::BigInt, nullable: false });
                
                let sample_segment = SlottedPageSegment::new(self.buffer_manager.clone(), sample_table.segment_id, sample_table.fsi_segment_id);
                let sample_heap = SlottedPageHeapStorage::new(sample_segment, sample_table.attributes.iter().map(|a| a.data_type).collect());
                // TODO: Implement the approximation if no tuple in the sample fully matches using these partial matching counts
                let mut partial_matching_counts = vec![0; selections.len()];
                let mut total_count = 0;
                let attribute_indices = selections.iter().map(|(attribute_id, _, _)| sample_table.get_attribute_index_by_id(*attribute_id).unwrap()).collect::<Vec<_>>();
                let matching_count = sample_heap.scan_all(|tup| {
                    total_count += 1;
                    let mut matches = true;
                    for (i, (_, value, operator)) in selections.iter().enumerate() {
                        let attribute_index = attribute_indices[i];
                        let attribute_value = &tup.values[attribute_index];
                        let matches_selection = match operator {
                            SelectionOperator::Eq => attribute_value == value,
                            SelectionOperator::LessThan => attribute_value < value,
                            SelectionOperator::LessThanOrEq => attribute_value <= value,
                            SelectionOperator::GreaterThan => attribute_value > value,
                            SelectionOperator::GreaterThanOrEq => attribute_value >= value,
                        };
                        if !matches_selection {
                            matches = false;
                        }
                        if matches_selection {
                            partial_matching_counts[i] += 1;
                        }
                    }
                    matches
                })?.count();
                let base_cardinality = statistics.table_statistics.cardinality.load(Ordering::Relaxed);
                let cardinality_estimate = (matching_count as f64 / total_count as f64 * base_cardinality as f64) as u64;
                cardinality_estimate.max(base_cardinality / (SAMPLE_SIZE as u64 * 2)).max(1)
            } else {
                statistics.table_statistics.cardinality.load(Ordering::Relaxed)
            };
            relation_cardinalities.insert(relation_id.clone(), cardinality);
        }    
        Ok((relation_cardinalities, relation_predicates))
    }

    fn plan_insert(&self, insert: InsertQuery) -> Result<PhysicalQueryPlan, PlannerError> {
        let root_operator = PhysicalQueryPlanOperator::Insert {
            table: insert.table,
            input: Box::new(PhysicalQueryPlanOperator::InlineTable {
                 tuples: insert.values,
            }),
        };
        Ok(PhysicalQueryPlan { root_operator, cost: 0.0 })
    }

    fn plan_create_table(&self, create_table: CreateTableQuery) -> Result<PhysicalQueryPlan, PlannerError> {
        let root_operator = PhysicalQueryPlanOperator::CreateTable {
            table: create_table.table,
        };
        Ok(PhysicalQueryPlan { root_operator, cost: 0.0 })
    }
}

impl<B: BufferManager> Planner for BottomUpPlanner<B> {
    fn plan(&self, query: Query) -> Result<PhysicalQueryPlan, PlannerError> {
        match query {
            Query::Select(select) => self.plan_select(&select),
            Query::Insert(insert) => self.plan_insert(insert),
            Query::CreateTable(create_table) => self.plan_create_table(create_table),
        }
    }
}

#[cfg(test)]
mod test {
    // TODO: Add tests

    use std::sync::Arc;

    use super::*;
    use crate::{catalog::{TableDesc, AttributeDesc}, types::TupleValueType, planner::{BoundAttribute, BoundTable}, storage::{buffer_manager::mock::MockBufferManager, page::PAGE_SIZE}, config::DbConfig};

    #[test]
    fn test_plan_single_relation_query() {
        let table0 = TableDesc {
            id: 0,
            segment_id: 1000,
            fsi_segment_id: 1001,
            sample_segment_id: 1002,
            sample_fsi_segment_id: 1003,
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
        };

        let query = Query::Select(SelectQuery {
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
                table: table0.clone()})].iter().cloned()),
            selections: Vec::new(),
            join_predicates: Vec::new(),
        });
        let mock_bm = MockBufferManager::new(PAGE_SIZE);
        let catalog = Catalog::new(mock_bm.clone(), Arc::new(DbConfig { n_threads: 4 })).unwrap();
        catalog.create_table(&table0).unwrap();
        let planner = BottomUpPlanner { buffer_manager: mock_bm.clone(), catalog };
        let plan = planner.plan(query).unwrap();
        assert_eq!(plan.cost, 0.0);
        match plan.root_operator {
            PhysicalQueryPlanOperator::Print { input, tuple_writer: _ } => {
                match *input {
                    PhysicalQueryPlanOperator::Projection { projection_ius, input } => {
                        assert_eq!(projection_ius, vec![0]);
                        match *input {
                            PhysicalQueryPlanOperator::Tablescan { table } => {
                                assert_eq!(table, table0);
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