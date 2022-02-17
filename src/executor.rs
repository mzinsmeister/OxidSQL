use crate::storage::SledStorageEngine;
use crate::planner::{QueryPlan, QueryPlanNode};
use std::error::Error;
use crate::analyze::{QueryColumn, SqlFilterExpression, SqlComparisonExpression, SqlLogicalExpression, SqlComparisonItem, TableDefinition};
use crate::database::TupleValue;
use crate::catalog::{Catalog, ColumnMeta, TableMeta};
use nom::lib::std::collections::BTreeMap;

pub struct Executor {
    storage_engine: SledStorageEngine,
    catalog: Catalog
}

impl Executor {
    pub fn new(storage_engine: SledStorageEngine) -> Executor {
        Executor { 
            storage_engine: storage_engine.clone(),
            catalog: Catalog::new(storage_engine) 
        }
    }

    pub fn execute(&self, plan: &QueryPlan) -> Result<ResultSet, Box<dyn Error>> {
        let intermediate_result = self.execute_node(&plan.root_node)?;
        let data: Result<Vec<_>, _> = intermediate_result.row_iterator.collect();
        Ok(ResultSet {
            columns: intermediate_result.columns,
            data: data?
        })
    }

    // TODO: Don't always expect an intermediate result with rows

    fn execute_node(&self, plan_node: &QueryPlanNode) -> Result<IntermediateResult, Box<dyn Error>> {
        match plan_node {
            QueryPlanNode::Project { columns, child } => {
                self.execute_projection(self.execute_node(child)?, columns)
            }
            QueryPlanNode::Scan { table_id, filter } =>
                self.execute_scan(*table_id, filter),
            QueryPlanNode::NestedLoopJoin { left, right, filter } =>
                self.execute_nestedloopjoin(self.execute_node(left)?, self.execute_node(right)?, filter),
            QueryPlanNode::PrimaryKeyLookup { table_id, pk_values } =>
                self.execute_primary_key_lookup(*table_id, pk_values),
            QueryPlanNode::Insert { table_id, values } =>
                self.execute_insert(*table_id, values),
            QueryPlanNode::CreateTable(definition) =>
                self.execute_create_table(definition),
        }
    }

    fn execute_projection(&self, input: IntermediateResult,
                          columns: &Vec<QueryColumn>) -> Result<IntermediateResult, Box<dyn Error>> {
        let number_of_columns = columns.len();
        let mut final_columns: Vec<ResultColumn> = Vec::with_capacity(number_of_columns);
        let mut old_indexes: Vec<usize> = Vec::with_capacity(number_of_columns);

        //println!("Test: {:#?}", result_set);
        for column in columns {
            let old_index = input.columns.iter().enumerate()
                .find(|(_, c)| c.column.id == column.column_id && c.table.id == column.table.table_id).unwrap().0;
            old_indexes.push(old_index);
            final_columns.push(input.columns[old_index].to_owned());
        }
        let final_iter = input.row_iterator
            .map(move |r| {
                match r {
                    Ok(row) => {
                        let mut new_row = Vec::with_capacity(number_of_columns);
                        for &old_index in old_indexes.iter() {
                            new_row.push(row[old_index].to_owned());
                        }
                        Ok(new_row)
                    },
                    Err(_) => r
                }
            });
        Ok(IntermediateResult {
            columns: final_columns,
            row_iterator: Box::new(final_iter)
        })
    }

    fn execute_nestedloopjoin(&self, left: IntermediateResult, right: IntermediateResult,
            filter: &Option<SqlFilterExpression>) -> Result<IntermediateResult, Box<dyn Error>> {
        let final_columns: Vec<ResultColumn> = left.columns.iter().chain(right.columns.iter()).cloned().collect();
        let final_iter = NestedLoopJoinIterator::new(left.row_iterator, right.row_iterator, filter.as_ref().map(|f| ScanFilter::compile(f.clone(), &final_columns)))?;
        Ok(IntermediateResult {
            columns: final_columns,
            row_iterator: Box::new(final_iter)
        })
    }

    fn execute_scan(&self, table_id: u32, where_clause: &Option<SqlFilterExpression>) -> Result<IntermediateResult, Box<dyn Error>> {
        let table_meta = self.catalog.find_table_by_id(table_id).unwrap();
        let columns: Vec<ResultColumn> = table_meta.columns.values().map(|c| ResultColumn::new(&table_meta, c)).collect();
        if let Some(w) = where_clause {
            let scan_filter =  ScanFilter::compile(w.to_owned(), &columns);
            let iter = self.storage_engine.scan_table(table_meta)
                .filter(move |r| {
                    if let Ok(row) = r {
                        scan_filter.check(row)
                    } else {
                        true
                    }
                });
            Ok(IntermediateResult {
                columns,
                row_iterator: Box::new(iter)
            })
        } else {
            let iter = self.storage_engine.scan_table(table_meta);
            Ok(IntermediateResult {
                columns: columns,
                row_iterator: Box::new(iter)
            })
        }
    }

    fn execute_primary_key_lookup(&self, table_id: u32,
                                  pk_values: &Vec<TupleValue>) -> Result<IntermediateResult, Box<dyn Error>> {
        let table = self.catalog.find_table_by_id(table_id).unwrap();
        let columns = table.columns.values().map(|c| ResultColumn::new(&table, c)).collect();
        let result = self.storage_engine
            .whole_row_primary_key_lookup(table, pk_values);
        if let Some(result_row) = result {
            Ok(IntermediateResult {
                columns,
                row_iterator: Box::new(std::iter::once(result_row))
            })
        } else {
            Ok(IntermediateResult {
                columns,
                row_iterator: Box::new(std::iter::empty())
            })
        }
    }


    fn execute_insert(&self, table_id: u32, values: &Vec<TupleValue>)
                      -> Result<IntermediateResult, Box<dyn Error>> {
        let table = self.catalog.find_table_by_id(table_id).unwrap();
        self.storage_engine.write_row(table, values)?;
        self.storage_engine.flush();
        Ok(IntermediateResult {
            columns: vec![],
            row_iterator: Box::new(std::iter::empty())
        })
    }

    fn execute_create_table(&self, table_definition: &TableDefinition)
        -> Result<IntermediateResult, Box<dyn Error>> {
        self.catalog.create_table(table_definition);
        self.storage_engine.flush();
        Ok(IntermediateResult {
            columns: vec![],
            row_iterator: Box::new(std::iter::empty())
        })
    }
}

struct NestedLoopJoinIterator {
    left_iter: RowIterator,
    right_index: usize,
    left_current: Option<Result<Vec<TupleValue>, sled::Error>>,
    right_complete: Vec<Vec<TupleValue>>,
    filter: Option<ScanFilter>
}

impl NestedLoopJoinIterator {

    fn new(mut left: RowIterator, right: RowIterator, filter: Option<ScanFilter>) -> Result<NestedLoopJoinIterator, sled::Error> {
        let right_complete = right.collect::<Result<Vec<Row>, sled::Error>>()?;
        let left_first = left.next();
        Ok(NestedLoopJoinIterator {
            left_iter: left,
            right_index: 0,
            right_complete, //TODO: Fix this (requires resettable executor iterators)
            left_current: left_first,
            filter
        })
    }

}

impl Iterator for NestedLoopJoinIterator {
    type Item = Result<Row, sled::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(left_current) = &self.left_current {
                match left_current {
                    Ok(v) => {
                        if self.right_index >= self.right_complete.len() {
                            self.left_current = self.left_iter.next();
                            self.right_index = 0;
                            continue;
                        } else {
                            let row = v.iter().cloned().chain(self.right_complete[self.right_index].iter().cloned()).collect();
                            self.right_index += 1;
                            if self.filter.as_ref().map_or(true, |f| f.check(&row)) {
                                return Some(Ok(row))
                            } else {
                                continue;
                            }
                        }
                    },
                    Err(e) => {
                        let error = e.clone();
                        self.left_current = self.left_iter.next();
                        return Some(Err(error))
                    }
                }
            } else {
                return None
            }
        }
    }
}


struct ScanFilter {
    where_clause: SqlFilterExpression,
    column_index: BTreeMap<(u32, u32), usize>
}

impl ScanFilter {
    fn compile(where_clause: SqlFilterExpression, columns: &Vec<ResultColumn>) -> ScanFilter {
        ScanFilter {
            where_clause,
            column_index: columns.iter().enumerate().map(|(i, v)| ((v.table.id, v.column.id), i)).collect()
        }
    }

    fn check(&self, tuple: &Vec<TupleValue>) -> bool {
       self.check_where_clause(&self.where_clause, tuple)
    }

    fn check_where_clause(&self, where_clause: &SqlFilterExpression, tuple: &Vec<TupleValue>) -> bool {
        match where_clause {
            SqlFilterExpression::ComparisonExpression(c_wc) =>
                self.check_comparison(c_wc, tuple),
            SqlFilterExpression::LogicalExpression(l_wc) =>
                self.check_logical(l_wc, tuple)
        }
    }

    fn check_comparison(&self, comparison: &SqlComparisonExpression, tuple: &Vec<TupleValue>) -> bool {
        match comparison {
            SqlComparisonExpression::Equals(l, r) =>
                self.check_where_clause_comparison(l, r, tuple,
                                                   |l, r| l == r),
            SqlComparisonExpression::NotEquals(l, r) =>
                self.check_where_clause_comparison(l, r, tuple,
                                                   |l, r| l != r),
            SqlComparisonExpression::LessThan(l, r) =>
                self.check_where_clause_comparison(l, r, tuple,
                                                   |l, r| l < r),
            SqlComparisonExpression::LessThanOrEquals(l, r) =>
                self.check_where_clause_comparison(l, r, tuple,
                                                   |l, r| l <= r),
        }
    }

    fn check_where_clause_comparison(&self,
                                     left: &SqlComparisonItem,
                                     right: &SqlComparisonItem,
                                     tuple: &Vec<TupleValue>,
                                     operation: fn (l: &TupleValue, r: &TupleValue) -> bool) -> bool  {
        let left_dereferenced = self.get_comparison_item(left, tuple);
        let right_dereferenced = self.get_comparison_item(right, tuple);
        operation(left_dereferenced, right_dereferenced)
    }

    fn get_comparison_item<'a>(&self, item: &'a SqlComparisonItem, tuple: &'a Vec<TupleValue>) -> &'a TupleValue {
        match item {
            SqlComparisonItem::Literal(l) => l,
            SqlComparisonItem::Reference(r) => {
                let tuple_index = self.column_index[&(r.table.table_id, r.column_id)];
                &tuple[tuple_index]
            }
        }
    }

    fn check_logical(&self, logical: &SqlLogicalExpression, tuple: &Vec<TupleValue>) -> bool {
        match logical {
            SqlLogicalExpression::And(l, r) =>
                self.check_where_clause(l, tuple) && self.check_where_clause(r, tuple),
            SqlLogicalExpression::Or(l, r) =>
                self.check_where_clause(l, tuple) || self.check_where_clause(r, tuple),
            SqlLogicalExpression::Not(wc) => !self.check_where_clause(wc, tuple)
        }
    }
}

#[derive(Clone, Debug)]
pub struct ResultColumnTable {
    pub id: u32,
    pub name: String
}
#[derive(Clone, Debug)]
pub struct ResultColumn {
    pub table: ResultColumnTable,
    pub column: ColumnMeta
}

impl ResultColumn {

    fn new(table_meta: &TableMeta, column_meta: &ColumnMeta) -> ResultColumn {
        ResultColumn {
            column: column_meta.clone(),
            table: ResultColumnTable {
                name: table_meta.name.clone(),
                id: table_meta.id
            }
        }
    }

}


#[derive(Debug)]
pub struct ResultSet {
    pub columns: Vec<ResultColumn>,
    pub data: Vec<Vec<TupleValue>>
}

type RowIterator = Box<dyn Iterator<Item=Result<Vec<TupleValue>, sled::Error>>>;
type Row = Vec<TupleValue>;

struct IntermediateResult {
    columns: Vec<ResultColumn>,
    row_iterator: RowIterator
}

