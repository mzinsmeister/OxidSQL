use crate::analyze::{Query, SqlFilterExpression, SelectQuery, QueryColumn, SqlComparisonExpression, SqlComparisonItem, InsertQuery, TableDefinition};
use crate::catalog::Catalog;
use crate::database::TupleValue;

#[derive(Debug)]
pub struct QueryPlan {
    pub root_node: QueryPlanNode
}

#[derive(Debug)]
pub enum QueryPlanNode {
    Project { columns: Vec<QueryColumn>, child: Box<QueryPlanNode> },
    NestedLoopJoin { left: Box<QueryPlanNode>, right: Box<QueryPlanNode>, filter: Option<SqlFilterExpression> },
    Scan { table_id: u32, filter: Option<SqlFilterExpression> },
    PrimaryKeyLookup { table_id: u32, pk_values: Vec<TupleValue> },
    Insert { table_id: u32, values: Vec<TupleValue> },
    CreateTable(TableDefinition)
}

pub struct Planner<'a> {
    catalog: Catalog<'a>
}

impl Planner<'_> {
    pub fn new(catalog: Catalog) -> Planner {
        Planner { catalog }
    }

    pub fn plan_query(&self, query: Query) -> QueryPlan {
        match query {
            Query::Select(select) => self.plan_select(select),
            Query::Insert(insert) => self.plan_insert(insert),
            Query::CreateTable(create_table) => self.plan_create_table(create_table),
        }
    }

    fn plan_select(&self, select_query: SelectQuery) -> QueryPlan {
        let base_node = match &select_query.where_clause {
            None => self.plan_joins(&select_query),
            Some(where_clause) => {
                match where_clause {
                    SqlFilterExpression::ComparisonExpression(clause) => {
                        match clause {
                            SqlComparisonExpression::Equals(a, b) => {
                                self.plan_equals_select(&select_query,&a, &b)
                            }
                            _ => self.plan_other_select(&select_query)
                        }
                    },
                    _ => self.plan_other_select(&select_query)
                }
            }
        };
        QueryPlan {
            root_node: QueryPlanNode::Project {
                columns: select_query.targets,
                child: Box::new(base_node)
            }
        }
    }

    fn plan_equals_select(&self, query: &SelectQuery, item1: &SqlComparisonItem, item2: &SqlComparisonItem) -> QueryPlanNode {
        let column = match item1 {
            SqlComparisonItem::Literal(_) => {
                match item2 {
                    SqlComparisonItem::Literal(_) => return self.plan_other_select(query),
                    SqlComparisonItem::Reference(r) => r
                }
            }
            SqlComparisonItem::Reference(r) => r
        };
        let literal = match item1 {
            SqlComparisonItem::Literal(l) => l,
            SqlComparisonItem::Reference(_) => {
                match item2 {
                    SqlComparisonItem::Literal(l) => l,
                    SqlComparisonItem::Reference(_) => return self.plan_other_select(query)
                }
            }
        };
        let referenced_column_table = self.catalog.find_table_by_id(column.table.table_id).unwrap();
        if referenced_column_table.primary_key_column_ids.len() == 1
            && referenced_column_table.primary_key_column_ids[0] == column.column_id {
            QueryPlanNode::PrimaryKeyLookup{
                        table_id: column.table.table_id,
                        pk_values: vec![literal.to_owned()]
                    }
        } else {
            QueryPlanNode::Scan {
                table_id: column.table.table_id,
                filter: query.where_clause.to_owned()
            }
        }
    }

    fn plan_other_select(&self, query: &SelectQuery) -> QueryPlanNode {
        self.plan_joins(query)
    }

    fn plan_joins(&self, select_query: &SelectQuery) -> QueryPlanNode {
        let mut inner = QueryPlanNode::Scan{
            table_id: select_query.from[0].table_id,
            filter: Option::None
        };
        for (i, table) in select_query.from.iter().enumerate().skip(1) {
            inner = QueryPlanNode::NestedLoopJoin{
                left: Box::new(inner),
                right: Box::new(QueryPlanNode::Scan{
                    table_id: table.table_id,
                    filter: Option::None
                }),
                filter: if i == select_query.from.len() - 1 { 
                    select_query.where_clause.clone()
                 } else { Option::None }
            }
        }
        inner
    }

    fn plan_insert(&self, query: InsertQuery) -> QueryPlan {
        QueryPlan {
            root_node: QueryPlanNode::Insert {
                table_id: query.table.table_id,
                values: query.values
            }
        }
    }

    fn plan_create_table(&self, query: TableDefinition) -> QueryPlan {
        QueryPlan {
            root_node: QueryPlanNode::CreateTable(query)
        }
    }
}
