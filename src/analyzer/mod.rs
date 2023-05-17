use std::{fmt::{Display, Debug}, collections::BTreeMap, error::Error};

use itertools::{Itertools, join};

use crate::{storage::buffer_manager::BufferManager, catalog::Catalog, planner::{Query, BoundTable, BoundAttribute, Selection, SelectionOperator}, parser::{ParseTree, SelectParseTree, ParseWhereClause, ParseWhereClauseItem, BoundParseAttribute}, execution::plan, types::TupleValue};

#[derive(Debug)]
pub enum AnalyzerError<B: BufferManager> {
    BufferManagerError(B::BError),
    UnimplementedError(String),
    RelationNotFoundError(String),
    AttributeNotFoundError(String),
    UnboundBinding(String),
    AmbiguousAttributeName(String)
}

impl<B: BufferManager> Display for AnalyzerError<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "")
    }
}

impl<B: BufferManager> Error for AnalyzerError<B> {}

pub struct Analyzer<B: BufferManager> {
    catalog: Catalog<B>
}

impl<B: BufferManager> Analyzer<B> {
    pub fn new(catalog: Catalog<B>) -> Self {
        Analyzer {
            catalog
        }
    }

    pub fn analyze(&self, parse_tree: ParseTree) -> Result<Query, AnalyzerError<B>> {
        match parse_tree {
            ParseTree::Select(select_tree) => self.analyze_select(select_tree),
            _ => Err(AnalyzerError::UnimplementedError("Only SELECT is supported at the moment".to_string()))
        }
    }

    fn analyze_select(&self, select_tree: SelectParseTree) -> Result<Query, AnalyzerError<B>> {
        let mut tables: Vec<BoundTable> = Vec::new();
        for parse_table in select_tree.from_tables {
            let found_table = self.catalog.find_table_by_name(&parse_table.name);
            match found_table {
                Ok(table) => match table {
                    Some(table) => {
                        let binding = parse_table.binding.map(|s| s.to_string());
                        tables.push(BoundTable::new(table, binding));
                    },
                    None => return Err(AnalyzerError::RelationNotFoundError(format!("{}", parse_table)))
                }
                Err(ce) => return Err(AnalyzerError::BufferManagerError(ce))
            }
        }
        let mut select = Vec::new();
        if let Some(parse_attributes) = select_tree.columns {
            for parse_attribute in parse_attributes {
                select.push(Self::find_bound_attribute(&parse_attribute, &tables)?);
            }
        } else {
            for table in &tables {
                for attribute in &table.table.attributes {
                    select.push(BoundAttribute { attribute: attribute.clone(), binding: table.binding.clone() });
                }
            }
        }
        let (selections, join_predicates) = if let Some(where_clause) = select_tree.where_clause {
            Self::analyze_where(&where_clause, &tables)?
        } else {
            (vec![], vec![])
        };
        
        let from = tables.into_iter()
            .map(|t| (t.to_ref(), t))
            .collect();

        //let (selections, join_predicates) = select_tree.where_clause.map_or((vec![], vec![]),|w| Self::analyze_where(&w)?);


        return Ok(Query {
            select,
            from,
            join_predicates,
            selections
        })
    }

    // The signature of that function is just temporary since as long as we only allow equals predicates
    // It's enough to store what should be equal to what
    fn analyze_where(where_clause: &ParseWhereClause, from_tables: &[BoundTable]) -> Result<(Vec<Selection>, Vec<(BoundAttribute, BoundAttribute)>), AnalyzerError<B>> {
        let mut selections = Vec::new();
        let mut join_predicates = Vec::new();

        Self::analyze_where_rec(where_clause, &mut selections, &mut join_predicates, from_tables)?;

        Ok((selections, join_predicates))
    }

    fn analyze_where_rec(where_clause: &ParseWhereClause, 
                        selections: &mut Vec<Selection>, 
                        join_predicates: &mut Vec<(BoundAttribute, BoundAttribute)>, 
                        from_tables: &[BoundTable]) -> Result<(), AnalyzerError<B>> {
        match where_clause {
            ParseWhereClause::Equals(l, r) => {
                let (l, r) = match l {
                    ParseWhereClauseItem::Name(n1) => match r {
                        ParseWhereClauseItem::Name(n2) => {
                            let left = Self::find_bound_attribute(n1, from_tables)?;
                            let right = Self::find_bound_attribute(n2, from_tables)?;
                            join_predicates.push((left, right));
                            return Ok(())
                        },
                        ParseWhereClauseItem::Value(v) => {
                            (n1, v)
                        },
                    },
                    ParseWhereClauseItem::Value(v) => match r {
                        ParseWhereClauseItem::Name(n) => {
                            (n, v)
                        },
                        ParseWhereClauseItem::Value(_) => return Err(AnalyzerError::UnimplementedError("literal = literal comparisons currentlly unimplemented".to_string())),
                    },
                };
                let selection = Selection {
                    attribute: Self::find_bound_attribute(l, from_tables)?,
                    value: r.clone(),
                    operator: SelectionOperator::Eq
                };
                selections.push(selection)
            },
            ParseWhereClause::NotEquals(_, _) => return Err(AnalyzerError::UnimplementedError("Not equals unimplemented".to_string())),
            ParseWhereClause::LessThan(l, r) 
            | ParseWhereClause::GreaterThan(r, l) 
            | ParseWhereClause::LessOrEqualThan(l, r)
            | ParseWhereClause::GreaterOrEqualThan(r, l)
             => {
                let (l, r) = match l {
                    ParseWhereClauseItem::Name(n1) => match r {
                        ParseWhereClauseItem::Name(n2) => return Err(AnalyzerError::UnimplementedError("Non-equijoins unimplemented".to_string())),
                        ParseWhereClauseItem::Value(v) => {
                            (n1, v)
                        },
                    },
                    ParseWhereClauseItem::Value(v) => match r {
                        ParseWhereClauseItem::Name(n) => {
                            (n, v)
                        },
                        ParseWhereClauseItem::Value(_) => return Err(AnalyzerError::UnimplementedError("literal = literal comparisons currentlly unimplemented".to_string())),
                    },
                };
                let selection = Selection {
                    attribute: Self::find_bound_attribute(l, from_tables)?,
                    value: r.clone(),
                    operator: match where_clause {
                        ParseWhereClause::LessThan(_, _) => SelectionOperator::LessThan,
                        ParseWhereClause::GreaterThan(_, _) => SelectionOperator::GreaterThan,
                        ParseWhereClause::LessOrEqualThan(_, _) => SelectionOperator::LessThanOrEq,
                        ParseWhereClause::GreaterOrEqualThan(_, _) => SelectionOperator::GreaterThanOrEq,
                        _ => unreachable!()
                    }
                };
                selections.push(selection)
            },
            ParseWhereClause::And(l, r) => todo!(),
            ParseWhereClause::Or(_, _) => return Err(AnalyzerError::UnimplementedError("Or unimplemented".to_string())),
        }
        Ok(())
    }

    fn find_bound_attribute(parse_attribute: &BoundParseAttribute, tables: &[BoundTable]) -> Result<BoundAttribute, AnalyzerError<B>> {
        let binding = parse_attribute.binding.map(|s| s.to_string());
        let attribute = {
            let found_attributes = tables.iter()
                .filter(|t| t.binding == binding)
                .filter_map(|t| t.table.get_attribute_by_name(parse_attribute.name))
                .exactly_one();
            match found_attributes {
                Ok(attribute) => attribute,
                Err(e) => match e.count() {
                    0 => return Err(AnalyzerError::AttributeNotFoundError(parse_attribute.name.to_string())),
                    _ => return Err(AnalyzerError::AmbiguousAttributeName(parse_attribute.name.to_string()))
                }
            }
        };
        Ok(BoundAttribute { attribute: attribute.clone(), binding: binding })
    }

}

#[cfg(test)]
mod test {

    use std::sync::Arc;

    use crate::{storage::{buffer_manager::mock::MockBufferManager, page::PAGE_SIZE}, catalog::{TableDesc, AttributeDesc}, types::TupleValueType, parser::BoundParseTable, planner::BoundTableRef};

    use super::*;

    fn get_catalog(bm: Arc<MockBufferManager>) -> Catalog<Arc<MockBufferManager>> {
        let catalog = Catalog::new(bm);
        catalog.create_table(TableDesc{
                id: 1, 
                name: "people".to_string(), 
                segment_id: 1024, 
                attributes: vec![
                    AttributeDesc{
                        id: 1,
                        name: "id".to_string(), 
                        data_type: TupleValueType::Int, 
                        nullable: false, 
                        table_ref: 1
                    },
                    AttributeDesc{
                        id: 2,
                        name: "name".to_string(), 
                        data_type: TupleValueType::VarChar(255), 
                        nullable: false, 
                        table_ref: 1
                    },
                    AttributeDesc{
                        id: 3,
                        name: "age".to_string(), 
                        data_type: TupleValueType::SmallInt, 
                        nullable: true, 
                        table_ref: 1
                    }
                ],
                cardinality: 0, // Irrelevant for analyzer tests
            });
            catalog
    }

    #[test]
    fn test_analyze_select_no_where() {
        let catalog = get_catalog(MockBufferManager::new(PAGE_SIZE));
        let analyzer = Analyzer::new(catalog);
        let parse_tree = ParseTree::Select(SelectParseTree {
            columns: Some(vec![
                BoundParseAttribute {
                    name: "id",
                    binding: None
                },
            ]),
            from_tables: vec![
                BoundParseTable {
                    name: "people",
                    binding: None
                }
            ],
            where_clause: None
        });
        let query = analyzer.analyze(parse_tree).unwrap();
        assert_eq!(query.select.len(), 1);
        assert_eq!(query.select[0].attribute.name, "id");
        assert_eq!(query.select[0].attribute.id, 1);
        assert_eq!(query.select[0].attribute.table_ref, 1);
        assert_eq!(query.select[0].binding, None);
        assert_eq!(query.from.len(), 1);
        assert_eq!(query.from[&BoundTableRef { table_ref: 1, binding: None }].table.id, 1);
        assert_eq!(query.from[&BoundTableRef { table_ref: 1, binding: None }].table.name, "people");
        assert_eq!(query.from[&BoundTableRef { table_ref: 1, binding: None }].binding, None);
        assert_eq!(query.join_predicates.len(), 0);
        assert_eq!(query.selections.len(), 0);
    }
}
