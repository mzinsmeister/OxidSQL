use std::{fmt::{Display, Debug}, collections::BTreeMap, error::Error};

use itertools::{Itertools, join};

use crate::{storage::buffer_manager::BufferManager, catalog::Catalog, planner::{Query, BoundTable, BoundAttribute}, parser::{ParseTree, SelectParseTree, ParseWhereClause, ParseWhereClauseItem, BoundParseAttribute}, execution::plan, types::TupleValue};

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
        for parse_attribute in select_tree.columns {
            select.push(Self::find_bound_attribute(&parse_attribute, &tables)?);
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
    fn analyze_where(where_clause: &ParseWhereClause, from_tables: &[BoundTable]) -> Result<(Vec<(BoundAttribute, TupleValue)>, Vec<(BoundAttribute, BoundAttribute)>), AnalyzerError<B>> {
        let mut selections = Vec::new();
        let mut join_predicates = Vec::new();

        Self::analyze_where_rec(where_clause, &mut selections, &mut join_predicates, from_tables)?;

        Ok((selections, join_predicates))
    }

    fn analyze_where_rec(where_clause: &ParseWhereClause, 
                        selections: &mut Vec<(BoundAttribute, TupleValue)>, 
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
                selections.push((Self::find_bound_attribute(l, from_tables)?, r.clone()))
            },
            ParseWhereClause::NotEquals(_, _) => return Err(AnalyzerError::UnimplementedError("Not equals unimplemented".to_string())),
            ParseWhereClause::LessThan(_, _) => return Err(AnalyzerError::UnimplementedError("Less than unimplemented".to_string())),
            ParseWhereClause::GreaterThan(_, _) => return Err(AnalyzerError::UnimplementedError("Greater than unimplemented".to_string())),
            ParseWhereClause::LessOrEqualThan(_, _) => return Err(AnalyzerError::UnimplementedError("Less than or equal unimplemented".to_string())),
            ParseWhereClause::GreaterOrEqualThan(_, _) => return Err(AnalyzerError::UnimplementedError("Greater than or equal unimplemented".to_string())),
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
            if let Ok(found_attributes) = found_attributes {
                found_attributes
            } else {
                return Err(AnalyzerError::AmbiguousAttributeName(parse_attribute.name.to_string()))
            }
        };
        Ok(BoundAttribute { attribute: attribute.clone(), binding: binding })
    }

}

