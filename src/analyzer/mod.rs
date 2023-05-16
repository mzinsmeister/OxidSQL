use std::{fmt::{Display, Debug}, collections::BTreeMap, error::Error};

use itertools::Itertools;

use crate::{storage::buffer_manager::BufferManager, catalog::Catalog, planner::{Query, BoundTable, BoundAttribute}, parser::{ParseTree, SelectParseTree}};

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
            select.push(BoundAttribute {
                attribute: attribute.clone(),
                binding: parse_attribute.binding.map(|s| s.to_string())
            });
        }
        // TODO: Add support for WHERE
        if select_tree.where_clause.is_some() {
            return Err(AnalyzerError::UnimplementedError("WHERE is not supported yet".to_string()))
        }
        let from = tables.into_iter()
            .map(|t| (t.to_ref(), t))
            .collect();
        return Ok(Query {
            select,
            from,
            join_predicates: Vec::new(),
            selections: Vec::new()
        })
    }
}