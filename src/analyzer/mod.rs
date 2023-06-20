use std::{fmt::{Display, Debug}, error::Error};

use itertools::{Itertools};

use crate::{storage::buffer_manager::BufferManager, catalog::{Catalog, TableDesc, AttributeDesc}, planner::{Query, BoundTable, BoundAttribute, Selection, SelectionOperator, SelectQuery, InsertQuery, CreateTableQuery}, parser::{ParseTree, SelectParseTree, ParseWhereClause, ParseWhereClauseItem, BoundParseAttribute, InsertParseTree, CreateTableParseTree, TableDefinitionItem}, types::{TupleValueConversionError}, access::tuple::Tuple};

#[derive(Debug)]
pub enum AnalyzerError<B: BufferManager> {
    BufferManagerError(B::BError),
    UnimplementedError(String),
    RelationNotFoundError(String),
    AttributeNotFoundError(String),
    UnboundBinding(String),
    AmbiguousAttributeName(String),
    UncomparableDataTypes(String),
    MissingValues(Vec<String>),
    TooManyValues{ expected: usize, actual: usize },
    DataTypeNotConvertible(usize, TupleValueConversionError),
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
            ParseTree::Insert(insert_tree) => self.analyze_insert(insert_tree),
            ParseTree::CreateTable(create_table_tree) => self.analyze_create_table(create_table_tree),
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


        return Ok(Query::Select(SelectQuery {
            select,
            from,
            join_predicates,
            selections
        }))
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
                            if !left.attribute.data_type.is_comparable_to(&right.attribute.data_type) {
                                return Err(AnalyzerError::UncomparableDataTypes(format!("{} and {}", left.attribute.data_type, right.attribute.data_type)));
                            }
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
                let attribute = Self::find_bound_attribute(l, from_tables)?;
                if !attribute.attribute.data_type.is_comparable_to_value(r) {
                    return Err(AnalyzerError::UncomparableDataTypes(format!("{} and {}", attribute.attribute.data_type, r)));
                }
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
                        ParseWhereClauseItem::Name(_) => return Err(AnalyzerError::UnimplementedError("Non-equijoins unimplemented".to_string())),
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
            ParseWhereClause::And(l, r) => {
                Self::analyze_where_rec(l, selections, join_predicates, from_tables)?;
                Self::analyze_where_rec(r, selections, join_predicates, from_tables)?;
            }
            ParseWhereClause::Or(_, _) => return Err(AnalyzerError::UnimplementedError("Or unimplemented".to_string())),
        }
        Ok(())
    }

    fn find_bound_attribute(parse_attribute: &BoundParseAttribute, tables: &[BoundTable]) -> Result<BoundAttribute, AnalyzerError<B>> {
        let binding = parse_attribute.binding.map(|s| s.to_string());
        if let Some(binding) = &binding {
            if !tables.iter().any(|t| t.binding == Some(binding.clone())) {
                return Err(AnalyzerError::UnboundBinding(binding.clone()));
            }
        }
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

    fn analyze_insert(&self, insert_tree: InsertParseTree) -> Result<Query, AnalyzerError<B>> {
        let table = match self.catalog.find_table_by_name(&insert_tree.table) {
            Ok(Some(table)) => table,
            Ok(None) => return Err(AnalyzerError::RelationNotFoundError(insert_tree.table.to_string())),
            Err(e) => return Err(AnalyzerError::BufferManagerError(e))
        };
        if insert_tree.values.len() < table.attributes.len() {
            return Err(AnalyzerError::MissingValues(table.attributes.iter().skip(insert_tree.values.len()).map(|a| a.name.to_string()).collect()));
        }
        if insert_tree.values.len() > table.attributes.len() {
            return Err(AnalyzerError::TooManyValues{ expected: table.attributes.len(), actual: insert_tree.values.len() });
        }
        let mut tuple = Tuple::new(Vec::with_capacity(table.attributes.len()));
        for (i, attribute) in table.attributes.iter().enumerate() {
            let typed_value = match insert_tree.values[i].as_ref().map(|v| v.try_convert_to(attribute.data_type)) {
                Some(Ok(v)) => Some(v),
                None => None,
                Some(Err(e)) => return Err(AnalyzerError::DataTypeNotConvertible(i, e))
            };
            tuple.values.push(typed_value);                
        }
        Ok(Query::Insert(InsertQuery {
            table: table,
            values: vec![tuple]
        }))

    }

    fn analyze_create_table(&self, create_table_tree: CreateTableParseTree) -> Result<Query, AnalyzerError<B>> {
        let mut attributes = Vec::with_capacity(create_table_tree.table_definition.len());
        for attribute_definition in create_table_tree.table_definition.iter() {
            if let TableDefinitionItem::ColumnDefinition { definition, .. } = attribute_definition {
                attributes.push((definition.name.to_string(), definition.column_type));
            }
        }
        let attribute_start_id = self.catalog.allocate_attribute_ids(attributes.len() as u32);
        let segment_id = self.catalog.allocate_segment_ids(4);
        let table_id = self.catalog.allocate_db_object_id();
        let table = TableDesc {
            id: table_id,
            name: create_table_tree.name.to_string(),
            segment_id,
            fsi_segment_id: segment_id + 1,
            sample_segment_id: segment_id + 2,
            sample_fsi_segment_id: segment_id + 3,
            attributes: attributes.into_iter().enumerate().map(|(i, (name, data_type))| AttributeDesc {
                id: attribute_start_id + i as u32,
                name,
                data_type,
                nullable: true,
                table_ref: table_id
            }).collect()
        };
        Ok(Query::CreateTable(CreateTableQuery {
            table
        }))
    }

}

#[cfg(test)]
mod test {

    use std::sync::Arc;

    use crate::{storage::{buffer_manager::mock::MockBufferManager, page::PAGE_SIZE}, catalog::{TableDesc, AttributeDesc}, types::{TupleValueType, TupleValue}, parser::{BoundParseTable, CreateTableColumn}, planner::BoundTableRef, config::DbConfig};

    use super::*;

    fn get_catalog(bm: Arc<MockBufferManager>) -> Catalog<Arc<MockBufferManager>> {
        let catalog = Catalog::new(bm, Arc::new(DbConfig { n_threads: 4 })).unwrap();
        catalog.create_table(&TableDesc{
                id: 1, 
                name: "people".to_string(), 
                segment_id: 1024,
                fsi_segment_id: 1025,
                sample_segment_id: 1026,
                sample_fsi_segment_id: 1027,
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
            }).unwrap();
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
        if let Query::Select(query) = query {
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
        } else {
            panic!("Query is not a select query");
        }
    }

    #[test]
    fn test_analyze_insert() {
        let catalog = get_catalog(MockBufferManager::new(PAGE_SIZE));
        let analyzer = Analyzer::new(catalog);
        let parse_tree = ParseTree::Insert(InsertParseTree {
            table: "people",
            values: vec![
                Some(TupleValue::BigInt(1)),
                Some(TupleValue::String("John".to_string())),
                Some(TupleValue::BigInt(42))
            ]
        });
        let query = analyzer.analyze(parse_tree).unwrap();
        if let Query::Insert(query) = query {
            assert_eq!(query.table.id, 1);
            assert_eq!(query.table.name, "people");
            assert_eq!(query.values.len(), 1);
            assert_eq!(query.values[0].values.len(), 3);
            assert_eq!(query.values[0].values[0].as_ref().unwrap().as_int(), 1);
            assert_eq!(query.values[0].values[1].as_ref().unwrap().as_string(), "John");
            assert_eq!(query.values[0].values[2].as_ref().unwrap().as_small_int(), 42);
        } else {
            panic!("Query is not an insert query");
        }
    }

    #[test]
    fn test_analyze_insert_missing_values() {
        let catalog = get_catalog(MockBufferManager::new(PAGE_SIZE));
        let analyzer = Analyzer::new(catalog);
        let parse_tree = ParseTree::Insert(InsertParseTree {
            table: "people",
            values: vec![
                Some(TupleValue::BigInt(1)),
                Some(TupleValue::String("John".to_string())),
            ]
        });
        let query = analyzer.analyze(parse_tree);
        assert!(query.is_err());
        if let Err(AnalyzerError::MissingValues(missing)) = query {
            assert_eq!(missing.len(), 1);
            assert_eq!(missing[0], "age");
        } else {
            panic!("Query is not an insert query");
        }
    }

    #[test]
    fn test_analyze_insert_too_many_values() {
        let catalog = get_catalog(MockBufferManager::new(PAGE_SIZE));
        let analyzer = Analyzer::new(catalog);
        let parse_tree = ParseTree::Insert(InsertParseTree {
            table: "people",
            values: vec![
                Some(TupleValue::BigInt(1)),
                Some(TupleValue::String("John".to_string())),
                Some(TupleValue::BigInt(42)),
                Some(TupleValue::BigInt(42))
            ]
        });
        let query = analyzer.analyze(parse_tree);
        assert!(query.is_err());
        if let Err(AnalyzerError::TooManyValues{ expected, actual }) = query {
            assert_eq!(expected, 3);
            assert_eq!(actual, 4);
        } else {
            panic!("Query is not an insert query");
        }
    }

    #[test]
    fn test_analyze_insert_wrong_type() {
        let catalog = get_catalog(MockBufferManager::new(PAGE_SIZE));
        let analyzer = Analyzer::new(catalog);
        let parse_tree = ParseTree::Insert(InsertParseTree {
            table: "people",
            values: vec![
                Some(TupleValue::BigInt(1)),
                Some(TupleValue::BigInt(2)),
                Some(TupleValue::BigInt(42)),
            ]
        });
        let query = analyzer.analyze(parse_tree);
        if let Err(AnalyzerError::DataTypeNotConvertible(index, conversion_error)) = query {
            assert_eq!(index, 1);
            assert_eq!(conversion_error, TupleValueConversionError::TypeNotConvertible);
        } else {
            panic!("Analyzer didn't return the correct error");
        }
    }

    #[test]
    fn test_create_table() {
        let catalog = get_catalog(MockBufferManager::new(PAGE_SIZE));
        let analyzer = Analyzer::new(catalog);
        let parse_tree = ParseTree::CreateTable(CreateTableParseTree {
            name: "people",
            table_definition: vec![
                TableDefinitionItem::ColumnDefinition { definition: CreateTableColumn {
                    name: "id",
                    column_type: TupleValueType::Int,
                }, is_primary_key: true },
                TableDefinitionItem::ColumnDefinition { definition: CreateTableColumn {
                    name: "name",
                    column_type: TupleValueType::VarChar(255),
                }, is_primary_key: false },
            ]
        });
        let query = analyzer.analyze(parse_tree).unwrap();
        if let Query::CreateTable(query) = query {
            assert_eq!(query.table.id, 1);
            assert_eq!(query.table.name, "people");
            assert_eq!(query.table.attributes.len(), 2);
            assert_eq!(query.table.attributes[0].id, 1);
            assert_eq!(query.table.attributes[0].name, "id");
            assert_eq!(query.table.attributes[0].data_type, TupleValueType::Int);
            assert_eq!(query.table.attributes[0].nullable, true);

            assert_eq!(query.table.attributes[1].id, 2);
            assert_eq!(query.table.attributes[1].name, "name");
            assert_eq!(query.table.attributes[1].data_type, TupleValueType::VarChar(255));
            assert_eq!(query.table.attributes[1].nullable, true);
        } else {
            panic!("Query is not a create table query");
        }
    }
}
