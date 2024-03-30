use std::{fmt::{Display, Debug}, error::Error};

use itertools::Itertools;
use sqlparser::ast::{Statement as AstStatement, SetExpr, Select, TableFactor, SelectItem, Expr, BinaryOperator};

use crate::{access::tuple::Tuple, catalog::{AttributeDesc, Catalog, IndexDesc, IndexType, TableDesc}, planner::{BoundAttribute, BoundTable, CreateIndexStatement, CreateTableStatement, InsertStatement, SelectQuery, Selection, SelectionOperator, Statement}, storage::buffer_manager::BufferManager, types::{TupleValue, TupleValueConversionError, TupleValueType}};

type ParseQuery = sqlparser::ast::Query;

#[derive(Debug)]
pub enum AnalyzerError<B: BufferManager> {
    BufferManagerError(B::BError),
    UnimplementedError(String),
    RelationNotFoundError(String),
    NameAlreadyExistsError(String),
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

struct InsertParseResult {
    table_name: String,
    values: Vec<Option<TupleValue>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableParseTree {
    pub name: String,
    pub table_definition: Vec<TableDefinitionItem>
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableColumn {
    name: String,
    column_type: TupleValueType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableDefinitionItem {
    PrimaryKeyDefinition(Vec<String>),
    ColumnDefinition{ definition: CreateTableColumn, is_primary_key: bool }
}

pub struct CreateIndexParseTree {
    name: String,
    table_name: String,
    columns: Vec<String>,
}


impl<B: BufferManager> Analyzer<B> {
    pub fn new(catalog: Catalog<B>) -> Self {
        Analyzer {
            catalog
        }
    }

    pub fn analyze(&self, parse_tree: AstStatement) -> Result<Statement, AnalyzerError<B>> {
        // TODO: This function isn't really nice with the sqlparser crate
        //       It's huge and will only get bigger. Maybe we should have a
        //       preprocessing step that converts the sqlparser AST into a
        //       custom AST. This would however lose some of the benefits
        //       of using the sqlparser crate in the first place.
        match parse_tree {
            AstStatement::Query(query) => {
                if query.with.is_some() {
                    return Err(AnalyzerError::UnimplementedError("With clause unimplemented".to_string()));
                }
                if query.order_by.len() > 0 {
                    return Err(AnalyzerError::UnimplementedError("Order by clause unimplemented".to_string()));
                }
                if query.limit.is_some() {
                    return Err(AnalyzerError::UnimplementedError("Limit clause unimplemented".to_string()));
                }
                if query.offset.is_some() {
                    return Err(AnalyzerError::UnimplementedError("Offset clause unimplemented".to_string()));
                }
                if query.fetch.is_some() {
                    return Err(AnalyzerError::UnimplementedError("Fetch clause unimplemented".to_string()));
                }
                if query.locks.len() > 0 {
                    return Err(AnalyzerError::UnimplementedError("Locks unimplemented".to_string()));
                }
                if let SetExpr::Select(select) = query.body.as_ref() {
                    self.analyze_query(select.as_ref())
                } else {
                    Err(AnalyzerError::UnimplementedError("Only simple select queries are supported".to_string()))
                }
            },
            AstStatement::Insert { 
                table_name, 
                columns, 
                source, 
                on, 
                returning, .. } => {
                    if on.is_some() {
                        return Err(AnalyzerError::UnimplementedError("Insert with on clause unimplemented".to_string()));
                    }
                    if returning.is_some() {
                        return Err(AnalyzerError::UnimplementedError("Insert with returning clause unimplemented".to_string()));
                    }
                    if table_name.0.len() != 1 {
                        return Err(AnalyzerError::UnimplementedError("Insert with qualified table name unimplemented".to_string()));
                    }
                    if columns.len() > 0 {
                        return Err(AnalyzerError::UnimplementedError("Insert with column list unimplemented".to_string()));
                    }
                    // TODO: Handle sources other than "VALUES"
                    let table_name = table_name.0[0].value.clone();
                    let values = Self::query_extract_values(&source)?;
                    let insert = InsertParseResult { table_name, values };
                    self.analyze_insert(insert)
            },
            AstStatement::CreateTable { 
                name, 
                columns, 
                constraints, 
                or_replace, 
                temporary, 
                external, 
                global, 
                if_not_exists, 
                transient, 
                table_properties, 
                with_options, 
                file_format, 
                location, 
                query, 
                without_rowid, 
                like, 
                clone, 
                engine, 
                default_charset, 
                collation, 
                on_commit, 
                on_cluster,
                order_by, 
                strict, .. } => {
                    if or_replace {
                        return Err(AnalyzerError::UnimplementedError("Create table with or replace unimplemented".to_string()));
                    }
                    if temporary {
                        return Err(AnalyzerError::UnimplementedError("Create table with temporary unimplemented".to_string()));
                    }
                    if external {
                        return Err(AnalyzerError::UnimplementedError("Create table with external unimplemented".to_string()));
                    }
                    if global.is_some() {
                        return Err(AnalyzerError::UnimplementedError("Create table with global unimplemented".to_string()));
                    }
                    if if_not_exists {
                        return Err(AnalyzerError::UnimplementedError("Create table with if not exists unimplemented".to_string()));
                    }
                    if transient {
                        return Err(AnalyzerError::UnimplementedError("Create table with transient unimplemented".to_string()));
                    }
                    if table_properties.len() > 0 {
                        return Err(AnalyzerError::UnimplementedError("Create table with table properties unimplemented".to_string()));
                    }
                    if with_options.len() > 0 {
                        return Err(AnalyzerError::UnimplementedError("Create table with with options unimplemented".to_string()));
                    }
                    if file_format.is_some() {
                        return Err(AnalyzerError::UnimplementedError("Create table with file format unimplemented".to_string()));
                    }
                    if location.is_some() {
                        return Err(AnalyzerError::UnimplementedError("Create table with location unimplemented".to_string()));
                    }
                    if query.is_some() {
                        return Err(AnalyzerError::UnimplementedError("Create table with query unimplemented".to_string()));
                    }
                    if without_rowid {
                        return Err(AnalyzerError::UnimplementedError("Create table with without rowid unimplemented".to_string()));
                    }
                    if like.is_some() {
                        return Err(AnalyzerError::UnimplementedError("Create table with like unimplemented".to_string()));
                    }
                    if clone.is_some() {
                        return Err(AnalyzerError::UnimplementedError("Create table with clone unimplemented".to_string()));
                    }
                    if engine.is_some() {
                        return Err(AnalyzerError::UnimplementedError("Create table with engine unimplemented".to_string()));
                    }
                    if default_charset.is_some() {
                        return Err(AnalyzerError::UnimplementedError("Create table with default charset unimplemented".to_string()));
                    }
                    if collation.is_some() {
                        return Err(AnalyzerError::UnimplementedError("Create table with collation unimplemented".to_string()));
                    }
                    if on_commit.is_some() {
                        return Err(AnalyzerError::UnimplementedError("Create table with on commit unimplemented".to_string()));
                    }
                    if on_cluster.is_some() {
                        return Err(AnalyzerError::UnimplementedError("Create table with on cluster unimplemented".to_string()));
                    }
                    if order_by.is_some() {
                        return Err(AnalyzerError::UnimplementedError("Create table with order by unimplemented".to_string()));
                    }
                    if strict {
                        return Err(AnalyzerError::UnimplementedError("Create table with strict unimplemented".to_string()));
                    }
                    let name = if name.0.len() == 1 {
                        name.0[0].value.clone()
                    } else {
                        return Err(AnalyzerError::UnimplementedError("Create table with qualified name unimplemented".to_string()));
                    };

                    let mut table_definition = Vec::new();

                    for column in columns {
                        let name = column.name.value;
                        let column_type = match column.data_type {
                            sqlparser::ast::DataType::SmallInt(_) => TupleValueType::SmallInt,
                            sqlparser::ast::DataType::Int(_) => TupleValueType::Int,
                            sqlparser::ast::DataType::BigInt(_) => TupleValueType::BigInt,
                            // TODO: Handle length octets and characters properly
                            sqlparser::ast::DataType::Varchar(l) => TupleValueType::VarChar(l.map(|l| l.length as u16).unwrap_or(u16::MAX)),
                            sqlparser::ast::DataType::Text => TupleValueType::VarChar(u16::MAX),
                            // TODO: Handle more aliases (like Int64 for BigInt)
                            _ => return Err(AnalyzerError::UnimplementedError("Only smallint, int, bigint, varchar and text are supported as column types".to_string()))
                        };
                        let primary_key = column.options.iter().any(|c| {
                            if let sqlparser::ast::ColumnOption::Unique{ is_primary } = c.option {
                                is_primary
                            } else {
                                false
                            }
                        });
                        table_definition.push(TableDefinitionItem::ColumnDefinition { definition: CreateTableColumn { name, column_type }, is_primary_key: primary_key });
                    }
                    for constraint in constraints {
                        match constraint {
                            sqlparser::ast::TableConstraint::Unique { columns, is_primary, .. } => {
                                if is_primary {
                                    table_definition.push(TableDefinitionItem::PrimaryKeyDefinition(columns.iter().map(|c| c.value.clone()).collect()));
                                }
                            },
                            _ => return Err(AnalyzerError::UnimplementedError("Only primary key constraints are supported".to_string()))
                        }
                    }
                    let create_table_tree = CreateTableParseTree {
                        name,
                        table_definition,
                    };

                    self.analyze_create_table(create_table_tree)
                },
            AstStatement::CreateIndex { 
                name, 
                table_name,
                columns,
                unique,
                if_not_exists,
                using,
            } => {
                if unique {
                    return Err(AnalyzerError::UnimplementedError("Create index with unique unimplemented".to_string()));
                }
                if if_not_exists {
                    return Err(AnalyzerError::UnimplementedError("Create index with if not exists unimplemented".to_string()));
                }
                if using.is_some() {
                    return Err(AnalyzerError::UnimplementedError("Create index with using unimplemented".to_string()));
                }
                let name = if name.0.len() == 1 {
                    name.0[0].value.clone()
                } else {
                    return Err(AnalyzerError::UnimplementedError("Create index with qualified name unimplemented".to_string()));
                };
                let table_name = if table_name.0.len() == 1 {
                    table_name.0[0].value.clone()
                } else {
                    return Err(AnalyzerError::UnimplementedError("Create index with qualified name unimplemented".to_string()));
                };
                let columns = columns.iter().map(|c| match &c.expr {
                    sqlparser::ast::Expr::Identifier(i) => Ok(i.value.clone()),
                    _ => Err(AnalyzerError::UnimplementedError("Only simple identifiers are supported in index columns".to_string()))
                }).collect::<Result<Vec<String>, _>>()?;
                let create_index_tree = CreateIndexParseTree {
                    name,
                    table_name,
                    columns
                };
                self.analyze_create_index(create_index_tree)
            },
            _ => Err(AnalyzerError::UnimplementedError("Only SELECT, INSERT and CREATE TABLE statements are supported".to_string()))
        }
    }


    fn convert_to_tuple_value(v: &sqlparser::ast::Value) -> Result<Option<TupleValue>, AnalyzerError<B>> {
        match v {
            sqlparser::ast::Value::Number(n, _) => {
                if n.is_integer() {
                    let (int, exponent) = n.as_bigint_and_exponent();
                    if exponent < 0 {
                        return Err(AnalyzerError::UnimplementedError("Only integer numbers up to 64 bits are supported".to_string()));
                    }
                    assert_eq!(exponent, 0);
                    Ok(Some(TupleValue::BigInt(int.try_into().unwrap())))
                } else {
                    Err(AnalyzerError::UnimplementedError("Only integer numbers are supported".to_string()))
                }
            }
            sqlparser::ast::Value::SingleQuotedString(s) => Ok(Some(TupleValue::String(s.clone()))),
            sqlparser::ast::Value::Null => Ok(None),
            _ => Err(AnalyzerError::UnimplementedError("Only integer numbers and strings (+ NULL) are supported".to_string()))
        }
    }

    fn query_extract_values(query: &ParseQuery) -> Result<Vec<Option<TupleValue>>, AnalyzerError<B>> {
        match query.body.as_ref() {
            SetExpr::Values(values) => {
                let rows = &values.rows;
                if rows.len() != 1 {
                    return Err(AnalyzerError::UnimplementedError("Only single row inserts are supported".to_string()));
                }
                let row = &rows[0];
                let mut tuple = Vec::with_capacity(row.len());
                for value in row {
                    match value {
                        sqlparser::ast::Expr::Value(v) => {
                            tuple.push(Self::convert_to_tuple_value(v)?);
                        },
                        _ => return Err(AnalyzerError::UnimplementedError("Only values are supported as source for insert queries".to_string()))
                    }
                }
                Ok(tuple)
            },
            _ => Err(AnalyzerError::UnimplementedError("Only values are supported as source for insert queries".to_string()))
        }
    }

    fn analyze_query(&self, select_tree: &Select) -> Result<Statement, AnalyzerError<B>> {
        // Check whether the query contains any of the things we don't support (yet)
        if select_tree.distinct.is_some() {
            return Err(AnalyzerError::UnimplementedError("Distinct unimplemented".to_string()));
        }
        if select_tree.group_by.len() > 0 {
            return Err(AnalyzerError::UnimplementedError("Group by unimplemented".to_string()));
        }
        if select_tree.having.is_some() {
            return Err(AnalyzerError::UnimplementedError("Having unimplemented".to_string()));
        }
        if select_tree.into.is_some() {
            return Err(AnalyzerError::UnimplementedError("Into unimplemented".to_string()));
        }
        if select_tree.named_window.len() > 0 {
            return Err(AnalyzerError::UnimplementedError("Named window unimplemented".to_string()));
        }

        let mut tables: Vec<BoundTable> = Vec::new();
        for parse_table in &select_tree.from {
            let (binding, name) = if let TableFactor::Table{ name, alias, .. }  = &parse_table.relation {
                if name.0.len() != 1 {
                    return Err(AnalyzerError::UnimplementedError("Only simple table names are supported in FROM".to_string()));
                }
                let name = name.0[0].value.clone();
                let binding = alias.clone().map(|a| a.name.to_string());
                (binding, name)
            } else {
                return Err(AnalyzerError::UnimplementedError("Only simple table names are supported in FROM".to_string()));
            };
            let found_table = self.catalog.find_table_by_name(&name);
            match found_table {
                Ok(table) => match table {
                    Some(table) => {
                        tables.push(BoundTable::new(table, binding));
                    },
                    None => return Err(AnalyzerError::RelationNotFoundError(format!("{}", parse_table)))
                }
                Err(ce) => return Err(AnalyzerError::BufferManagerError(ce))
            }
        }
        let mut select = Vec::new();
        let wildcard = if select_tree.projection.len() == 1 {
                if let SelectItem::Wildcard(_) = select_tree.projection[0] { true } else { false }
            } else { false };
        if wildcard {
            for table in &tables {
                for attribute in &table.table.attributes {
                    select.push(BoundAttribute { attribute: attribute.clone(), binding: table.binding.clone() });
                }
            }
        } else {
            for attribute in &select_tree.projection {
                match attribute {
                    SelectItem::UnnamedExpr(e) => {
                        match e {
                            Expr::Identifier(i) => {
                                let attribute = Self::find_bound_attribute(&i.value, None, &tables)?;
                                select.push(attribute);
                            },
                            Expr::CompoundIdentifier(i) => {
                                if i.len() != 2 {
                                    return Err(AnalyzerError::UnimplementedError("Only simple identifiers or compound identifiers with two parts are supported in SELECT".to_string()));
                                }
                                let attribute = Self::find_bound_attribute(&i[1].value, Some(&i[0].value), &tables)?;
                                select.push(attribute);
                            },
                            _ => return Err(AnalyzerError::UnimplementedError("Only simple identifiers are supported in SELECT".to_string()))
                        }
                    },
                    _ => return Err(AnalyzerError::UnimplementedError("Only simple identifiers are supported in SELECT".to_string()))
                }
            }
        }
        let (selections, join_predicates) = if let Some(where_clause) = &select_tree.selection {
            Self::analyze_where(where_clause, &tables)?
        } else {
            (vec![], vec![])
        };
        
        let from = tables.into_iter()
            .map(|t| (t.to_ref(), t))
            .collect();

        //let (selections, join_predicates) = select_tree.where_clause.map_or((vec![], vec![]),|w| Self::analyze_where(&w)?);


        return Ok(Statement::Select(SelectQuery {
            select,
            from,
            join_predicates,
            selections
        }))
    }

    // The signature of that function is just temporary since as long as we only allow equals predicates
    // It's enough to store what should be equal to what
    fn analyze_where(where_clause: &Expr, from_tables: &[BoundTable]) -> Result<(Vec<Selection>, Vec<(BoundAttribute, BoundAttribute)>), AnalyzerError<B>> {
        let mut selections = Vec::new();
        let mut join_predicates = Vec::new();

        Self::analyze_where_rec(where_clause, &mut selections, &mut join_predicates, from_tables)?;

        Ok((selections, join_predicates))
    }

    fn convert_binary_operator(op: &BinaryOperator) -> Result<SelectionOperator, AnalyzerError<B>> {
        match op {
            BinaryOperator::Eq => Ok(SelectionOperator::Eq),
            BinaryOperator::Lt => Ok(SelectionOperator::LessThan),
            BinaryOperator::Gt => Ok(SelectionOperator::GreaterThan),
            BinaryOperator::LtEq => Ok(SelectionOperator::LessThanOrEq),
            BinaryOperator::GtEq => Ok(SelectionOperator::GreaterThanOrEq),
            _ => Err(AnalyzerError::UnimplementedError("Only basic comparisons (=,<,>,>=,<=) and 'AND' are supported in WHERE".to_string()))
        }
    }

    fn analyze_where_rec(where_clause: &Expr, 
                        selections: &mut Vec<Selection>, 
                        join_predicates: &mut Vec<(BoundAttribute, BoundAttribute)>, 
                        from_tables: &[BoundTable]) -> Result<(), AnalyzerError<B>> {
        match where_clause {
            Expr::BinaryOp { left, op, right } => {
                match op {
                    // TODO: This is quite ugly
                    BinaryOperator::Eq => {
                        let (attribute, value) = match left.as_ref() {
                            Expr::Identifier(i) => match right.as_ref() {
                                Expr::Identifier(i2) => {
                                    let left = Self::find_bound_attribute(&i.value, None, from_tables)?;
                                    let right = Self::find_bound_attribute(&i2.value, None, from_tables)?;
                                    if !left.attribute.data_type.is_comparable_to(&right.attribute.data_type) {
                                        return Err(AnalyzerError::UncomparableDataTypes(format!("{} and {}", left.attribute.data_type, right.attribute.data_type)));
                                    }
                                    join_predicates.push((left, right));
                                    return Ok(())
                                },
                                Expr::CompoundIdentifier(i2) => {
                                    if i2.len() != 2 {
                                        return Err(AnalyzerError::UnimplementedError("Only simple identifiers or compound identifiers with two parts are supported in WHERE".to_string()));
                                    }
                                    let left = Self::find_bound_attribute(&i.value, None, from_tables)?;
                                    let right = Self::find_bound_attribute(&i2[1].value, Some(&i2[0].value), from_tables)?;
                                    if !left.attribute.data_type.is_comparable_to(&right.attribute.data_type) {
                                        return Err(AnalyzerError::UncomparableDataTypes(format!("{} and {}", left.attribute.data_type, right.attribute.data_type)));
                                    }
                                    join_predicates.push((left, right));
                                    return Ok(())
                                },
                                Expr::Value(v) => {
                                    (Self::find_bound_attribute(&i.value, None, from_tables)?, Self::convert_to_tuple_value(v)?)
                                },
                                _ => return Err(AnalyzerError::UnimplementedError("Non-equijoins unimplemented".to_string())),
                            },
                            Expr::CompoundIdentifier(i) => {
                                if i.len() != 2 {
                                    return Err(AnalyzerError::UnimplementedError("Only simple identifiers or compound identifiers with two parts are supported in WHERE".to_string()));
                                }
                                match right.as_ref() {
                                    Expr::Identifier(i2) => {
                                        let left = Self::find_bound_attribute(&i[1].value, Some(&i[0].value), from_tables)?;
                                        let right = Self::find_bound_attribute(&i2.value, None, from_tables)?;
                                        if !left.attribute.data_type.is_comparable_to(&right.attribute.data_type) {
                                            return Err(AnalyzerError::UncomparableDataTypes(format!("{} and {}", left.attribute.data_type, right.attribute.data_type)));
                                        }
                                        join_predicates.push((left, right));
                                        return Ok(())
                                    },
                                    Expr::CompoundIdentifier(i2) => {
                                        if i2.len() != 2 {
                                            return Err(AnalyzerError::UnimplementedError("Only simple identifiers or compound identifiers with two parts are supported in WHERE".to_string()));
                                        }
                                        let left = Self::find_bound_attribute(&i[1].value, Some(&i[0].value), from_tables)?;
                                        let right = Self::find_bound_attribute(&i2[1].value, Some(&i2[0].value), from_tables)?;
                                        if !left.attribute.data_type.is_comparable_to(&right.attribute.data_type) {
                                            return Err(AnalyzerError::UncomparableDataTypes(format!("{} and {}", left.attribute.data_type, right.attribute.data_type)));
                                        }
                                        join_predicates.push((left, right));
                                        return Ok(())
                                    },
                                    Expr::Value(v) => {
                                        (Self::find_bound_attribute(&i[1].value, Some(&i[0].value), from_tables)?, Self::convert_to_tuple_value(v)?)
                                    },
                                    _ => return Err(AnalyzerError::UnimplementedError("Non-equijoins unimplemented".to_string())),
                                }
                            }                            
                            Expr::Value(v) => match right.as_ref() {
                                Expr::Identifier(i) => {
                                    (Self::find_bound_attribute(&i.value, None, from_tables)?, Self::convert_to_tuple_value(v)?)
                                },
                                Expr::CompoundIdentifier(i) => {
                                    if i.len() != 2 {
                                        return Err(AnalyzerError::UnimplementedError("Only simple identifiers or compound identifiers with two parts are supported in WHERE".to_string()));
                                    }
                                    (Self::find_bound_attribute(&i[1].value, Some(&i[0].value), from_tables)?, Self::convert_to_tuple_value(v)?)
                                },
                                Expr::Value(_) => return Err(AnalyzerError::UnimplementedError("literal = literal comparisons currentlly unimplemented".to_string())),
                                _ => return Err(AnalyzerError::UnimplementedError("Non-equijoins unimplemented".to_string())),
                            },
                            _ => return Err(AnalyzerError::UnimplementedError("Only identifier-value or identifier-identifier operations allowed in WHERE".to_string())),
                        };
                        if value.as_ref().map(|r| !attribute.attribute.data_type.is_comparable_to_value(&r)).unwrap_or(false) {
                            return Err(AnalyzerError::UncomparableDataTypes(format!("{} and {}", attribute.attribute.data_type, value.unwrap())));
                        }
                        let selection = Selection {
                            attribute,
                            value,
                            operator: SelectionOperator::Eq
                        };
                        selections.push(selection)
                    },
                    BinaryOperator::Lt | BinaryOperator::Gt | BinaryOperator::LtEq | BinaryOperator::GtEq => {
                        let (attribute, value, operator) = match left.as_ref() {
                            Expr::Identifier(i) => match right.as_ref() {
                                Expr::Identifier(_) => {
                                    return Err(AnalyzerError::UnimplementedError("Non-equijoins unimplemented".to_string()));
                                },
                                Expr::CompoundIdentifier(_) => {
                                    return Err(AnalyzerError::UnimplementedError("Non-equijoins unimplemented".to_string()));
                                },
                                Expr::Value(v) => {
                                    (Self::find_bound_attribute(&i.value, None, from_tables)?, Self::convert_to_tuple_value(v)?, Self::convert_binary_operator(op)?)
                                },
                                _ => return Err(AnalyzerError::UnimplementedError("Only identifier-value or identifier-identifier operations allowed in WHERE".to_string())),
                            },
                            Expr::CompoundIdentifier(i) => match right.as_ref() {
                                Expr::Identifier(_) => {
                                    return Err(AnalyzerError::UnimplementedError("Non-equijoins unimplemented".to_string()));
                                },
                                Expr::CompoundIdentifier(_) => {
                                    return Err(AnalyzerError::UnimplementedError("Non-equijoins unimplemented".to_string()));
                                },
                                Expr::Value(v) => {
                                    if i.len() != 2 {
                                        return Err(AnalyzerError::UnimplementedError("Only simple identifiers or compound identifiers with two parts are supported in WHERE".to_string()));
                                    }
                                    (Self::find_bound_attribute(&i[1].value, Some(&i[0].value), from_tables)?, Self::convert_to_tuple_value(v)?, Self::convert_binary_operator(op)?)
                                },
                                _ => return Err(AnalyzerError::UnimplementedError("Only identifier-value or identifier-identifier operations allowed in WHERE".to_string())),
                            }
                            Expr::Value(v) => match right.as_ref() {
                                Expr::Identifier(i) => {
                                    (Self::find_bound_attribute(&i.value, None, from_tables)?, Self::convert_to_tuple_value(v)?, Self::convert_binary_operator(op)?.invert())
                                },
                                Expr::CompoundIdentifier(i) => {
                                    if i.len() != 2 {
                                        return Err(AnalyzerError::UnimplementedError("Only simple identifiers or compound identifiers with two parts are supported in WHERE".to_string()));
                                    }
                                    (Self::find_bound_attribute(&i[1].value, Some(&i[0].value), from_tables)?, Self::convert_to_tuple_value(v)?, Self::convert_binary_operator(op)?.invert())
                                },
                                Expr::Value(_) => return Err(AnalyzerError::UnimplementedError("literal = literal comparisons currentlly unimplemented".to_string())),
                                _ => return Err(AnalyzerError::UnimplementedError("Only identifier-value or identifier-identifier operations allowed in WHERE".to_string())),
                            },
                            _ => return Err(AnalyzerError::UnimplementedError("Only identifier-value or identifier-identifier operations allowed in WHERE".to_string())),
                        };
                        let selection = Selection {
                            attribute,
                            value,
                            operator
                        };
                        selections.push(selection)
                    },
                    BinaryOperator::And => {
                        Self::analyze_where_rec(left, selections, join_predicates, from_tables)?;
                        Self::analyze_where_rec(right, selections, join_predicates, from_tables)?;
                    },
                    _ => return Err(AnalyzerError::UnimplementedError("Only basic comparisons (=,<,>,>=,<=) and 'AND' are supported in WHERE".to_string()))
                }
            },
            _ => return Err(AnalyzerError::UnimplementedError("Only basic comparisons (=,<,>,>=,<=) and 'AND' are supported in WHERE".to_string()))
        }
        Ok(())
    }

    fn find_bound_attribute(name: &str, binding: Option<&str>, tables: &[BoundTable]) -> Result<BoundAttribute, AnalyzerError<B>> {
        let binding = binding.map(|s| s.to_string());
        if let Some(binding) = &binding {
            if !tables.iter().any(|t| t.binding == Some(binding.clone())) {
                return Err(AnalyzerError::UnboundBinding(binding.clone()));
            }
        }
        let attribute = {
            let found_attributes = tables.iter()
                .filter(|t| t.binding == binding)
                .filter_map(|t| t.table.get_attribute_by_name(&name))
                .exactly_one();
            match found_attributes {
                Ok(attribute) => attribute,
                Err(e) => match e.count() {
                    0 => return Err(AnalyzerError::AttributeNotFoundError(name.to_string())),
                    _ => return Err(AnalyzerError::AmbiguousAttributeName(name.to_string()))
                }
            }
        };
        Ok(BoundAttribute { attribute: attribute.clone(), binding: binding })
    }

    fn analyze_insert(&self, insert_tree: InsertParseResult) -> Result<Statement, AnalyzerError<B>> {
        let table = match self.catalog.find_table_by_name(&insert_tree.table_name) {
            Ok(Some(table)) => table,
            Ok(None) => return Err(AnalyzerError::RelationNotFoundError(insert_tree.table_name.to_string())),
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
        Ok(Statement::Insert(InsertStatement {
            table: table,
            values: vec![tuple]
        }))

    }

    fn analyze_create_table(&self, create_table_tree: CreateTableParseTree) -> Result<Statement, AnalyzerError<B>> {
        if self.catalog.find_table_by_name(&create_table_tree.name).unwrap().is_some() {
            return Err(AnalyzerError::NameAlreadyExistsError(create_table_tree.name.to_string()));
        }
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
            }).collect(),
            indexes: vec![]
        };
        Ok(Statement::CreateTable(CreateTableStatement {
            table
        }))
    }

    fn analyze_create_index(&self, create_index_tree: CreateIndexParseTree) -> Result<Statement, AnalyzerError<B>> {
        if self.catalog.find_index_by_name(&create_index_tree.name).unwrap().is_some() {
            return Err(AnalyzerError::NameAlreadyExistsError(create_index_tree.name.to_string()));
        }
        let table = match self.catalog.find_table_by_name(&create_index_tree.table_name) {
            Ok(Some(table)) => table,
            Ok(None) => return Err(AnalyzerError::RelationNotFoundError(create_index_tree.table_name.to_string())),
            Err(e) => return Err(AnalyzerError::BufferManagerError(e))
        };
        let mut attributes = Vec::with_capacity(create_index_tree.columns.len());
        for column in create_index_tree.columns.iter() {
            let attribute = match table.get_attribute_by_name(&column) {
                Some(attribute) => attribute,
                None => return Err(AnalyzerError::AttributeNotFoundError(column.to_string()))
            };
            attributes.push(attribute);
        }
        let index_id = self.catalog.allocate_db_object_id();
        let segment_id = self.catalog.allocate_segment_ids(1);
        let index = IndexDesc {
            id: index_id,
            name: create_index_tree.name.to_string(),
            index_type: IndexType::BTree,
            indexed_id: table.id,
            attributes: attributes.into_iter().map(|a| a.id).collect(),
            segment_id,
            fsi_segment_id: None
        };
        Ok(Statement::CreateIndex(CreateIndexStatement { index }))
    }
}

#[cfg(test)]
mod test {

    use std::sync::Arc;

    use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

    use crate::{storage::{buffer_manager::mock::MockBufferManager, page::PAGE_SIZE}, catalog::{TableDesc, AttributeDesc}, types::TupleValueType, planner::BoundTableRef, config::DbConfig};

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
                indexes: vec![]
            }).unwrap();
            catalog
    }

    #[test]
    fn test_analyze_select_no_where() {
        let catalog = get_catalog(MockBufferManager::new(PAGE_SIZE));
        let analyzer = Analyzer::new(catalog);
        let parse_tree = Parser::parse_sql(&PostgreSqlDialect {}, "SELECT id FROM people").unwrap().pop().unwrap();
        let query = analyzer.analyze(parse_tree).unwrap();
        if let Statement::Select(query) = query {
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
    fn test_analyze_select_with_table_qualifiers() {
        let catalog = get_catalog(MockBufferManager::new(PAGE_SIZE));
        let analyzer = Analyzer::new(catalog);
        let parse_tree = Parser::parse_sql(&PostgreSqlDialect {}, "SELECT p.id FROM people p where p.age > 50").unwrap().pop().unwrap();
        let query = analyzer.analyze(parse_tree).unwrap();
        if let Statement::Select(query) = query {
            assert_eq!(query.select.len(), 1);
            assert_eq!(query.select[0].attribute.name, "id");
            assert_eq!(query.select[0].attribute.id, 1);
            assert_eq!(query.select[0].attribute.table_ref, 1);
            assert_eq!(query.select[0].binding, Some("p".to_string()));
            assert_eq!(query.from.len(), 1);
            assert_eq!(query.from[&BoundTableRef { table_ref: 1, binding: Some("p".to_string()) }].table.id, 1);
            assert_eq!(query.from[&BoundTableRef { table_ref: 1, binding: Some("p".to_string()) }].table.name, "people");
            assert_eq!(query.from[&BoundTableRef { table_ref: 1, binding: Some("p".to_string()) }].binding, Some("p".to_string()));
            assert_eq!(query.join_predicates.len(), 0);
            assert_eq!(query.selections.len(), 1);
            assert_eq!(query.selections[0].attribute.attribute, AttributeDesc {
                id: 3,
                name: "age".to_string(),
                data_type: TupleValueType::SmallInt,
                nullable: true,
                table_ref: 1
            });
            assert_eq!(query.selections[0].attribute.binding, Some("p".to_string()));
            assert_eq!(query.selections[0].value, Some(TupleValue::BigInt(50)));
            assert_eq!(query.selections[0].operator, SelectionOperator::GreaterThan);
        } else {
            panic!("Query is not a select query");
        }
    }

    #[test]
    fn test_analyze_insert() {
        let catalog = get_catalog(MockBufferManager::new(PAGE_SIZE));
        let analyzer = Analyzer::new(catalog);
        let parse_tree = Parser::parse_sql(&PostgreSqlDialect {}, "INSERT INTO people VALUES (1, 'John', 42)").unwrap().pop().unwrap();
        let query = analyzer.analyze(parse_tree).unwrap();
        if let Statement::Insert(query) = query {
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
        let parse_tree = Parser::parse_sql(&PostgreSqlDialect {}, "INSERT INTO people VALUES (1, 'John')").unwrap().pop().unwrap();
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
        let parse_tree = Parser::parse_sql(&PostgreSqlDialect {}, "INSERT INTO people VALUES (1, 'John', 42, 42)").unwrap().pop().unwrap();
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
        let parse_tree = Parser::parse_sql(&PostgreSqlDialect {}, "INSERT INTO people VALUES (1, 2, 42)").unwrap().pop().unwrap();
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
        let parse_tree = Parser::parse_sql(&PostgreSqlDialect {}, "CREATE TABLE people1 (id INT PRIMARY KEY, name VARCHAR(255))").unwrap().pop().unwrap();
        let query = analyzer.analyze(parse_tree).unwrap();
        if let Statement::CreateTable(query) = query {
            assert_eq!(query.table.id, 1);
            assert_eq!(query.table.name, "people1");
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

    #[test]
    fn test_create_already_existing_table() {
        let catalog = get_catalog(MockBufferManager::new(PAGE_SIZE));
        let analyzer = Analyzer::new(catalog);
        let parse_tree = Parser::parse_sql(&PostgreSqlDialect {}, "CREATE TABLE people (id INT PRIMARY KEY, name VARCHAR(255))").unwrap().pop().unwrap();
        let query = analyzer.analyze(parse_tree);
        assert!(query.is_err());
        if let Err(AnalyzerError::NameAlreadyExistsError(name)) = query {
            assert_eq!(name, "people");
        } else {
            panic!("Analyzer didn't return the correct error");
        }
    }

    #[test]
    fn test_create_index() {
        let catalog = get_catalog(MockBufferManager::new(PAGE_SIZE));
        let analyzer = Analyzer::new(catalog);
        let parse_tree = Parser::parse_sql(&PostgreSqlDialect {}, "CREATE INDEX idx_name ON people (name)").unwrap().pop().unwrap();
        let query = analyzer.analyze(parse_tree).unwrap();
        if let Statement::CreateIndex(query) = query {
            assert_eq!(query.index.id, 1);
            assert_eq!(query.index.name, "idx_name");
            assert_eq!(query.index.indexed_id, 1);
            assert_eq!(query.index.attributes.len(), 1);
            assert_eq!(query.index.attributes[0], 2);
        } else {
            panic!("Query is not a create index query");
        }
    }
}
