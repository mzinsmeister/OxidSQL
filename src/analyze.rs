use crate::catalog::{Catalog, Type};
use crate::parser::{QueryTree, SelectQueryTree, ParseWhereClause, ParseWhereClauseItem, InsertQueryTree, CreateTableQueryTree, TableDefinitionItem};
use std::error::Error;
use std::fmt::{Display, Formatter, Debug};
use crate::database::TupleValue;
use crate::analyze::AnalyzerError::MultiplePrimaryKeysError;

#[derive(Debug)]
pub enum Query {
    Select(SelectQuery),
    Insert(InsertQuery),
    CreateTable(TableDefinition)
}

#[derive(Debug)]
pub struct SelectQuery {
    pub targets: Vec<QueryColumn>,
    pub from: Vec<QueryTable>,
    pub where_clause: Option<SqlFilterExpression>
}

#[derive(Debug)]
pub struct InsertQuery {
    pub table: QueryTable,
    pub values: Vec<TupleValue>
}

#[derive(Clone, Debug)]
pub struct QueryColumn {
    pub column_id: u32,
    pub table: QueryTable,
}

#[derive(Clone, Debug)]
pub struct QueryTable {
    pub table_id: u32
}

#[derive(Clone, Debug)]
pub enum SqlFilterExpression {
    ComparisonExpression(SqlComparisonExpression),
    LogicalExpression(SqlLogicalExpression)
}

#[derive(Clone, Debug)]
pub enum SqlComparisonExpression {
    Equals(SqlComparisonItem, SqlComparisonItem),
    NotEquals(SqlComparisonItem, SqlComparisonItem),
    LessThan(SqlComparisonItem, SqlComparisonItem),
    LessThanOrEquals(SqlComparisonItem, SqlComparisonItem)
}

#[derive(Clone, Debug)]
pub enum SqlComparisonItem {
    Literal(TupleValue),
    Reference(QueryColumn)
}

#[derive(Clone, Debug)]
pub enum SqlLogicalExpression {
    And(Box<SqlFilterExpression>, Box<SqlFilterExpression>),
    Or(Box<SqlFilterExpression>, Box<SqlFilterExpression>),
    Not(Box<SqlFilterExpression>)
}

#[derive(Clone, Debug)]
pub struct TableDefinition {
    pub name: String,
    pub columns: Vec<ColumnDefinition>,
    pub primary_key: Vec<usize>
}

#[derive(Clone, Debug)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: Type,
}


#[derive(Debug)]
pub enum AnalyzerError {
    TableNotFoundError(String),
    ColumnNotFoundError(String),
    MultiplePrimaryKeysError,
    NoPrimaryKeyError
}

impl Display for AnalyzerError {
    fn fmt(&self,  f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AnalyzerError::TableNotFoundError(table_name) =>
                write!(f, "Table {} not found", table_name),
            AnalyzerError::ColumnNotFoundError(column_name) =>
                write!(f, "Column {} not found", column_name),
            AnalyzerError::MultiplePrimaryKeysError =>
                write!(f, "Multiple Primary Key definitions found"),
            AnalyzerError::NoPrimaryKeyError =>
                write!(f, "No Primary Key definition found"),
        }
    }
}

impl Error for AnalyzerError{}

pub struct Analyzer {
    catalog: Catalog
}

impl Analyzer {

    pub fn new(catalog: Catalog) -> Analyzer {
        Analyzer { catalog }
    }

    // Maybe it would be good to take ownership of the query tree here so that
    // we don't have to copy the few values that we could just take from the query tree
    // but i think it's better the way it is. Best way would probably be to use a Rc or an Arc
    // for the parts that could be shared (especially stuff like values of inserts)
    pub fn analyze_query_tree(&self, query_tree: &QueryTree) -> Query {
        match query_tree {
            QueryTree::Select(s) =>
                Query::Select(self.analyze_select_query_tree(s).unwrap()),
            QueryTree::Insert(i) =>
                Query::Insert(self.analyze_insert_query_tree(i).unwrap()),
            QueryTree::CreateTable(c) =>
                Query::CreateTable(self.analyze_create_table_query_tree(c).unwrap())
        }
    }

    fn analyze_select_query_tree(&self, select_query_tree: &SelectQueryTree)
        -> Result<SelectQuery, Box<dyn Error>> {
        let from = self.analyze_from(&select_query_tree.from_tables)?;
        let targets = self.analyze_columns(&from, &select_query_tree.columns)?;
        let where_clause = match &select_query_tree.where_clause {
            Some(w) => Some(self.analyze_where_clause(w, &from)?),
            None => None
        };
        Ok(SelectQuery { targets, from, where_clause })
    }

    fn analyze_from(&self, from_tables: &Vec<&str>) -> Result<Vec<QueryTable>, AnalyzerError> {
        let mut tables: Vec<QueryTable> = Vec::new();
        for table_name in from_tables {
            let table_id_opt = self.catalog
                .lookup_table_id_by_name(table_name);
            match table_id_opt {
                Some(table_id) =>
                    tables.push(QueryTable{ table_id }),
                None => return Err(AnalyzerError::TableNotFoundError(String::from(*table_name)))
            }
        }
        Ok(tables)
    }

    fn analyze_columns(&self, from: &Vec<QueryTable>, column_names: &Vec<&str>)
        -> Result<Vec<QueryColumn>, AnalyzerError> {
        let mut columns: Vec<QueryColumn> = Vec::new();
        'column_loop:
        for column_name in column_names {
            for table in from.iter() {
                let found_column =
                    self.catalog.lookup_column_id_by_name(table.table_id, &column_name);
                if let Some(c) = found_column {
                    columns.push(QueryColumn {
                        table: QueryTable { table_id: table.table_id },
                        column_id: c
                    });
                    continue 'column_loop;
                }
            }
            return Err(AnalyzerError::ColumnNotFoundError(String::from(*column_name)))
        }
        Ok(columns)
    }

    fn analyze_where_clause(&self, parse_where_clause: &ParseWhereClause, from: &Vec<QueryTable>)
        -> Result<SqlFilterExpression, AnalyzerError> {
        match parse_where_clause {
            ParseWhereClause::Equals(left, right) =>
                Ok(SqlFilterExpression::ComparisonExpression(SqlComparisonExpression::Equals(
                    self.analyze_where_clause_item(left, from)?,
                    self.analyze_where_clause_item(right, from)?))),
            ParseWhereClause::NotEquals(left, right) =>
                Ok(SqlFilterExpression::ComparisonExpression(SqlComparisonExpression::NotEquals(
                    self.analyze_where_clause_item(left, from)?,
                    self.analyze_where_clause_item(right, from)?))),
            ParseWhereClause::LessThan(left, right) =>
                Ok(SqlFilterExpression::ComparisonExpression(SqlComparisonExpression::LessThan(
                    self.analyze_where_clause_item(left, from)?,
                    self.analyze_where_clause_item(right, from)?))),
            ParseWhereClause::GreaterThan(left, right) =>
                Ok(SqlFilterExpression::ComparisonExpression(SqlComparisonExpression::LessThan(
                    self.analyze_where_clause_item(right, from)?,
                    self.analyze_where_clause_item(left, from)?))),
            ParseWhereClause::LessOrEqualThan(left, right) =>
                Ok(SqlFilterExpression::ComparisonExpression(SqlComparisonExpression::LessThanOrEquals(
                    self.analyze_where_clause_item(left, from)?,
                    self.analyze_where_clause_item(right, from)?))),
            ParseWhereClause::GreaterOrEqualThan(left, right) =>
                Ok(SqlFilterExpression::ComparisonExpression(SqlComparisonExpression::LessThanOrEquals(
                    self.analyze_where_clause_item(right, from)?,
                    self.analyze_where_clause_item(left, from)?))),
            ParseWhereClause::And(left, right) =>
                Ok(SqlFilterExpression::LogicalExpression(SqlLogicalExpression::And(
                    Box::new(self.analyze_where_clause(left, from)?),
                    Box::new(self.analyze_where_clause(right, from)?)))),
            ParseWhereClause::Or(left, right) =>
                Ok(SqlFilterExpression::LogicalExpression(SqlLogicalExpression::Or(
                    Box::new(self.analyze_where_clause(left, from)?),
                    Box::new(self.analyze_where_clause(right, from)?))))
        }
    }

    fn analyze_where_clause_item(&self,
                                 parse_where_clause_item: &ParseWhereClauseItem,
                                 from: &Vec<QueryTable>)
        -> Result<SqlComparisonItem, AnalyzerError> {
        match parse_where_clause_item {
            ParseWhereClauseItem::Value(value) =>
                Ok(SqlComparisonItem::Literal(value.to_owned())),
            ParseWhereClauseItem::Name(name) => self.search_column_in_froms(name, from)
        }
    }

    fn search_column_in_froms(&self, name: &str, from: &Vec<QueryTable>)
        -> Result<SqlComparisonItem, AnalyzerError> {
        // TODO: Would probably be good to check here if the literals have a compatible type to the column
        for table in from {
            let table = self.catalog.find_table_by_id(table.table_id).unwrap();
            let find_result = self.catalog.lookup_column_id_by_name(table.id, name);
            if let Some(result) = find_result {
                return Ok(SqlComparisonItem::Reference(
                    QueryColumn {
                        column_id: result,
                        table: QueryTable { table_id: table.id }
                    }));
            }
        }
        Err(AnalyzerError::ColumnNotFoundError(String::from(name)))
    }

    fn analyze_insert_query_tree(&self, insert_query_tree: &InsertQueryTree)
        ->  Result<InsertQuery, Box<dyn Error>> {
        let table_lookup_result =
            self.catalog.lookup_table_id_by_name(insert_query_tree.table);
        let table_id = if let Some(table_id) = table_lookup_result {
            table_id
        } else {
            return Err(Box::new(
                AnalyzerError::TableNotFoundError(String::from(insert_query_tree.table))));
        };
        // TODO: It would probably be good to check if the values have the right types here
        Ok(InsertQuery {
            table: QueryTable { table_id },
            values: insert_query_tree.values.to_owned()
        })
    }

    fn analyze_create_table_query_tree(&self, create_table_query_tree: &CreateTableQueryTree)
                                       ->  Result<TableDefinition, AnalyzerError> {
        let mut primary_key_names: Option<Vec<&str>> = None;
        let mut columns: Vec<ColumnDefinition> = Vec::new();
        for item in &create_table_query_tree.table_definition {
            match item {
                TableDefinitionItem::PrimaryKeyDefinition(column_names) => {
                    if primary_key_names.is_some() {
                        return Err(MultiplePrimaryKeysError)
                    } else {
                        primary_key_names = Some(column_names.to_owned())
                    }
                },
                TableDefinitionItem::ColumnDefinition { definition, is_primary_key } => {
                    let column_definition = ColumnDefinition {
                        name: String::from(definition.name),
                        data_type: definition.column_type.to_owned()
                    };
                    columns.push(column_definition);
                    if *is_primary_key {
                        if primary_key_names.is_some() {
                            return Err(MultiplePrimaryKeysError)
                        } else {
                            primary_key_names = Some(vec![definition.name])
                        }
                    }
                }
            }
        }
        let mut primary_key: Vec<usize> = Vec::new();
        if let Some(primary_key_names) = primary_key_names {
            for name in primary_key_names {
                let found_column = columns.iter()
                    .enumerate()
                    .find(|(_, c)| c.name == name)
                    .map(|(i, _)| i);
                if let Some(column_index) = found_column {
                    primary_key.push(column_index)
                } else {
                    return Err(AnalyzerError::ColumnNotFoundError(String::from(name)))
                }
            }
        } else {
            return Err(AnalyzerError::NoPrimaryKeyError)
        }
        Ok(TableDefinition {
            name: String::from(create_table_query_tree.name),
            columns,
            primary_key
        })
    }
}
