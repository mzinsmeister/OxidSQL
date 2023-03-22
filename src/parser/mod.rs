/*
    TODO: Implement a parser for the SQL dialect supported by the database (likely using the nom
          parser combinator library). The parser should produce an abstract syntax tree (AST) that 
          can be analyzed by the analyzer which for now will also live in this module. The SQL
          dialect should for now be compatible with postgres.

          Options are:
            - Use the parser written for my hacky v1 database (also with Nom)
            - Use a library exposing the actual postgres parser in Rust
            - Write a parser from scratch
 */

 #[derive(Debug)]
 pub enum ParseTree<'a> {
     Select(SelectStmtTree<'a>),
     Insert(InsertStmtTree<'a>),
     CreateTable(CreateTableStmtTree<'a>)
 }
 
 #[derive(Debug)]
 pub struct SelectParseTree<'a> {
     pub columns: Vec<&'a str>,
     pub from_tables: Vec<&'a str>,
     pub where_clause: Option<ParseWhereClause<'a>>
 }
 
 #[derive(Debug)]
 pub enum ParseWhereClause<'a> {
     Equals(ParseWhereClauseItem<'a>, ParseWhereClauseItem<'a>),
     NotEquals(ParseWhereClauseItem<'a>, ParseWhereClauseItem<'a>),
     LessThan(ParseWhereClauseItem<'a>, ParseWhereClauseItem<'a>),
     GreaterThan(ParseWhereClauseItem<'a>, ParseWhereClauseItem<'a>),
     LessOrEqualThan(ParseWhereClauseItem<'a>, ParseWhereClauseItem<'a>),
     GreaterOrEqualThan(ParseWhereClauseItem<'a>, ParseWhereClauseItem<'a>),
     And(Box<ParseWhereClause<'a>>, Box<ParseWhereClause<'a>>),
     Or(Box<ParseWhereClause<'a>>, Box<ParseWhereClause<'a>>)
 }
 
 #[derive(Debug)]
 pub enum ParseWhereClauseItem<'a> {
     Name(&'a str),
     Value(TupleValue)
 }

 #[derive(Debug, Clone)]
pub struct InsertQueryTree<'a> {
    pub table: &'a str,
    pub values: Vec<TupleValue>
}

#[derive(Debug, Clone)]
pub struct CreateTableQueryTree<'a> {
    pub name: &'a str,
    pub table_definition: Vec<TableDefinitionItem<'a>>
}

#[derive(Debug, Clone)]
pub struct CreateTableColumn<'a> {
    pub name: &'a str,
    pub column_type: Type,
}

#[derive(Debug, Clone)]
pub enum TableDefinitionItem<'a> {
    PrimaryKeyDefinition(Vec<&'a str>),
    ColumnDefinition{ definition: CreateTableColumn<'a>, is_primary_key: bool }
}

#[derive(Debug)]
pub enum BoundQuery {
  Select(BoundSelectStmt),
  Insert(BoundInsertStmt),
  CreateTable(BoundCreateTableStmt)
}

#[derive(Debug)]
pub struct BoundSelectStmt {
    pub attributes: Vec<ColumnRef>,
    pub tables: Vec<TableRef>,
    pub where_clause: Option<ParseWhereClause<'a>>
}

 