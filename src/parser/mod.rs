// First gen parser for OxidSQL. I will probably integrate the sqlparser crate instead. 
// There's not really much to gain from writing a parser from scratch, and it's a lot of work.

use sqlparser::ast::Statement;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::{Parser, ParserError};

pub fn parse_query<'a>(query: &'a str) -> Result<Vec<Statement>, ParserError> {
    let dialect = PostgreSqlDialect {};
    Parser::parse_sql(&dialect, query)
}
