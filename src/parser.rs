use nom::IResult;
use nom::bytes::complete::{tag_no_case, tag, is_not, is_a, take_while1};
use nom::character::complete::{multispace0, multispace1, char, digit1};
use nom::combinator::{opt, map_res, map};
use nom::sequence::{delimited, tuple};
use crate::database::TupleValue;
use nom::branch::alt;
use nom::character::{is_alphanumeric};
use nom::multi::separated_list1;
use nom::error::ParseError;
use crate::catalog::Type;

#[derive(Debug)]
pub enum QueryTree<'a> {
    Select(SelectQueryTree<'a>),
    Insert(InsertQueryTree<'a>),
    CreateTable(CreateTableQueryTree<'a>)
}

#[derive(Debug)]
pub struct SelectQueryTree<'a> {
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

pub fn parse_query(query: &str) -> IResult<&str, QueryTree> {
    alt((parse_select, parse_insert, parse_create_table))(query)
}

fn parse_select(rest_query: &str) -> IResult<&str, QueryTree> {
    let (rest_query, _) = tag_no_case("SELECT")(rest_query)?;
    let (rest_query, _) = multispace1(rest_query)?;
    let (rest_query, select) = parse_sql_list(rest_query)?;
    let (rest_query, _) = tag_no_case("FROM")(rest_query)?;
    let (rest_query, _) = multispace1(rest_query)?;
    let (rest_query, from) = parse_sql_list(rest_query)?;
    let (_, where_clause) = opt(parse_where_and_where_clause)(rest_query)?;
    Ok(("", QueryTree::Select(SelectQueryTree { columns: select, from_tables: from, where_clause })))
}

fn parse_sql_list(rest_query: &str) -> IResult<&str, Vec<&str>> {
    let (rest_query, list) =
        separated_list1(parse_sql_list_separator, take_while1(is_sql_identifier_char))(rest_query)?;
    let (rest_query, _) = multispace0(rest_query)?;
    Ok((rest_query, list))
}

fn parse_sql_list_separator(rest_query: &str) -> IResult<&str, &str> {
    let (rest_query, _) = multispace0(rest_query)?;
    let (rest_query, separator) =tag(",")(rest_query)?;
    let (rest_query, _) = multispace0(rest_query)?;
    Ok((rest_query, separator))
}

fn parse_where_and_where_clause(rest_query: &str) -> IResult<&str, ParseWhereClause> {
    let (rest_query, _) = tag_no_case("WHERE")(rest_query)?;
    let (rest_query, _) = multispace1(rest_query)?;
    parse_where_clause(rest_query)
}

fn parse_where_clause(rest_query: &str) -> IResult<&str, ParseWhereClause> {
    let (rest_query, left) = parse_where_term(rest_query)?;
    parse_or_and_right(rest_query, left)
}

fn parse_where_term(rest_query: &str) -> IResult<&str, ParseWhereClause> {
    let (rest_query, left) = parse_atomic_where_clause(rest_query)?;
    parse_and_and_right(rest_query, left)
}

fn parse_and_and_right<'a>(rest_query: &'a str, left_side: ParseWhereClause<'a>)
    -> IResult<&'a str, ParseWhereClause<'a>> {
    let (rest_query, _) = multispace0(rest_query)?;
    let (rest_query, operator_option) = opt(tag_no_case("and"))(rest_query)?;
    if operator_option == None {
        return Ok((rest_query, left_side))
    };
    let (rest_query, _) = multispace0(rest_query)?;
    let (rest_query, right_side) = parse_atomic_where_clause(rest_query)?;
    let where_clause = ParseWhereClause::And(Box::from(left_side), Box::from(right_side));
    parse_and_and_right(rest_query, where_clause)
}

fn parse_atomic_where_clause(rest_query: &str) -> IResult<&str, ParseWhereClause> {
    alt((parse_bracketed_where_clause, parse_comparison_where_clause))(rest_query)
}

fn parse_bracketed_where_clause(rest_query: &str) -> IResult<&str, ParseWhereClause> {
    delimited(char('('), parse_where_clause, char(')'))(rest_query)
}

fn parse_comparison_where_clause(rest_query: &str) -> IResult<&str, ParseWhereClause> {
    let (rest_query, left_side) = with_optional_whitespace_padding(is_not(" \t,)=<>"))(rest_query)?;
    let (rest_query, operator) = parse_comp_operator(rest_query)?;
    let (rest_query, right_side) = with_optional_whitespace_padding(is_not(" \t,)=<>"))(rest_query)?;
    let left_side_item = parse_where_clause_item(left_side);
    let right_side_item = parse_where_clause_item(right_side);
    match operator {
        "=" => Ok((rest_query, ParseWhereClause::Equals(left_side_item, right_side_item))),
        "<" => Ok((rest_query, ParseWhereClause::LessThan(left_side_item, right_side_item))),
        "<=" => Ok((rest_query, ParseWhereClause::LessOrEqualThan(left_side_item, right_side_item))),
        ">" => Ok((rest_query, ParseWhereClause::GreaterThan(left_side_item, right_side_item))),
        ">=" => Ok((rest_query, ParseWhereClause::GreaterOrEqualThan(left_side_item, right_side_item))),
        "<>" => Ok((rest_query, ParseWhereClause::NotEquals(left_side_item, right_side_item))),
        _ => panic!("The operator was parsed succesfully but not in Operator match")
    }
}

fn parse_comp_operator(rest_query: &str) -> IResult<&str, &str> {
    let (rest_query, _) = multispace0(rest_query)?;
    let (rest_query, operator) = alt((
        tag("="), tag("<"), tag("<="), tag(">"), tag(">="), tag("<>")))(rest_query)?;
    let (rest_query, _) = multispace0(rest_query)?;
    Ok((rest_query, operator))
}

fn parse_where_clause_item(item: &str) -> ParseWhereClauseItem {
    if item.starts_with("'") && item.ends_with("'") {
        ParseWhereClauseItem::Value(TupleValue::String(String::from(&item[1..(item.len()-1)])))
    } else if item.parse::<i64>().is_ok() {
        ParseWhereClauseItem::Value(TupleValue::Long(item.parse::<i64>().unwrap()))
    } else {
        ParseWhereClauseItem::Name(item)
    }
}

fn parse_or_and_right<'a>(rest_query: &'a str, left_side: ParseWhereClause<'a>)
    -> IResult<&'a str, ParseWhereClause<'a>> {
    let (rest_query, _) = multispace0(rest_query)?;
    let (rest_query, operator_option) = opt(tag_no_case("or"))(rest_query)?;
    if let None = operator_option {
        return Ok((rest_query, left_side));
    }
    let (rest_query, _) = multispace0(rest_query)?;
    let (rest_query, right_side) = parse_where_term(rest_query)?;
    let where_clause = ParseWhereClause::Or(Box::from(left_side), Box::from(right_side));
    parse_or_and_right(rest_query, where_clause)
}

#[derive(Debug, Clone)]
pub struct InsertQueryTree<'a> {
    pub table: &'a str,
    pub values: Vec<TupleValue>
}

fn parse_insert(rest_query: &str) -> IResult<&str, QueryTree> {
    let (rest_query, _) = tag_no_case("INSERT")(rest_query)?;
    let (rest_query, _) = multispace1(rest_query)?;
    let (rest_query, _) = tag_no_case("INTO")(rest_query)?;
    let (rest_query, _) = multispace1(rest_query)?;
    let (rest_query, table_name) = take_while1(is_sql_identifier_char)(rest_query)?;
    let (rest_query, _) = multispace1(rest_query)?;
    let (rest_query, _) = tag_no_case("VALUES")(rest_query)?;
    let (rest_query, _) = multispace1(rest_query)?;
    let (_, values) = delimited(
        with_optional_whitespace_padding(is_a("(")),
        separated_list1(parse_sql_list_separator,alt((parse_string_value, parse_long_value))),
        with_optional_whitespace_padding(is_a(")")))(rest_query)?;
    Ok(("", QueryTree::Insert(InsertQueryTree { table: table_name, values })))
}

fn is_sql_identifier_char(char: char) -> bool {
    is_alphanumeric(char as u8) || char == '_'
}

fn parse_string_value(rest_query: &str) -> IResult<&str, TupleValue> {
    let (rest_query, string) = delimited(
        char('\''), is_not("'"), char('\''))(rest_query)?;
    Ok((rest_query, TupleValue::String(String::from(string))))
}

fn parse_long_value(rest_query: &str) -> IResult<&str, TupleValue> {
    let (rest_query, value) = map_res(digit1, str::parse::<i64>)(rest_query)?;
    Ok((rest_query, TupleValue::Long(value)))
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

fn parse_create_table(rest_query: &str) -> IResult<&str, QueryTree> {
    let (rest_query, _) = tag_no_case("CREATE")(rest_query)?;
    let (rest_query, _) = multispace1(rest_query)?;
    let (rest_query, _) = tag_no_case("TABLE")(rest_query)?;
    let (rest_query, _) = multispace1(rest_query)?;
    let (rest_query, name) = take_while1(is_sql_identifier_char)(rest_query)?;
    let (rest_query, _) = multispace1(rest_query)?;
    let (rest_query, table_definition) =
        delimited(char('('), parse_table_definition, char(')'))(rest_query)?;
    Ok((rest_query, QueryTree::CreateTable(CreateTableQueryTree {
        name,
        table_definition
    })))
}

fn parse_table_definition(rest_query: &str) -> IResult<&str, Vec<TableDefinitionItem>> {
    separated_list1(char(','), parse_table_definition_item)(rest_query)
}

fn parse_table_definition_item(ret_query: &str) -> IResult<&str, TableDefinitionItem> {
    delimited(multispace0,
              alt((parse_primary_key_specifier, parse_column_definition)),
              multispace0)(ret_query)
}

fn parse_primary_key_specifier(rest_query: &str) -> IResult<&str, TableDefinitionItem> {
    let (rest_query, _) = tag_no_case("PRIMARY")(rest_query)?;
    let (rest_query, _) = multispace1(rest_query)?;
    let (rest_query, _) = tag_no_case("KEY")(rest_query)?;
    let (rest_query, _) = multispace0(rest_query)?;
    let (rest_query, primary_key_list) = delimited(
        char('('),
        separated_list1(char(','),
                        with_optional_whitespace_padding(
                            take_while1(is_sql_identifier_char))),
        char(')')
    )(rest_query)?;
    Ok((rest_query, TableDefinitionItem::PrimaryKeyDefinition(primary_key_list)))
}

fn parse_column_definition(rest_query: &str) -> IResult<&str, TableDefinitionItem> {
    let (rest_query, name) = take_while1(is_sql_identifier_char)(rest_query)?;
    let (rest_query, _) = multispace1(rest_query)?;
    let (rest_query, column_type) = parse_sql_type(rest_query)?;
    let (rest_query, pk_option) = opt(tuple((
        multispace1,
        tag_no_case("PRIMARY"),
        multispace1,
        tag_no_case("KEY")))
    )(rest_query)?;
    Ok((rest_query,TableDefinitionItem::ColumnDefinition {
        definition: CreateTableColumn { name, column_type },
        is_primary_key: pk_option.is_some()
    }))
}

fn parse_sql_type(rest_query: &str) -> IResult<&str, Type> {
    alt((
        map(tag_no_case("LONG"), |_| Type::Long),
        map(tuple((
            tag_no_case("VARCHAR"),
            delimited(
                char('('),
                map_res(digit1, |s: &str| s.parse::<u16>()),
                char(')')
            ))), |(_, len)| Type::Varchar(len)),
    ))(rest_query)
}

fn with_optional_whitespace_padding<I: nom::InputTakeAtPosition, O, E: ParseError<I>, F>(parser: F)
                                                                                         -> impl Fn(I) -> IResult<I, O, E>
    where F: Fn(I) -> IResult<I, O, E>,
          <I as nom::InputTakeAtPosition>::Item: nom::AsChar + Clone, {
    move |input: I| {
        let (rest_input, _) = multispace0(input)?;
        let (rest_input, output) = parser(rest_input)?;
        let (rest_input, _) = multispace0(rest_input)?;
        Ok((rest_input, output))
    }
}
