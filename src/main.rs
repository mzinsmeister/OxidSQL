mod analyze;
mod catalog;
mod parser;
mod storage;
mod database;
mod planner;
mod executor;

use crate::parser::parse_query;
use crate::analyze::Analyzer;
use crate::catalog::Catalog;
use crate::storage::SledStorageEngine;
use crate::planner::Planner;
use crate::executor::Executor;

fn main() {
    // delete database files
    std::fs::remove_dir_all("sled_test/testdb").unwrap_or(());

    let storage_engine = SledStorageEngine::new();
    let catalog = Catalog::new(&storage_engine);
    catalog.init_catalog();
    let analyzer = Analyzer::new(catalog);
    let planner = Planner::new(Catalog::new(&storage_engine));
    let executor = Executor::new(&storage_engine);

    println!("###############CREATE TABLE 1################");
    let parsed_query = parse_query("CREATE TABLE test (id long, text varchar(100), PRIMARY KEY(id))").unwrap().1;
    println!("-----------------Parser-----------------");
    println!("{:#?}", &parsed_query);
    let analyzed_query = analyzer.analyze_query_tree(&parsed_query);
    println!("----------------Analyzer----------------");
    println!("{:#?}", analyzed_query);
    let query_plan = planner.plan_query(analyzed_query);
    println!("----------------Planner-----------------");
    println!("{:#?}", query_plan);
    let result = executor.execute(&query_plan).unwrap();
    println!("----------------Executor-----------------");
    println!("Result: {:#?}", result);

    println!("###############CREATE TABLE 2################");
    let parsed_query = parse_query("CREATE TABLE test2 (2id long, 2text varchar(100), PRIMARY KEY(2id))").unwrap().1;
    println!("-----------------Parser-----------------");
    println!("{:#?}", &parsed_query);
    let analyzed_query = analyzer.analyze_query_tree(&parsed_query);
    println!("----------------Analyzer----------------");
    println!("{:#?}", analyzed_query);
    let query_plan = planner.plan_query(analyzed_query);
    println!("----------------Planner-----------------");
    println!("{:#?}", query_plan);
    let result = executor.execute(&query_plan).unwrap();
    println!("----------------Executor-----------------");
    println!("Result: {:#?}", result);

    println!("#################INSERT##################");
    let parsed_query = parse_query("INSERT INTO test VALUES (3, 'ttt')").unwrap().1;
    println!("-----------------Parser-----------------");
    println!("{:#?}", &parsed_query);
    let analyzed_query = analyzer.analyze_query_tree(&parsed_query);
    println!("----------------Analyzer----------------");
    println!("{:#?}", analyzed_query);
    let query_plan = planner.plan_query(analyzed_query);
    println!("----------------Planner-----------------");
    println!("{:#?}", query_plan);
    let result = executor.execute(&query_plan).unwrap();
    println!("----------------Executor-----------------");
    println!("Result: {:#?}", result);

    println!("#################INSERT##################");
    let parsed_query = parse_query("INSERT INTO test VALUES (2, 'tt')").unwrap().1;
    println!("-----------------Parser-----------------");
    println!("{:#?}", &parsed_query);
    let analyzed_query = analyzer.analyze_query_tree(&parsed_query);
    println!("----------------Analyzer----------------");
    println!("{:#?}", analyzed_query);
    let query_plan = planner.plan_query(analyzed_query);
    println!("----------------Planner-----------------");
    println!("{:#?}", query_plan);
    let result = executor.execute(&query_plan).unwrap();
    println!("----------------Executor-----------------");
    println!("Result: {:#?}", result);

    println!("#################INSERT##################");
    let parsed_query = parse_query("INSERT INTO test2 VALUES (4, 'tttt')").unwrap().1;
    println!("-----------------Parser-----------------");
    println!("{:#?}", &parsed_query);
    let analyzed_query = analyzer.analyze_query_tree(&parsed_query);
    println!("----------------Analyzer----------------");
    println!("{:#?}", analyzed_query);
    let query_plan = planner.plan_query(analyzed_query);
    println!("----------------Planner-----------------");
    println!("{:#?}", query_plan);
    let result = executor.execute(&query_plan).unwrap();
    println!("----------------Executor-----------------");
    println!("Result: {:#?}", result);



    /*println!("#################SELECT##################");
    let parsed_select = parse_query("SELECT text, id from test where id = 3").unwrap().1;
    println!("-----------------Parser-----------------");
    println!("{:#?}", &parsed_select);
    let analyzed_query = analyzer.analyze_query_tree(&parsed_select);
    println!("----------------Analyzer----------------");
    println!("{:#?}", analyzed_query);
    let query_plan = planner.plan_query(analyzed_query);
    println!("----------------Planner-----------------");
    println!("{:#?}", query_plan);
    let result = executor.execute(&query_plan).unwrap();
    println!("----------------Executor-----------------");
    println!("Result: {:#?}", result);*/

    println!("#################JOIN##################");
    let parsed_select = parse_query("SELECT text, id, 2text, 2id from test, test2 WHERE id = 2 AND 2id = 4").unwrap().1;
    println!("-----------------Parser-----------------");
    println!("{:#?}", &parsed_select);
    let analyzed_query = analyzer.analyze_query_tree(&parsed_select);
    println!("----------------Analyzer----------------");
    println!("{:#?}", analyzed_query);
    let query_plan = planner.plan_query(analyzed_query);
    println!("----------------Planner-----------------");
    println!("{:#?}", query_plan);
    let result = executor.execute(&query_plan).unwrap();
    println!("----------------Executor-----------------");
    println!("Result: {:#?}", result);


    /*let start_time = std::time::SystemTime::now();
    // Load test inserts:
    for i in 0u32..1400 {
        let query = format!("INSERT INTO test VALUES ({0}, 'ttt{0}  tt')", i);
        let parsed_query = parse_query(&query).unwrap().1;
        let analyzed_query = analyzer.analyze_query_tree(&parsed_query);
        let query_plan = planner.plan_query(analyzed_query);
        executor.execute(&query_plan).unwrap();
    }

    println!("Time elapsed: {}", start_time.elapsed().unwrap().as_millis());

    let start_time = std::time::SystemTime::now();
    // Load test selects:
    for i in 0u32..8000 {
        let query = format!("SELECT id, text FROM test where id = {}", i);
        let parsed_query = parse_query(&query).unwrap().1;
        let analyzed_query = analyzer.analyze_query_tree(&parsed_query);
        let query_plan = planner.plan_query(analyzed_query);
        executor.execute(&query_plan).unwrap();
    }
    println!("Time elapsed: {}", start_time.elapsed().unwrap().as_millis());*/

}
