mod database;
mod resettable_iterator;
mod analyze;
mod catalog;
mod parser;
mod storage;
mod planner;
mod executor;

use crate::database::OxidSqlDatabase;

fn main() {
    // delete database files
    std::fs::remove_dir_all("sled_test/testdb").unwrap_or(());

    let db = OxidSqlDatabase::new();

    println!("###############CREATE TABLE 1################");
    db.query("CREATE TABLE test (id long, text varchar(100), PRIMARY KEY(id))").unwrap();

    println!("###############CREATE TABLE 2################");
    db.query("CREATE TABLE test2 (2id long, 2text varchar(100), PRIMARY KEY(2id))").unwrap();
    

    println!("#################INSERT##################");
    db.query("INSERT INTO test VALUES (3, 'ttt')").unwrap();
    db.query("INSERT INTO test VALUES (2, 'tt')").unwrap();
    db.query("INSERT INTO test2 VALUES (4, 'ttt')").unwrap();

    println!("#################SELECT##################");
    db.query("SELECT text, id from test where id = 3").unwrap();

    println!("#################JOIN##################");
    db.query("SELECT text, id, 2text, 2id from test, test2 WHERE text = 2text").unwrap();


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
