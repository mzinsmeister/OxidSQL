use std::{path::PathBuf, env};

use rustyline::{DefaultEditor, error::ReadlineError};

use crate::database::OxidSQLDatabase;

mod config;
mod types;
mod util;
mod storage;
mod access;
mod execution;
mod statistics;
mod optimizer;
mod planner;
mod catalog;
mod analyzer;
mod parser;
pub mod database;

fn main() {
    println!("OxidSQL - Rusty SQL Database\n");
    println!("Currently supported SQL commands: SELECT, INSERT, CREATE TABLE");
    println!("SELECT syntax: \n      SELECT <attribute list> FROM <table name> [WHERE <condition>]");
    println!("INSERT syntax (specifying attributes currently not supported):\n      INSERT INTO <table name> VALUES <tuple>");
    println!("CREATE TABLE syntax (PRIMARY KEY can be specified, but currently doesn't do anything):");
    println!("      CREATE TABLE <table name> (<attribute list>)\n");

    let args: Vec<String> = env::args().collect();
    let path = if args.len() > 1 {
        PathBuf::from(&args[1])
    } else {
        PathBuf::from("./data")
    };
    // Create the directory if it doesn't exist
    if !path.exists() {
        std::fs::create_dir_all(&path).unwrap();
    }
    // Initialize the database if the path is empty
    let database = OxidSQLDatabase::new(path.clone(), 1024usize).unwrap();
    if path.exists() && path.is_dir() && path.read_dir().unwrap().count() > 0 {
        println!("Database already exists at {:?}", path);
    } else {
        println!("Initializing database at {:?}", path);
        database.demo_init();
    }

    println!();

    let mut rl = DefaultEditor::new().unwrap();
    loop {
        let readline = rl.readline("OxidSQL> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str()).unwrap();
                if line.as_str() == "\\q" {
                    break;
                }
                if let Err(e) = database.query(line.as_str()) {
                    println!("Error: {:?}", e);
                }
            },
            Err(ReadlineError::Interrupted) => {
                break
            },
            Err(ReadlineError::Eof) => {
                break
            },
            Err(err) => {
                println!("Error: {:?}", err);
                break
            }
        }
    }
}
