use std::{path::PathBuf, env};

use petgraph::{visit::Data, data};
use rustyline::{DefaultEditor, error::ReadlineError};

use crate::database::OxidSQLDatabase;


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
    println!("OxidSQL - Rusty SQL Database");

    let args: Vec<String> = env::args().collect();
    let path = if args.len() > 0 {
        PathBuf::from(&args[1])
    } else {
        PathBuf::from("./data")
    };
    // Initialize the database if the path is empty
    let database = OxidSQLDatabase::new(path.clone(), 1024usize);
    if path.exists() && path.is_dir() && path.read_dir().unwrap().count() > 0 {
        println!("Database already exists at {:?}", path);
    } else {
        println!("Initializing database at {:?}", path);
        database.demo_init();
    }

    let mut rl = DefaultEditor::new().unwrap();
    loop {
        let readline = rl.readline("oxidsql> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str()).unwrap();
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
