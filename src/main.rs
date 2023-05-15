use std::time::SystemTime;

use crate::{access::tuple::Tuple, types::TupleValueType};

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


fn main() {
    println!("OxidSQL - Rusty SQL Database");    
}
