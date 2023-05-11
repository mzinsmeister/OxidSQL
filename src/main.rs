use std::time::SystemTime;

use crate::{access::tuple::Tuple, types::TupleValueType};

mod types;
mod util;
mod storage;
mod access;
mod execution;
mod optimizer;
mod planner;
mod catalog;

fn main() {
    println!("OxidSQL - Rusty SQL Database");    
}
