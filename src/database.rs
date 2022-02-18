use std::{error::Error, fmt::Display};

use log::debug;

use crate::{storage::SledStorageEngine, catalog::Catalog, planner::Planner, executor::{Executor, ResultSet}, analyze::Analyzer, parser::parse_query};

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub enum TupleValue {
    Long(i64),
    UnsignedInt(u32),
    String(String),
    Bytes(Vec<u8>)
}

impl TupleValue {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            TupleValue::Long(l) => Vec::from(l.to_be_bytes()),
            TupleValue::UnsignedInt(u) => Vec::from(u.to_be_bytes()),
            TupleValue::String(s) => s.as_bytes().to_vec(),
            TupleValue::Bytes(b) => b.to_vec()
        }
    }

    pub fn get_as_string(self) -> String {
        if let Self::String(str) = self {
            str
        } else {
            panic!("Not a string");
        }
    }

    pub fn get_as_bytes(self) -> Vec<u8> {
        if let Self::Bytes(bytes) = self {
            bytes
        } else {
            panic!("Not a byte array");
        }
    }

    pub fn get_as_unsigned_int(self) -> u32 {
        if let Self::UnsignedInt(uint) = self {
            uint
        } else {
            panic!("Not an unsigned int");
        }
    }
}

impl Display for TupleValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TupleValue::Long(v) => write!(f, "{}", v),
            TupleValue::UnsignedInt(v) => write!(f, "{}", v),
            TupleValue::String(v) => write!(f, "{}", v),
            TupleValue::Bytes(_) => unimplemented!("Bytes can't be displayed at the moment"),
        }
    }
}

pub struct OxidSqlDatabase {
    analyzer: Analyzer,
    planner: Planner,
    executor: Executor
}

impl OxidSqlDatabase {
    pub fn new() -> OxidSqlDatabase {
        let storage_engine = SledStorageEngine::new();
        let catalog = Catalog::new(storage_engine.clone());
        catalog.init_catalog();
        let analyzer = Analyzer::new(catalog);
        let planner = Planner::new(Catalog::new(storage_engine.clone()));
        let executor = Executor::new(storage_engine);
        OxidSqlDatabase {
            analyzer,
            planner,
            executor
        }
    }

    pub fn query(&self, query: &str) -> Result<ResultSet, Box<dyn Error>> {
        let parsed_query = parse_query(&query)
            .map_err(|err| err
                .map(|err| nom::error::Error::new(err.input.to_string(), err.code)))?.1;
        debug!("-----------------Parser-----------------");
        debug!("{:#?}", &parsed_query);
        let analyzed_query = self.analyzer.analyze_query_tree(&parsed_query);
        debug!("----------------Analyzer----------------");
        debug!("{:#?}", &analyzed_query);
        let query_plan = self.planner.plan_query(analyzed_query);
        debug!("----------------Planner-----------------");
        debug!("{:#?}", &query_plan);
        let result = self.executor.execute(&query_plan)?;
        debug!("----------------Executor-----------------");
        debug!("{:#?}", &result);
        Ok(result)
    }

}
