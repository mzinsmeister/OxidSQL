/*
    TODO: Implement a query planner that can take a parsed and analyzed query and produce a plan
          by applying rewrite rules and possibly using optimization techniques implemented in
          the optimizer module.
 */
mod bottomup;

use std::{collections::BTreeMap, fmt::{Formatter, Debug}};

use crate::{catalog::{AttributeDesc, TableDesc}, types::TupleValue, execution::plan::PhysicalQueryPlan};

#[derive(Clone)]
pub struct BoundTable {
    pub table: TableDesc,
    pub binding: Option<String>
}

impl BoundTable {
    pub fn new(table: TableDesc, binding: Option<String>) -> Self {
        Self {
            table,
            binding
        }
    }

    pub fn to_ref(&self) -> BoundTableRef {
        BoundTableRef {
            table_ref: self.table.id,
            binding: self.binding.clone()
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct BoundTableRef {
    pub table_ref: u32,
    pub binding: Option<String>
}

#[derive(Clone)]
pub struct BoundAttribute {
    pub attribute: AttributeDesc,
    pub binding: Option<String>
}

impl BoundAttribute {
    pub fn to_ref(&self) -> BoundAttributeRef {
        BoundAttributeRef {
            attribute_ref: self.attribute.id,
            table_ref: self.attribute.table_ref,
            binding: self.binding.clone()
        }
    }

    pub fn get_table_ref(&self) -> BoundTableRef {
        BoundTableRef {
            table_ref: self.attribute.table_ref,
            binding: self.binding.clone()
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct BoundAttributeRef {
    pub attribute_ref: u32,
    pub table_ref: u32,
    pub binding: Option<String>
}

impl BoundAttributeRef {
    pub fn get_table_ref(&self) -> BoundTableRef {
        BoundTableRef {
            table_ref: self.table_ref,
            binding: self.binding.clone()
        }
    }
}

pub struct Query {
    pub select: Vec<BoundAttribute>,
    pub from: BTreeMap<BoundTableRef, BoundTable>,
    pub selections: Vec<(BoundAttribute, TupleValue)>,
    pub join_predicates: Vec<(BoundAttribute, BoundAttribute)>,
}

pub trait Planner {
    fn plan(&self, query: &Query) -> Result<PhysicalQueryPlan, PlannerError>;
}

 pub enum PlannerError {}

 impl Debug for PlannerError {
     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
         write!(f, "PlannerError")
     }
 } 