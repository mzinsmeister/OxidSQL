/*
    TODO: Implement a query planner that can take a parsed and analyzed query and produce a plan
          by applying rewrite rules and possibly using optimization techniques implemented in
          the optimizer module.
 */
pub mod bottomup;

use std::{collections::BTreeMap, fmt::{Formatter, Debug, Display}};

use crate::{catalog::{AttributeDesc, TableDesc}, types::TupleValue, execution::plan::PhysicalQueryPlan, access::tuple::Tuple};

#[derive(Debug, Clone)]
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

#[derive(Clone, Debug)]
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

    pub fn get_qualified_name(&self) -> String {
        if let Some(binding) = &self.binding {
            format!("{}.{}", binding, self.attribute.name)
        } else {
            self.attribute.name.clone()
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
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

#[derive(Debug, Clone)]
pub enum Query {
    Select(SelectQuery),
    Insert(InsertQuery),
    CreateTable(CreateTableQuery),
    //Update(UpdateQuery),
    //Delete(DeleteQuery)
}

#[derive(Debug, Clone)]
pub struct SelectQuery {
    pub select: Vec<BoundAttribute>,
    pub from: BTreeMap<BoundTableRef, BoundTable>,
    pub selections: Vec<Selection>,
    pub join_predicates: Vec<(BoundAttribute, BoundAttribute)>,
}

#[derive(Copy, Clone, Debug)]
pub enum SelectionOperator {
    Eq,
    LessThan,
    LessThanOrEq,
    GreaterThan,
    GreaterThanOrEq
}

#[derive(Debug, Clone)]
pub struct Selection {
    pub attribute: BoundAttribute,
    pub value: TupleValue,
    pub operator: SelectionOperator
}

#[derive(Debug, Clone)]
pub struct InsertQuery {
    pub table: TableDesc,
    pub values: Vec<Tuple>
}

#[derive(Debug, Clone)]
pub struct CreateTableQuery {
    pub table: TableDesc
}

pub trait Planner {
    fn plan(&self, query: Query) -> Result<PhysicalQueryPlan, PlannerError>;
}

 pub enum PlannerError {
    Other(Box<dyn std::error::Error>)
 }

 impl Debug for PlannerError {
     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
         write!(f, "PlannerError")
     }
 } 

impl Display for PlannerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "PlannerError")
    }
}

impl std::error::Error for PlannerError {}