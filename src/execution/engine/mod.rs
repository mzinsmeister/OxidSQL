use std::error::Error;

use crate::storage::buffer_manager::BufferManager;

use super::plan::PhysicalQueryPlan;

pub mod volcano_style;


pub trait ExecutionEngine<B: BufferManager> {
    fn execute(&self, plan: PhysicalQueryPlan, buffer_manager: B) -> Result<(), Box<dyn Error>>;
}
