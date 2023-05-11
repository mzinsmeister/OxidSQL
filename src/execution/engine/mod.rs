use crate::storage::buffer_manager::{BufferManager, BufferManagerError};

use super::plan::PhysicalQueryPlan;

pub mod volcano_style;


trait ExecutionEngine<B: BufferManager> {
    fn execute(&self, plan: PhysicalQueryPlan, buffer_manager: B) -> Result<(), BufferManagerError>;
}
