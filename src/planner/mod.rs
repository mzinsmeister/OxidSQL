/*
    TODO: Implement a query planner that can take a parsed and analyzed query and produce a plan
          by applying rewrite rules and possibly using optimization techniques implemented in
          the optimizer module.
 */


 pub enum PlannerError {}
 
 /*
 pub trait Planner {
     fn plan(&self, query: &Query) -> Result<PhysicalPlan, PlannerError>;
 }
 
 // Default Planner using the DPccp algorithm for optimization
 pub type DefaultPlanner = DPccpPlanner;
 
 pub struct DPccpPlanner {}
 
 impl Planner for DPccpPlanner {
     fn plan(&self, query: &Query) -> Result<PhysicalPlan, PlannerError> {
         unimplemented!()
     }
 }*/