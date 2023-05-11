
/*
    Here are the data structures needed to construct query execution plans (physical plans).
    The plan is constructed by the planner module and then executed by the execution module.
    The plan is a tree of operators. Ideally it should be possible to implement different
    execution engines that can all execute the same plan. The only thing that we might need
    to consider later is that some execution engines might be able to execute DAG structured plans.
    This would probably require a rearchitecture of the plan data structures or two different ones
    for DAGs and trees respectively. The planner would then have something like a plan-tree and a 
    plan-dag function.
 */

use crate::{types::TupleValue, catalog::TableDesc};

pub type IURef = usize; // index of child operator's output

 // This is for now our minimal set of operators.
 // We will add more operators later when the storage layer supports e.g. indices.
 // The Operators won't be able to execute themselves. They will instead be executed by
 // the execution engine. The execution engine will actually implement the operators
 // and can do so however it wants. It can e.g. compile the operators to native code
 // or it can interpret them. It can also choose to use a push or pull model and it can
 // choose to use a tuple-at-a-time or a vector-at-a-time model. The plan consisting
 // of PhysicalQueryPlanOperators will just be a contract/api for the execution engine.

 pub enum PhysicalQueryPlanOperator {
     Tablescan {
         table: TableDesc,
     },
     Selection {
         predicate: BooleanExpression,
         input: Box<PhysicalQueryPlanOperator>,
     },
     Projection {
         projection_ius: Vec<IURef>,
         input: Box<PhysicalQueryPlanOperator>,
     },
     // For now we will only support equi-joins. Those are usually implemented most efficiently
     // by hash joins nowerdays. A (Blockwise) nested loop join will probably be added in the future.
     // It would be a goal long term to be able to live without a normal nested loop join because
     // technically speaking you can unnest all nested subqueries therefore eliminating the need
     // for nested loop joins (https://cs.emis.de/LNI/Proceedings/Proceedings241/383.pdf). 
     // This is probably quite some work to implement though.
     HashJoin {
         left: Box<PhysicalQueryPlanOperator>,
         right: Box<PhysicalQueryPlanOperator>,
         on: Vec<(IURef, IURef)>, // Pairs of left = right equi-join predicates
     },
     // Dummy output operator. Something like "call this callback" or "send back over this socket"
     // would be added later
     Print {
        input: Box<PhysicalQueryPlanOperator>,
        attribute_names: Vec<String>,
        writeln: Box<dyn FnMut(&str)>,
    }
 }

pub enum BooleanExpression {
    And(Box<BooleanExpression>, Box<BooleanExpression>),
    //Or(Box<BooleanExpression>, Box<BooleanExpression>),
    //Not(Box<BooleanExpression>),
    Eq(ArithmeticExpression, ArithmeticExpression)
}

pub enum ArithmeticExpression{
    Column(IURef),
    Literal(TupleValue),
    // Add, Sub, Mul, ...
}
 
 pub struct PhysicalQueryPlan {
     pub root_operator: PhysicalQueryPlanOperator,
     pub cost: f64
 }

 impl PhysicalQueryPlan {
        pub fn new(root_operator: PhysicalQueryPlanOperator, cost: f64) -> Self {
            Self { root_operator, cost }
        }
 }