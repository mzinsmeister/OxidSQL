
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

use std::fmt::Debug;

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

#[derive(Clone, Debug)]
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
        tuple_writer: Box<dyn TupleWriter>,
    }
 }

pub trait TupleWriter: Debug {
    fn write_tuple(&mut self, tuple: Vec<Option<TupleValue>>);
    fn clone_box(&self) -> Box<dyn TupleWriter>;
}

impl Clone for Box<dyn TupleWriter> {
    fn clone(&self) -> Self {
        self.as_ref().clone_box()
    }
}

#[derive(Clone, Debug)]
pub struct StdOutTupleWriter {
    attribute_names: Vec<String>,
    header_written: bool,
}

impl StdOutTupleWriter {
    pub fn new(attribute_names: Vec<String>) -> Self {
        Self {
            attribute_names,
            header_written: false,
        }
    }

    fn format_tuple_value(value: &Option<TupleValue>) -> String {
        match value {
            Some(v) => format!("{}", v),
            None => "[null]".to_string(),
        }
    }
}

impl TupleWriter for StdOutTupleWriter {
    fn write_tuple(&mut self, tuple: Vec<Option<TupleValue>>) {
        if !self.header_written {
            println!("{}", self.attribute_names.join(" | "));
            self.header_written = true;
        }
        let line = tuple.iter()
            .map(|v| Self::format_tuple_value(v))
            .collect::<Vec<String>>().join(" | ");
        println!("{}", line);
    }

    fn clone_box(&self) -> Box<dyn TupleWriter> {
        Box::new(self.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BooleanExpression {
    And(Box<BooleanExpression>, Box<BooleanExpression>),
    //Or(Box<BooleanExpression>, Box<BooleanExpression>),
    //Not(Box<BooleanExpression>),
    Eq(ArithmeticExpression, ArithmeticExpression)
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

 #[cfg(test)]
 pub mod mock {
    use std::{cell::RefCell, rc::Rc};

    use crate::{types::TupleValue, access::tuple::Tuple};

    use super::TupleWriter;


    #[derive(Clone, Debug)]
    pub struct MockTupleWriter {
        pub tuples: Rc<RefCell<Vec<Tuple>>>,
    }

    impl MockTupleWriter {
        pub fn new() -> Self {
            Self {
                tuples: Rc::new(RefCell::new(Vec::new())),
            }
        }
    }

    impl TupleWriter for MockTupleWriter {
        fn write_tuple(&mut self, tuple: Vec<Option<TupleValue>>) {
            self.tuples.borrow_mut().push(Tuple::new(tuple.iter().map(|v| v.clone()).collect()));
        }

        fn clone_box(&self) -> Box<dyn TupleWriter> {
            Box::new(self.clone())
        }
    }
 }