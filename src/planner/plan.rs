

type TableRef = usize; // Placeholder
type IURef = usize; // Placeholder

// This is for now our minimal set of operators.
// We will add more operators later when the storage layer supports e.g. indices.
// The Operators won't be able to execute themselves. They will instead be executed by
// the execution engine. The execution engine will actually implement the operators
// and can do so however it wants. It can e.g. compile the operators to native code
// or it can interpret them. It can also choose to use a push or pull model and it can
// choose to use a tuple-at-a-time or a vector-at-a-time model. The plan consisting
// of PhysicalPlanOperators will just be a contract/api for the execution engine.
pub enum PhysicalPlanOperator {
    Tablescan {
        table: TableRef,
    },
    Selection {
        predicate: Expression,
        input: Box<PhysicalPlan>,
    },
    Projection {
        projection_ius: Vec<IURef>,
        input: Box<PhysicalPlan>,
    },
    // For now we will only support equi-joins. Those are usually implemented most efficiently
    // by hash joins nowerdays. A (Blockwise) nested loop join will probably be added in the future.
    // It would be a goal long term to be able to live without a normal nested loop join because
    // technically speaking you can unnest all nested subqueries therefore eliminating the need
    // for nested loop joins (https://cs.emis.de/LNI/Proceedings/Proceedings241/383.pdf). 
    // This is probably quite some work to implement though.
    HashJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        on: Vec<(IURef, IURef)>, // Pairs of left = right equi-join predicates
    }
}

pub enum Expression {
    Column(IURef),
    Literal(TupleValue),
    BinaryOp {
        op: BinaryOperator,
        left: Box<Expression>,
        right: Box<Expression>,
    },
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expression>,
    },
}

pub enum BinaryOperator {
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    /*Neq,
    Lt,
    Lte,
    Gt,
    Gte,*/
    And,
    // Or, // OR is a PITA. We're saving this fun for later...
}

pub enum UnaryOperator {
    Not,
}

pub struct PhysicalPlan {
    pub root_operator: PhysicalPlanOperator,
    pub cost: f64
}

pub enum PlannerError {}

pub trait Planner {
    fn plan(&self, query: &Query) -> Result<PhysicalPlan, PlannerError>;
}

// Default Planner using the DPccp algorithm for optimization
pub type DefaultPlanner = DPccpPlanner;

pub struct DPccpPlanner {}

impl Planner for DPccpPlanner {
    fn plan(&self, query: &Query) -> Result<PhysicalPlan, PlannerError> {
        
    }
}
