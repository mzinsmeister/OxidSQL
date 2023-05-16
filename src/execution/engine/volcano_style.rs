use std::{cell::RefCell, rc::Rc, collections::HashMap, error::Error};

use crate::{catalog::TableDesc, access::{SlottedPageScan, SlottedPageSegment, tuple::Tuple}, types::{TupleValueType, TupleValue}, execution::plan::{self, PhysicalQueryPlanOperator, PhysicalQueryPlan, TupleWriter}, storage::buffer_manager::BufferManager};

use super::{ExecutionEngine};

pub type Register = Rc<RefCell<Option<TupleValue>>>; // Only single threaded execution for now

trait Operator {
    fn next(&mut self)-> Result<bool, Box<dyn Error>> ;
    fn get_output(&self) -> &[Register];
}

struct TableScan<B: BufferManager, F: FnMut(&[u8]) -> Option<Tuple>> {
    table_desc: TableDesc,
    scan: SlottedPageScan<B, Tuple, F>,
    registers: Vec<Register>
}

// This is super ugly but there's no way to implement this inside the impl blocks
// Because otherwise you would have to provide the F parameter for calling the function
fn new_scan<'a, B: BufferManager>(sp_segment: SlottedPageSegment<B>, table_desc: TableDesc) -> TableScan<B, impl FnMut(&[u8]) -> Option<Tuple>> {
    let attribute_types: Vec<TupleValueType> = table_desc.attributes.iter().map(|a| a.data_type).collect();
    let mut registers = Vec::with_capacity(table_desc.attributes.len());
    for _ in 0..table_desc.attributes.len() {
        registers.push(Rc::new(RefCell::new(None)));
    }
    TableScan { 
        registers,
        table_desc,
        scan: sp_segment.scan(Box::new(move |raw: &[u8]| {
            let tuple = Tuple::parse_binary(&attribute_types, raw);
            Some(tuple)
        }))
    }
}

impl<'a, B: BufferManager, F: FnMut(&[u8]) -> Option<Tuple>> Operator for TableScan<B, F> {
    fn next(&mut self) -> Result<bool, Box<dyn Error>> {
        if let Some(tuple) = self.scan.next() {
            let (_, mut tuple) = tuple?;
            for i in 0..self.table_desc.attributes.len() {
                self.registers[i].replace(tuple.values[i].take());
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn get_output(&self) -> &[Register] {
        &self.registers
    }
}

pub enum ArithmeticExpression {
    Value(Register)
}

impl ArithmeticExpression {
    fn evaluate(&self) -> Option<TupleValue> {
        match self {
            ArithmeticExpression::Value(v) => v.borrow().to_owned(),
        }
    }
}

pub enum BooleanExpression {
    And(Box<BooleanExpression>, Box<BooleanExpression>),
    //Or(Box<BooleanExpression>, Box<BooleanExpression>),
    Eq(ArithmeticExpression, ArithmeticExpression)
}

impl BooleanExpression {
    fn evaluate(&self) -> bool {
        match self {
            BooleanExpression::And
                (left, right) => {
                    left.evaluate() && right.evaluate()
                },
            BooleanExpression::Eq
                (left, right) => {
                    left.evaluate() == right.evaluate()
                },
        }
    }
}

struct Selection {
    child: Box<dyn Operator>,
    predicate: BooleanExpression
}

impl Selection {
    fn new(child: Box<dyn Operator>, predicate: BooleanExpression) -> Self {
        Selection {
            child,
            predicate
        }
    }
}

impl Operator for Selection {
    fn next(&mut self) -> Result<bool, Box<dyn Error>> {
        while self.child.next()? {
            if self.predicate.evaluate() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn get_output(&self) -> &[Register] {
        self.child.get_output()
    }
}

struct Projection {
    child: Box<dyn Operator>,
    output_registers: Vec<Register>,
}

impl Projection {
    fn new(child: Box<dyn Operator>, projection_ius: Vec<usize>) -> Self {
        let mut output_registers = Vec::with_capacity(projection_ius.len());
        for i in projection_ius {
            output_registers.push(child.get_output()[i].clone());
        }
        Projection {
            child,
            output_registers
        }
    }
}

impl Operator for Projection {
    fn next(&mut self) -> Result<bool, Box<dyn Error>> {
        self.child.next()
    }

    fn get_output(&self) -> &[Register] {
        &self.output_registers
    }
}

struct HashJoin {
    left: Box<dyn Operator>,
    right: Box<dyn Operator>,
    on: Vec<(Register, Register)>, // Left = Right equi-join predicates
    output_registers: Vec<Register>,
    hash_table: Option<HashMap<Vec<TupleValue>, Vec<Tuple>>>
}

impl HashJoin {
    fn new(left: Box<dyn Operator>, right: Box<dyn Operator>, on: Vec<(usize, usize)>) -> Self {
        let mut output_registers = Vec::with_capacity(left.get_output().len() + right.get_output().len());
        for register in left.get_output() {
            output_registers.push(register.clone());
        }
        for register in right.get_output() {
            output_registers.push(register.clone());
        }
        HashJoin {
            on: on.into_iter().map(|(left_i, right_i)| {
                    (left.get_output()[left_i].clone(), 
                    right.get_output()[right_i].clone())
                }).collect(),
            left,
            right,
            output_registers,
            hash_table: None,
        }
    }
}

impl Operator for HashJoin {
    fn next(&mut self)-> Result<bool, Box<dyn Error>>  {
        let ht = if self.hash_table.is_some() {
            self.hash_table.as_ref().unwrap()
        } else {
            // First call to next. Build hash table
            let mut ht = HashMap::new();
            while self.left.next()? {
                let mut key = Vec::with_capacity(self.on.len());
                for (left, _) in &self.on {
                    key.push(left.borrow().to_owned().unwrap());
                }
                let mut tuple = Tuple::new(Vec::with_capacity(self.left.get_output().len()));
                for register in self.left.get_output() {
                    tuple.values.push(register.borrow().to_owned());
                }
                ht.entry(key).or_insert_with(Vec::new).push(tuple);
            }
            self.hash_table = Some(ht);
            self.hash_table.as_ref().unwrap()
        };
        while self.right.next()? {
            let mut key = Vec::with_capacity(self.on.len());
            for (_, right) in &self.on {
                key.push(right.borrow().to_owned().unwrap());
            }
            if let Some(tuples) = ht.get(&key) {
                for tuple in tuples {
                    for i in 0..tuple.values.len() {
                        self.output_registers[i].replace(tuple.values[i].to_owned());
                    }
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    fn get_output(&self) -> &[Register] {
        &self.output_registers
    }
}

struct Print {
    child: Box<dyn Operator>,
    tuple_writer: Box<dyn TupleWriter>
}

impl Print {
    fn new(child: Box<dyn Operator>, tuple_writer: Box<dyn TupleWriter>) -> Self {
        Print {
            child,
            tuple_writer
        }
    }
}

impl Operator for Print {
    fn next(&mut self) -> Result<bool, Box<dyn Error>> {
        if self.child.next()? {
            let elems = self.child.get_output().iter()
                .map(|register| register.borrow().clone())
                .collect::<Vec<Option<TupleValue>>>();
            self.tuple_writer.write_tuple(elems);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn get_output(&self) -> &[Register] {
        self.child.get_output()
    }
}


pub struct Engine;

impl Engine {
    pub fn new() -> Self {
        Engine
    }

    fn convert_predicate_to_volcano_style(op: plan::BooleanExpression, input_registers: &[Register]) -> BooleanExpression {
        match op {
            plan::BooleanExpression::And(left, right) => {
                let left_conv = Engine::convert_predicate_to_volcano_style(*left, input_registers);
                let right_conv = Engine::convert_predicate_to_volcano_style(*right, input_registers);
                BooleanExpression::And(Box::new(left_conv), Box::new(right_conv))
            },
            plan::BooleanExpression::Eq(left, right) => {
                let left_conv = Engine::convert_arith_expression_to_volcano_style(left, input_registers);
                let right_conv = Engine::convert_arith_expression_to_volcano_style(right, input_registers);
                BooleanExpression::Eq(left_conv, right_conv)
            },
        }
    }

    fn convert_arith_expression_to_volcano_style(expression: plan::ArithmeticExpression, input_registers: &[Register]) -> ArithmeticExpression {
        match expression {
            plan::ArithmeticExpression::Column(c) => ArithmeticExpression::Value(input_registers[c].clone()),
            plan::ArithmeticExpression::Literal(value) => ArithmeticExpression::Value(Rc::new(RefCell::new(Some(value)))),
        }
    }

    fn convert_physical_plan_to_volcano_plan<B: BufferManager + 'static>(buffer_manager: B, operator: PhysicalQueryPlanOperator) -> Box<dyn Operator> {
        match operator {
            PhysicalQueryPlanOperator::Tablescan { table } => {
                let segment = SlottedPageSegment::new(buffer_manager, table.segment_id, table.segment_id + 1);
                Box::new(new_scan(segment, table))
            },
            PhysicalQueryPlanOperator::Print { input, tuple_writer } => {
                let child = Self::convert_physical_plan_to_volcano_plan(buffer_manager, *input);
                Box::new(Print::new(child, tuple_writer))
            },
            PhysicalQueryPlanOperator::Selection { predicate, input } => {
                let child = Self::convert_physical_plan_to_volcano_plan(buffer_manager, *input);
                let predicate = Self::convert_predicate_to_volcano_style(predicate, child.get_output());
                Box::new(Selection::new(child, predicate))
            },
            PhysicalQueryPlanOperator::HashJoin { left, right, on } => {
                let left = Self::convert_physical_plan_to_volcano_plan(buffer_manager.clone(), *left);
                let right = Self::convert_physical_plan_to_volcano_plan(buffer_manager, *right);
                Box::new(HashJoin::new(left, right, on))
            },
            PhysicalQueryPlanOperator::Projection { projection_ius, input } => {
                let child = Self::convert_physical_plan_to_volcano_plan(buffer_manager, *input);
                Box::new(Projection::new(child, projection_ius))
            }
        }
    }
}

impl<B: BufferManager> ExecutionEngine<B> for Engine {
    fn execute(&self, plan: PhysicalQueryPlan, buffer_manager: B) -> Result<(), Box<dyn Error>> {
        let mut volcano_plan = Self::convert_physical_plan_to_volcano_plan(buffer_manager, plan.root_operator);
        while volcano_plan.next()? {
            // Do nothing
        }
        Ok(())
    }
}

#[cfg(test)]
mod mock {
    use std::collections::VecDeque;

    use crate::storage::buffer_manager::mock::MockBufferManager;

    use super::*;

    pub struct MockVolcanoSourceOperator {
        output: Vec<Register>,
        tuples: VecDeque<Tuple>,
    }

    impl MockVolcanoSourceOperator {
        pub fn new(tuples: VecDeque<Tuple>) -> Self {
            let mut output = Vec::with_capacity(tuples[0].values.len());
            for _ in 0..tuples[0].values.len() {
                output.push(Rc::new(RefCell::new(None)));
            }
            MockVolcanoSourceOperator { output, tuples }
        }
    }

    impl Operator for MockVolcanoSourceOperator {
        fn next(&mut self) -> Result<bool, Box<dyn Error>> {
            if let Some(mut tuple) = self.tuples.pop_front() {
                for i in 0..self.output.len() {
                    self.output[i].replace(tuple.values[i].take());
                }
                Ok(true)
            } else {
                Ok(false)
            }
        }

        fn get_output(&self) -> &[Register] {
            &self.output
        }
    }
}

#[cfg(test)]
mod test {
    use std::{rc::Rc, cell::RefCell};

    use crate::{storage::{page::PAGE_SIZE, buffer_manager::mock::MockBufferManager}, access::{SlottedPageSegment, tuple::Tuple}, types::{TupleValue, TupleValueType}, execution::{plan::{PhysicalQueryPlan, PhysicalQueryPlanOperator, mock::MockTupleWriter}, engine::volcano_style::{Selection, ArithmeticExpression, Print}}, catalog::{AttributeDesc, TableDesc}};

    use super::{super::ExecutionEngine, Operator, mock::MockVolcanoSourceOperator, new_scan}; 

    fn get_testtable_desc() -> TableDesc {
        TableDesc {
            id: 0,
            name: "TESTTABLE".to_string(),
            attributes: vec![
                AttributeDesc {
                    id: 0,
                    name: "a".to_string(),
                    data_type: TupleValueType::Int,
                    nullable: false,
                    table_ref: 0
                },
                AttributeDesc {
                    id: 1,
                    name: "b".to_string(),
                    data_type: TupleValueType::VarChar(1),
                    nullable: true,
                    table_ref: 0
                }
            ],
            segment_id: 0,
            cardinality: 2,
        }
    }

    #[test]
    fn test_tablescan() {
        let buffer_manager = MockBufferManager::new(PAGE_SIZE);
        let sp_segment = SlottedPageSegment::new(buffer_manager.clone(), 0, 1);
        let tuples = vec![
            Tuple::new(vec![Some(TupleValue::Int(1)), Some(TupleValue::String("a".to_string()))]),
            Tuple::new(vec![Some(TupleValue::Int(2)), Some(TupleValue::String("b".to_string()))]),
            Tuple::new(vec![Some(TupleValue::Int(3)), Some(TupleValue::String("c".to_string()))]),
        ];
        for tuple in &tuples {
            sp_segment.insert_record(&tuple.get_binary()).unwrap();
        }
        let mut operator = new_scan(sp_segment, get_testtable_desc());
        for tuple in tuples {
            assert!(operator.next().unwrap());
            for i in 0..tuple.values.len() {
                assert_eq!(operator.get_output()[i].borrow().as_ref(), tuple.values[i].as_ref());
            }
        }
    }
    #[test]
    // TODO: Test selection way more. Also test predicate evaluation separately
    fn test_basic_eq_selection() {
        let tuples = vec![
            Tuple::new(vec![Some(TupleValue::Int(1)), Some(TupleValue::String("a".to_string()))]),
            Tuple::new(vec![Some(TupleValue::Int(2)), Some(TupleValue::String("b".to_string()))]),
            Tuple::new(vec![Some(TupleValue::Int(3)), Some(TupleValue::String("c".to_string()))]),
        ];
        let mock_source = MockVolcanoSourceOperator::new(tuples.into());
        let predicate = super::BooleanExpression::Eq(
            ArithmeticExpression::Value(mock_source.get_output()[0].clone()), 
            ArithmeticExpression::Value(Rc::new(RefCell::new(Some(TupleValue::Int(2))))), 
        );
        let mut operator = Selection::new(Box::new(mock_source), predicate);
        assert!(operator.next().unwrap());
        assert_eq!((*operator.get_output()[0]).to_owned().into_inner(), Some(TupleValue::Int(2)));
        assert_eq!((*operator.get_output()[1]).to_owned().into_inner(), Some(TupleValue::String("b".to_string())));
        assert!(!operator.next().unwrap());
    }

    #[test]
    fn test_empty_selection() {
        let tuples = vec![
            Tuple::new(vec![Some(TupleValue::Int(1)), Some(TupleValue::String("a".to_string()))]),
            Tuple::new(vec![Some(TupleValue::Int(2)), Some(TupleValue::String("b".to_string()))]),
            Tuple::new(vec![Some(TupleValue::Int(3)), Some(TupleValue::String("c".to_string()))]),
        ];
        let mock_source = MockVolcanoSourceOperator::new(tuples.into());
        let predicate = super::BooleanExpression::Eq(
            ArithmeticExpression::Value(Rc::new(RefCell::new(Some(TupleValue::Int(1))))), 
            ArithmeticExpression::Value(Rc::new(RefCell::new(Some(TupleValue::Int(2))))), 
        );
        let mut operator = Selection::new(Box::new(mock_source), predicate);
        assert!(!operator.next().unwrap());
    }

    #[test]
    fn test_tautology_predicate_selection() {
        let tuples = vec![
            Tuple::new(vec![Some(TupleValue::Int(1)), Some(TupleValue::String("a".to_string()))]),
            Tuple::new(vec![Some(TupleValue::Int(2)), Some(TupleValue::String("b".to_string()))]),
            Tuple::new(vec![Some(TupleValue::Int(3)), Some(TupleValue::String("c".to_string()))]),
        ];
        let mock_source = MockVolcanoSourceOperator::new(tuples.into());
        let predicate = super::BooleanExpression::Eq(
            ArithmeticExpression::Value(Rc::new(RefCell::new(Some(TupleValue::Int(1))))), 
            ArithmeticExpression::Value(Rc::new(RefCell::new(Some(TupleValue::Int(1))))), 
        );
        let mut operator = Selection::new(Box::new(mock_source), predicate);
        assert!(operator.next().unwrap());
        assert_eq!((*operator.get_output()[0]).to_owned().into_inner(), Some(TupleValue::Int(1)));
        assert_eq!((*operator.get_output()[1]).to_owned().into_inner(), Some(TupleValue::String("a".to_string())));
        assert!(operator.next().unwrap());
        assert_eq!((*operator.get_output()[0]).to_owned().into_inner(), Some(TupleValue::Int(2)));
        assert_eq!((*operator.get_output()[1]).to_owned().into_inner(), Some(TupleValue::String("b".to_string())));
        assert!(operator.next().unwrap());
        assert_eq!((*operator.get_output()[0]).to_owned().into_inner(), Some(TupleValue::Int(3)));
        assert_eq!((*operator.get_output()[1]).to_owned().into_inner(), Some(TupleValue::String("c".to_string())));
        assert!(!operator.next().unwrap());
    }

    #[test]
    fn test_and_predicate_selection() {
        let tuples = vec![
            Tuple::new(vec![Some(TupleValue::Int(1)), Some(TupleValue::String("a".to_string()))]),
            Tuple::new(vec![Some(TupleValue::Int(2)), Some(TupleValue::String("b".to_string()))]),
            Tuple::new(vec![Some(TupleValue::Int(3)), Some(TupleValue::String("c".to_string()))]),
        ];
        let mock_source = MockVolcanoSourceOperator::new(tuples.into());
        let predicate1 = super::BooleanExpression::Eq(
            ArithmeticExpression::Value(mock_source.get_output()[0].clone()), 
            ArithmeticExpression::Value(Rc::new(RefCell::new(Some(TupleValue::Int(2))))), 
        );
        let predicate2 = super::BooleanExpression::Eq(
            ArithmeticExpression::Value(mock_source.get_output()[1].clone()), 
            ArithmeticExpression::Value(Rc::new(RefCell::new(Some(TupleValue::String("b".to_string()))))), 
        );
        let predicate = super::BooleanExpression::And(Box::new(predicate1), Box::new(predicate2));
        let mut operator = Selection::new(Box::new(mock_source), predicate);
        assert!(operator.next().unwrap());
        assert_eq!((*operator.get_output()[0]).to_owned().into_inner(), Some(TupleValue::Int(2)));
        assert_eq!((*operator.get_output()[1]).to_owned().into_inner(), Some(TupleValue::String("b".to_string())));
        assert!(!operator.next().unwrap());
    }

    #[test]
    fn test_projection() {
        let mock_source = MockVolcanoSourceOperator::new(vec![
            Tuple::new(vec![Some(TupleValue::Int(1)), Some(TupleValue::String("a".to_string()))]),
        ].into());
        let mut operator = super::Projection::new(Box::new(mock_source), vec![0]);
        assert_eq!(operator.get_output().len(), 1);
        assert!(operator.next().unwrap());
        assert_eq!(operator.get_output()[0].borrow().as_ref(), Some(&TupleValue::Int(1)));
        assert!(!operator.next().unwrap());
    }

    #[test]
    fn test_print() {
        let tuples = vec![
            Tuple::new(vec![Some(TupleValue::Int(1)), Some(TupleValue::String("a".to_string()))]),
            Tuple::new(vec![Some(TupleValue::Int(2)), Some(TupleValue::String("b".to_string()))]),
            Tuple::new(vec![Some(TupleValue::Int(3)), Some(TupleValue::String("c".to_string()))]),
        ];
        let mock_source = MockVolcanoSourceOperator::new(tuples.clone().into());
        let tuple_writer = MockTupleWriter::new();
        let mut operator = Print::new(Box::new(mock_source), Box::new(tuple_writer.clone()));
        while operator.next().unwrap() {
            // Do nothing
        }
        assert_eq!(tuple_writer.tuples.borrow().clone(), tuples);
    }

    #[test]
    fn test_hash_join() {
        let tuples1 = vec![
            Tuple::new(vec![Some(TupleValue::Int(1)), Some(TupleValue::String("a".to_string()))]),
            Tuple::new(vec![Some(TupleValue::Int(2)), Some(TupleValue::String("b".to_string()))]),
            Tuple::new(vec![Some(TupleValue::Int(3)), Some(TupleValue::String("c".to_string()))]),
        ];
        let mock_source1 = MockVolcanoSourceOperator::new(tuples1.into());

        let tuples2 = vec![
            Tuple::new(vec![Some(TupleValue::String("aa".to_string())), Some(TupleValue::Int(3))]),
            Tuple::new(vec![Some(TupleValue::String("bb".to_string())), Some(TupleValue::Int(2))]),
            Tuple::new(vec![Some(TupleValue::String("cc".to_string())), Some(TupleValue::Int(33))]),
        ];
        let mock_source2 = MockVolcanoSourceOperator::new(tuples2.into());

        let mut operator = super::HashJoin::new(Box::new(mock_source1), Box::new(mock_source2), vec![(0, 1)]);
        assert!(operator.next().unwrap());
        assert_eq!((*operator.get_output()[0]).to_owned().into_inner(), Some(TupleValue::Int(3)));
        assert_eq!((*operator.get_output()[1]).to_owned().into_inner(), Some(TupleValue::String("c".to_string())));
        assert_eq!((*operator.get_output()[2]).to_owned().into_inner(), Some(TupleValue::String("aa".to_string())));
        assert_eq!((*operator.get_output()[3]).to_owned().into_inner(), Some(TupleValue::Int(3)));
        assert!(operator.next().unwrap());
        assert_eq!((*operator.get_output()[0]).to_owned().into_inner(), Some(TupleValue::Int(2)));
        assert_eq!((*operator.get_output()[1]).to_owned().into_inner(), Some(TupleValue::String("b".to_string())));
        assert_eq!((*operator.get_output()[2]).to_owned().into_inner(), Some(TupleValue::String("bb".to_string())));
        assert_eq!((*operator.get_output()[3]).to_owned().into_inner(), Some(TupleValue::Int(2)));
        assert!(!operator.next().unwrap());
    }

    #[test]
    fn test_tablescan_print() {
        let buffer_manager = MockBufferManager::new(PAGE_SIZE);
        let sp_segment = SlottedPageSegment::new(buffer_manager.clone(), 0, 1);
        let tuples = vec![
            Tuple::new(vec![Some(TupleValue::Int(1)), Some(TupleValue::String("a".to_string()))]),
            Tuple::new(vec![Some(TupleValue::Int(2)), Some(TupleValue::String("b".to_string()))]),
            Tuple::new(vec![Some(TupleValue::Int(3)), Some(TupleValue::String("c".to_string()))]),
        ];
        for tuple in &tuples {
            sp_segment.insert_record(&tuple.get_binary()).unwrap();
        }
        let lines: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(Vec::new()));
        let lines2 = lines.clone();
        let tuple_writer = MockTupleWriter::new();
        let root_operator = PhysicalQueryPlan::new(
            PhysicalQueryPlanOperator::Print {
                input: Box::new(
                    PhysicalQueryPlanOperator::Tablescan {
                        table: get_testtable_desc()
                    },
                ),
                tuple_writer: Box::new(tuple_writer.clone())
            },
            200.0
        );
        let engine = super::Engine::new();
        engine.execute(root_operator, buffer_manager).unwrap();
        assert_eq!(tuple_writer.tuples.borrow().clone(), tuples);
    }
}