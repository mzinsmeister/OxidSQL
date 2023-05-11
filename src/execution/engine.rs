use std::{cell::RefCell, rc::Rc};

use crate::{catalog::TableDesc, access::{SlottedPageScan, SlottedPageSegment, tuple::Tuple}, storage::buffer_manager::{BufferManager, self, BufferManagerError}, types::{TupleValueType, TupleValue}};

use super::plan::{PhysicalQueryPlan, PhysicalQueryPlanOperator, BooleanExpression, ArithmeticExpression};


trait ExecutionEngine<B: BufferManager> {
    fn execute(&self, plan: PhysicalQueryPlan, buffer_manager: B) -> Result<(), BufferManagerError>;
}

pub type Register = Rc<RefCell<Option<TupleValue>>>; // Only single threaded execution for now

trait VolcanoStyleEngineOperator {
    fn next(&mut self)-> Result<bool, BufferManagerError> ;
    fn get_output(&self) -> &[Register];
}

struct VolcanoStyleEngineTableScan<B: BufferManager, F: FnMut(&[u8]) -> Option<Tuple>> {
    table_desc: TableDesc,
    scan: SlottedPageScan<B, Tuple, F>,
    registers: Vec<Register>
}

// This is super ugly but there's no way to implement this inside the impl blocks
// Because otherwise you would have to provide the F parameter for calling the function
fn new_scan<'a, B: BufferManager>(sp_segment: SlottedPageSegment<B>, table_desc: TableDesc) -> VolcanoStyleEngineTableScan<B, impl FnMut(&[u8]) -> Option<Tuple>> {
    let attribute_types: Vec<TupleValueType> = table_desc.attributes.iter().map(|a| a.data_type).collect();
    let mut registers = Vec::with_capacity(table_desc.attributes.len());
    for _ in 0..table_desc.attributes.len() {
        registers.push(Rc::new(RefCell::new(None)));
    }
    VolcanoStyleEngineTableScan { 
        registers,
        table_desc,
        scan: sp_segment.scan(Box::new(move |raw: &[u8]| {
            let tuple = Tuple::parse_binary(&attribute_types, raw);
            Some(tuple)
        }))
    }
}

impl<'a, B: BufferManager, F: FnMut(&[u8]) -> Option<Tuple>> VolcanoStyleEngineOperator for VolcanoStyleEngineTableScan<B, F> {
    fn next(&mut self) -> Result<bool, BufferManagerError> {
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

pub enum VolcanoStyleArithmeticExpression {
    Value(Register)
}

impl VolcanoStyleArithmeticExpression {
    fn evaluate(&self) -> Option<TupleValue> {
        match self {
            VolcanoStyleArithmeticExpression::Value(v) => v.borrow().to_owned(),
        }
    }
}

pub enum VolcanoStyleEngineBooleanExpression {
    And(Box<VolcanoStyleEngineBooleanExpression>, Box<VolcanoStyleEngineBooleanExpression>),
    //Or(Box<VolcanoStyleEngineBooleanExpression>, Box<VolcanoStyleEngineBooleanExpression>),
    Eq(VolcanoStyleArithmeticExpression, VolcanoStyleArithmeticExpression)
}

impl VolcanoStyleEngineBooleanExpression {
    fn evaluate(&self) -> bool {
        match self {
            VolcanoStyleEngineBooleanExpression::And
                (left, right) => {
                    left.evaluate() && right.evaluate()
                },
            VolcanoStyleEngineBooleanExpression::Eq
                (left, right) => {
                    left.evaluate() == right.evaluate()
                },
        }
    }
}

struct VolcanoStyleEngineSelection {
    child: Box<dyn VolcanoStyleEngineOperator>,
    predicate: VolcanoStyleEngineBooleanExpression
}

impl VolcanoStyleEngineSelection {
    fn new(child: Box<dyn VolcanoStyleEngineOperator>, predicate: VolcanoStyleEngineBooleanExpression) -> Self {
        VolcanoStyleEngineSelection {
            child,
            predicate
        }
    }
}

impl VolcanoStyleEngineOperator for VolcanoStyleEngineSelection {
    fn next(&mut self) -> Result<bool, BufferManagerError> {
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

struct VolcanoStyleEnginePrint<W: FnMut(&str)> {
    header_printed: bool,
    attribute_names: Vec<String>,
    child: Box<dyn VolcanoStyleEngineOperator>,
    writer: W
}

impl<W: FnMut(&str)> VolcanoStyleEnginePrint<W> {
    fn new(child: Box<dyn VolcanoStyleEngineOperator>, attribute_names: Vec<String>, writer: W) -> Self {
        VolcanoStyleEnginePrint {
            header_printed: false,
            attribute_names,
            child,
            writer
        }
    }
}

impl<W: FnMut(&str)> VolcanoStyleEngineOperator for VolcanoStyleEnginePrint<W> {
    fn next(&mut self) -> Result<bool, BufferManagerError> {
        if !self.header_printed {
            self.header_printed = true;
            (self.writer)(&self.attribute_names.join(" | "));
        }
        if self.child.next()? {
            let elems = self.child.get_output().iter()
                .map(|register| format!("{:?}", register.borrow().as_ref()))
                .collect::<Vec<String>>();
            (self.writer)(&elems.join(" | "));
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn get_output(&self) -> &[Register] {
        self.child.get_output()
    }
}


struct VolcanoStyleEngine;

impl VolcanoStyleEngine {
    fn new() -> Self {
        VolcanoStyleEngine
    }

    fn convert_predicate_to_volcano_style(op: BooleanExpression, input_registers: &[Register]) -> VolcanoStyleEngineBooleanExpression {
        match op {
            BooleanExpression::And(left, right) => {
                let left_conv = VolcanoStyleEngine::convert_predicate_to_volcano_style(*left, input_registers);
                let right_conv = VolcanoStyleEngine::convert_predicate_to_volcano_style(*right, input_registers);
                VolcanoStyleEngineBooleanExpression::And(Box::new(left_conv), Box::new(right_conv))
            },
            BooleanExpression::Eq(left, right) => {
                let left_conv = VolcanoStyleEngine::convert_arith_expression_to_volcano_style(left, input_registers);
                let right_conv = VolcanoStyleEngine::convert_arith_expression_to_volcano_style(right, input_registers);
                VolcanoStyleEngineBooleanExpression::Eq(left_conv, right_conv)
            },
        }
    }

    fn convert_arith_expression_to_volcano_style(expression: ArithmeticExpression, input_registers: &[Register]) -> VolcanoStyleArithmeticExpression {
        match expression {
            ArithmeticExpression::Column(c) => VolcanoStyleArithmeticExpression::Value(input_registers[c].clone()),
            ArithmeticExpression::Literal(value) => VolcanoStyleArithmeticExpression::Value(Rc::new(RefCell::new(Some(value)))),
        }
    }

    fn convert_physical_plan_to_volcano_plan<B: BufferManager + 'static>(buffer_manager: B, operator: PhysicalQueryPlanOperator) -> Box<dyn VolcanoStyleEngineOperator> {
        match operator {
            PhysicalQueryPlanOperator::Tablescan { table } => {
                let segment = SlottedPageSegment::new(buffer_manager, table.segment_id, table.segment_id + 1);
                Box::new(new_scan(segment, table))
            },
            PhysicalQueryPlanOperator::Print { input, attribute_names, writeln } => {
                let child = Self::convert_physical_plan_to_volcano_plan(buffer_manager, *input);
                Box::new(VolcanoStyleEnginePrint::new(child, attribute_names, writeln))
            },
            PhysicalQueryPlanOperator::Selection { predicate, input } => {
                let child = Self::convert_physical_plan_to_volcano_plan(buffer_manager, *input);
                let predicate = Self::convert_predicate_to_volcano_style(predicate, child.get_output());
                Box::new(VolcanoStyleEngineSelection::new(child, predicate))
            },
            _ => unimplemented!()
        }
    }
}

impl<B: BufferManager> ExecutionEngine<B> for VolcanoStyleEngine {
    fn execute(&self, plan: PhysicalQueryPlan, buffer_manager: B) -> Result<(), BufferManagerError> {
        let mut volcano_plan = Self::convert_physical_plan_to_volcano_plan(buffer_manager, plan.root_operator);
        while volcano_plan.next()? {
            // Do nothing
        }
        Ok(())
    }
}

mod mock {
    use std::collections::VecDeque;

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

    impl VolcanoStyleEngineOperator for MockVolcanoSourceOperator {
        fn next(&mut self) -> Result<bool, BufferManagerError> {
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

    use crate::{storage::{buffer_manager::mock::MockBufferManager, page::PAGE_SIZE}, access::{SlottedPageSegment, tuple::Tuple}, types::{TupleValue, TupleValueType}, execution::{plan::{PhysicalQueryPlan, PhysicalQueryPlanOperator}, engine::VolcanoStyleArithmeticExpression}, catalog::{AttributeDesc, TableDesc}};

    use super::{ExecutionEngine, VolcanoStyleEngineOperator, VolcanoStyleEnginePrint, mock::MockVolcanoSourceOperator, new_scan, VolcanoStyleEngineSelection}; 

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
            segment_id: 0
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
        let predicate = super::VolcanoStyleEngineBooleanExpression::Eq(
            VolcanoStyleArithmeticExpression::Value(mock_source.get_output()[0].clone()), 
            VolcanoStyleArithmeticExpression::Value(Rc::new(RefCell::new(Some(TupleValue::Int(2))))), 
        );
        let mut operator = VolcanoStyleEngineSelection::new(Box::new(mock_source), predicate);
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
        let predicate = super::VolcanoStyleEngineBooleanExpression::Eq(
            VolcanoStyleArithmeticExpression::Value(Rc::new(RefCell::new(Some(TupleValue::Int(1))))), 
            VolcanoStyleArithmeticExpression::Value(Rc::new(RefCell::new(Some(TupleValue::Int(2))))), 
        );
        let mut operator = VolcanoStyleEngineSelection::new(Box::new(mock_source), predicate);
        assert!(!operator.next().unwrap());
    }

    #[test]
    fn test_print() {
        let tuples = vec![
            Tuple::new(vec![Some(TupleValue::Int(1)), Some(TupleValue::String("a".to_string()))]),
            Tuple::new(vec![Some(TupleValue::Int(2)), Some(TupleValue::String("b".to_string()))]),
            Tuple::new(vec![Some(TupleValue::Int(3)), Some(TupleValue::String("c".to_string()))]),
        ];
        let mut lines: Vec<String> = Vec::new();
        let mock_source = MockVolcanoSourceOperator::new(tuples.into());
        let mut operator = VolcanoStyleEnginePrint::new(Box::new(mock_source), vec!["a".to_string(), "b".to_string()], Box::new(|s: &str| lines.push(s.to_string())));
        while operator.next().unwrap() {
            // Do nothing
        }
        assert_eq!(lines.join("\n"), concat!("a | b\n",
                                        "Some(Int(1)) | Some(String(\"a\"))\n",
                                        "Some(Int(2)) | Some(String(\"b\"))\n",
                                        "Some(Int(3)) | Some(String(\"c\"))"));
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
        let root_operator = PhysicalQueryPlan::new(
            PhysicalQueryPlanOperator::Print {
                input: Box::new(
                    PhysicalQueryPlanOperator::Tablescan {
                        table: get_testtable_desc()
                    },
                ),
                attribute_names: vec![String::from("a"), String::from("b")],
                writeln: Box::new(move |s: &str| lines2.borrow_mut().push(s.to_string())),
            },
            200.0
        );
        let engine = super::VolcanoStyleEngine::new();
        engine.execute(root_operator, buffer_manager).unwrap();
        assert_eq!(lines.borrow().join("\n"), concat!("a | b\n",
                                            "Some(Int(1)) | Some(String(\"a\"))\n",
                                            "Some(Int(2)) | Some(String(\"b\"))\n",
                                            "Some(Int(3)) | Some(String(\"c\"))"));
    }
}
