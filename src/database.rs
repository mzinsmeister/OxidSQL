
// This is just a hacky implementation throwing all the parts i implemented together to
// Get an interactive SQL shell. It's currently just a proof of concept and will be replaced
// by some proper interface, likely through the network.

use std::{sync::Arc, path::PathBuf};

use crate::{catalog::{Catalog, AttributeDesc, TableDesc}, storage::{buffer_manager::{HashTableBufferManager, BufferManagerError}, clock_replacer::ClockReplacer, disk::DiskManager}, planner::{bottomup::BottomUpPlanner, Planner}, execution::engine::{volcano_style, ExecutionEngine}, analyzer::Analyzer, parser, types::{TupleValueType, TupleValue}, access::{SlottedPageSegment, tuple::Tuple, StatisticsCollectingSPHeapStorage, HeapStorage}, config::DbConfig};

type BufferManagerType = Arc<HashTableBufferManager<ClockReplacer, DiskManager>>;
pub struct OxidSQLDatabase {
    catalog: Catalog<BufferManagerType>,
    buffer_manager: BufferManagerType,
    planner: BottomUpPlanner<BufferManagerType>,
    executor: volcano_style::Engine<BufferManagerType>,
    analyzer: Analyzer<BufferManagerType>
}

impl OxidSQLDatabase {
    pub fn new(data_path: PathBuf, buffer_size: usize) -> Result<OxidSQLDatabase, BufferManagerError> {
        let disk_manager = DiskManager::new(data_path);
        let buffer_manager = HashTableBufferManager::new(disk_manager, ClockReplacer::new(), buffer_size);
        let catalog = Catalog::new(buffer_manager.clone(), Arc::new(DbConfig::new()))?;
        let analyzer = Analyzer::new(catalog.clone());
        let planner = BottomUpPlanner::new(buffer_manager.clone(), catalog.clone());
        let executor = volcano_style::Engine::new(catalog.clone());
        Ok(OxidSQLDatabase {
            catalog,
            buffer_manager,
            planner,
            executor,
            analyzer
        })
    }

    pub fn query<'a>(&self, query: &'a str) -> Result<(), Box<dyn std::error::Error + 'a>> {
        let (rest_query, parse_tree) = parser::parse_query(query)?;
        if rest_query.trim().len() > 0 {
            return Err(format!("Query was not parsed fully. Trailing suffix: '{}'", rest_query).into());
        }
        let analyzed_query = self.analyzer.analyze(parse_tree)?;
        let plan = self.planner.plan(analyzed_query)?;
        self.executor.execute(plan, self.buffer_manager.clone())?;
        Ok(())
    }

    pub fn demo_init(&self) {
        self.query("CREATE TABLE people (id INT, name VARCHAR(255), age INT)").unwrap();
        self.query("CREATE TABLE cars (id INT, model VARCHAR(255), owner_id INT)").unwrap();
        // Init demo data
        let people = self.catalog.find_table_by_name("people").unwrap().unwrap();
        let cars = self.catalog.find_table_by_name("cars").unwrap().unwrap();
        let people_heap = StatisticsCollectingSPHeapStorage::new(&people, self.buffer_manager.clone(), self.catalog.clone(), 1024).unwrap();
        let cars_heap = StatisticsCollectingSPHeapStorage::new(&cars, self.buffer_manager.clone(), self.catalog.clone(), 1024).unwrap();

        let mut people_tuples = vec![
            Tuple::new(vec![
                Some(TupleValue::Int(1)),
                Some(TupleValue::String("Elon".to_string())),
                Some(TupleValue::Int(20))
            ]),
            Tuple::new(vec![
                Some(TupleValue::Int(2)),
                Some(TupleValue::String("Dr. Emmett L. „Doc“ Brown".to_string())),
                Some(TupleValue::Int(30))
            ]),
            Tuple::new(vec![
                Some(TupleValue::Int(3)),
                Some(TupleValue::String("Marty McFly".to_string())),
                None
            ])
        ];
        let mut cars_tuples = vec![
            Tuple::new(vec![
                Some(TupleValue::Int(1)),
                Some(TupleValue::String("Tesla Model 3".to_string())),
                Some(TupleValue::Int(1))
            ]),
            Tuple::new(vec![
                Some(TupleValue::Int(2)),
                Some(TupleValue::String("DeLorean DMC-12".to_string())),
                Some(TupleValue::Int(2))
            ])
        ];

        people_heap.insert_tuples(&mut people_tuples).unwrap();

        cars_heap.insert_tuples(&mut cars_tuples).unwrap();

        self.buffer_manager.flush().unwrap();
    }
}