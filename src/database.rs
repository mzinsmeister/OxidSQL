
// This is just a hacky implementation throwing all the parts i implemented together to
// Get an interactive SQL shell. It's currently just a proof of concept and will be replaced
// by some proper interface, likely through the network.

use std::{sync::Arc, path::PathBuf};

use crate::{catalog::{Catalog, AttributeDesc, TableDesc}, storage::{buffer_manager::HashTableBufferManager, clock_replacer::ClockReplacer, disk::DiskManager}, planner::{bottomup::BottomUpPlanner, Planner}, execution::engine::{volcano_style, ExecutionEngine}, analyzer::Analyzer, parser, types::{TupleValueType, TupleValue}, access::{SlottedPageSegment, tuple::Tuple}};

type BufferManagerType = Arc<HashTableBufferManager<ClockReplacer, DiskManager>>;
pub struct OxidSQLDatabase {
    catalog: Catalog<BufferManagerType>,
    buffer_manager: BufferManagerType,
    planner: BottomUpPlanner,
    executor: volcano_style::Engine,
    analyzer: Analyzer<BufferManagerType>
}

impl OxidSQLDatabase {
    pub fn new(data_path: PathBuf, buffer_size: usize) -> OxidSQLDatabase {
        let disk_manager = DiskManager::new(data_path);
        let buffer_manager = HashTableBufferManager::new(disk_manager, ClockReplacer::new(), buffer_size);
        let replacer = ClockReplacer::new();
        let catalog = Catalog::new(buffer_manager.clone());
        let analyzer = Analyzer::new(catalog.clone());
        let planner = BottomUpPlanner::new();
        let executor = volcano_style::Engine::new();
        OxidSQLDatabase {
            catalog,
            buffer_manager,
            replacer,
            planner,
            executor,
            analyzer
        }
    }

    pub fn query<'a>(&self, query: &'a str) -> Result<(), Box<dyn std::error::Error + 'a>> {
        let (_, parse_tree) = parser::parse_query(query)?;
        let analyzed_query = self.analyzer.analyze(parse_tree)?;
        let plan = self.planner.plan(&analyzed_query)?;
        self.executor.execute(plan, self.buffer_manager.clone())?;
        Ok(())
    }

    pub fn demo_init(&self) {
        self.catalog.create_table(TableDesc {
            id: 1,
            segment_id: 1024,
            name: "people".to_string(),
            cardinality: 3,
            attributes: vec![
                AttributeDesc {
                    id: 1,
                    name: "id".to_string(),
                    data_type: TupleValueType::Int,
                    nullable: false,
                    table_ref: 1
                },
                AttributeDesc {
                    id: 2,
                    name: "name".to_string(),
                    data_type: TupleValueType::VarChar(255),
                    nullable: false,
                    table_ref: 1
                },
                AttributeDesc {
                    id: 3,
                    name: "age".to_string(),
                    data_type: TupleValueType::Int,
                    nullable: true,
                    table_ref: 1
                }
            ]
        });
        self.catalog.create_table(TableDesc {
            id: 2,
            segment_id: 1026,
            name: "cars".to_string(),
            cardinality: 2,
            attributes: vec![
                AttributeDesc {
                    id: 4,
                    name: "id".to_string(),
                    data_type: TupleValueType::Int,
                    nullable: false,
                    table_ref: 2
                },
                AttributeDesc {
                    id: 5,
                    name: "model".to_string(),
                    data_type: TupleValueType::VarChar(255),
                    nullable: false,
                    table_ref: 2
                },
                AttributeDesc {
                    id: 6,
                    name: "owner_id".to_string(),
                    data_type: TupleValueType::Int,
                    nullable: false,
                    table_ref: 2
                },
            ]
        });
        // Init demo data
        let people = self.catalog.find_table_by_name("people").unwrap().unwrap();
        let cars = self.catalog.find_table_by_name("cars").unwrap().unwrap();
        let people_segment = SlottedPageSegment::new(self.buffer_manager.clone(), people.segment_id, people.segment_id + 1);
        let cars_segment = SlottedPageSegment::new(self.buffer_manager.clone(), cars.segment_id, cars.segment_id + 1);

        let people_tuples = vec![
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
        let cars_tuples = vec![
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

        for tuple in people_tuples {
            people_segment.insert_record(&tuple.get_binary()).unwrap();
        }

        for tuple in cars_tuples {
            cars_segment.insert_record(&tuple.get_binary()).unwrap();
        }

        self.buffer_manager.flush().unwrap();
    }
}