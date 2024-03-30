use std::sync::{atomic::AtomicU64, Arc};

use crate::{access::{tuple::Tuple, HeapStorage, SlottedPageHeapStorage, SlottedPageSegment}, config::DbConfig, statistics::{counting_hyperloglog::CountingHyperLogLog, sampling::ReservoirSampler}, storage::buffer_manager::BufferManager, types::{RelationTID, TupleValue, TupleValueType}};

use super::{AttributeStatistics, TableStatistics, ATTRIBUTE_STATISTICS_CATALOG_SEGMENT_ID, SAMPLE_SIZE, TABLE_STATISTICS_CATALOG_SEGMENT_ID};

pub(super) struct TableStatisticsCatalogSegment<B: BufferManager> {
    sp_segment: SlottedPageHeapStorage<B>,
    db_config: Arc<DbConfig>
}

impl<B: BufferManager> TableStatisticsCatalogSegment<B> {
    pub fn new(buffer_manager: B, db_config: Arc<DbConfig>) -> TableStatisticsCatalogSegment<B> {
        let attributes = vec![
            TupleValueType::Int, // db_object_id
            TupleValueType::BigInt, // cardinality
            TupleValueType::VarBinary(u16::MAX), // ReservoirSampler snapshot
        ];
        let segment = SlottedPageSegment::new(buffer_manager, TABLE_STATISTICS_CATALOG_SEGMENT_ID, TABLE_STATISTICS_CATALOG_SEGMENT_ID + 1);
        TableStatisticsCatalogSegment {
            sp_segment: SlottedPageHeapStorage::new(segment, attributes),
            db_config
        }
    }

    pub fn get_table_statistics_by_db_object(&self, _db_object_id: u32) -> Result<Option<TableStatistics>, B::BError> {
        let result = self.sp_segment.scan_all(|data| {
            let db_object_id = match data.values[0] {
                Some(TupleValue::Int(db_object_id)) => db_object_id as u32,
                _ => unreachable!()
            };
            db_object_id == db_object_id
        })?.next();
        match result {
            Some(Ok((tid, tuple))) => {
                let db_object_id = match tuple.values[0] {
                    Some(TupleValue::Int(db_object_id)) => db_object_id as u32,
                    _ => unreachable!()
                };
                let cardinality = match tuple.values[1] {
                    Some(TupleValue::BigInt(cardinality)) => Arc::new(AtomicU64::new(cardinality as u64)),
                    _ => unreachable!()
                };
                let sampler = match tuple.values[2] {
                    Some(TupleValue::ByteArray(ref sampler)) => Arc::new(ReservoirSampler::parse(sampler.as_ref(), self.db_config.n_threads)),
                    _ => unreachable!()
                };
                Ok(Some(TableStatistics { tid, db_object_id, cardinality, sampler }))
            },
            Some(Err(e)) => Err(e),
            None => Ok(None)
        }
    }

    pub fn create_table_statistics(&self, db_object_id: u32) -> Result<TableStatistics, B::BError> {
        let sampler = Arc::new(ReservoirSampler::new(SAMPLE_SIZE, self.db_config.n_threads));
        let cardinality = Arc::new(AtomicU64::new(0));
        let tuple = Tuple::new(vec![
            Some(TupleValue::Int(db_object_id as i32)), // db_object_id
            Some(TupleValue::BigInt(0)), // cardinality
            Some(TupleValue::ByteArray(sampler.snapshot().into_boxed_slice())), // ReservoirSampler
        ]);
        let tid = self.sp_segment.insert_tuples(&mut [tuple])?[0];
        Ok(TableStatistics { tid, db_object_id, cardinality, sampler })
    }

    pub fn update_table_statistics(&self, table_statistics: &TableStatistics) -> Result<(), B::BError> {
        let tuple = Tuple::from(table_statistics);
        self.sp_segment.update_tuple(table_statistics.tid, tuple)?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn delete_table_statistics(&self, table_statistics: TableStatistics) -> Result<(), B::BError> {
        self.sp_segment.delete_tuple(table_statistics.tid)?;
        Ok(())
    }
}

pub(super) struct AttributeStatisticsCatalogSegment<B: BufferManager> {
    sp_segment: SlottedPageHeapStorage<B>,
}

impl<B: BufferManager> AttributeStatisticsCatalogSegment<B> {
    pub fn new(buffer_manager: B) -> AttributeStatisticsCatalogSegment<B> {
        let attributes = vec![
            TupleValueType::Int, // db_object_id
            TupleValueType::Int, // attribute_id
            TupleValueType::VarBinary(u16::MAX), // CountingHyperLogLog
        ];
        let segment = SlottedPageSegment::new(buffer_manager, ATTRIBUTE_STATISTICS_CATALOG_SEGMENT_ID, ATTRIBUTE_STATISTICS_CATALOG_SEGMENT_ID + 1);
        AttributeStatisticsCatalogSegment {
            sp_segment: SlottedPageHeapStorage::new(segment, attributes)
        }
    }

    pub fn get_attribute_statistics_by_db_object(&self, db_object_id: u32) -> Result<Vec<AttributeStatistics>, B::BError> {
        self.sp_segment.scan_all(|data| {
            let db_object_id = match data.values[0] {
                Some(TupleValue::Int(db_object_id)) => db_object_id as u32,
                _ => unreachable!()
            };
            db_object_id == db_object_id
        })?.map(|f| f.map(|t| {
            let attribute_id = match t.1.values[1] {
                Some(TupleValue::Int(attribute_id)) => attribute_id as u32,
                _ => unreachable!()
            };
            let counting_hyperloglog = match t.1.values[2] {
                Some(TupleValue::ByteArray(ref counting_hyperloglog)) => Arc::new(CountingHyperLogLog::from_bytes(counting_hyperloglog.as_ref().try_into().unwrap())),
                _ => unreachable!()
            };
            AttributeStatistics { tid: t.0, db_object_id, attribute_id, counting_hyperloglog }
        })).collect()
    }

    pub fn create_attribute_statistics(&self, db_object_id: u32, attribute_id: u32) -> Result<AttributeStatistics, B::BError> {
        let counting_hyperloglog = Arc::new(CountingHyperLogLog::new());
        let tuple = Tuple::new(vec![
            Some(TupleValue::Int(db_object_id as i32)), // db_object_id
            Some(TupleValue::Int(attribute_id as i32)), // attribute_id
            Some(TupleValue::ByteArray(Box::new(counting_hyperloglog.to_bytes()))), // CountingHyperLogLog
        ]);
        let tid = self.sp_segment.insert_tuples(&mut [tuple])?[0];
        Ok(AttributeStatistics { tid: tid, db_object_id, attribute_id, counting_hyperloglog })
    }

    pub fn update_attribute_statistics(&self, attribute_statistics: &AttributeStatistics) -> Result<(), B::BError> {
        let tuple = Tuple::from(attribute_statistics);
        self.sp_segment.update_tuple(attribute_statistics.tid, tuple)?;
        Ok(())
    }

    #[allow(dead_code)]
    fn delete_attribute_statistics(&self, attribute_statistics: AttributeStatistics) -> Result<(), B::BError> {
        self.sp_segment.delete_tuple(attribute_statistics.tid)?;
        Ok(())
    }
}