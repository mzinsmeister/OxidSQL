use std::sync::Arc;

use crate::{types::{RelationTID, TupleValue, TupleValueType}, statistics::counting_hyperloglog::CountingHyperLogLog, access::{tuple::Tuple, SlottedPageHeapStorage, SlottedPageSegment, HeapStorage}, storage::buffer_manager::BufferManager};

use super::{TableStatistics, ATTRIBUTE_STATISTICS_CATALOG_SEGMENT_ID};

#[derive(Debug, Clone)]
pub struct CombinedTableStatistics {
    pub table_statistics: TableStatistics,
    pub attribute_statistics: Vec<AttributeStatistics>
}

#[derive(Debug, Clone)]
pub struct AttributeStatistics {
    pub tid: RelationTID,
    pub db_object_id: u32,
    pub attribute_id: u32,
    pub counting_hyperloglog: Arc<CountingHyperLogLog<fn(f64) -> bool>>
}

impl From<&AttributeStatistics> for Tuple {
    fn from(value: &AttributeStatistics) -> Self {
        Tuple::new(vec![
            Some(TupleValue::Int(value.db_object_id as i32)), // db_object_id
            Some(TupleValue::Int(value.attribute_id as i32)), // attribute_id
            Some(TupleValue::ByteArray(Box::new(value.counting_hyperloglog.to_bytes()))), // CountingHyperLogLog
        ])
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