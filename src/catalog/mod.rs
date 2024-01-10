pub mod statistics;
mod db_object;
mod attribute;

use std::{sync::{Arc, atomic::{Ordering, AtomicU64, AtomicU32}}, collections::BTreeMap};

use parking_lot::RwLock;

use crate::{access::{SlottedPageSegment, tuple::Tuple, SlottedPageHeapStorage, HeapStorage}, storage::{buffer_manager::BufferManager, page::SegmentId}, types::{TupleValueType, TupleValue, RelationTID}, statistics::{counting_hyperloglog::{CountingHyperLogLog}, sampling::ReservoirSampler}, config::DbConfig};

use self::{statistics::{CombinedTableStatistics, AttributeStatisticsCatalogSegment}, db_object::{DbObjectCatalogSegment, DbObjectType, DbObjectDesc}};

type DbObjectRef = u32;
type AttributeRef = u32;

/*
    Catalog tables concept:
    General namespaces:
    (eventually it might be a good idea to have similar catalogs to postgres but for now 
    we won't enable runtime changing of types/access methods/aggregate functions, ...)

    Segment ids 0-1023: reserved for system tables, their fsis and indices
    Description of system tables (segment id n+1 is always reserved for the corresponding fsi. 
    n+2 for sample segment):
    0: db_object (tables, indices, views, ...)
    8: attribute (table attributes)
    16: statistics (attribute statistics (extra table to avoid touching the attribute table so often))

    (that's it FOR NOW. Additional columns will be added over time)

    Here's how to add an attribute to a catalog table:
    1. If applicable: Add the attribute to the corresponding specific in/output struct (e.g. TableDesc)
    2. Add the attribute to the underlying catalog table struct (e.g. DbObjectCatalog)
    3. IMPORTANT! (This is the only step where compilation won't fail if you forget it):
       Add the tuple to the functions mapping the struct to a raw tuple
    4. Add the tuple to the functions mapping a raw tuple to the struct
 */

const DB_OBJECT_CATALOG_SEGMENT_ID: SegmentId = 0;
const ATTRIBUTE_CATALOG_SEGMENT_ID: SegmentId = 2;
const ATTRIBUTE_STATISTICS_CATALOG_SEGMENT_ID: SegmentId = 4;
const TABLE_STATISTICS_CATALOG_SEGMENT_ID: SegmentId = 6;

pub const SAMPLE_SIZE: u32 = 1024;


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableDesc {
    pub id: DbObjectRef,
    pub name: String,
    pub attributes: Vec<AttributeDesc>,
    pub segment_id: SegmentId,
    pub fsi_segment_id: SegmentId,
    pub sample_segment_id: SegmentId,
    pub sample_fsi_segment_id: SegmentId,
    //pub indexes: Vec<IndexDesc>
}

impl TableDesc {
    pub fn get_attribute_by_name(&self, name: &str) -> Option<&AttributeDesc> {
        self.attributes.iter().find(|a| a.name == name)
    }

    #[allow(dead_code)]
    pub fn get_attribute_by_id(&self, id: u32) -> Option<&AttributeDesc> {
        self.attributes.iter().find(|a| a.id == id)
    }

    pub fn get_attribute_index_by_id(&self, id: u32) -> Option<usize> {
        self.attributes.iter().position(|a| a.id == id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexDesc {
    pub id: DbObjectRef,
    pub name: String,
    pub table_ref: DbObjectRef,
    pub attributes: Vec<AttributeRef>,
    pub segment_id: SegmentId,
}

#[derive(Clone)]
pub struct Catalog<B: BufferManager> {
    cache: Arc<CatalogCache<B>>,
}

impl<B: BufferManager> Catalog<B> {
    pub fn new(buffer_manager: B, db_config: Arc<DbConfig>) -> Result<Catalog<B>, B::BError> {
        Ok(Catalog {
            cache: Arc::new(CatalogCache::new(buffer_manager, db_config.clone())?),
        })
    }

    pub fn find_table_by_name(&self, name: &str) -> Result<Option<TableDesc>, B::BError> {
        self.cache.find_table_by_name(name)
    }

    pub fn create_table(&self, table: &TableDesc) -> Result<(), B::BError> {
        self.cache.create_table(table)
    }

    pub fn create_index(&self, index: &TableDesc) -> Result<(), B::BError> {
        todo!()
    }

    pub fn get_statistics(&self, table_id: u32) -> Result<Option<CombinedTableStatistics>, B::BError> {
        self.cache.get_statistics(table_id)
    }

    pub fn allocate_db_object_id(&self) -> u32 {
        self.cache.allocate_db_object_id()
    }

    pub fn allocate_attribute_ids(&self, n: u32) -> u32 {
        self.cache.allocate_attribute_ids(n)
    }

    pub fn allocate_segment_ids(&self, n: u32) -> u32 {
        self.cache.allocate_segment_ids(n)
    }
}

struct CatalogCache<B:BufferManager> {
    bm: B,
    db_object_segment: DbObjectCatalogSegment<B>,
    attribute_segment: AttributeCatalogSegment<B>,
    table_statistics_segment: TableStatisticsCatalogSegment<B>,
    attribute_statistics_segment: AttributeStatisticsCatalogSegment<B>,
    table_cache: RwLock<BTreeMap<u32, TableDesc>>,
    table_name_index: RwLock<BTreeMap<String, u32>>,
    statistics: RwLock<BTreeMap<u32, CombinedTableStatistics>>,
    db_object_id_counter: AtomicU32,
    attribute_id_counter: AtomicU32,
    segment_id_counter: AtomicU32,
}

impl<B: BufferManager> CatalogCache<B> {
    fn new(buffer_manager: B, db_config: Arc<DbConfig>) -> Result<CatalogCache<B>, B::BError> {
        let db_object_segment = DbObjectCatalogSegment::new(buffer_manager.clone());
        let attribute_segment = AttributeCatalogSegment::new(buffer_manager.clone());
        // Kind of hacky way to get counters for allocating new ids
        // Scan the db_object table for the highest id
        let (max_db_object_id, max_segment_id) = db_object_segment.get_max_id_and_segment_id()?;
        let max_attribute_id = attribute_segment.get_max_id()?;
        let db_object_id_counter = AtomicU32::new(max_db_object_id + 1);
        let attribute_id_counter = AtomicU32::new(max_attribute_id + 1);
        let segment_id_counter = AtomicU32::new(max_segment_id.max(1023) + 1);
        Ok(CatalogCache {
            bm: buffer_manager.clone(),
            db_object_segment,
            attribute_segment,
            table_statistics_segment: TableStatisticsCatalogSegment::new(buffer_manager.clone(), db_config.clone()),   
            attribute_statistics_segment: AttributeStatisticsCatalogSegment::new(buffer_manager.clone()),
            table_cache: RwLock::new(BTreeMap::new()),
            table_name_index: RwLock::new(BTreeMap::new()),
            statistics: RwLock::new(BTreeMap::new()),
            db_object_id_counter,
            attribute_id_counter,
            segment_id_counter,
        })
    }

    fn find_table_by_name(&self, name: &str) -> Result<Option<TableDesc>, B::BError> {
        if let Some(table_id) = self.table_name_index.read().get(name) {
            let cached_table = &self.table_cache.read()[table_id];
            let table = cached_table.clone();
            return Ok(Some(table));
        }
        let db_object = self.db_object_segment.find_db_object_by_name(DbObjectType::Relation, name)?;
        if let Some(db_object) = db_object {
            let attributes = self.attribute_segment.get_attributes_by_db_object(db_object.id)?;
            let table = TableDesc { 
                id: db_object.id, 
                name: db_object.name, 
                attributes, 
                segment_id: db_object.segment_id, 
                fsi_segment_id: db_object.fsi_segment_id.unwrap(), 
                sample_segment_id: db_object.sample_segment_id.unwrap(), 
                sample_fsi_segment_id: db_object.sample_fsi_segment_id.unwrap()
            };
            let mut table_name_cache = self.table_name_index.write();
            let mut table_cache = self.table_cache.write();
            if let Some(table) = table_cache.get(&table.id) {
                let cached_table = &self.table_cache.read()[&table.id];
                let table = cached_table.clone();
                return Ok(Some(table));
            } else {
                table_name_cache.insert(table.name.clone(), table.id);
                table_cache.insert(table.id, table.clone());
                return Ok(Some(table));
            }
        } else {
            Ok(None)
        }
    }

    fn create_table(&self, table: &TableDesc) -> Result<(), B::BError> {
        // TODO: Properly synchronize this. Make sure that the table is only visible
        //       after it has been fully created including statistics.
        let mut table_cache = self.table_cache.write();
        table_cache.insert(table.id, table.clone());
        let db_object = DbObjectDesc { id: table.id, name: table.name.clone(), class_type: DbObjectType::Relation, segment_id: table.segment_id, fsi_segment_id: Some(table.fsi_segment_id), sample_segment_id: Some(table.sample_segment_id), sample_fsi_segment_id: Some(table.sample_fsi_segment_id) };
        self.db_object_segment.insert_db_object(&db_object).unwrap();
        for attribute in &table.attributes {
            self.attribute_segment.insert_attribute(attribute).unwrap();
        }
        self.create_statistics(&table)?;
        // This should be enough for synchronization for now. Queries wanting to access the table
        // go through the table name cache where the new table is only inserted after everything is done.
        let mut table_name_cache = self.table_name_index.write();
        table_name_cache.insert(table.name.clone(), table.id);
        Ok(())
    }

    fn create_statistics(&self, table: &TableDesc) -> Result<(), B::BError> {
        let mut statistics = self.statistics.write();
        let table_statistics = self.table_statistics_segment.create_table_statistics(table.id)?;
        let mut attribute_statistics = Vec::new();
        for attribute in &table.attributes {
            let attribute_statistic = self.attribute_statistics_segment.create_attribute_statistics(table.id, attribute.id)?;
            attribute_statistics.push(attribute_statistic);
        }
        statistics.insert(table.id, CombinedTableStatistics { table_statistics, attribute_statistics });
        Ok(())
    }

    fn get_statistics(&self, table_id: u32) -> Result<Option<CombinedTableStatistics>, B::BError> {
        if let Some(statistics) = self.statistics.read().get(&table_id) {
            return Ok(Some(statistics.clone()));
        }
        let mut statistics_write = self.statistics.write();
        if let Some(statistics) = statistics_write.get(&table_id) {
            return Ok(Some(statistics.clone()));
        }
        let table_statistics = self.table_statistics_segment.get_table_statistics_by_db_object(table_id)?;
        if let Some(table_statistics) = table_statistics {
            let attribute_statistics = self.attribute_statistics_segment.get_attribute_statistics_by_db_object(table_id)?;
            let combined_statistics = CombinedTableStatistics { table_statistics, attribute_statistics };
            statistics_write.insert(table_id, combined_statistics.clone());
            return Ok(Some(combined_statistics));
        }
        Ok(None)
    }

    pub fn allocate_db_object_id(&self) -> u32 {
        self.db_object_id_counter.fetch_add(1, Ordering::Relaxed)
    }

    pub fn allocate_segment_ids(&self, n: u32) -> u32 {
        self.segment_id_counter.fetch_add(n, Ordering::Relaxed)
    }

    pub fn allocate_attribute_ids(&self, n: u32) -> u32 {
        self.attribute_id_counter.fetch_add(n, Ordering::Relaxed)
    }
}

impl<B: BufferManager> Drop for CatalogCache<B> {
    fn drop(&mut self) {
        // write back statistics
        for (_, statistics) in self.statistics.read().iter() {
            self.table_statistics_segment.update_table_statistics(&statistics.table_statistics).unwrap();
            for attribute_statistics in &statistics.attribute_statistics {
                self.attribute_statistics_segment.update_attribute_statistics(attribute_statistics).unwrap();
            }
        }
        self.bm.flush().unwrap();
    }
}



#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AttributeDesc {
    pub id: u32,
    pub name: String,
    pub data_type: TupleValueType,
    pub nullable: bool,
    /*default_value: Option<String>,*/
    pub table_ref: DbObjectRef,
}


impl From<&Tuple> for AttributeDesc {
    fn from(value: &Tuple) -> Self {
        let table_ref = match value.values[0] {
            Some(TupleValue::Int(id)) => id as u32,
            _ => unreachable!()
        };
        let id = match value.values[1] {
            Some(TupleValue::Int(id)) => id,
            _ => unreachable!()
        };
        let name = match value.values[2] {
            Some(TupleValue::String(ref name)) => name.clone(),
            _ => unreachable!()
        };
        let data_type = match value.values[3] {
            Some(TupleValue::SmallInt(data_type)) => {
                match data_type {
                    0 => TupleValueType::BigInt,
                    1 => TupleValueType::VarChar(value.values[4].as_ref().unwrap().as_small_int() as u16),
                    2 => TupleValueType::Int,
                    3 => TupleValueType::SmallInt,
                    _ => unreachable!()
                }
            },
            _ => unreachable!()
        };
        let nullable = match value.values[5] {
            Some(TupleValue::SmallInt(integrity_constraint_flags)) => integrity_constraint_flags == 0,
            _ => unreachable!()
        };
        AttributeDesc { id: id as u32, name, data_type, nullable, table_ref }
    }
}

impl From<&AttributeDesc> for Tuple {
    fn from(value: &AttributeDesc) -> Self {
        let (data_type, length) = match value.data_type {
            TupleValueType::BigInt => (0, 0),
            TupleValueType::VarChar(length) => (1, length as i32),
            TupleValueType::Int => (2, 0),
            TupleValueType::SmallInt => (3, 0),
            TupleValueType::VarBinary(length) => (4, length as i32)
        };
        let integrity_constraint_flags = if value.nullable { 0 } else { 1 };
        Tuple::new(vec![
            Some(TupleValue::Int(value.table_ref as i32)), // db_object_id
            Some(TupleValue::Int(value.id as i32)), // id
            Some(TupleValue::String(value.name.clone())), // name
            Some(TupleValue::SmallInt(data_type)), // data_type
            Some(TupleValue::SmallInt(length as i16)), // length (where applicable, otherwhise don't care)
            Some(TupleValue::SmallInt(integrity_constraint_flags)), // integrity_constraint_flags
        ])
    }
}

struct AttributeCatalogSegment<B: BufferManager> {
    sp_segment: SlottedPageHeapStorage<B>,
}

impl<B: BufferManager> AttributeCatalogSegment<B> {
    fn new(buffer_manager: B) -> AttributeCatalogSegment<B> {
        let attributes = vec![
            TupleValueType::Int, // db_object_id
            TupleValueType::Int, // id
            TupleValueType::VarChar(u16::MAX), // name
            TupleValueType::SmallInt, // data_type
            TupleValueType::SmallInt, // length (where applicable, otherwhise don't care)
            TupleValueType::SmallInt, // integrity_constraint_flags
        ];
        let segment = SlottedPageSegment::new(buffer_manager, ATTRIBUTE_CATALOG_SEGMENT_ID, ATTRIBUTE_CATALOG_SEGMENT_ID + 1);
        AttributeCatalogSegment {
            sp_segment: SlottedPageHeapStorage::new(segment, attributes)
        }
    }

    fn get_max_id(&self) -> Result<u32, B::BError> {
        self.sp_segment.scan_all(|_| true)?
            .map(|f| f.map(|t| AttributeDesc::from(&t.1).id))
            .fold(Ok(0), |acc, id| {
                match (acc, id) {
                    (Ok(acc), Ok(id)) => Ok(std::cmp::max(acc, id)),
                    (Err(e), _) => Err(e),
                    (_, Err(e)) => Err(e)
                }
            })
    }

    fn get_attributes_by_db_object(&self, db_object_id: u32) -> Result<Vec<AttributeDesc>, B::BError> {
        self.sp_segment.scan_all(|data| {
            let attribute_desc = AttributeDesc::from(data);
            attribute_desc.table_ref == db_object_id
        })?.map(|f| f.map(|t| AttributeDesc::from(&t.1)))
        .collect()
    }

    #[allow(dead_code)]
    fn get_attribute_by_db_object_and_name(&self, db_object_id: u32, name: &str) -> Result<Option<AttributeDesc>, B::BError> {
        let result = self.sp_segment.scan_all(|data| {
            let attribute_desc = AttributeDesc::from(data);
            attribute_desc.table_ref == db_object_id && attribute_desc.name == name
        })?.next();
        match result {
            Some(Ok((_, tuple))) => Ok(Some(AttributeDesc::from(&tuple))),
            Some(Err(e)) => Err(e),
            None => Ok(None)
        }
    }

    fn insert_attribute(&self, attribute: &AttributeDesc) -> Result<(), B::BError> {
        let tuple = Tuple::from(attribute);
        self.sp_segment.insert_tuples(&mut [tuple])?;
        Ok(())
    }

    #[allow(dead_code)]
    fn update_attribute(&self, attribute: AttributeDesc) -> Result<(), B::BError> {
        let (tid, _) = self.sp_segment.scan_all(|data| {
            let attribute_desc = AttributeDesc::from(data);
            attribute_desc.id == attribute.id
        })?.next().unwrap().unwrap();
        let tuple = Tuple::from(&attribute);
        self.sp_segment.update_tuple(tid, tuple)?;
        Ok(())
    }
}


#[derive(Debug, Clone)]
pub struct TableStatistics {
    pub tid: RelationTID,
    pub db_object_id: u32,
    pub cardinality: Arc<AtomicU64>,
    pub sampler: Arc<ReservoirSampler>
}

impl From<&TableStatistics> for Tuple {
    fn from(value: &TableStatistics) -> Self {
        Tuple::new(vec![
            Some(TupleValue::Int(value.db_object_id as i32)), // db_object_id
            Some(TupleValue::BigInt(value.cardinality.load(Ordering::Relaxed) as i64)), // cardinality
            Some(TupleValue::ByteArray(value.sampler.snapshot().into_boxed_slice())), // ReservoirSampler
        ])
    }
}

struct TableStatisticsCatalogSegment<B: BufferManager> {
    sp_segment: SlottedPageHeapStorage<B>,
    db_config: Arc<DbConfig>
}

impl<B: BufferManager> TableStatisticsCatalogSegment<B> {
    fn new(buffer_manager: B, db_config: Arc<DbConfig>) -> TableStatisticsCatalogSegment<B> {
        let attributes = vec![
            TupleValueType::Int, // db_object_id
            TupleValueType::BigInt, // cardinality
            TupleValueType::VarBinary(u16::MAX), // ReservoirSampler Snapshot
        ];
        let segment = SlottedPageSegment::new(buffer_manager, TABLE_STATISTICS_CATALOG_SEGMENT_ID, TABLE_STATISTICS_CATALOG_SEGMENT_ID + 1);
        TableStatisticsCatalogSegment {
            sp_segment: SlottedPageHeapStorage::new(segment, attributes),
            db_config
        }
    }

    fn get_table_statistics_by_db_object(&self, _db_object_id: u32) -> Result<Option<TableStatistics>, B::BError> {
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

    fn create_table_statistics(&self, db_object_id: u32) -> Result<TableStatistics, B::BError> {
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

    fn update_table_statistics(&self, table_statistics: &TableStatistics) -> Result<(), B::BError> {
        let tuple = Tuple::from(table_statistics);
        self.sp_segment.update_tuple(table_statistics.tid, tuple)?;
        Ok(())
    }

    #[allow(dead_code)]
    fn delete_table_statistics(&self, table_statistics: TableStatistics) -> Result<(), B::BError> {
        self.sp_segment.delete_tuple(table_statistics.tid)?;
        Ok(())
    }
}

