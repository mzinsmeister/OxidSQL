mod statistics;
mod db_object;
mod attribute;
mod index;

use std::{collections::BTreeMap, sync::{atomic::{AtomicU32, AtomicU64, Ordering}, Arc}};

use parking_lot::RwLock;

use crate::{access::tuple::Tuple, config::DbConfig, statistics::{counting_hyperloglog::CountingHyperLogLog, sampling::ReservoirSampler}, storage::{buffer_manager::BufferManager, page::SegmentId}, types::{RelationTID, TupleValue, TupleValueType}};

use self::{attribute::AttributeCatalogSegment, db_object::{DbObjectCatalogSegment, DbObjectDesc, DbObjectType}, index::{IndexCatalogSegment, IndexDetailsDesc}, statistics::{AttributeStatisticsCatalogSegment, TableStatisticsCatalogSegment}};

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
const INDEX_CATALOG_SEGMENT_ID: SegmentId = 8;

pub const SAMPLE_SIZE: u32 = 1024;


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableDesc {
    pub id: DbObjectRef,
    pub name: String,
    pub attributes: Vec<AttributeDesc>,
    pub indexes: Vec<IndexDesc>,
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


#[repr(u16)]
#[derive(Copy, Clone, PartialEq, Eq, Debug, Hash)]
pub enum IndexType {
    BTree = 0,
}

impl IndexType {
    fn from_u16(value: u16) -> IndexType {
        match value {
            0 => IndexType::BTree,
            _ => unreachable!()
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct IndexDesc {
    pub id: DbObjectRef,
    pub name: String,
    pub indexed_id: DbObjectRef,
    pub index_type: IndexType,
    pub attributes: Vec<AttributeRef>,
    pub segment_id: SegmentId,
    pub fsi_segment_id: Option<SegmentId>,
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

#[derive(Debug, Clone)]
pub struct CombinedTableStatistics {
    pub table_statistics: TableStatistics,
    pub attribute_statistics: Vec<AttributeStatistics>
}

#[derive(Debug, Clone)]
pub struct TableStatistics {
    pub tid: RelationTID,
    pub db_object_id: u32,
    pub cardinality: Arc<AtomicU64>,
    pub sampler: Arc<ReservoirSampler>,
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

    pub fn find_table_by_id(&self, id: u32) -> Result<Option<TableDesc>, B::BError> {
        self.cache.find_table_by_id(id)
    }

    pub fn find_table_by_name(&self, name: &str) -> Result<Option<TableDesc>, B::BError> {
        self.cache.find_table_by_name(name)
    }

    pub fn create_table(&self, table: &TableDesc) -> Result<(), B::BError> {
        self.cache.create_table(table)
    }
    pub fn find_index_by_name(&self, name: &str) -> Result<Option<IndexDesc>, B::BError> {
        self.cache.find_index_by_name(name)
    }

    pub fn create_index(&self, index: &IndexDesc) -> Result<(), B::BError> {
        self.cache.create_index(index)
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
    index_segment: IndexCatalogSegment<B>,
    table_cache: RwLock<BTreeMap<u32, TableDesc>>,
    table_name_index: RwLock<BTreeMap<String, u32>>,
    index_name_index: RwLock<BTreeMap<String, (u32, u32)>>,
    statistics: RwLock<BTreeMap<u32, CombinedTableStatistics>>,
    db_object_id_counter: AtomicU32,
    attribute_id_counter: AtomicU32,
    segment_id_counter: AtomicU32,
}

impl<B: BufferManager> CatalogCache<B> {
    fn new(buffer_manager: B, db_config: Arc<DbConfig>) -> Result<CatalogCache<B>, B::BError> {
        let db_object_segment = DbObjectCatalogSegment::new(buffer_manager.clone());
        let attribute_segment = AttributeCatalogSegment::new(buffer_manager.clone());
        let table_statistics_segment = TableStatisticsCatalogSegment::new(buffer_manager.clone(), db_config.clone());
        let attribute_statistics_segment = AttributeStatisticsCatalogSegment::new(buffer_manager.clone());
        let index_segment = IndexCatalogSegment::new(buffer_manager.clone());
        // Kind of hacky way to get counters for allocating new ids
        // Scan the db_object table for the highest id
        let (max_db_object_id, max_segment_id) = db_object_segment.get_max_id_and_segment_id()?;
        let max_attribute_id = attribute_segment.get_max_id()?;
        let db_object_id_counter = AtomicU32::new(max_db_object_id + 1);
        let attribute_id_counter = AtomicU32::new(max_attribute_id + 1);
        let segment_id_counter = AtomicU32::new(max_segment_id.max(1023) + 1);
        // TODO: Load all tables and indices into the cache
        Ok(CatalogCache {
            bm: buffer_manager.clone(),
            db_object_segment,
            attribute_segment,
            table_statistics_segment,   
            attribute_statistics_segment,
            index_segment,
            table_cache: RwLock::new(BTreeMap::new()),
            table_name_index: RwLock::new(BTreeMap::new()),
            index_name_index: RwLock::new(BTreeMap::new()),
            statistics: RwLock::new(BTreeMap::new()),
            db_object_id_counter,
            attribute_id_counter,
            segment_id_counter,
        })
    }

    fn find_table_by_id(&self, id: u32) -> Result<Option<TableDesc>, B::BError> {
        // TODO: Eliminate duplicate code
        if let Some(table) = self.table_cache.read().get(&id) {
            return Ok(Some(table.clone()));
        } else {
            let db_object = self.db_object_segment.get_db_object_by_id(id)?;
            if let Some(db_object) = db_object {
                if db_object.class_type != DbObjectType::Relation {
                    return Ok(None);
                }
                let attributes = self.attribute_segment.get_attributes_by_db_object(db_object.id)?;
                let indexes = self.index_segment.get_by_indexed_db_obj_id(db_object.id)?.iter()
                    .map(|i| {
                        let db_object = self.db_object_segment.get_db_object_by_id(i.db_obj_id).unwrap().unwrap();
                        IndexDesc {
                            id: i.db_obj_id,
                            name: db_object.name,
                            indexed_id: i.indexed_db_obj_id,
                            index_type: i.index_type,
                            attributes: i.attributes.clone(),
                            segment_id: db_object.segment_id,
                            fsi_segment_id: db_object.fsi_segment_id
                        }
                    })
                    .collect();
                let table = TableDesc {
                    id: db_object.id,
                    name: db_object.name,
                    attributes,
                    segment_id: db_object.segment_id,
                    fsi_segment_id: db_object.fsi_segment_id.unwrap(),
                    sample_segment_id: db_object.sample_segment_id.unwrap(),
                    sample_fsi_segment_id: db_object.sample_fsi_segment_id.unwrap(),
                    indexes
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
    }

    fn find_table_by_name(&self, name: &str) -> Result<Option<TableDesc>, B::BError> {
        if let Some(table_id) = self.table_name_index.read().get(name) {
            if let Some(table) = self.table_cache.read().get(table_id) {
                let cached_table = &self.table_cache.read()[table_id];
                let table = cached_table.clone();
                return Ok(Some(table));
            } else {
                return Ok(None);
            }
        }
        let db_object = self.db_object_segment.find_db_object_by_name(DbObjectType::Relation, name)?;
        if let Some(db_object) = db_object {
            let attributes = self.attribute_segment.get_attributes_by_db_object(db_object.id)?;
            let indexes = self.index_segment.get_by_indexed_db_obj_id(db_object.id)?.iter()
                .map(|i| {
                    let db_object = self.db_object_segment.get_db_object_by_id(i.db_obj_id).unwrap().unwrap();
                    IndexDesc { 
                        id: i.db_obj_id, 
                        name: db_object.name,
                        indexed_id: i.indexed_db_obj_id, 
                        index_type: i.index_type, 
                        attributes: i.attributes.clone(), 
                        segment_id: db_object.segment_id, 
                        fsi_segment_id: db_object.fsi_segment_id 
                    }
                })
                .collect();
            let table = TableDesc { 
                id: db_object.id, 
                name: db_object.name, 
                attributes, 
                segment_id: db_object.segment_id, 
                fsi_segment_id: db_object.fsi_segment_id.unwrap(), 
                sample_segment_id: db_object.sample_segment_id.unwrap(), 
                sample_fsi_segment_id: db_object.sample_fsi_segment_id.unwrap(),
                indexes
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
        //       Also once removing tables and indices is implemented we must hold 
        //       a read lock to the schema/catalog/db_object or something as long as we are doing something
        //       and a write lock when we are changing the schema.
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

    pub fn find_index_by_name(&self, name: &str) -> Result<Option<IndexDesc>, B::BError> {
        let (table_id, index_id) = if let Some((indexed_id, index_id)) = self.index_name_index.read().get(name) {
            (*indexed_id, *index_id)
        } else {
            let index = self.db_object_segment.find_db_object_by_name(DbObjectType::Index, name)?;
            if let Some(index) = index {
                let index_details_desc = self.index_segment.get_by_db_obj_id(index.id)?.unwrap();
                let index = IndexDesc {
                    id: index.id,
                    name: index.name,
                    indexed_id: index_details_desc.indexed_db_obj_id,
                    index_type: index_details_desc.index_type,
                    attributes: index_details_desc.attributes.clone(),
                    segment_id: index.segment_id,
                    fsi_segment_id: index.fsi_segment_id
                };
                let mut index_name_cache = self.index_name_index.write();
                index_name_cache.insert(index.name.clone(), (index.indexed_id, index.id));
                (index.indexed_id, index.id)
            } else {
                return Ok(None);
            }
        };

        if let Some(table) = self.table_cache.read().get(&table_id) {
            let index = table.indexes.iter().find(|i| i.id == index_id).unwrap();
            Ok(Some(index.clone()))
        } else {
            // TODO: Remove this duplicated code
            let table = self.db_object_segment.get_db_object_by_id(table_id).unwrap().unwrap();
            let attributes = self.attribute_segment.get_attributes_by_db_object(table_id)?;
            let indexes = self.index_segment.get_by_indexed_db_obj_id(table_id)?.iter()
                .map(|i| {
                    let db_object = self.db_object_segment.get_db_object_by_id(i.db_obj_id).unwrap().unwrap();
                    IndexDesc { 
                        id: i.db_obj_id, 
                        name: db_object.name,
                        indexed_id: i.indexed_db_obj_id, 
                        index_type: i.index_type, 
                        attributes: i.attributes.clone(), 
                        segment_id: db_object.segment_id, 
                        fsi_segment_id: db_object.fsi_segment_id 
                    }
                })
                .collect();
            let table = TableDesc { 
                id: table.id, 
                name: table.name, 
                attributes, 
                segment_id: table.segment_id, 
                fsi_segment_id: table.fsi_segment_id.unwrap(), 
                sample_segment_id: table.sample_segment_id.unwrap(), 
                sample_fsi_segment_id: table.sample_fsi_segment_id.unwrap(),
                indexes
            };
            let mut table_cache = self.table_cache.write();
            table_cache.entry(table.id).or_insert(table.clone());
            Ok(table.indexes.iter().find(|i| i.id == index_id).cloned())
        }
    }

    pub fn create_index(&self, index: &IndexDesc) -> Result<(), B::BError> {
        let mut table_cache = self.table_cache.write();
        let table = table_cache.get_mut(&index.indexed_id).unwrap();
        table.indexes.push(index.clone());
        let details = IndexDetailsDesc { 
            db_obj_id: index.id, 
            indexed_db_obj_id: index.indexed_id, 
            index_type: index.index_type, 
            attributes: index.attributes.clone()
        };
        self.index_segment.insert_index(&details).unwrap();
        let db_obj = DbObjectDesc { 
            id: index.id, 
            name: index.name.clone(), 
            class_type: DbObjectType::Index, 
            segment_id: index.segment_id, 
            fsi_segment_id: index.fsi_segment_id,
            sample_segment_id: None,
            sample_fsi_segment_id: None
        };
        self.db_object_segment.insert_db_object(&db_obj).unwrap();
        self.index_name_index.write().insert(index.name.clone(), (index.id, index.indexed_id));
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



