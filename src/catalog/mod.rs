use std::{sync::{Arc, atomic::{Ordering, AtomicU64}}, collections::BTreeMap};

use parking_lot::RwLock;

use crate::{access::{SlottedPageSegment, tuple::Tuple}, storage::{buffer_manager::BufferManager, page::SegmentId}, types::{TupleValueType, TupleValue, RelationTID}};

type DbObjectRef = u32;

pub struct ColumnRef {
    table_ref: DbObjectRef,
    column_id: u32
}

/*
    Catalog tables concept:
    General namespaces:
    (eventually it might be a good idea to have similar catalogs to postgres but for now 
    we won't enable runtime changing of types/access methods/aggregate functions, ...)

    Segment ids 0-1023: reserved for system tables, their fsis and indices
    Description of system tables (segment id n+1 is always reserved for the corresponding fsi):
    0: db_object (tables, indices, views, ...)
    2: attribute (table attributes)

    (that's it FOR NOW. Additional columns will be added over time)
 */

const DB_OBJECT_CATALOG_SEGMENT_ID: u16 = 0;
const ATTRIBUTE_CATALOG_SEGMENT_ID: u16 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableDesc {
    pub id: u32,
    pub name: String,
    pub attributes: Vec<AttributeDesc>,
    pub segment_id: SegmentId,
    pub cardinality: u64
}

impl TableDesc {
    pub fn get_attribute_by_name(&self, name: &str) -> Option<&AttributeDesc> {
        self.attributes.iter().find(|a| a.name == name)
    }

    pub fn get_attribute_by_id(&self, id: u32) -> Option<&AttributeDesc> {
        self.attributes.iter().find(|a| a.id == id)
    }

    pub fn get_attribute_index_by_id(&self, id: u32) -> Option<usize> {
        self.attributes.iter().position(|a| a.id == id)
    }
}

#[derive(Clone)]
pub struct Catalog<B: BufferManager> {
    cache: Arc<CatalogCache<B>>,
}

impl<B: BufferManager> Catalog<B> {
    pub fn new(buffer_manager: B) -> Catalog<B> {
        Catalog {
            cache: Arc::new(CatalogCache::new(buffer_manager))
        }
    }

    pub fn find_table_by_name(&self, name: &str) -> Result<Option<TableDesc>, B::BError> {
        self.cache.find_table_by_name(name)
    }

    pub fn create_table(&self, table: TableDesc) {
        self.cache.create_table(table)
    }

    pub fn change_table_cardinality(&self, table_id: u32, delta: u64) -> Result<(), B::BError> {
        self.cache.change_table_cardinality(table_id, delta)
    }
}

struct CatalogCache<B:BufferManager> {
    bm: B,
    db_object_segment: DbObjectCatalogSegment<B>,
    attribute_segment: AttributeCatalogSegment<B>,
    table_cache: RwLock<BTreeMap<u32, (TableDesc, AtomicU64)>>, // TODO: Fix this statistics hack
    table_name_index: RwLock<BTreeMap<String, u32>>
    // TODO: Agressively Cache stuff here
}

impl<B: BufferManager> CatalogCache<B> {
    fn new(buffer_manager: B) -> CatalogCache<B> {
        CatalogCache {
            bm: buffer_manager.clone(),
            db_object_segment: DbObjectCatalogSegment::new(buffer_manager.clone()),
            attribute_segment: AttributeCatalogSegment::new(buffer_manager),
            table_cache: RwLock::new(BTreeMap::new()),
            table_name_index: RwLock::new(BTreeMap::new())
        }
    }

    fn find_table_by_name(&self, name: &str) -> Result<Option<TableDesc>, B::BError> {
        if let Some(table_id) = self.table_name_index.read().get(name) {
            let (cached_table, cardinality) = &self.table_cache.read()[table_id];
            let mut table = cached_table.clone();
            table.cardinality = cardinality.load(Ordering::SeqCst);
            return Ok(Some(table));
        }
        let db_object = self.db_object_segment.find_db_object_by_name(DbObjectType::Relation, name);
        if let Some(db_object) = db_object {
            let attributes = self.attribute_segment.get_attributes_by_db_object(db_object.id);
            // TODO: Fix statistics hack so that you don't need to scan all tables on startup first
            let slotted_page = SlottedPageSegment::new(self.bm.clone(), db_object.segment_id, db_object.segment_id + 1);
            let cardinality = slotted_page.clone().scan(|data| { Some(()) }).count() as u64;
            let table = TableDesc { id: db_object.id, name: db_object.name, attributes, segment_id: db_object.segment_id, cardinality: cardinality };
            let mut table_name_cache = self.table_name_index.write();
            let mut table_cache = self.table_cache.write();
            if let Some(table) = table_cache.get(&table.id) {
                let (cached_table, cardinality) = &self.table_cache.read()[&table.0.id];
                let mut table = cached_table.clone();
                table.cardinality = cardinality.load(Ordering::SeqCst);
                return Ok(Some(table));
            } else {
                table_name_cache.insert(table.name.clone(), table.id);
                table_cache.insert(table.id, (table.clone(), AtomicU64::new(cardinality)));
                return Ok(Some(table));
            }
        } else {
            Ok(None)
        }
    }

    fn change_table_cardinality(&self, table_id: u32, delta: u64) -> Result<(), B::BError> {
        let table_cache = self.table_cache.read();
        let table = table_cache.get(&table_id).unwrap();
        table.1.fetch_add(delta, Ordering::SeqCst);
        Ok(())
    }

    fn create_table(&self, table: TableDesc) {
        let mut table_cache = self.table_cache.write();
        table_cache.insert(table.id, (table.clone(), AtomicU64::new(0)));
        let db_object = DbObjectDesc { id: table.id, name: table.name.clone(), class_type: DbObjectType::Relation, segment_id: table.segment_id };
        self.db_object_segment.insert_db_object(&db_object).unwrap();
        for attribute in table.attributes {
            self.attribute_segment.insert_attribute(attribute).unwrap();
        }
        let mut table_name_cache = self.table_name_index.write();
        table_name_cache.insert(table.name.clone(), table.id);
    }
}

#[repr(u16)]
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
enum DbObjectType {
    Relation = 0
}

impl DbObjectType {
    fn from_u16(value: u16) -> DbObjectType {
        match value {
            0 => DbObjectType::Relation,
            _ => unreachable!()
        }
    }
}

#[derive(Clone, Debug)]
struct DbObjectDesc {
    id: u32,
    name: String,
    class_type: DbObjectType,
    segment_id: SegmentId, //convention: the FSI segment will always get the next segment id if the object requires one
}

impl From<&[u8]> for DbObjectDesc {

    fn from(value: &[u8]) -> Self {
        let parsed_tuple = Tuple::parse_binary(&vec![
            TupleValueType::Int,
            TupleValueType::VarChar(u16::MAX),
            TupleValueType::SmallInt,
            TupleValueType::SmallInt
        ], value);
        let id = match parsed_tuple.values[0] {
            Some(TupleValue::Int(id)) => id,
            _ => unreachable!()
        };
        let name = match parsed_tuple.values[1] {
            Some(TupleValue::String(ref name)) => name.clone(),
            _ => unreachable!()
        };
        let class_type = match parsed_tuple.values[2] {
            Some(TupleValue::SmallInt(class_type)) => DbObjectType::from_u16(class_type as u16),
            _ => unreachable!()
        };
        let segment_id = match parsed_tuple.values[3] {
            Some(TupleValue::SmallInt(segment_id)) => segment_id,
            _ => unreachable!()
        };
        DbObjectDesc { id: id as u32, name, class_type, segment_id: segment_id as u16 }
    }
}

impl From<&DbObjectDesc> for Tuple {
    fn from(value: &DbObjectDesc) -> Self {
        Tuple::new(vec![
            Some(TupleValue::Int(value.id as i32)), // id
            Some(TupleValue::String(value.name.clone())), // name
            Some(TupleValue::SmallInt(value.class_type as i16)), // data_type
            Some(TupleValue::SmallInt(value.segment_id as i16)) // segment_id
        ])
    }
}

struct DbObjectCatalogSegment<B: BufferManager> {
    sp_segment: SlottedPageSegment<B>,
}

impl<B: BufferManager> DbObjectCatalogSegment<B> {
    fn new(buffer_manager: B) -> DbObjectCatalogSegment<B> {
        DbObjectCatalogSegment {
            sp_segment: SlottedPageSegment::new(buffer_manager, DB_OBJECT_CATALOG_SEGMENT_ID, DB_OBJECT_CATALOG_SEGMENT_ID + 1)
        }
    }

    fn find_first_db_object<F: Fn(&DbObjectDesc) -> bool>(&self, filter: F) -> Option<(RelationTID, DbObjectDesc)> {
        let mut scan = self.sp_segment.clone().scan(|data| {
            let db_object_desc = DbObjectDesc::from(data);
            if filter(&db_object_desc) {
                Some(db_object_desc)
            } else {
                None
            }
        });
        scan.next().map(|f| f.unwrap())
    }

    fn get_db_object_by_id(&self, id: u32) -> Option<DbObjectDesc> {
        self.find_first_db_object(|db_object_desc| db_object_desc.id == id).map(|(_, d)| d)
    }

    fn find_db_object_by_name(&self, obj_type: DbObjectType, name: &str) -> Option<DbObjectDesc> {
        self.find_first_db_object(|db_object_desc| db_object_desc.class_type == obj_type && db_object_desc.name == name).map(|(_, d)| d)
    }

    fn insert_db_object(&self, db_object: &DbObjectDesc) -> Result<(), B::BError> {
        let tuple = Tuple::from(db_object);
        let mut data = vec![0; tuple.calculate_binary_length()];
        tuple.write_binary(&mut data);
        self.sp_segment.insert_record(&data)?;
        Ok(())
    }

    fn update_db_object(&self, db_object: &DbObjectDesc) -> Result<(), B::BError> {
        let (tid, _) = self.find_first_db_object(|db_object_desc| db_object_desc.id == db_object.id).unwrap();
        let tuple = Tuple::from(db_object);
        let buffer: Box<[u8]> = tuple.into();
        self.sp_segment.write_record(tid, &buffer)?;
        Ok(())
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


impl From<&[u8]> for AttributeDesc {
    fn from(value: &[u8]) -> Self {
        let parsed_tuple = Tuple::parse_binary(&vec![
            TupleValueType::Int, // db_object_id
            TupleValueType::Int, // id
            TupleValueType::VarChar(u16::MAX), // name
            TupleValueType::SmallInt, // data_type
            TupleValueType::SmallInt, // length (where applicable, otherwhise don't care)
            TupleValueType::SmallInt, // integrity_constraint_flags
        ], value);
        let table_ref = match parsed_tuple.values[0] {
            Some(TupleValue::Int(id)) => id as u32,
            _ => unreachable!()
        };
        let id = match parsed_tuple.values[1] {
            Some(TupleValue::Int(id)) => id,
            _ => unreachable!()
        };
        let name = match parsed_tuple.values[2] {
            Some(TupleValue::String(ref name)) => name.clone(),
            _ => unreachable!()
        };
        let data_type = match parsed_tuple.values[3] {
            Some(TupleValue::SmallInt(data_type)) => {
                match data_type {
                    0 => TupleValueType::BigInt,
                    1 => TupleValueType::VarChar(parsed_tuple.values[4].as_ref().unwrap().as_small_int() as u16),
                    2 => TupleValueType::Int,
                    3 => TupleValueType::SmallInt,
                    _ => unreachable!()
                }
            },
            _ => unreachable!()
        };
        let nullable = match parsed_tuple.values[5] {
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
            TupleValueType::SmallInt => (3, 0)
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
    sp_segment: SlottedPageSegment<B>,
}

impl<B: BufferManager> AttributeCatalogSegment<B> {
    fn new(buffer_manager: B) -> AttributeCatalogSegment<B> {
        AttributeCatalogSegment {
            sp_segment: SlottedPageSegment::new(buffer_manager, ATTRIBUTE_CATALOG_SEGMENT_ID, ATTRIBUTE_CATALOG_SEGMENT_ID + 1)
        }
    }

    fn get_attributes_by_db_object(&self, db_object_id: u32) -> Vec<AttributeDesc> {
        self.sp_segment.clone().scan(|data| {
            let attribute_desc = AttributeDesc::from(data);
            if attribute_desc.table_ref == db_object_id {
                Some(attribute_desc)
            } else {
                None
            }
        }).map(|f| f.unwrap().1).collect()
    }

    fn get_attribute_by_db_object_and_name(&self, db_object_id: u32, name: &str) -> Option<AttributeDesc> {
        self.sp_segment.clone().scan(|data| {
            let attribute_desc = AttributeDesc::from(data);
            if attribute_desc.table_ref == db_object_id && attribute_desc.name == name {
                Some(attribute_desc)
            } else {
                None
            }
        }).next().map(|f| f.unwrap().1)
    }

    fn insert_attribute(&self, attribute: AttributeDesc) -> Result<(), B::BError> {
        let tuple = Tuple::from(&attribute);
        let mut data = vec![0; tuple.calculate_binary_length()];
        tuple.write_binary(&mut data);
        self.sp_segment.insert_record(&data)?;
        Ok(())
    }

    fn update_attribute(&self, attribute: AttributeDesc) -> Result<(), B::BError> {
        let (tid, _) = self.sp_segment.clone().scan(|data| {
            let attribute_desc = AttributeDesc::from(data);
            if attribute_desc.id == attribute.id {
                Some(attribute_desc)
            } else {
                None
            }
        }).next().unwrap().unwrap();
        let tuple = Tuple::from(&attribute);
        let buffer: Box<[u8]> = tuple.into();
        self.sp_segment.write_record(tid, &buffer)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::{storage::{page::PAGE_SIZE, buffer_manager::mock::MockBufferManager}};


    #[test]
    fn test_catalog_segment_db_object_by_id() {
        let buffer_manager = MockBufferManager::new(PAGE_SIZE);
        let catalog_segment = DbObjectCatalogSegment::new(buffer_manager.clone());
        let db_object_desc = DbObjectDesc { id: 0, name: "db_object".to_string(), class_type: DbObjectType::Relation, segment_id: 1 };
        catalog_segment.insert_db_object(&db_object_desc).unwrap();
        let db_object_desc = catalog_segment.get_db_object_by_id(0).unwrap();
        assert_eq!(db_object_desc.name, "db_object");
        assert_eq!(db_object_desc.class_type, crate::catalog::DbObjectType::Relation);
        assert_eq!(db_object_desc.segment_id, 1);
    }

    #[test]
    fn test_catalog_segment_db_object_by_name() {
        let buffer_manager = MockBufferManager::new(PAGE_SIZE);
        let catalog_segment = DbObjectCatalogSegment::new(buffer_manager.clone());
        let db_object_desc = DbObjectDesc { id: 0, name: "db_object".to_string(), class_type: DbObjectType::Relation, segment_id: 1 };
        catalog_segment.insert_db_object(&db_object_desc).unwrap();
        let db_object_desc = catalog_segment.find_db_object_by_name(DbObjectType::Relation, "db_object").unwrap();
        assert_eq!(db_object_desc.id, 0);
        assert_eq!(db_object_desc.name, "db_object");
        assert_eq!(db_object_desc.class_type, crate::catalog::DbObjectType::Relation);
        assert_eq!(db_object_desc.segment_id, 1);
    }

    fn test_catalog_segment_attributes_by_db_object() {
        let buffer_manager = MockBufferManager::new(PAGE_SIZE);
        let catalog_segment = DbObjectCatalogSegment::new(buffer_manager.clone());
        let attribute_catalog_segment = AttributeCatalogSegment::new(buffer_manager.clone());
        let db_object_desc = DbObjectDesc { id: 0, name: "db_object".to_string(), class_type: DbObjectType::Relation, segment_id: 1 };
        catalog_segment.insert_db_object(&db_object_desc).unwrap();
        let attribute_descs = vec![
            AttributeDesc { id: 0, name: "attribute".to_string(), data_type: TupleValueType::VarChar(232), nullable: false, table_ref: 0 },
            AttributeDesc { id: 1, name: "abc".to_string(), data_type: TupleValueType::BigInt, nullable: true, table_ref: 0 },
            AttributeDesc { id: 2, name: "cba".to_string(), data_type: TupleValueType::SmallInt, nullable: true, table_ref: 1 }
        ];
        attribute_catalog_segment.insert_attribute(attribute_descs[0].clone()).unwrap();
        attribute_catalog_segment.insert_attribute(attribute_descs[1].clone()).unwrap();
        attribute_catalog_segment.insert_attribute(attribute_descs[2].clone()).unwrap();
        let attributes = attribute_catalog_segment.get_attributes_by_db_object(0);
        assert_eq!(attributes.len(), 2);
        for attribute in attribute_descs {
            if attribute.table_ref == 0 {
                assert!(attributes.contains(&attribute));
            }
        }
    }

    fn test_catalog_segment_attribute_by_db_object_and_name() {
        let buffer_manager = MockBufferManager::new(PAGE_SIZE);
        let catalog_segment = DbObjectCatalogSegment::new(buffer_manager.clone());
        let attribute_catalog_segment = AttributeCatalogSegment::new(buffer_manager.clone());
        let db_object_desc = DbObjectDesc { id: 0, name: "db_object".to_string(), class_type: DbObjectType::Relation, segment_id: 1 };
        catalog_segment.insert_db_object(&db_object_desc).unwrap();
        let attribute_desc = AttributeDesc { id: 0, name: "attribute".to_string(), data_type: TupleValueType::Int, nullable: false, table_ref: 0 };
        attribute_catalog_segment.insert_attribute(attribute_desc.clone()).unwrap();
        let attribute_desc = attribute_catalog_segment.get_attribute_by_db_object_and_name(0, "attribute").unwrap();
        assert_eq!(attribute_desc.id, 0);
        assert_eq!(attribute_desc.name, "attribute");
        assert_eq!(attribute_desc.data_type, TupleValueType::Int);
        assert_eq!(attribute_desc.nullable, false);
        assert_eq!(attribute_desc.table_ref, 0);
    }
}