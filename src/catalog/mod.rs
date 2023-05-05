use std::{sync::Arc};

use crate::{access::{SlottedPageSegment, tuple::Tuple}, storage::buffer_manager::BufferManager, types::{TupleValueType, TupleValue}};

type DbObjectRef = u16;

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

struct Catalog<B: BufferManager> {
    cache: Arc<CatalogCache<B>>,
}

struct CatalogCache<B:BufferManager> {
    catalog_sp_segment: SlottedPageSegment<B>
    // TODO: Agressively Cache stuff here
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
    segment_id: u16, //convention: the FSI segment will always get the next segment id if the object requires one
}

impl From<&[u8]> for DbObjectDesc {

    fn from(value: &[u8]) -> Self {
        let parsed_tuple = Tuple::parse_binary(vec![
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
            Some(TupleValue::Int(value.id as i32)),
            Some(TupleValue::String(value.name.clone())),
            Some(TupleValue::SmallInt(value.class_type as i16)),
            Some(TupleValue::SmallInt(value.segment_id as i16))
        ])
    }
}

struct DbObjectCatalogSegment<B: BufferManager> {
    sp_segment: SlottedPageSegment<B>,
}

impl<B: BufferManager> DbObjectCatalogSegment<B> {
    fn new(buffer_manager: Arc<B>) -> DbObjectCatalogSegment<B> {
        DbObjectCatalogSegment {
            sp_segment: SlottedPageSegment::new(buffer_manager, DB_OBJECT_CATALOG_SEGMENT_ID, DB_OBJECT_CATALOG_SEGMENT_ID + 1)
        }
    }

    fn find_first_db_object<F: Fn(&DbObjectDesc) -> bool>(&self, filter: F) -> Option<DbObjectDesc> {
        let mut scan = self.sp_segment.scan(|data| {
            let db_object_desc = DbObjectDesc::from(data);
            if filter(&db_object_desc) {
                Some(db_object_desc)
            } else {
                None
            }
        });
        scan.next().map(|f| f.unwrap().1)
    }

    fn get_db_object_by_id(&self, id: u32) -> Option<DbObjectDesc> {
        self.find_first_db_object(|db_object_desc| db_object_desc.id == id)
    }

    fn find_db_object_by_name(&self, obj_type: DbObjectType, name: &str) -> Option<DbObjectDesc> {
        self.find_first_db_object(|db_object_desc| db_object_desc.class_type == obj_type && db_object_desc.name == name)
    }

    fn insert_db_object(&self, db_object: &DbObjectDesc) {
        let tuple = Tuple::from(db_object);
        let mut data = vec![0; tuple.calculate_binary_length()];
        tuple.write_binary(&mut data);
        self.sp_segment.insert_record(&data).unwrap();
    }
}

struct AttributeDesc {
    id: u32,
    name: String,
    data_type: TupleValueType,
    length: u32,
    nullable: bool,
    /*default_value: Option<String>,*/
    table_ref: DbObjectRef,
}

struct AttributeCatalogSegment<B: BufferManager> {
    sp_segment: SlottedPageSegment<B>,
}

impl<B: BufferManager> AttributeCatalogSegment<B> {
    fn new(buffer_manager: Arc<B>) -> AttributeCatalogSegment<B> {
        AttributeCatalogSegment {
            sp_segment: SlottedPageSegment::new(buffer_manager, ATTRIBUTE_CATALOG_SEGMENT_ID, ATTRIBUTE_CATALOG_SEGMENT_ID + 1)
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::*;
    use crate::{storage::{page::PAGE_SIZE, buffer_manager::mock::MockBufferManager}};


    #[test]
    fn test_catalog_segment_db_object_by_id() {
        let buffer_manager = Arc::new(MockBufferManager::new(PAGE_SIZE));
        let catalog_segment = DbObjectCatalogSegment::new(buffer_manager.clone());
        let db_object_desc = DbObjectDesc { id: 0, name: "db_object".to_string(), class_type: DbObjectType::Relation, segment_id: 1 };
        catalog_segment.insert_db_object(&db_object_desc);
        let db_object_desc = catalog_segment.get_db_object_by_id(0).unwrap();
        assert_eq!(db_object_desc.name, "db_object");
        assert_eq!(db_object_desc.class_type, crate::catalog::DbObjectType::Relation);
        assert_eq!(db_object_desc.segment_id, 1);
    }

    #[test]
    fn test_catalog_segment_db_object_by_name() {
        let buffer_manager = Arc::new(MockBufferManager::new(PAGE_SIZE));
        let catalog_segment = DbObjectCatalogSegment::new(buffer_manager.clone());
        let db_object_desc = DbObjectDesc { id: 0, name: "db_object".to_string(), class_type: DbObjectType::Relation, segment_id: 1 };
        catalog_segment.insert_db_object(&db_object_desc);
        let db_object_desc = catalog_segment.find_db_object_by_name(DbObjectType::Relation, "db_object").unwrap();
        assert_eq!(db_object_desc.id, 0);
        assert_eq!(db_object_desc.name, "db_object");
        assert_eq!(db_object_desc.class_type, crate::catalog::DbObjectType::Relation);
        assert_eq!(db_object_desc.segment_id, 1);
    }
}