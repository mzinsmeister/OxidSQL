use std::{sync::Arc};

use crate::{access::{SlottedPageSegment, tuple::Tuple}, storage::buffer_manager::BufferManager, types::{TupleValueType, TupleValue}};

pub struct DbObjectRef {
    table_id: u16
}

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

struct Catalog<B: BufferManager> {
    cache: Arc<CatalogCache<B>>,
}

struct CatalogCache<B:BufferManager> {
    catalog_sp_segment: SlottedPageSegment<B>
    // TODO: Agressively Cache stuff here
}

#[repr(u16)]
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

struct DbObjectDesc {
    id: u32,
    name: String,
    class_type: DbObjectType,
    segment_id: u16, //convention: the FSI segment will always get the next segment id if the object requires one
}

impl DbObjectDesc {
    fn parse_tuple(tuple: &[u8]) -> DbObjectDesc {
        let parsed_tuple = Tuple::parse_binary(vec![
            TupleValueType::Int,
            TupleValueType::String,
            TupleValueType::SmallInt,
            TupleValueType::SmallInt
        ], tuple);
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

struct DbObjectCatalogSegment<B: BufferManager> {
    sp_segment: SlottedPageSegment<B>,
}

impl<B: BufferManager> DbObjectCatalogSegment<B> {
    fn get_db_object_by_id(&self, id: u32) -> Option<DbObjectDesc> {
        unimplemented!()
    }

    fn find_db_object_by_name(&self, obj_type: DbObjectType, name: &str) -> Option<DbObjectDesc> {
        unimplemented!()
    }
}