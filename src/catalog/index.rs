use crate::{access::{tuple::Tuple, HeapStorage, SlottedPageHeapStorage, SlottedPageSegment}, storage::buffer_manager::BufferManager, types::{RelationTID, TupleValue, TupleValueType}};

use super::{AttributeRef, DbObjectRef, IndexType, INDEX_CATALOG_SEGMENT_ID};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct IndexDetailsDesc {
    pub db_obj_id: DbObjectRef,
    pub indexed_db_obj_id: DbObjectRef,
    pub index_type: IndexType,
    pub attributes: Vec<AttributeRef>,
}

impl From<&Tuple> for IndexDetailsDesc {

    fn from(value: &Tuple) -> Self {
        let db_obj_id = match value.values[0] {
            Some(TupleValue::Int(db_obj_id)) => db_obj_id as DbObjectRef,
            _ => unreachable!()
        };
        let indexed_db_obj_id = match value.values[1] {
            Some(TupleValue::Int(indexed_db_obj_id)) => indexed_db_obj_id as DbObjectRef,
            _ => unreachable!()
        };
        let index_type = match value.values[2] {
            Some(TupleValue::SmallInt(index_type)) => IndexType::from_u16(index_type as u16),
            _ => unreachable!()
        };
        let attributes = match value.values[5] {
            Some(TupleValue::ByteArray(ref attributes_bytes)) => {
                let mut attributes = Vec::with_capacity(attributes_bytes.len() / std::mem::size_of::<u32>());
                for i in 0..attributes_bytes.len() / std::mem::size_of::<u32>() {
                    let mut bytes = [0; 4];
                    bytes.copy_from_slice(&attributes_bytes[i * std::mem::size_of::<u32>()..(i + 1) * std::mem::size_of::<u32>()]);
                    attributes.push(u32::from_ne_bytes(bytes));
                }
                attributes
            },
            _ => unreachable!()
        };
        IndexDetailsDesc { db_obj_id, indexed_db_obj_id, index_type, attributes }
    }
}

impl From<&IndexDetailsDesc> for Tuple {
    fn from(value: &IndexDetailsDesc) -> Self {
        let mut bytes = Vec::with_capacity(value.attributes.len() * std::mem::size_of::<u32>());
        for attribute_id in &value.attributes {
            bytes.extend_from_slice(&attribute_id.to_ne_bytes());
        }
        Tuple::new(vec![
            Some(TupleValue::Int(value.db_obj_id as i32)),
            Some(TupleValue::Int(value.indexed_db_obj_id as i32)),
            Some(TupleValue::SmallInt(value.index_type as u16 as i16)),
            Some(TupleValue::ByteArray(bytes.into_boxed_slice())),
        ])
    }
}

pub(super) struct IndexCatalogSegment<B: BufferManager> {
    sp_segment: SlottedPageHeapStorage<B>,
}

impl<B: BufferManager> IndexCatalogSegment<B> {
    pub fn new(buffer_manager: B) -> IndexCatalogSegment<B> {
        let attributes = vec![
            TupleValueType::Int, // db_object_id
            TupleValueType::Int, // indexed_db_object_id
            TupleValueType::SmallInt, // index_type
            TupleValueType::VarBinary(u16::MAX), // attributes
        ];
        let segment = SlottedPageSegment::new(buffer_manager, INDEX_CATALOG_SEGMENT_ID, INDEX_CATALOG_SEGMENT_ID + 1);
        IndexCatalogSegment {
            sp_segment: SlottedPageHeapStorage::new(segment, attributes)
        }
    }

    
    pub fn find_first_index_desc<F: Fn(&IndexDetailsDesc) -> bool>(&self, filter: F) -> Result<Option<(RelationTID, IndexDetailsDesc)>, B::BError>{
        // ParsingDbObjectDesc twice. Could be more efficient.
        let mut scan = self.sp_segment.scan_all(|data| {
            let db_object_desc = IndexDetailsDesc::from(data);
            filter(&db_object_desc)
        })?;
        if let Some(first) = scan.next() {
            first.map(|f| Some((f.0, IndexDetailsDesc::from(&f.1))))
        } else {
            Ok(None)
        }
    }

    #[allow(dead_code)]
    pub fn get_by_db_obj_id(&self, db_obj_id: u32) -> Result<Option<IndexDetailsDesc>, B::BError> {
        self.find_first_index_desc(|index_desc| index_desc.db_obj_id == db_obj_id).map(|e| e.map(|(_, d)| d))
    }

    pub fn get_by_indexed_db_obj_id(&self, indexed_db_obj_id: u32) -> Result<Vec<IndexDetailsDesc>, B::BError> {
        self.sp_segment.scan_all(|data| {
            let db_object_desc = IndexDetailsDesc::from(data);
            db_object_desc.indexed_db_obj_id == indexed_db_obj_id
        })?.map(|f| f.map(|t| IndexDetailsDesc::from(&t.1)))
        .collect()
    }

    pub fn insert_index(&self, index: &IndexDetailsDesc) -> Result<(), B::BError> {
        let tuple = Tuple::from(index);
        self.sp_segment.insert_tuples(&mut [tuple])?;
        Ok(())
    }
}