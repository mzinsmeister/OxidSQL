use crate::{access::{tuple::Tuple, HeapStorage, SlottedPageHeapStorage, SlottedPageSegment}, storage::buffer_manager::BufferManager, types::TupleValueType};

use super::{AttributeDesc, ATTRIBUTE_CATALOG_SEGMENT_ID};

pub(super) struct AttributeCatalogSegment<B: BufferManager> {
    sp_segment: SlottedPageHeapStorage<B>,
}

impl<B: BufferManager> AttributeCatalogSegment<B> {
    pub fn new(buffer_manager: B) -> AttributeCatalogSegment<B> {
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

    pub fn get_all_attributes(&self) -> Result<Vec<AttributeDesc>, B::BError> {
        self.sp_segment.scan_all(|_| true)?
            .map(|f| f.map(|t| AttributeDesc::from(&t.1)))
            .collect()
    }

    pub fn get_max_id(&self) -> Result<u32, B::BError> {
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

    pub fn get_attributes_by_db_object(&self, db_object_id: u32) -> Result<Vec<AttributeDesc>, B::BError> {
        self.sp_segment.scan_all(|data| {
            let attribute_desc = AttributeDesc::from(data);
            attribute_desc.table_ref == db_object_id
        })?.map(|f| f.map(|t| AttributeDesc::from(&t.1)))
        .collect()
    }

    #[allow(dead_code)]
    pub fn get_attribute_by_db_object_and_name(&self, db_object_id: u32, name: &str) -> Result<Option<AttributeDesc>, B::BError> {
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

    pub fn insert_attribute(&self, attribute: &AttributeDesc) -> Result<(), B::BError> {
        let tuple = Tuple::from(attribute);
        self.sp_segment.insert_tuples(&mut [tuple])?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn update_attribute(&self, attribute: AttributeDesc) -> Result<(), B::BError> {
        let (tid, _) = self.sp_segment.scan_all(|data| {
            let attribute_desc = AttributeDesc::from(data);
            attribute_desc.id == attribute.id
        })?.next().unwrap().unwrap();
        let tuple = Tuple::from(&attribute);
        self.sp_segment.update_tuple(tid, tuple)?;
        Ok(())
    }
}


