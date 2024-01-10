use crate::{access::{tuple::Tuple, SlottedPageHeapStorage, SlottedPageSegment, HeapStorage}, types::{TupleValue, TupleValueType, RelationTID}, storage::{page::SegmentId, buffer_manager::BufferManager}};

use super::DB_OBJECT_CATALOG_SEGMENT_ID;

#[repr(u16)]
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum DbObjectType {
    Relation = 0,
    Index = 1,
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
pub(super) struct DbObjectDesc {
    pub id: u32,
    pub name: String,
    pub class_type: DbObjectType,
    pub segment_id: SegmentId,
    pub fsi_segment_id: Option<SegmentId>,
    pub sample_segment_id: Option<SegmentId>,
    pub sample_fsi_segment_id: Option<SegmentId>,
}

impl From<&Tuple> for DbObjectDesc {

    fn from(value: &Tuple) -> Self {
        let id = match value.values[0] {
            Some(TupleValue::Int(id)) => id,
            _ => unreachable!()
        };
        let name = match value.values[1] {
            Some(TupleValue::String(ref name)) => name.clone(),
            _ => unreachable!()
        };
        let class_type = match value.values[2] {
            Some(TupleValue::SmallInt(class_type)) => DbObjectType::from_u16(class_type as u16),
            _ => unreachable!()
        };
        let segment_id = match value.values[3] {
            Some(TupleValue::Int(segment_id)) => segment_id as u32,
            _ => unreachable!()
        };
        let fsi_segment_id = match value.values[4] {
            Some(TupleValue::Int(segment_id)) => Some(segment_id as u32),
            None => None,
            _ => unreachable!()
        };
        let sample_id = match value.values[5] {
            Some(TupleValue::Int(segment_id)) => Some(segment_id as u32),
            None => None,
            _ => unreachable!()
        };
        let sample_fsi_id = match value.values[6] {
            Some(TupleValue::Int(segment_id)) => Some(segment_id as u32),
            None => None,
            _ => unreachable!()
        };
        DbObjectDesc { id: id as u32, name, class_type, segment_id: segment_id, fsi_segment_id: fsi_segment_id, sample_segment_id: sample_id, sample_fsi_segment_id: sample_fsi_id }
    }
}

impl From<&DbObjectDesc> for Tuple {
    fn from(value: &DbObjectDesc) -> Self {
        Tuple::new(vec![
            Some(TupleValue::Int(value.id as i32)), // id
            Some(TupleValue::String(value.name.clone())), // name
            Some(TupleValue::SmallInt(value.class_type as i16)), // data_type
            Some(TupleValue::Int(value.segment_id as i32)), // segment_id
            value.fsi_segment_id.map(|v| TupleValue::Int(v as i32)), // fsi_segment_id
            value.sample_segment_id.map(|v| TupleValue::Int(v as i32)), // sample_segment_id
            value.sample_fsi_segment_id.map(|v| TupleValue::Int(v as i32)) // sample_fsi_segment_id
        ])
    }
}

pub(super) struct DbObjectCatalogSegment<B: BufferManager> {
    sp_segment: SlottedPageHeapStorage<B>,
}

impl<B: BufferManager> DbObjectCatalogSegment<B> {
    pub fn new(buffer_manager: B) -> DbObjectCatalogSegment<B> {
        let attributes = vec![
            TupleValueType::Int,
            TupleValueType::VarChar(u16::MAX),
            TupleValueType::SmallInt,
            TupleValueType::Int,
            TupleValueType::Int,
            TupleValueType::Int,
            TupleValueType::Int
        ];
        let segment = SlottedPageSegment::new(buffer_manager, DB_OBJECT_CATALOG_SEGMENT_ID, DB_OBJECT_CATALOG_SEGMENT_ID + 1);
        DbObjectCatalogSegment {
            sp_segment: SlottedPageHeapStorage::new(segment, attributes)
        }
    }

    pub fn get_max_id_and_segment_id(&self) -> Result<(u32, u32), B::BError> {
        self.sp_segment.scan_all(|_| true)?
            .map(|f| f.map(|t| {
                let t_desc = DbObjectDesc::from(&t.1);
                let max_segment_id = t_desc.segment_id
                    .max(t_desc.fsi_segment_id.unwrap_or(0))
                    .max(t_desc.sample_segment_id.unwrap_or(0))
                    .max(t_desc.sample_fsi_segment_id.unwrap_or(0));
                (t_desc.id, max_segment_id)
            }))
            .fold(Ok((0,0)), |acc, id| {
                match (acc, id) {
                    (Ok(acc), Ok((id, segment_id))) => Ok((acc.0.max(id), acc.1.max(segment_id))),
                    (Err(e), _) => Err(e),
                    (_, Err(e)) => Err(e)
                }
            })
    }

    pub fn find_first_db_object<F: Fn(&DbObjectDesc) -> bool>(&self, filter: F) -> Result<Option<(RelationTID, DbObjectDesc)>, B::BError>{
        // ParsingDbObjectDesc twice. Could be more efficient.
        let mut scan = self.sp_segment.scan_all(|data| {
            let db_object_desc = DbObjectDesc::from(data);
            filter(&db_object_desc)
        })?;
        if let Some(first) = scan.next() {
            first.map(|f| Some((f.0, DbObjectDesc::from(&f.1))))
        } else {
            Ok(None)
        }
    }

    #[allow(dead_code)]
    fn get_db_object_by_id(&self, id: u32) -> Result<Option<DbObjectDesc>, B::BError> {
        self.find_first_db_object(|db_object_desc| db_object_desc.id == id).map(|e| e.map(|(_, d)| d))
    }

    pub fn find_db_object_by_name(&self, obj_type: DbObjectType, name: &str) -> Result<Option<DbObjectDesc>, B::BError> {
        self.find_first_db_object(|db_object_desc| db_object_desc.class_type == obj_type && db_object_desc.name == name).map(|e| e.map(|(_, d)| d))
    }

    pub fn insert_db_object(&self, db_object: &DbObjectDesc) -> Result<(), B::BError> {
        let tuple = Tuple::from(db_object);
        self.sp_segment.insert_tuples(&mut [tuple])?;
        Ok(())
    }

    #[allow(dead_code)]
    fn update_db_object(&self, db_object: &DbObjectDesc) -> Result<(), B::BError> {
        let (tid, _) = self.find_first_db_object(|db_object_desc| db_object_desc.id == db_object.id)?.unwrap();
        let tuple = Tuple::from(db_object);
        self.sp_segment.update_tuple(tid, tuple)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::{storage::{page::PAGE_SIZE, buffer_manager::mock::MockBufferManager}, catalog::{AttributeCatalogSegment, AttributeDesc}};


    #[test]
    fn test_catalog_segment_db_object_by_id() {
        let buffer_manager = MockBufferManager::new(PAGE_SIZE);
        let catalog_segment = DbObjectCatalogSegment::new(buffer_manager.clone());
        let db_object_desc = DbObjectDesc { id: 0, name: "db_object".to_string(), class_type: DbObjectType::Relation, segment_id: 1, fsi_segment_id: Some(2), sample_segment_id: None, sample_fsi_segment_id: None };
        catalog_segment.insert_db_object(&db_object_desc).unwrap();
        let db_object_desc = catalog_segment.get_db_object_by_id(0).unwrap().unwrap();
        assert_eq!(db_object_desc.name, "db_object");
        assert_eq!(db_object_desc.class_type, crate::catalog::DbObjectType::Relation);
        assert_eq!(db_object_desc.segment_id, 1);
    }

    #[test]
    fn test_catalog_segment_db_object_by_name() {
        let buffer_manager = MockBufferManager::new(PAGE_SIZE);
        let catalog_segment = DbObjectCatalogSegment::new(buffer_manager.clone());
        let db_object_desc = DbObjectDesc { id: 0, name: "db_object".to_string(), class_type: DbObjectType::Relation, segment_id: 1, fsi_segment_id: Some(2), sample_segment_id: None, sample_fsi_segment_id: None };
        catalog_segment.insert_db_object(&db_object_desc).unwrap();
        let db_object_desc = catalog_segment.find_db_object_by_name(DbObjectType::Relation, "db_object").unwrap().unwrap();
        assert_eq!(db_object_desc.id, 0);
        assert_eq!(db_object_desc.name, "db_object");
        assert_eq!(db_object_desc.class_type, crate::catalog::DbObjectType::Relation);
        assert_eq!(db_object_desc.segment_id, 1);
    }

    #[test]
    fn test_catalog_segment_attributes_by_db_object() {
        let buffer_manager = MockBufferManager::new(PAGE_SIZE);
        let _catalog_segment = DbObjectCatalogSegment::new(buffer_manager.clone());
        let attribute_catalog_segment = AttributeCatalogSegment::new(buffer_manager.clone());
        let _db_object_desc = DbObjectDesc { id: 0, name: "db_object".to_string(), class_type: DbObjectType::Relation, segment_id: 1, fsi_segment_id: Some(2), sample_segment_id: None,  sample_fsi_segment_id: None };
        let attribute_descs = vec![
            AttributeDesc { id: 0, name: "attribute".to_string(), data_type: TupleValueType::VarChar(232), nullable: false, table_ref: 0 },
            AttributeDesc { id: 1, name: "abc".to_string(), data_type: TupleValueType::BigInt, nullable: true, table_ref: 0 },
            AttributeDesc { id: 2, name: "cba".to_string(), data_type: TupleValueType::SmallInt, nullable: true, table_ref: 1 }
        ];
        attribute_catalog_segment.insert_attribute(&attribute_descs[0]).unwrap();
        attribute_catalog_segment.insert_attribute(&attribute_descs[1]).unwrap();
        attribute_catalog_segment.insert_attribute(&attribute_descs[2]).unwrap();
        let attributes = attribute_catalog_segment.get_attributes_by_db_object(0).unwrap();
        assert_eq!(attributes.len(), 2);
        for attribute in attribute_descs {
            if attribute.table_ref == 0 {
                assert!(attributes.contains(&attribute));
            }
        }
    }

    #[test]
    fn test_catalog_segment_attribute_by_db_object_and_name() {
        let buffer_manager = MockBufferManager::new(PAGE_SIZE);
        let catalog_segment = DbObjectCatalogSegment::new(buffer_manager.clone());
        let attribute_catalog_segment = AttributeCatalogSegment::new(buffer_manager.clone());
        let db_object_desc = DbObjectDesc { id: 0, name: "db_object".to_string(), class_type: DbObjectType::Relation, segment_id: 1, fsi_segment_id: Some(2), sample_segment_id: None, sample_fsi_segment_id: None };
        catalog_segment.insert_db_object(&db_object_desc).unwrap();
        let attribute_desc = AttributeDesc { id: 0, name: "attribute".to_string(), data_type: TupleValueType::Int, nullable: false, table_ref: 0 };
        attribute_catalog_segment.insert_attribute(&attribute_desc).unwrap();
        let attribute_desc = attribute_catalog_segment.get_attribute_by_db_object_and_name(0, "attribute").unwrap().unwrap();
        assert_eq!(attribute_desc.id, 0);
        assert_eq!(attribute_desc.name, "attribute");
        assert_eq!(attribute_desc.data_type, TupleValueType::Int);
        assert_eq!(attribute_desc.nullable, false);
        assert_eq!(attribute_desc.table_ref, 0);
    }
}