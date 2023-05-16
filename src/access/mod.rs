use crate::storage::buffer_manager::BufferManager;
use crate::types::RelationTID;

mod free_space_inventory;
mod btree_segment;
mod slotted_page_segment;
pub mod tuple;

pub use self::btree_segment::*;
pub use self::slotted_page_segment::*;

/*
    TODO: Abstract over access layer: Have an abstract Sorted/Unsorted Segment and 
          create Row/Column-Wise access methods (maybe with (slow) default implementations 
          if you only implement one of them).

 */

pub trait HeapSegment<B: BufferManager> {
    fn get_record<T, F: Fn(&[u8]) -> T>(&self, tid: RelationTID, operation: F) -> Result<T, B::BError>;
    fn insert_record(&self, data: &[u8]) -> Result<RelationTID, B::BError>;
    fn update_record(&self, tid: RelationTID, data: &[u8]) -> Result<(), B::BError>;
}

pub trait IndexSegment {

}