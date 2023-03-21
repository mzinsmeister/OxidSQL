use crate::{storage::buffer_manager::BufferManagerError, types::RelationTID};

mod free_space_inventory;
mod btree_segment;
mod slotted_page_segment;

pub use self::btree_segment::*;
pub use self::slotted_page_segment::*;

/*
    TODO: Abstract over access layer: Have an abstract Sorted/Unsorted Segment and 
          create Row/Column-Wise access methods (maybe with (slow) default implementations 
          if you only implement one of them).

 */

pub trait HeapSegment {
    fn get_record<T, F: Fn(&[u8]) -> T>(&self, tid: RelationTID, operation: F) -> Result<T, BufferManagerError>;
    fn insert_record(&self, data: &[u8]) -> Result<RelationTID, BufferManagerError>;
    fn update_record(&self, tid: RelationTID, data: &[u8]) -> Result<(), BufferManagerError>;
}

pub trait IndexSegment {

}