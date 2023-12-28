mod free_space_inventory;
mod btree_segment;
mod slotted_page_segment;
mod heap;
mod index;
pub mod tuple;

pub use self::btree_segment::*;
pub use self::slotted_page_segment::*;
pub use self::heap::*;


/*
    TODO: Abstract over access layer: Have an abstract Sorted/Unsorted Segment and 
          create Row/Column-Wise access methods (maybe with (slow) default implementations 
          if you only implement one of them).

 */

