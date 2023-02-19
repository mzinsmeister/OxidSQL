mod free_space_inventory;
pub mod btree;
pub mod slotted_page_segment;

/*
    TODO: Abstract over access layer: Have an abstract Sorted/Unsorted Segment and 
          create Row/Column-Wise access methods (maybe with (slow) default implementations 
          if you only implement one of them).

 */