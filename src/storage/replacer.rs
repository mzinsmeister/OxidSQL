use std::option::Option;

use super::{page::PageId, buffer_pool_manager::RefCountAccessor};

pub trait Replacer {
  fn find_victim(&mut self, refcount_accessor: &RefCountAccessor) -> Option<PageId>;
  fn use_page(&mut self, page_id: PageId);
  fn load_page(&mut self, page_id: PageId);
  fn swap_pages(&mut self, old_page: PageId, new_page: PageId);
  fn clear(&mut self);
}