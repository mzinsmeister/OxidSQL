use std::option::Option;

use super::{page::PageId, buffer_manager::RefCountAccessor};

#[cfg(test)]
use mockall::automock;

#[cfg_attr(test, automock)]
pub trait Replacer {
  fn find_victim<'a>(&mut self, refcount_accessor: &dyn RefCountAccessor) -> Option<PageId>;
  fn use_page(&mut self, page_id: PageId);
  fn has_page(&self, page_id: PageId) -> bool;
  fn load_page(&mut self, page_id: PageId);
  fn swap_pages(&mut self, old_page: PageId, new_page: PageId);
  fn clear(&mut self);
}