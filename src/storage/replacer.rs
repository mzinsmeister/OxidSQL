use std::option::Option;

use super::{page::PageId, buffer_pool_manager::RefCountAccessor};

#[cfg(test)]
use mockall::automock;

#[cfg_attr(test, automock)]
pub trait Replacer {
  fn find_victim<'a>(&mut self, refcount_accessor: &'a RefCountAccessor<'a>) -> Option<PageId>;
  fn use_page(&mut self, page_id: PageId);
  fn load_page(&mut self, page_id: PageId);
  fn swap_pages(&mut self, old_page: PageId, new_page: PageId);
  fn clear(&mut self);
}