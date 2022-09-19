use std::option::Option;

use super::page::PageId;

pub trait Replacer {
  fn find_victim(&mut self) -> Option<PageId>;
  fn use_page(&mut self, page_id: PageId);
  fn load_page(&mut self, page_id: PageId);
  fn swap_pages(&mut self, old_page: PageId, new_page: PageId);
  fn clear(&mut self);
}