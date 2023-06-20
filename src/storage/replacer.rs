use std::{option::Option, sync::Arc, fmt::Debug};

use super::{page::{PageId, Page}, buffer_manager::PageType};

#[cfg(test)]
use mockall::automock;
use parking_lot::RwLock;

#[cfg_attr(test, automock)]
pub trait Replacer: Debug {
  fn find_victim<'a>(&mut self) -> Option<PageId>;
  fn use_page(&mut self, page_id: PageId);
  fn has_page(&self, page_id: PageId) -> bool;
  fn load_page(&mut self, page_id: PageId, page: PageType);
  fn swap_pages(&mut self, old_page_id: PageId, new_page_id: PageId, new_page: Arc<RwLock<Page>>);
  fn clear(&mut self);
}

pub fn can_page_be_replaced(page: &PageType) -> bool {
  // One reference from the buffer pool manager, one from the replacer
  Arc::strong_count(&page) == 2
}
