use std::{collections::HashMap, sync::{Arc}};

use parking_lot::RwLock;

use crate::storage::replacer::can_page_be_replaced;

use super::{page::{PageId, Page}, replacer::Replacer, buffer_manager::PageType};


#[derive(Debug)]
struct ClockPageInfo {
  clock_used: bool,
  page_id: PageId,
  page: PageType
}

#[derive(Debug)]
pub(crate) struct ClockReplacer {
  // Would probably be more elegant to have a replacer specific struct included with every BufferFrame
  // But this is fine for now. At least guarantees O(1) lookup time for a specific Page and 
  // the Buffer Pool Manager is the only component touching the Replacer API
  page_clock_pos_mapping: HashMap<PageId, usize>,
  clock: Vec<ClockPageInfo>,
  clock_position: usize
}

impl ClockReplacer {
  pub fn new() -> Self {
    // Don't care about capacities for now. They will barely affect performance or memory use
    Self { page_clock_pos_mapping: HashMap::new(), clock: Vec::new(), clock_position: 0 }
  }
}

impl Replacer for ClockReplacer {

  fn find_victim(&mut self) -> Option<PageId> {
    assert_eq!(self.clock.len(), self.page_clock_pos_mapping.len());
    let mut victim = Option::None;
    let mut n_iterated = 0;
    // We do at best two sweeps. First sweep takes into account clock status
    // Second basically just automatically takes the first that isn't currently referenced
    while n_iterated < self.clock.len() * 2 && victim.is_none() {
      let element = &mut self.clock[self.clock_position];
      if !element.clock_used && can_page_be_replaced(&element.page) {
        victim = Option::Some(element.page_id);
      }
      element.clock_used = false;
      self.clock_position = (self.clock_position + 1) % self.clock.len();
      n_iterated += 1;
    }
    assert_eq!(self.clock.len(), self.page_clock_pos_mapping.len());
    victim
  }

  fn load_page(&mut self, page_id: PageId, page: PageType) {
    assert_eq!(self.clock.len(), self.page_clock_pos_mapping.len());
    // Where we add this really doesn't matter since this will really only ever happen
    // Before the buffer pool is full anyway which is before find_victim is called once anyway
    self.clock.push(ClockPageInfo { clock_used: true, page_id: page_id, page});
    self.page_clock_pos_mapping.insert(page_id, self.clock.len() - 1);
    assert_eq!(self.clock.len(), self.page_clock_pos_mapping.len());
  }

  fn swap_pages(&mut self, old_page_id: PageId, new_page_id: PageId, new_page: Arc<RwLock<Page>>) {
    assert_eq!(self.clock.len(), self.page_clock_pos_mapping.len());
    let pos = self.page_clock_pos_mapping.remove(&old_page_id).unwrap();
    self.clock[pos] = ClockPageInfo { page_id: new_page_id, page: new_page, clock_used: true };
    if self.page_clock_pos_mapping.insert(new_page_id, pos).is_some() {
      panic!("clock already contained swapped page");
    }
    assert_eq!(self.clock.len(), self.page_clock_pos_mapping.len());
  }
  
  fn use_page(&mut self, page: PageId) {
    let pos = self.page_clock_pos_mapping[&page];
    self.clock[pos].clock_used = true;
  }

  fn has_page(&self, page: PageId) -> bool {
    self.page_clock_pos_mapping.contains_key(&page)
  }

  fn clear(&mut self) {
    self.page_clock_pos_mapping = HashMap::new();
    self.clock = Vec::new();
    self.clock_position = 0;
  }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parking_lot::RwLock;

    use crate::storage::page::Page;


  #[test]
  fn single_page_test() {
    use crate::storage::{replacer::Replacer, page::PageId};

    use super::ClockReplacer;

    let mut testee = ClockReplacer::new();
    let page: Arc<RwLock<Page>> = Arc::new(RwLock::new(Page::new(PageId::new(1, 1))));
    testee.load_page(PageId::new(1, 1), page.clone());
    assert!(testee.has_page(PageId::new(1,1)), "Replacer should have page now");
  }

  #[test]
  fn swap_pages_test() {
    use crate::storage::{replacer::Replacer, page::PageId};

    use super::ClockReplacer;

    let mut testee = ClockReplacer::new();
    let page: Arc<RwLock<Page>> = Arc::new(RwLock::new(Page::new(PageId::new(1, 1))));
    testee.load_page(PageId::new(1, 1), page.clone());
    let page2: Arc<RwLock<Page>> = Arc::new(RwLock::new(Page::new(PageId::new(1, 1))));
    testee.swap_pages(PageId::new(1,1), PageId::new(2, 2), page2.clone());
    assert!(!testee.has_page(PageId::new(1, 1)), "Replacer should no longer have old page");
    assert!(testee.has_page(PageId::new(2, 2)), "Replacer should have page now");
  }

  #[test]
  fn find_victim_single_test() {
    use crate::storage::{replacer::Replacer, page::PageId};

    use super::ClockReplacer;

    let mut testee = ClockReplacer::new();
    let page: Arc<RwLock<Page>> = Arc::new(RwLock::new(Page::new(PageId::new(1, 1))));
    testee.load_page(PageId::new(1, 1), page.clone());
    assert_eq!(testee.find_victim(), Some(PageId::new(1, 1)));
  }


  #[test]
  fn find_victim_multiple_test() {
    use crate::storage::{replacer::Replacer, page::PageId};

    use super::ClockReplacer;

    let mut testee = ClockReplacer::new();
    let page_id = PageId::new(1, 1);
    let page1: Arc<RwLock<Page>> = Arc::new(RwLock::new(Page::new(PageId::new(1, 1))));
    testee.load_page(page_id, page1.clone());
    let page_id = PageId::new(1, 2);
    let page2: Arc<RwLock<Page>> = Arc::new(RwLock::new(Page::new(PageId::new(1, 2))));
    testee.load_page(page_id, page2.clone());
    let page_id = PageId::new(2, 3);
    let page3: Arc<RwLock<Page>> = Arc::new(RwLock::new(Page::new(PageId::new(2, 3))));
    testee.load_page(page_id, page3.clone());
    assert_eq!(testee.find_victim(), Some(PageId::new(1, 1)));
    assert_eq!(testee.find_victim(), Some(PageId::new(1, 2)));
    assert_eq!(testee.find_victim(), Some(PageId::new(2, 3)));
  }

  #[test]
  fn find_victim_multiple_with_references_test() {
    use crate::storage::{replacer::Replacer, page::PageId};

    use super::ClockReplacer;

    let mut testee = ClockReplacer::new();
    let page_id = PageId::new(1, 1);
    let page1: Arc<RwLock<Page>> = Arc::new(RwLock::new(Page::new(page_id)));
    testee.load_page(page_id, page1.clone());
    let page_id = PageId::new(1, 2);
    let page2: Arc<RwLock<Page>> = Arc::new(RwLock::new(Page::new(page_id)));
    testee.load_page(page_id, page2.clone());
    let page_id = PageId::new(2, 3);
    let page3: Arc<RwLock<Page>> = Arc::new(RwLock::new(Page::new(page_id)));
    testee.load_page(page_id, page3.clone());

    let _page1_2 = page1.clone();
    assert_eq!(testee.find_victim(), Some(PageId::new(1, 2)));
    assert_eq!(testee.find_victim(), Some(PageId::new(2, 3)));
    assert_eq!(testee.find_victim(), Some(PageId::new(1, 2)));
  }
}