use std::{collections::HashMap};

use super::{page::PageId, replacer::Replacer, buffer_pool_manager::RefCountAccessor};


struct ClockPageInfo {
  clock_used: bool,
  page_id: PageId
}

pub(super) struct ClockReplacer {
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

  fn find_victim(&mut self, refcount_accessor: &RefCountAccessor) -> Option<PageId> {
    assert_eq!(self.clock.len(), self.page_clock_pos_mapping.len());
    let mut victim = Option::None;
    let mut n_iterated = 0;
    // We do at best two sweeps. First sweep takes into account clock status
    // Second basically just automatically takes the first that isn't currently referenced
    while n_iterated < self.clock.len() * 2 && victim.is_none() {
      let mut element = &mut self.clock[self.clock_position];
      if !element.clock_used && refcount_accessor.get_refcount(element.page_id) == 1 {
        victim = Option::Some(element.page_id);
      }
      element.clock_used = false;
      self.clock_position = (self.clock_position + 1) % self.clock.len();
      n_iterated += 1;
    }
    assert_eq!(self.clock.len(), self.page_clock_pos_mapping.len());
    victim
  }

  fn load_page(&mut self, page: PageId) {
    assert_eq!(self.clock.len(), self.page_clock_pos_mapping.len());
    // Where we add this really doesn't matter since this will really only ever happen
    // Before the buffer pool is full anyway which is before find_victim is called once anyway
    self.clock.push(ClockPageInfo { clock_used: true, page_id: page });
    self.page_clock_pos_mapping.insert(page, self.clock.len() - 1);
    assert_eq!(self.clock.len(), self.page_clock_pos_mapping.len());
  }

  fn swap_pages(&mut self, old_page: PageId, new_page: PageId) {
    assert_eq!(self.clock.len(), self.page_clock_pos_mapping.len());
    let pos = self.page_clock_pos_mapping.remove(&old_page).unwrap();
    self.clock[pos] = ClockPageInfo { page_id: new_page, clock_used: true };
    if self.page_clock_pos_mapping.insert(new_page, pos).is_some() {
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