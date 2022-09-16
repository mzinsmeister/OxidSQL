use std::{collections::{VecDeque, HashMap}, sync::Arc};

use super::{buffer_pool_manager::PageTableType, page::PageId};
use std::sync::MutexGuard;


struct ClockPageInfo {
  clock_used: bool,
  page_id: PageId
}

pub(super) struct ClockReplacer {
  // Would probably be more elegant to have a replacer specific struct included with every BufferFrame
  // But this is fine for now. At least guarantees O(1) lookup time for a specific Page and 
  // the Buffer Pool Manager is the only component touching the Replacer API
  page_clock_pos_mapping: HashMap<PageId, usize>,
  // We'll use a trick on this one. What we do is to avoid constant moving around of elements.
  // Instead of providing a remove API, we're just gonna provide a swap API which will be cheap
  // If we ever were to need actual remove, either swap with some dummy values or swap with the
  // front or back of the Queue and then just pop from there
  clock: Vec<ClockPageInfo>,
  clock_position: usize
}

impl ClockReplacer {
  pub fn new() -> Self {
    // Don't care about capacities for now. They will barely affect performance or memory use
    Self { page_clock_pos_mapping: HashMap::new(), clock: Vec::new(), clock_position: 0 }
  }

  pub fn find_victim(&mut self, page_table_guard: &MutexGuard<PageTableType>) -> Option<PageId> {
    let victim = Option::None;
    let mut n_iterated = 0;
    // We do at best two sweeps. First sweep takes into account clock status
    // Second basically just automatically takes the first that isn't currently referenced
    while n_iterated < self.clock.len() * 2 && victim.is_none() {
      let mut element = &mut self.clock[self.clock_position];
      let page_arc = &page_table_guard[&element.page_id];
      if !element.clock_used && Arc::strong_count(page_arc) == 1 {
        victim = Option::Some(element.page_id);
      }
      element.clock_used = false;
      self.clock_position = (self.clock_position + 1) % self.clock.len();
      n_iterated += 1;
    }
    victim
  }

  pub fn add_page(&mut self, page: PageId) {
    // Where we add this really doesn't matter since this will really only ever happen
    // Before the buffer pool is full anyway which is before find_victim is called once anyway
    self.clock.push(ClockPageInfo { clock_used: true, page_id: page }); 
    self.page_clock_pos_mapping.insert(page, self.clock.len() - 1);
  }

  pub fn swap_pages(&mut self, old_page: PageId, new_page: PageId) {
    let pos = self.page_clock_pos_mapping.remove(&old_page).unwrap();
    self.clock[pos] = ClockPageInfo { page_id: new_page, clock_used: true };
    self.page_clock_pos_mapping.insert(new_page, pos);
  }

  pub fn use_page(&mut self, page: PageId) {
    let pos = self.page_clock_pos_mapping[&page];
    self.clock[pos].clock_used = true;
  }
}