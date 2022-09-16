use crate::storage::page::{Page, PAGE_SIZE};
use std::collections::{HashMap, LinkedList};
use std::sync::{Arc, RwLock, Mutex};

use super::disk::DiskManager;
use super::clock_replacer::ClockReplacer;
use super::page::{PageId, RelationIdType};

pub type PageType = Arc<RwLock<Page>>;
pub(super) type PageTableType = HashMap<PageId, Arc<RwLock<Page>>>;

pub struct BufferPoolManager {
    // To avoid deadlocks:
    // Always take pagetable lock before replacer lock unless
    // You only need one of them. API basically enforces this anyway
    pagetable: Mutex<PageTableType>,
    disk_manager: DiskManager,
    replacer: Mutex<ClockReplacer>
}

impl BufferPoolManager {
    pub fn new(disk_manager: DiskManager, size: usize) -> BufferPoolManager {
        return BufferPoolManager {
            pagetable: HashMap::new(),
            disk_manager,
            replacer: ClockReplacer::new();
        };
    }

    fn find_victim(&mut self) -> (usize, PageType) {
        let found: Option<(usize, PageType)> = self
            .freelist
            .pop_front()
            .map(|i| (i, self.pages[i].page.clone()));
        if let Some(found) = found {
            return found;
        }
        loop {
            let mut check_page = &mut self.pages[self.clock_pos];
            let is_victim = !check_page.clock_used;
            check_page.clock_used = false;
            if is_victim && Arc::strong_count(&check_page.page) == 1 {
                return (self.clock_pos, check_page.page.clone());
            }
            self.clock_pos += 1;
            self.clock_pos %= self.pages.len();
        }
    }

    fn load(&mut self, page: PageId) -> usize {
        let (victim_index, victim) = self.find_victim();
        let mut victim_write = victim.write().unwrap();
        if let Some(id) = victim_write.id {
            if victim_write.dirty {
                // TODO: Actually do this outside the global pagetable lock
                // It's possible but hard to do. As i only really care about correctness for now
                // i don't really care
                self.disk_manager.write_page(id, &victim_write.data);
            }
            self.pagetable.remove(&id);
        }
        self.disk_manager.read_page(page, &mut victim_write.data);
        victim_write.id = Option::Some(page);
        victim_write.dirty = false;
        self.pagetable.insert(page, victim_index);
        victim_index
    }

    pub fn get_page(&mut self, page: PageId) -> PageType {
        if let Some(&result) = self.pagetable.get(&page) {
            return self.pages[result].page.clone();
        }
        let index = self.load(page);
        let mut page = &mut self.pages[index];
        page.clock_used = true;
        page.page.clone()
    }

    pub fn get_total_num_pages(&self, relation_id: RelationIdType) -> u64 {
        self.disk_manager.get_relation_size(relation_id)
    }

    pub fn flush(&mut self) {
        for clock_page in self.pages.iter() {
            let mut page = clock_page.page.write().unwrap();
            if let Some(id) = page.id {
                if page.dirty {
                    self.disk_manager.write(id, &page.data).unwrap();
                    page.dirty = false;
                }
            }
        }
    }
}
