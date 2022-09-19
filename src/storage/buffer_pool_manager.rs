use crate::storage::page::{Page};
use std::collections::{HashMap};
use std::sync::{Arc, RwLock, Mutex};

use super::disk::DiskManager;
use super::clock_replacer::ClockReplacer;
use super::page::{PageId, RelationIdType};
use super::replacer::Replacer;

pub type PageType = Arc<RwLock<Page>>;
pub(super) type PageTableType = HashMap<PageId, Arc<RwLock<Page>>>;

pub struct BufferPoolManager {
    // To avoid deadlocks:
    // Always take pagetable lock before replacer lock unless
    // You only need one of them.
    pagetable: Mutex<PageTableType>,
    disk_manager: DiskManager,
    replacer: Mutex<Box<dyn Replacer>>,
    size: usize
}

impl BufferPoolManager {
    pub fn new(disk_manager: DiskManager, size: usize) -> BufferPoolManager {
        return BufferPoolManager {
            pagetable: Mutex::new(HashMap::new()),
            disk_manager,
            replacer: Mutex::new(Box::new(ClockReplacer::new())),
            size
        };
    }

    fn load(&mut self, page_id: PageId) {
        let pagetable = self.pagetable.lock().unwrap();
        let replacer = self.replacer.lock().unwrap();
        let page = if pagetable.len() < self.size {
            let page = Arc::new(RwLock::new(Page::new()));
            pagetable[&page_id] = page.clone();
            let page = page.write().unwrap();
            drop(pagetable);
            replacer.load_page(page_id);
            drop(replacer);
        } else {
            let victim = replacer.find_victim();
        };
        self.disk_manager.read_page(page_id, &mut page.data);
        if let Some(victim_id) = victim {
             else {
                
            }
        } else {

        }
        drop(replacer);
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

pub struct RefCountAccessor {
    page_table: Arc<Mutex<PageTableType>>
}
