use crate::storage::page::{Page};
use std::collections::{HashMap};
use std::error::Error;
use std::fmt::Display;
use std::sync::{Arc, RwLock, Mutex, MutexGuard};

use super::disk::StorageManager;
use super::clock_replacer::ClockReplacer;
use super::page::{PageId, RelationIdType};
use super::replacer::Replacer;

pub type PageType = Arc<RwLock<Page>>;
pub(super) type PageTableType = HashMap<PageId, Arc<RwLock<Page>>>;

pub struct BufferManager {
    // To avoid deadlocks:
    // Always take pagetable lock before replacer lock unless
    // You only need one of them.
    pagetable: Mutex<PageTableType>,
    disk_manager: Box<dyn StorageManager>,
    replacer: Mutex<Box<dyn Replacer>>,
    size: usize
}

pub struct RefCountAccessor<'a> {
    pagetable: &'a PageTableType
}

impl<'a> RefCountAccessor<'a> {
    pub fn get_refcount(&self, page_id: PageId) -> usize {
        Arc::strong_count(&self.pagetable[&page_id])
    }
}

#[derive(Debug)]
enum BufferManagerError {
    NoBufferFrameAvailable,
    DiskError(std::io::Error)
}

impl From<std::io::Error> for BufferManagerError {
    // TODO: Create an actual disk error
    fn from(io_error: std::io::Error) -> Self {
        BufferManagerError::DiskError(io_error)
    } 
}

impl Display for BufferManagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{:?}", self);
        Ok(())
    }
}

impl Error for BufferManagerError {
    
}

impl BufferManager {
    pub fn new(disk_manager: Box<dyn StorageManager>, size: usize) -> BufferManager {
        return BufferManager {
            pagetable: Mutex::new(HashMap::new()),
            disk_manager,
            replacer: Mutex::new(Box::new(ClockReplacer::new())),
            size
        };
    }

    fn load(&self, page_id: PageId, pagetable: MutexGuard<PageTableType>) -> Result<PageType, BufferManagerError> {
        let mut pagetable = pagetable;
        let mut replacer = self.replacer.lock().unwrap();
        let mut page;
        let page_write = if pagetable.len() < self.size {
            page = Arc::new(RwLock::new(Page::new()));
            replacer.load_page(page_id);
            page.try_write().expect("Locking new page must not block")
        } else {
            let refcount_accessor = RefCountAccessor { pagetable: &&pagetable };
            let mut victim_opt = replacer.find_victim(&refcount_accessor);
            
            if let Some(victim) = victim_opt {
                page = pagetable[&victim].clone();
                // Since the replacer has to make sure there's only one reference to the page
                // locking it should under no circumstances block!
                let mut page_write = page.try_write().expect("Locking replacer victim must not block!");
                if page_write.dirty {
                    drop(pagetable);
                    self.disk_manager.write_page(page_write.id.expect("Dirty page cannot be new page"), &page_write.data)?;
                    page_write.dirty = false;
                    pagetable = self.pagetable.lock().unwrap();
                } 
                // Check that only we and the pagetable have a reference (noone got one in the meantime if the page was dirty)
                // We can't just remove the page from the pagetable before writing out to disk because otherwise someone else could
                // Load it again with the old data
                if Arc::strong_count(&page) == 2 { 
                    page_write
                } else {
                    loop {
                        drop(page_write);
                        let refcount_accessor = RefCountAccessor { pagetable: &&pagetable };
                        victim_opt = replacer.find_victim(&refcount_accessor);
                        drop(refcount_accessor);
                        if let Some(victim) = victim_opt {
                            page = pagetable[&victim].clone();
                            // Since the replacer has to make sure there's only one reference to the page
                            // locking it should under no circumstances block!
                            page_write = page.try_write().expect("Locking replacer victim must not block!");
                            if page_write.dirty {
                                drop(pagetable);
                                self.disk_manager.write_page(page_write.id.expect("Dirty page cannot be new page"), &page_write.data)?;
                                page_write.dirty = false;
                                pagetable = self.pagetable.lock().unwrap();
                            } 
                            // Check that only we and the pagetable have a reference (noone got one in the meantime if the page was dirty)
                            // We can't just remove the page from the pagetable before writing out to disk because otherwise someone else could
                            // Load it again with the old data
                            if Arc::strong_count(&page) == 2 { 
                                break;
                            }
                        } else {
                            return Err(BufferManagerError::NoBufferFrameAvailable);
                        }
                    }
                    replacer.swap_pages(victim, page_id);
                    page_write
                }
            } else {
                return Err(BufferManagerError::NoBufferFrameAvailable);
            }
        };
        drop(replacer);
        drop(pagetable);
        self.disk_manager.read_page(page_id, &mut page_write.data)?;
        Ok(page)
    }

    pub fn get_page(&self, page_id: PageId) -> Result<PageType, BufferManagerError> {
        let pagetable = self.pagetable.lock().unwrap();
        let page = if let Some(result) = pagetable.get(&page_id) {
            result.clone()
        } else {
            self.load(page_id, pagetable)?
        };
        self.replacer.lock().unwrap().use_page(page_id);
        Ok(page)
    }

    pub fn get_total_num_pages(&self, relation_id: RelationIdType) -> u64 {
        self.disk_manager.get_relation_size(relation_id)
    }

    pub fn flush(&mut self) -> Result<(), BufferManagerError> {
        // Flushing is expensive: It will possibly hold a lock on the pagetable for a very long time
        let pagetable = self.pagetable.lock().unwrap();
        for (page_id, page) in pagetable.iter() {
            let mut page = page.write().unwrap();
            if let Some(id) = page.id {
                if page.dirty {
                    self.disk_manager.write_page(id, &page.data)?;
                    page.dirty = false;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
