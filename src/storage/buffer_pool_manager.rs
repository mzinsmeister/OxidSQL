use crate::storage::page::{Page};
use std::collections::{HashMap};
use std::error::Error;
use std::fmt::Display;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, Mutex, MutexGuard};

use super::disk::StorageManager;
use super::page::{PageId, RelationIdType, PAGE_SIZE};
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
    size: AtomicUsize
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
pub enum BufferManagerError {
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
        writeln!(f, "{:?}", self).unwrap();
        Ok(())
    }
}

impl Error for BufferManagerError {}

impl BufferManager {
    pub fn new(disk_manager: Box<dyn StorageManager>, replacer: Box<dyn Replacer>, size: usize) -> BufferManager {
        return BufferManager {
            pagetable: Mutex::new(HashMap::new()),
            disk_manager,
            replacer: Mutex::new(replacer),
            size: AtomicUsize::new(size)
        };
    }

    fn load(&self, page_id: PageId, pagetable: MutexGuard<PageTableType>) -> Result<PageType, BufferManagerError> {
        let mut pagetable = pagetable;
        let mut replacer = self.replacer.lock().unwrap();
        if pagetable.len() < self.size.load(Ordering::Relaxed) {
            let mut page_raw = Page::new();
            page_raw.id = Some(page_id);
            let page = Arc::new(RwLock::new(page_raw));
            pagetable.insert(page_id, page.clone());
            replacer.load_page(page_id);
            drop(replacer);
            let mut page_write = page.try_write().expect("new page must not block on locking");
            drop(pagetable);
            if self.disk_manager.get_relation_size(page_id.relation_id) / PAGE_SIZE as u64 > page_id.offset_id {
                self.disk_manager.read_page(page_id, &mut page_write.data)?;
            }
            drop(page_write);
            return Ok(page);     
        } else {
            let mut page;
            loop {
                let refcount_accessor = RefCountAccessor { pagetable: &&pagetable };
                let victim_opt = replacer.find_victim(&refcount_accessor);
                drop(refcount_accessor);
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
                        replacer.swap_pages(victim, page_id);
                        drop(replacer);
                        drop(pagetable);
                        if self.disk_manager.get_relation_size(page_id.relation_id) / PAGE_SIZE as u64 > page_id.offset_id {
                            self.disk_manager.read_page(page_id, &mut page_write.data)?;
                        }
                        page_write.id = Some(page_id);
                        drop(page_write);
                        return Ok(page);
                    }
                } else {
                    return Err(BufferManagerError::NoBufferFrameAvailable);
                }
            }
        }
        
    }

    // This is mainly a testing and debugging interface
    #[cfg(test)]
    pub fn get_buffered_pages(&self) -> Vec<PageType> {
        self.pagetable.lock().unwrap().iter()
                                        .map(|(k, v)| v)
                                        .cloned()
                                        .collect()
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
        for (_, page) in pagetable.iter() {
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

impl Drop for BufferManager {
    fn drop(&mut self) {
        // Not sure whether you actually want to unwrap here.
        // I don't really know whether there's a better choice here.
        self.flush().unwrap(); 
    }
}

#[cfg(test)]
mod tests {

    use std::{path::PathBuf, thread, sync::atomic::{AtomicUsize, AtomicU32, Ordering}};

    use bitvec::{store::BitStore, macros::internal::funty::Numeric};
    use rand::{SeedableRng, Rng};

    use crate::storage::{disk::DiskManager, page::{PageId, PAGE_SIZE, Page}, clock_replacer::ClockReplacer, replacer::MockReplacer};

    use super::BufferManager;

    #[test]
    fn first_page_test() {
        let datadir = tempfile::tempdir().unwrap();
        let disk_manager = DiskManager::new(datadir.into_path());
        let bpm = BufferManager::new(Box::new(disk_manager), Box::new(ClockReplacer::new()), 10);
        let page = bpm.get_page(PageId::new(1,1)).unwrap();
        let page = page.try_read().unwrap();
        assert_eq!(page.dirty, false);
        assert_eq!(page.data.len(), PAGE_SIZE);
    }

    #[test]
    fn flush_test() {
        let datadir = tempfile::tempdir().unwrap();
        let datadir_path = PathBuf::from(datadir.path());
        let disk_manager = DiskManager::new(datadir_path.clone());
        let bpm = BufferManager::new(Box::new(disk_manager), Box::new(ClockReplacer::new()), 10);
        let page = bpm.get_page(PageId::new(1,1)).unwrap();
        let mut page = page.try_write().unwrap();
        fill_page(PageId::new(1,1), &mut page);
        drop(page);
        drop(bpm);
        let disk_manager = DiskManager::new(datadir_path);
        let bpm = BufferManager::new(Box::new(disk_manager), Box::new(ClockReplacer::new()), 10);
        let page = bpm.get_page(PageId::new(1,1)).unwrap();
        let page = page.try_read().unwrap();
        assert_page(PageId::new(1, 1), &page)
    }

    #[test]
    fn replace_test() {
        let datadir = tempfile::tempdir().unwrap();
        let datadir_path = PathBuf::from(datadir.path());
        let disk_manager = DiskManager::new(datadir_path);
        let mut replacer_mock = MockReplacer::new();
        replacer_mock.expect_find_victim().return_const(PageId::new(1, 1));
        replacer_mock.expect_load_page().return_const(());
        replacer_mock.expect_use_page().return_const(());
        replacer_mock.expect_swap_pages().return_const(());
        let bpm = BufferManager::new(Box::new(disk_manager), Box::new(replacer_mock), 1);
        bpm.get_page(PageId::new(1,1)).unwrap();
        bpm.get_page(PageId::new(1, 2)).unwrap();
        let pages = bpm.get_buffered_pages();
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].try_read().unwrap().id.unwrap(), PageId::new(1, 2));
    }

    #[test]
    fn replace_dirty_test() {
        let datadir = tempfile::tempdir().unwrap();
        let datadir_path = PathBuf::from(datadir.path());
        let disk_manager = DiskManager::new(datadir_path);
        let mut replacer_mock = MockReplacer::new();
        replacer_mock.expect_find_victim().return_const(PageId::new(1, 1));
        replacer_mock.expect_load_page().return_const(());
        replacer_mock.expect_use_page().return_const(());
        replacer_mock.expect_swap_pages().return_const(());
        let bpm = BufferManager::new(Box::new(disk_manager), Box::new(replacer_mock), 1);
        let page = bpm.get_page(PageId::new(1,1)).unwrap();
        let mut page_write = page.try_write().unwrap();
        fill_page(PageId::new(1, 2), &mut page_write);
        drop(page_write);
        drop(page);
        bpm.get_page(PageId::new(1, 2)).unwrap();
        let page = bpm.get_page(PageId::new(1,1)).unwrap();
        let page = page.try_read().unwrap();
        assert_page(PageId::new(1, 2), &page);
    }



    #[test]
    fn multithreaded_contention_test() {
        let datadir = tempfile::tempdir().unwrap();
        let disk_manager = DiskManager::new(datadir.into_path());
        let bpm = BufferManager::new(Box::new(disk_manager), Box::new(ClockReplacer::new()), 100);
        let page_counter = AtomicU32::new(0);
        for i in 0..20 {
            thread::spawn(move || {
                let rng: rand::rngs::StdRng = rand::SeedableRng::seed_from_u64(42 * i);
                for j in 0..1000 {
                    let r: u32 = rng.gen();
                    if page_counter.load(Ordering::Relaxed) < 5 || r % 10 == 0 {
                        // create new
                        let next_offset_id = page_counter.fetch_add(1, Ordering::Relaxed) as u64;
                        let page = bpm.get_page(PageId::new(1, next_offset_id)).unwrap();
                        let page_write = page.write().unwrap();
                        page_write.data[0..7].copy_from_slice(&next_offset_id.to_be_bytes());
                        page_write.dirty = true;
                    }
                    if j % 3 == 0 {
                        // Write
                        let page = bpm.get_page(PageId::new(1, (r % page_counter.load(Ordering::Relaxed)).into()));
                        let page_write = page.write().unwrap();
                        page_write.data[0..7].copy_from_slice(&next_offset_id.to_be_bytes());
                        page_write.dirty = true;
                    } else {
                        // Read
                    }
                }
                bpm.get_page(page_id)
            });
        }
        let page = bpm.get_page(PageId::new(1,1)).unwrap();
        let page = page.try_read().unwrap();
    }

    fn fill_page(page_id: PageId, page_write: &mut Page) {
        page_write.data[0] = page_id.relation_id.to_be_bytes()[0];
        page_write.data[1] = page_id.relation_id.to_be_bytes()[1];
        page_write.data[2] = page_id.offset_id.to_be_bytes()[0];
        page_write.data[3] = page_id.offset_id.to_be_bytes()[1];
        page_write.data[4] = page_id.offset_id.to_be_bytes()[2];
        page_write.data[5] = page_id.offset_id.to_be_bytes()[3];
        page_write.data[6] = page_id.offset_id.to_be_bytes()[4];
        page_write.data[7] = page_id.offset_id.to_be_bytes()[5];
        page_write.data[8] = page_id.offset_id.to_be_bytes()[6];
        page_write.data[9] = page_id.offset_id.to_be_bytes()[7];
        page_write.data[100] = 123;
        page_write.data[1000] = 234;
        page_write.data[PAGE_SIZE - 1] = 21;
        page_write.dirty = true;
    }

    fn assert_page(page_id: PageId, page: &Page) {
        assert_eq!(page.data[0], page_id.relation_id.to_be_bytes()[0]);
        assert_eq!(page.data[1], page_id.relation_id.to_be_bytes()[1]);
        assert_eq!(page.data[2], page_id.offset_id.to_be_bytes()[0]);
        assert_eq!(page.data[3], page_id.offset_id.to_be_bytes()[1]);
        assert_eq!(page.data[4], page_id.offset_id.to_be_bytes()[2]);
        assert_eq!(page.data[5], page_id.offset_id.to_be_bytes()[3]);
        assert_eq!(page.data[6], page_id.offset_id.to_be_bytes()[4]);
        assert_eq!(page.data[7], page_id.offset_id.to_be_bytes()[5]);
        assert_eq!(page.data[8], page_id.offset_id.to_be_bytes()[6]);
        assert_eq!(page.data[9], page_id.offset_id.to_be_bytes()[7]);
        assert_eq!(page.data[100], 123);
        assert_eq!(page.data[1000], 234);
        assert_eq!(page.data[PAGE_SIZE - 1], 21);
    }
}
