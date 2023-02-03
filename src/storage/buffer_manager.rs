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
    replacer: Mutex<Box<dyn Replacer + Send>>,
    size: AtomicUsize
}

#[cfg(test)]
use mockall::automock;

#[cfg_attr(test, automock)]
pub trait RefCountAccessor {
    fn get_refcount(&self, page_id: PageId) -> usize;
}

pub struct RefCountAccessorImpl<'a> {
    pagetable: &'a PageTableType
}

impl<'a> RefCountAccessor for RefCountAccessorImpl<'a> {
    fn get_refcount(&self, page_id: PageId) -> usize {
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
    pub fn new(disk_manager: Box<dyn StorageManager>, replacer: Box<dyn Replacer + Send>, size: usize) -> BufferManager {
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
        let new_page = Arc::new(RwLock::new(Page::new()));
        let mut new_page_write = new_page.try_write().unwrap();
        new_page_write.id = Some(page_id);
        pagetable.insert(page_id, new_page.clone());
        if pagetable.len() <= self.size.load(Ordering::Relaxed) {
            replacer.load_page(page_id);
            drop(replacer);
            drop(pagetable);
            new_page_write.data = Box::new([0; PAGE_SIZE]);
            if self.disk_manager.get_relation_size(page_id.relation_id) / PAGE_SIZE as u64 > page_id.offset_id {
                self.disk_manager.read_page(page_id, &mut new_page_write.data)?;
            }
            drop(new_page_write);
            return Ok(new_page);     
        } else {
            loop {
                let refcount_accessor = RefCountAccessorImpl { pagetable: &&pagetable };
                let victim_opt = replacer.find_victim(&refcount_accessor);
                drop(refcount_accessor);
                if let Some(victim) = victim_opt {
                    let victim_page = pagetable[&victim].clone();
                    // Since the replacer has to make sure there's only one reference to the page
                    // locking it should under no circumstances block!
                    let mut victim_page_write = victim_page.try_write().expect("Locking replacer victim must not block!");
                    if victim_page_write.dirty {
                        drop(replacer);
                        drop(pagetable);
                        self.disk_manager.write_page(victim_page_write.id.expect("Dirty page cannot be new page"), &victim_page_write.data)?;
                        victim_page_write.dirty = false;
                        victim_page_write.is_new = false;
                        pagetable = self.pagetable.lock().unwrap();
                        replacer = self.replacer.lock().unwrap();
                    } 
                    // Check that only we and the pagetable have a reference (noone got one in the meantime if the page was dirty)
                    // We can't just remove the page from the pagetable before writing out to disk because otherwise someone else could
                    // Load it again with the old data
                    assert!(Arc::strong_count(&victim_page) >= 2);
                    if Arc::strong_count(&victim_page) == 2 {
                        drop(victim_page_write);
                        pagetable.remove(&victim);
                        replacer.swap_pages(victim, page_id);
                        drop(victim_page);
                        drop(replacer);
                        drop(pagetable);
                        new_page_write.data = Box::new([0; PAGE_SIZE]);
                        if self.disk_manager.get_relation_size(page_id.relation_id) / PAGE_SIZE as u64 > page_id.offset_id {
                            self.disk_manager.read_page(page_id, &mut new_page_write.data)?;
                            new_page_write.is_new = false
                        } else {
                            new_page_write.is_new = true;
                        }
                        drop(new_page_write);
                        return Ok(new_page);
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
        // TODO: HACK(kinda): To avoid key not found panics for new pages that are currently beeing loaded
        //       we currently only set the used flag if the page is actually there
        let mut replacer = self.replacer.lock().unwrap();
        if replacer.has_page(page_id) {
            replacer.use_page(page_id);
        }
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

    use std::{path::PathBuf, thread::{self, JoinHandle}, sync::{atomic::{AtomicU32, Ordering}, Arc, RwLock}};
    use mockall::Sequence;
    use rand::{Rng};

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
        let page = bpm.get_page(PageId::new(1,0)).unwrap();
        let mut page_write = page.try_write().unwrap();
        fill_page(PageId::new(1,0), &mut page_write);
        drop(page_write);
        drop(page);
        drop(bpm);
        let disk_manager = DiskManager::new(datadir_path);
        let bpm = BufferManager::new(Box::new(disk_manager), Box::new(ClockReplacer::new()), 10);
        let page = bpm.get_page(PageId::new(1,0)).unwrap();
        let page_read = page.try_read().unwrap();
        assert_page(PageId::new(1, 0), &page_read)
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
        replacer_mock.expect_has_page().return_const(true);
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
        replacer_mock.expect_find_victim().times(1)
            .return_const(PageId::new(1, 1));
        replacer_mock.expect_find_victim()
            .return_const(PageId::new(1, 2));
        replacer_mock.expect_load_page().return_const(());
        replacer_mock.expect_use_page().return_const(());
        replacer_mock.expect_swap_pages().return_const(());
        replacer_mock.expect_has_page().return_const(true);
        let bpm = BufferManager::new(Box::new(disk_manager), Box::new(replacer_mock), 1);
        let page = bpm.get_page(PageId::new(1,1)).unwrap();
        let mut page_write = page.try_write().unwrap();
        fill_page(PageId::new(1, 1), &mut page_write);
        drop(page_write);
        drop(page);
        bpm.get_page(PageId::new(1, 2)).unwrap();
        let page = bpm.get_page(PageId::new(1,1)).unwrap();
        let page_read = page.try_read().unwrap();
        assert_page(PageId::new(1, 1), &page_read);
    }



    #[test]
    fn multithreaded_contention_test() {
        let datadir = tempfile::tempdir().unwrap();
        let disk_manager = DiskManager::new(datadir.into_path());
        let bpm = Arc::new(BufferManager::new(Box::new(disk_manager), Box::new(ClockReplacer::new()), 100));
        let page_counter = Arc::new(AtomicU32::new(0));
        let pages: Arc<RwLock<Vec<PageId>>> = Arc::new(RwLock::new(Vec::new()));
        let mut jhs: Vec<JoinHandle<()>> = Vec::new();
        for i in 0..20 {
            let bpm = bpm.clone();
            let pages = pages.clone();
            let page_counter = page_counter.clone();
            jhs.push(thread::spawn(move || {
                let mut rng: rand::rngs::StdRng = rand::SeedableRng::seed_from_u64(42 * i);
                for j in 0..1000 {
                    let r: u64 = rng.gen();
                    if page_counter.load(Ordering::Relaxed) < 5 || r % 10 == 0 {
                        // create new
                        let next_offset_id = page_counter.fetch_add(1, Ordering::Relaxed) as u64;
                        let page_id = PageId::new(1, next_offset_id);
                        let page = bpm.get_page(page_id).unwrap();
                        let mut page_write = page.write().unwrap();
                        let mut pages_write = pages.write().unwrap();
                        pages_write.push(page_id);
                        drop(pages_write);
                        let id_slice = next_offset_id.to_be_bytes();
                        page_write.data[0..8].copy_from_slice(&id_slice);
                        page_write.data[8..16].copy_from_slice(&r.to_be_bytes());
                        page_write.dirty = true;
                    }
                    if j % 3 == 0 {
                        // Write
                        let pages_read = pages.read().unwrap();
                        let page_id = pages_read.get(r as usize % pages_read.len()).unwrap().clone();
                        drop(pages_read);
                        let page = bpm.get_page(page_id).unwrap();
                        let mut page_write = page.write().unwrap();
                        let id_slice = page_id.offset_id.to_be_bytes();
                        page_write.data[0..8].copy_from_slice(&id_slice);
                        page_write.data[8..16].copy_from_slice(&r.to_be_bytes());
                        page_write.dirty = true;
                    } else {
                        // Read
                        let pages_read = pages.read().unwrap();
                        let page_id = pages_read.get(r as usize % pages_read.len()).unwrap().clone();
                        drop(pages_read);
                        let page = bpm.get_page(page_id).unwrap();
                        let page_read = page.read().unwrap();
                        let read_id = u64::from_be_bytes(page_read.data[0..8].try_into().unwrap());
                        assert_eq!(read_id, page_id.offset_id);
                    }
                }
            }));
        }
        for jh in jhs {
            jh.join().unwrap();
        }
        for offset_id in 0..page_counter.load(Ordering::Acquire) as u64 {
            let page_id = PageId::new(1, offset_id);
            let page = bpm.get_page(page_id).unwrap();
            let page_read = page.read().unwrap();
            assert_eq!(u64::from_be_bytes(page_read.data[0..8].try_into().unwrap()), page_id.offset_id);
        }
    }

    fn fill_page(page_id: PageId, page_write: &mut Page) {
        let page_data = &mut page_write.data;
        page_data[0] = page_id.relation_id.to_be_bytes()[0];
        page_data[1] = page_id.relation_id.to_be_bytes()[1];
        page_data[2] = page_id.offset_id.to_be_bytes()[0];
        page_data[3] = page_id.offset_id.to_be_bytes()[1];
        page_data[4] = page_id.offset_id.to_be_bytes()[2];
        page_data[5] = page_id.offset_id.to_be_bytes()[3];
        page_data[6] = page_id.offset_id.to_be_bytes()[4];
        page_data[7] = page_id.offset_id.to_be_bytes()[5];
        page_data[8] = page_id.offset_id.to_be_bytes()[6];
        page_data[9] = page_id.offset_id.to_be_bytes()[7];
        page_data[100] = 123;
        page_data[1000] = 234;
        page_data[PAGE_SIZE - 1] = 21;
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
