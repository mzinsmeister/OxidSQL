use crate::storage::page::{Page, PageState};
use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, Mutex, MutexGuard};

use super::disk::StorageManager;
use super::page::{PageId, RelationIdType, PAGE_SIZE, self};
use super::replacer::Replacer;

pub type PageType = Arc<RwLock<Page>>;
// Self built (kinda) chaining hash table but with a Vec because Rusts LinkedList is useless
pub(super) type PageTableType = Vec<Mutex<PageTableBucket>>; 

pub(super) struct PageTableBucket {
    bucket: Vec<(PageId, PageType)>
}

fn hash_page_id(page_id: PageId) -> usize {
    let mut hasher = DefaultHasher::new();
    page_id.hash(&mut hasher);
    hasher.finish() as usize
}

impl PageTableBucket {

    pub fn new() -> PageTableBucket {
        PageTableBucket {
            bucket: Vec::new()
        }
    }

    pub fn get(&self, page_id: PageId) -> Option<Arc<RwLock<Page>>> {
        for (id, page) in self.bucket.iter() {
            if id == &page_id {
                return Some(page.clone());
            }
        }
        return None;
    }

    pub fn insert(&mut self, page_id: PageId, page: Arc<RwLock<Page>>) {
        self.bucket.push((page_id, page));
    }

    pub fn remove(&mut self, page_id: PageId) -> bool {
        let pos = self.bucket.iter().position(|(p, _)| *p == page_id);
        if let Some(pos) = pos {
            self.bucket.swap_remove(pos);
            true
        } else {
            false
        }
    }
}

pub struct BufferManager {
    // To avoid deadlocks:
    // Always take pagetable lock before replacer lock unless
    // You only need one of them.
    pagetable: PageTableType,
    pagetable_size: AtomicUsize,
    disk_manager: Box<dyn StorageManager>,
    replacer: Mutex<Box<dyn Replacer + Send>>,
    size: usize
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
        let mut pagetable = PageTableType::with_capacity(size * 2);
        for _ in 0..size * 2 {
            pagetable.push(Mutex::new(PageTableBucket::new()));
        }
        return BufferManager {
            pagetable,
            pagetable_size: AtomicUsize::new(0),
            disk_manager,
            replacer: Mutex::new(replacer),
            size
        };
    }

    fn get_pagetable_index(&self, page_id: PageId) -> usize {
        hash_page_id(page_id) % self.pagetable.len()
    }

    fn load(&self, page_id: PageId, mut page_bucket: MutexGuard<PageTableBucket>) -> Result<PageType, BufferManagerError> {
        let new_page = Arc::new(RwLock::new(Page::new()));
        let mut new_page_write = new_page.try_write().unwrap();
        new_page_write.id = Some(page_id);
        page_bucket.insert(page_id, new_page.clone());
        drop(page_bucket);
        if self.pagetable_size.fetch_update(Ordering::Relaxed, Ordering::Relaxed,
                 |x| if x < self.size { Some(x + 1) } else { None }).is_ok() {
            let mut replacer = self.replacer.lock().unwrap();
            replacer.load_page(page_id, new_page.clone());
            drop(replacer);
            new_page_write.data = Box::new([0; PAGE_SIZE]);
            if self.disk_manager.get_relation_size(page_id.segment_id) / PAGE_SIZE as u64 > page_id.offset_id {
                self.disk_manager.read_page(page_id, &mut new_page_write.data)?;
            }
            drop(new_page_write);
            return Ok(new_page);     
        } else {
            loop {
                let mut replacer = self.replacer.lock().unwrap();
                let victim_opt = replacer.find_victim();
                drop(replacer);
                if let Some(victim) = victim_opt {
                    let mut victim_bucket = self.pagetable[self.get_pagetable_index(victim)].lock().unwrap();
                    let victim_page = if let Some(p) = victim_bucket.get(victim) {
                        p
                    } else {
                        continue;
                    };
                    let victim_page_write_result = victim_page.try_write();
                    if victim_page_write_result.is_ok() {
                        let mut victim_page_write = victim_page_write_result.unwrap();
                        if victim_page_write.state == PageState::DIRTY {
                            drop(victim_bucket);                      
                            self.disk_manager.write_page(victim_page_write.id.expect("Dirty page cannot be new page"), &victim_page_write.data)?;
                            victim_page_write.state = PageState::CLEAN;
                            victim_bucket = self.pagetable[self.get_pagetable_index(victim)].lock().unwrap();
                        } 
                        // Check that only we and the pagetable have a reference (noone got one in the meantime)
                        // We can't just remove the page from the pagetable before writing out to disk because otherwise someone else could
                        // Load it again with the old data
                        assert!(Arc::strong_count(&victim_page) >= 3);
                        if Arc::strong_count(&victim_page) == 3 {
                            drop(victim_page_write);
                            replacer = self.replacer.lock().unwrap();
                            victim_bucket.remove(victim);
                            replacer.swap_pages(victim, page_id, new_page.clone());
                            replacer.use_page(page_id);
                            drop(victim_page);
                            drop(replacer);
                            drop(victim_bucket);
                            new_page_write.data = Box::new([0; PAGE_SIZE]);
                            if self.disk_manager.get_relation_size(page_id.segment_id) / PAGE_SIZE as u64 > page_id.offset_id {
                                self.disk_manager.read_page(page_id, &mut new_page_write.data)?;
                                new_page_write.state = PageState::CLEAN;
                            } else {
                                new_page_write.state = PageState::NEW;
                            }
                            drop(new_page_write);
                            return Ok(new_page);
                        }
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
        self.pagetable.iter()
                            .map(|l| l.lock().unwrap().bucket.iter().map(|(_, page)| page).cloned().collect::<Vec<PageType>>())
                            .flatten()
                            .collect()
    }

    pub fn get_page(&self, page_id: PageId) -> Result<PageType, BufferManagerError> {
        let page_bucket = self.pagetable[self.get_pagetable_index(page_id)].lock().unwrap();
        let page = if let Some(result) = page_bucket.get(page_id) {
            drop(page_bucket);
            let mut replacer = self.replacer.lock().unwrap();
            if replacer.has_page(page_id) {
                replacer.use_page(page_id);
            }
            result.clone()
        } else {
            self.load(page_id, page_bucket)?
        };
        // TODO: HACK(kinda): To avoid key not found panics for new pages that are currently beeing loaded
        //       we currently only set the used flag if the page is actually there
        Ok(page)
    }

    pub fn get_total_num_pages(&self, segment_id: RelationIdType) -> u64 {
        self.disk_manager.get_relation_size(segment_id)
    }

    pub fn flush(&self) -> Result<(), BufferManagerError> {
        for bucket in &self.pagetable {
            for (_, page) in bucket.lock().unwrap().bucket.iter() {
                let mut page = page.write().unwrap();
                if let Some(id) = page.id {
                    if page.state.is_dirtyish() {
                        self.disk_manager.write_page(id, &page.data)?;
                        page.state = PageState::CLEAN;
                    }
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
    use rand::{Rng};

    use crate::storage::{disk::DiskManager, page::{PageId, PAGE_SIZE, Page, PageState}, clock_replacer::ClockReplacer, replacer::MockReplacer};

    use super::BufferManager;

    #[test]
    fn first_page_test() {
        let datadir = tempfile::tempdir().unwrap();
        let disk_manager = DiskManager::new(datadir.into_path());
        let bpm = BufferManager::new(Box::new(disk_manager), Box::new(ClockReplacer::new()), 10);
        let page = bpm.get_page(PageId::new(1,1)).unwrap();
        let page = page.try_read().unwrap();
        assert_eq!(page.state, PageState::NEW);
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
        let _page = bpm.get_page(PageId::new(1,1)).unwrap();
        let _page2 = bpm.get_page(PageId::new(1, 2)).unwrap();
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
        let _page2 = bpm.get_page(PageId::new(1, 2)).unwrap();
        let page1 = bpm.get_page(PageId::new(1,1)).unwrap();
        let page1_read = page1.try_read().unwrap();
        assert_page(PageId::new(1, 1), &page1_read);
    }



    #[test]
    fn multithreaded_contention_test() {
        let datadir = tempfile::tempdir().unwrap();
        let disk_manager = DiskManager::new(datadir.into_path());
        let bpm = Arc::new(BufferManager::new(Box::new(disk_manager), Box::new(ClockReplacer::new()), 100));
        let next_offset_id = Arc::new(AtomicU32::new(0));
        let page_counter = Arc::new(AtomicU32::new(0));
        let pages: Arc<RwLock<Vec<PageId>>> = Arc::new(RwLock::new(Vec::new()));
        let mut jhs: Vec<JoinHandle<()>> = Vec::new();
        for i in 0..20 {
            let bpm = bpm.clone();
            let pages = pages.clone();
            let page_counter = page_counter.clone();
            let next_offset_id = next_offset_id.clone();
            let page_counter = page_counter.clone();
            jhs.push(thread::spawn(move || {
                let mut rng: rand::rngs::StdRng = rand::SeedableRng::seed_from_u64(42 * i);
                for j in 0..10_000 {
                    let r: u64 = rng.gen();
                    if page_counter.load(Ordering::Relaxed) < 5 || r % 10 == 0 {
                        // create new
                        let next_offset_id = next_offset_id.fetch_add(1, Ordering::Relaxed) as u64;
                        let page_id = PageId::new(1, next_offset_id);
                        let page = bpm.get_page(page_id).unwrap();
                        let mut page_write = page.write().unwrap();
                        let mut pages_write = pages.write().unwrap();
                        pages_write.push(page_id);
                        page_counter.fetch_add(1, Ordering::Relaxed);
                        drop(pages_write);
                        let id_slice = next_offset_id.to_be_bytes();
                        page_write.data[0..8].copy_from_slice(&id_slice);
                        page_write.data[8..16].copy_from_slice(&r.to_be_bytes());
                        page_write.state = PageState::DIRTY;
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
                        page_write.state = PageState::DIRTY;
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
        page_data[0] = page_id.segment_id.to_be_bytes()[0];
        page_data[1] = page_id.segment_id.to_be_bytes()[1];
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
        page_write.state = PageState::DIRTY;
    }

    fn assert_page(page_id: PageId, page: &Page) {
        assert_eq!(page.data[0], page_id.segment_id.to_be_bytes()[0]);
        assert_eq!(page.data[1], page_id.segment_id.to_be_bytes()[1]);
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
