use crate::storage::page::{Page, PageState};
use crate::util::align::alligned_slice;
use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, Mutex, MutexGuard};

use super::disk::StorageManager;
use super::page::{PageId, RelationIdType, PAGE_SIZE};
use super::replacer::Replacer;

// TODO: Abstract this away so that e.g. optimistic latches could be used.
//       Most likely to something that takes an idempotent lambda would be best.
pub type PageType = Arc<RwLock<Page>>;
// Self built (kinda) chaining hash table but with a Vec because Rusts LinkedList is useless
pub(super) type PageTableType = Vec<Mutex<PageTableBucket>>; 

fn new_page(page_id: PageId) -> PageType {
    Arc::new(RwLock::new(Page::new(page_id)))
}

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

/*
    TODO: Abstract over access methods (per buffer manager implementation).
          This would allow implementing something like the LeanStore/Umbra buffer manager
          using pointer swizzling that requires everything to be done in trees. Also maybe allow
          the API to work with variable sized pages. For the current buffer manager implementation
          variable sized pages would theoretically be possible but it would require allocating
          a new buffer for every new page with a different size as long as the replacer is not
          aware of the different sizes. The clock replacer could for example be made aware of this
          by adding the size to the clock entries and then maybe skipping ahead a few elements to
          get one that has the same size. I'm pretty sure we still want powers of 2 only since
          this strategy would otherwise be very likely to not find anything or waste space anyway.
          You could for example go something like 10% around the clock and if you don't find one
          with the correct size you just greedily pick from the front until you have enough space.

          In general the in-memory case should probably be optimized for more than the disk case
          nowerdays. One buffer manager choice that would be interesting to implement and should
          even be possible with the current interface (but not in safe Rust) would be Victor Leis's
          Virtual Memory assisted Buffer Management (vmcache)
          (https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/_my_direct_uploads/vmcache.pdf)
          which should offer a rather good performance for real world workloads that are mainly 
          in-memory and isn't as invasive as the LeanStore approach. Maybe try a hybrid approach
          with a vmcache and a hashtable based buffer manager and try to get hot pages into the 
          vmcache and pages which are only infrequently accessed into the hashtable 
          based buffer manager.
 */

pub trait BufferManager: Sync + Send {
    fn fix_page(&self, page_id: PageId) -> Result<PageType, BufferManagerError>;
    fn flush(&self) -> Result<(), BufferManagerError>;
}

pub struct HashTableBufferManager<R: Replacer + Send, S: StorageManager> {
    pagetable: PageTableType,
    pagetable_size: AtomicUsize,
    disk_manager: S,
    replacer: Mutex<R>,
    size: usize
}

#[derive(Debug)]
pub enum BufferManagerError {
    NoBufferFrameAvailable,
    DiskError(std::io::Error),
    PageNotFound
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

#[cfg(unix)]
use crate::util::align::AlignedSlice;

#[cfg(unix)]
fn new_buffer_frame(size: usize) -> AlignedSlice {
    alligned_slice(size, 4096)
}

#[cfg(not(unix))]
fn new_buffer_frame(size: usize) -> Box<[u8]> {
    let mut vec = Vec::with_capacity(size);
    vec.resize(size, 0);
    vec.into_boxed_slice()
}

impl<R: Replacer + Send, S: StorageManager> HashTableBufferManager<R, S> {
    pub fn new(disk_manager: S, replacer: R, size: usize) -> HashTableBufferManager<R, S> {
        let mut pagetable = PageTableType::with_capacity(size * 2);
        for _ in 0..size * 2 {
            pagetable.push(Mutex::new(PageTableBucket::new()));
        }
        return HashTableBufferManager {
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
        let new_page = new_page(page_id);
        let mut new_page_write = new_page.try_write().unwrap();
        page_bucket.insert(page_id, new_page.clone());
        drop(page_bucket);
        if self.pagetable_size.fetch_update(Ordering::Relaxed, Ordering::Relaxed,
                 |x| if x < self.size { Some(x + 1) } else { None }).is_ok() {
            let mut replacer = self.replacer.lock().unwrap();
            replacer.load_page(page_id, new_page.clone());
            drop(replacer);
            new_page_write.data = new_buffer_frame(PAGE_SIZE);
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
                            self.disk_manager.write_page(victim_page_write.id, &victim_page_write.data)?;
                            victim_page_write.state = PageState::CLEAN;
                            victim_bucket = self.pagetable[self.get_pagetable_index(victim)].lock().unwrap();
                        } 
                        // Check that only we and the pagetable have a reference (noone got one in the meantime)
                        // We can't just remove the page from the pagetable before writing out to disk because otherwise someone else could
                        // Load it again with the old data
                        assert!(Arc::strong_count(&victim_page) >= 3);
                        if Arc::strong_count(&victim_page) == 3 {
                            // Little trick that should give us better performance without
                            // actually having to reuse the buffer frame since it means we can do 
                            // buffer replacement without any allocations
                            std::mem::swap(&mut victim_page_write.data, &mut new_page_write.data);
                            drop(victim_page_write);
                            replacer = self.replacer.lock().unwrap();
                            victim_bucket.remove(victim);
                            replacer.swap_pages(victim, page_id, new_page.clone());
                            replacer.use_page(page_id);
                            drop(victim_page);
                            drop(replacer);
                            drop(victim_bucket);
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
}

impl<R: Replacer + Send, S: StorageManager> BufferManager for HashTableBufferManager<R,S> {
    fn fix_page(&self, page_id: PageId) -> Result<PageType, BufferManagerError> {
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

    fn flush(&self) -> Result<(), BufferManagerError> {
        for bucket in &self.pagetable {
            for (_, page) in bucket.lock().unwrap().bucket.iter() {
                let mut page = page.write().unwrap();
                if page.state.is_dirtyish() {
                    self.disk_manager.write_page(page.id, &page.data)?;
                    page.state = PageState::CLEAN;
                }
            }
        }
        Ok(())
    }
}

impl<R: Replacer + Send, S: StorageManager> Drop for HashTableBufferManager<R, S> {
    fn drop(&mut self) {
        // Not sure whether you actually want to unwrap here.
        // I don't really know whether there's a better choice here.
        self.flush().unwrap(); 
    }
}

#[cfg(test)]
pub mod mock {
    use std::{collections::HashMap, sync::{Mutex, RwLock}};

    use crate::{storage::page::{PageId, Page}, util::align::{AlignedSlice, alligned_slice}};

    use super::{PageType, BufferManager, new_page};

    pub struct MockBufferManager {
        page_size: usize,
        segments: Mutex<HashMap<u16, HashMap<u64, PageType>>>
    }

    impl MockBufferManager {
        pub fn new(page_size: usize) -> MockBufferManager {
            MockBufferManager {
                segments: Mutex::new(HashMap::new()),
                page_size
            }
        }
    }

    impl BufferManager for MockBufferManager {
        fn fix_page(&self, page_id: PageId) -> Result<PageType, super::BufferManagerError> {
            let mut segments = self.segments.lock().unwrap();
            let page = segments
                .get(&page_id.segment_id)
                .and_then(|segment| segment.get(&page_id.offset_id));
            if let Some(page) = page {
                Ok(page.clone())
            } else {
                segments.insert(page_id.segment_id, HashMap::new());
                let new_page = new_page(page_id);
                let mut new_page_guard = new_page.write().unwrap();
                new_page_guard.data = alligned_slice(self.page_size, 1);
                drop(new_page_guard);
                segments.get_mut(&page_id.segment_id).unwrap().insert(page_id.offset_id, new_page);
                Ok(segments[&page_id.segment_id][&page_id.offset_id].clone())
            }
        }

        fn flush(&self) -> Result<(), super::BufferManagerError> {
            let segments = self.segments.lock().unwrap();
            for (_, segment) in segments.iter() {
                for (_, page) in segment.iter() {
                    let mut page = page.write().unwrap();
                    if page.state.is_dirtyish() {
                        page.state = super::PageState::CLEAN;
                    }
                }
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {

    use std::{path::PathBuf, thread::{self, JoinHandle}, sync::{atomic::{AtomicU32, Ordering}, Arc, RwLock}};
    use rand::{Rng};

    use crate::storage::{disk::{DiskManager, StorageManager}, page::{PageId, PAGE_SIZE, Page, PageState}, clock_replacer::ClockReplacer, replacer::MockReplacer, buffer_manager::BufferManager};

    use super::HashTableBufferManager;

    #[test]
    fn first_page_test() {
        let datadir = tempfile::tempdir().unwrap();
        let disk_manager = DiskManager::new(datadir.into_path());
        let bpm = HashTableBufferManager::new(disk_manager, ClockReplacer::new(), 10);
        let page = bpm.fix_page(PageId::new(1,1)).unwrap();
        let page = page.try_read().unwrap();
        assert_eq!(page.state, PageState::NEW);
        assert_eq!(page.data.len(), PAGE_SIZE);
    }

    #[test]
    fn flush_test() {
        let datadir = tempfile::tempdir().unwrap();
        let datadir_path = PathBuf::from(datadir.path());
     /*
    TODO: Implement a parser for the SQL dialect supported by the database (likely using the nom
          parser combinator library). The parser should produce an abstract syntax tree (AST) that 
          can be analyzed by the analyzer which for now will also live in this module. The SQL
          dialect should for now be compatible with postgres.

          Options are:
            - Use the parser written for my hacky v1 database (also with Nom)
            - Use a library exposing the actual postgres parser in Rust
            - Write a parser from scratch
 */   let disk_manager = DiskManager::new(datadir_path.clone());
        let bpm = HashTableBufferManager::new(disk_manager, ClockReplacer::new(), 10);
        let page = bpm.fix_page(PageId::new(1,0)).unwrap();
        let mut page_write = page.try_write().unwrap();
        fill_page(PageId::new(1,0), &mut page_write);
        drop(page_write);
        drop(page);
        drop(bpm);
        let disk_manager = DiskManager::new(datadir_path);
        let bpm = HashTableBufferManager::new(disk_manager, ClockReplacer::new(), 10);
        let page = bpm.fix_page(PageId::new(1,0)).unwrap();
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
        let bpm = HashTableBufferManager::new(disk_manager, replacer_mock, 1);
        let _page = bpm.fix_page(PageId::new(1,1)).unwrap();
        let _page2 = bpm.fix_page(PageId::new(1, 2)).unwrap();
        let pages = bpm.get_buffered_pages();
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].try_read().unwrap().id, PageId::new(1, 2));
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
        let bpm = HashTableBufferManager::new(disk_manager, replacer_mock, 1);
        let page = bpm.fix_page(PageId::new(1,1)).unwrap();
        let mut page_write = page.try_write().unwrap();
        fill_page(PageId::new(1, 1), &mut page_write);
        drop(page_write);
        let _page2 = bpm.fix_page(PageId::new(1, 2)).unwrap();
        let page1 = bpm.fix_page(PageId::new(1,1)).unwrap();
        let page1_read = page1.try_read().unwrap();
        assert_page(PageId::new(1, 1), &page1_read);
    }



    #[test]
    fn multithreaded_contention_test() {
        let datadir = tempfile::tempdir().unwrap();
        let disk_manager = DiskManager::new(datadir.into_path());
        disk_manager.create_relation(1);
        let bpm = Arc::new(HashTableBufferManager::new(disk_manager, ClockReplacer::new(), 200));
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
                for j in 0..200 {
                    let r: u64 = rng.gen();
                    if page_counter.load(Ordering::Relaxed) < 5 || r % 10 == 0 {
                        // create new
                        let next_offset_id = next_offset_id.fetch_add(1, Ordering::Relaxed) as u64;
                        let page_id = PageId::new(1, next_offset_id);
                        let page = bpm.fix_page(page_id).unwrap();
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
                        let page = bpm.fix_page(page_id).unwrap();
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
                        let page = bpm.fix_page(page_id).unwrap();
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
            let page = bpm.fix_page(page_id).unwrap();
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
