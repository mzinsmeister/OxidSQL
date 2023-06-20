
use std::collections::LinkedList;
use std::fmt::{Debug, Formatter};
use std::io::{Cursor, Write};
/*
    Essentially an implementation of this paper: "Concurrent Online Sampling for All, for Free" 
    (https://altan.birler.co/static/papers/2020DAMON_ConcurrentOnlineSampling.pdf)
 

    First we need a "List of skips" datastructure
    This is a linked list of skips, where each skip is a node in the list
    We assume a fixed (maximum) number of threads. This makes sense anyway since
    having a number of worker threads much larger than the number of CPU cores
    will usually not improve performance but rather degrade it due to context switching.
    The number of threads must be less than u32::MAX but for all practical purposes it will
    probably remain less than u32::MAX for at least the next 100 years or until the singularity, 
    whichever comes first.

    When we're deleting a lot of the data we will also delete a lot of samples which leads
    to the fixed size linked list of skips overflowing with deteriorating performance. 
    To still be able to handle this case we add an overflow list synchronized by a single 
    mutex that is allocated on the heap. The order that we get back the skips in is not
    perfect (not like it would be without overflow) this way but it should be good enough.
    Once we want to delete a sample but have no more space we will put the head of the list 
    into the overflow list and then insert a new 0 element. We will later use a deterministic
    algorithm to determine the element position from the index of the skip (Probably just AES). 
    The first m numbers will just be mapped to themselves so we can eliminate the special "preload"
    phase.
    
    The samples will be stored in a regular transactional segment so
    we will just update the samples in place on the main segment anyway. 
    Samples will have to store the index of the skip they came from as well as
    the w value that produced the skip. We will eventually just write changes of skips 
    (length of skip with index x changed from a to b) to the write ahead log and then apply them back 
    to the list of skips on recovery. That seems to be the simplest way to keep it consistent. This will then
    also be played back on R1 recovery to set the skips back. It could however turn out to be best/simplest to draw
    a new sample through a single scan of the table on R2 recovery.
*/
use std::sync::atomic::{AtomicU64, AtomicU32, AtomicUsize};
use std::sync::atomic::Ordering::Relaxed;

use atomic::Ordering;
use byteorder::{ReadBytesExt, BigEndian, WriteBytesExt};
use parking_lot::{RwLock, RwLockReadGuard, Mutex};

// Quick and Hacky Atomic f32, u32 tuple
pub struct AtomicU32F32Tup {
    storage: AtomicU64,
}
impl AtomicU32F32Tup {
    pub fn new(value: (u32, f32)) -> Self {
        let as_u32 = value.1.to_bits();
        Self { storage: AtomicU64::new((value.0 as u64) << 32 | as_u32 as u64) }
    }

    #[allow(dead_code)]
    pub fn store(&self, value: (u32, f32), ordering: Ordering) {
        let as_u32 = value.1.to_bits();
        self.storage.store((value.0 as u64) << 32 | as_u32 as u64, ordering)
    }

    pub fn load(&self, ordering: Ordering) -> (u32, f32) {
        let as_u64 = self.storage.load(ordering);
        let right_as_u32 = (as_u64 & 0xFFFFFFFF) as u32;
        let right_as_f32 = f32::from_bits(right_as_u32);
        let left_as_u32 = (as_u64 >> 32) as u32;
        (left_as_u32, right_as_f32)
    }

    pub fn compare_exchange(&self, current: (u32, f32), new: (u32, f32), success: Ordering, failure: Ordering) -> Result<(u32, f32), (u32, f32)> {
        let current_as_u32 = current.1.to_bits();
        let new_as_u32 = new.1.to_bits();
        let current_as_u64 = (current.0 as u64) << 32 | current_as_u32 as u64;
        let new_as_u64 = (new.0 as u64) << 32 | new_as_u32 as u64;
        match self.storage.compare_exchange(current_as_u64, new_as_u64, success, failure) {
            Ok(as_u64) => {
                let right_as_u32 = (as_u64 & 0xFFFFFFFF) as u32;
                let right_as_f32 = f32::from_bits(right_as_u32);
                let left_as_u32 = (as_u64 >> 32) as u32;
                Ok((left_as_u32, right_as_f32))
            }
            Err(as_u64) => {
                let right_as_u32 = (as_u64 & 0xFFFFFFFF) as u32;
                let right_as_f32 = f32::from_bits(right_as_u32);
                let left_as_u32 = (as_u64 >> 32) as u32;
                Err((left_as_u32, right_as_f32))
            }
        }
    }
}

impl Debug for AtomicU32F32Tup {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let as_u64 = self.storage.load(Relaxed);
        let right_as_u32 = (as_u64 & 0xFFFFFFFF) as u32;
        let right_as_f32 = f32::from_bits(right_as_u32);
        let left_as_u32 = (as_u64 >> 32) as u32;
        write!(f, "({}, {})", left_as_u32, right_as_f32)
    }
}

#[derive(Debug)]
struct Skip {
    index: AtomicU32,
    // w: AtomicF32, We might need this one but we currently don't really
    length: AtomicU64,
    next: AtomicU32, // u32::MAX means end of list
}

#[derive(Debug)]
struct ListOfSkips<R: Fn() -> f32> {
    index_and_w_next: AtomicU32F32Tup,
    // we will access this concurrently so it must be fixed size at runtime
    // we will therefore allocate all the skips we will need at the start
    list: Box<[Skip]>,
    head: AtomicU64,
    free_head: AtomicU64,
    m: u32,
    random: R,
    // The overflow list is there to store Skips if we have lots of deletes
    // This is hella slow but it allows us to still remain correct
    // The overflow is used to allocate new Skips instead of generating them from index_and_w_next
    // To avoid having to lock the mutex every time, we track the length that the list will
    // AT LEAST have when the lock is taken. This also means you decrease the count before 
    // you take the lock and increase it before releasing the lock when adding something
    overflow_skips: Mutex<LinkedList<Skip>>,
    num_overflow_skips: AtomicUsize,
}

pub struct SkipGuard<'a, R: Fn() -> f32 + Clone> {
    skip_count: u64,
    proccessed: i64, // can also be negative meaning we add length to the skip
    pub skip_index: u32,
    list_index: u32,
    skip: &'a Skip,
    sampler: &'a ReservoirSampler<R>,
    // The snapshot latch is there to enable atomic snapshots of the list of skips
    // Every SkipGuard gets a RwReadGuard so that we can make sure to only take
    // a snapshot when no other thread is currently owning any SkipGuard
    _snapshot_latch_guard: RwLockReadGuard<'a, ()>,
}

impl<'a, R: Fn() -> f32 + Clone> SkipGuard<'a, R> {
    pub fn next(&mut self) -> Option<bool> {
        if self.proccessed < self.skip_count as i64 {
            self.proccessed += 1;
            Some(false)
        } else if self.proccessed == self.skip_count as i64 {
            self.proccessed += 1;
            Some(true)
        } else {
            None
        }
    }

    pub fn get_new(self) -> SkipGuard<'a, R> {
        // This is somewhat inefficient since we will
        // Sometimes add a Skip to the free list
        // just to take it out again immediately  
        let sampler = self.sampler;      
        drop(self);
        sampler.get_skip()
    }
}

impl<'a, R: Fn() -> f32 + Clone> Drop for SkipGuard<'a, R> {
    fn drop(&mut self) {
        self.sampler.list_of_skips.return_skip(&self)
    }
}

impl<'a, R: Fn() -> f32 + Clone> ListOfSkips<R> {
    pub fn new(n_threads: u32, n_samples: u32, random: R) -> Self {
        let mut list = Vec::with_capacity(n_threads as usize);
        let w = (((random)()).ln()/n_samples as f32).exp();
        list.push(Skip {
            // floor(log(random())/log(1 − Wi ))
            index: AtomicU32::new(0),
            //w: AtomicF32::new(w),
            length: AtomicU64::new(0),
            next: AtomicU32::new(u32::MAX),
        });
        for i in 1..=n_threads { // Theoretically every thread could get an overflow skip
            list.push(Skip {
                index: AtomicU32::new(u32::MAX),  // These are essentially 
                // w: AtomicF32::new(w),      // undefined
                length: AtomicU64::new(0), // at this point
                next: AtomicU32::new(i+1),
            });
        }
        list.last_mut().unwrap().next.store(u32::MAX, Relaxed);
        Self {
            index_and_w_next: AtomicU32F32Tup::new((1, w)),
            list: list.into_boxed_slice(),
            head: AtomicU64::new(0),
            free_head: AtomicU64::new(1),
            m: n_samples,
            random,
            overflow_skips: Mutex::new(LinkedList::new()),
            num_overflow_skips: AtomicUsize::new(0),
        }
    }

    fn put_to_head(&'a self, i: u32) {
        let skip = &self.list[i as usize];
        loop {
            let head = self.head.load(Ordering::Acquire);
            let new_version = ((head >> 32) + 1) << 32;
            skip.next.store(head as u32, Relaxed);
            let new_head = i as u64 | new_version;
            let ce_result = self.head.compare_exchange(head, new_head, Ordering::AcqRel, Relaxed);
            if ce_result.is_ok() {
                // We successfully put the new skip into the list
                return;
            }
        }
    }

    fn allocate_new_with_free(&'a self, free: u32) -> (u32, &'a Skip) {
        let (mut i, mut w) = self.index_and_w_next.load(Relaxed);
        loop {
            let w_next = if i >= self.m { 
                w * ((((self.random)()).ln()/(self.m as f32)).exp())
            } else { w };
            let cs_result = self.index_and_w_next.compare_exchange((i, w), (i+1, w_next), Ordering::AcqRel, Relaxed);
            if let Err(w_actual) = cs_result {
                (i, w) = w_actual;
            } else {
                break;
            }
        }
        // We own this skip now
        let free_skip = &self.list[free as usize];
        let new_skip = if i >= self.m {
            let random = (self.random)();
            (random.log2()/(1.0-w).log2()) as u64
        } else {
            0
        };
        free_skip.index.store(i, Relaxed);
        free_skip.length.store(new_skip, Relaxed);
        loop {
            // Swap the current head with the new skip
            let head = self.head.load(Ordering::Acquire);
            //let w_prev = self.list[(head as u32) as usize].w.load(Relaxed).0;
            //let w_next = w_prev * (((self.random)()).ln()/self.m as f32).exp();
            //free_skip.w.store((w_next, i), Relaxed);
            let head_next = self.list[(head as u32) as usize].next.load(Ordering::Acquire);
            let new_version = ((head >> 32) + 1) << 32;
            free_skip.next.store(head_next, Relaxed);
            let new_head = free as u64 | new_version;
            let ce_result = self.head.compare_exchange(head, new_head, Ordering::AcqRel, Relaxed);
            if ce_result.is_ok() {
                let head_skip = &self.list[(head as u32) as usize];
                if head_skip.index.load(Relaxed) < self.m {
                    assert_eq!(head_skip.length.load(Relaxed), 0);
                }
                // We successfully put the new skip into the list
                return (head as u32, head_skip)
                }
        }
    }

    pub fn allocate_new(&'a self) -> (u32, &'a Skip) {
        loop {
            let free_head = self.free_head.load(Ordering::Acquire);
            if free_head as u32 == u32::MAX {
                // No free skips. This must not happen since we
                // allocated enough skips for all threads at the start
                panic!("No free skips");
            }
            let new_version = ((free_head >> 32) + 1) << 32;
            let free_head_next = self.list[(free_head as u32) as usize].next.load(Relaxed);
            let new_free_head = free_head_next as u64 | new_version;
            let ce_result = self.free_head.compare_exchange(free_head, new_free_head, Ordering::AcqRel, Relaxed);
            if ce_result.is_ok() {
                // We successfully allocated a new skip
                // Put it into the list
                // We don't have to worry about the previous head here
                return self.allocate_new_with_free(free_head as u32);
            }
        }
    }


    pub fn get_skip(&'a self) -> (u32, &'a Skip) {
        loop {
            let head = self.head.load(Ordering::Acquire);
            let head_index = (head as u32) as usize;
            let head_skip = &self.list[head_index];
            // Check whether it has a next, else we need to allocate a new skip
            // If someone else happens to do the same thing in parallel, we don't care
            let head_next = head_skip.next.load(Relaxed);
            if head_next == u32::MAX {
                return self.allocate_new()
            } else {
                // We can just use the next skip
                let head_skip = &self.list[(head as u32) as usize];
                let new_version = (head >> 32) + 1;
                let new_head = (head_next as u64 & 0x00000000FFFFFFFF) | (new_version << 32);
                let ce_result = self.head.compare_exchange(head, new_head, Ordering::AcqRel, Relaxed);
                if ce_result.is_ok() {
                    // We successfully got a new skip
                    return (head as u32, head_skip)
                }
            }
        }
    }

    fn return_skip(&self, skip_guard: &SkipGuard<R>) {
        if skip_guard.proccessed > skip_guard.skip_count as i64 {
            // We have processed the skip, return it to the free list unless our overflow list
            // is not empty
            let overflow = self.num_overflow_skips.fetch_update(Ordering::AcqRel, Relaxed, |c| {
                if c > 0 {
                    Some(c - 1)
                } else {
                    None
                }
            });
            if overflow.is_ok() {
                // We have 'reserved' an element from the overflow list
                let mut overflow_list = self.overflow_skips.lock(); 
                let elem = overflow_list.pop_front().unwrap();
                drop(overflow_list);
                skip_guard.skip.index.store(elem.index.load(Relaxed), Relaxed);
                //self.skip.w.store(elem.w.load(Relaxed), Relaxed);
                skip_guard.skip.length.store(elem.length.load(Relaxed), Relaxed);
            } else {
                loop {
                    // We have processed the skip, return it to the free list
                    let free_head = self.free_head.load(Relaxed);
                    let new_version = ((free_head >> 32) + 1) << 32;
                    let new_free_head = skip_guard.list_index as u64 | new_version;
                    skip_guard.skip.next.store(free_head as u32, Relaxed);
                    let ce_result = self.free_head.compare_exchange(free_head, new_free_head, Ordering::AcqRel, Relaxed);
                    if ce_result.is_ok() {
                        return;
                    }
                }
            }
        } else {
            assert_eq!(skip_guard.skip.length.load(Relaxed), skip_guard.skip_count);
            skip_guard.skip.length.store((skip_guard.skip_count as i64 - skip_guard.proccessed) as u64, Relaxed);
        }
        // Return it to the head of the list of skips
        self.put_to_head(skip_guard.list_index)
    }
    
    // TODO: We could implement something like a "replace skip" here
    //       That reuses the already reserved skip for a performance improvement

    fn delete_sample(&self, index: u32) {
        // If we still have space in the list, we insert a new 0-skip
        // at the head, otherwise we put it into the overflow list
        loop {
            let free_head = self.free_head.load(Ordering::Acquire);
            if free_head as u32 == u32::MAX {
                // Put current head into overflow list
                let (skip_i, skip) = self.get_skip();
                let mut overflow_list = self.overflow_skips.lock();
                overflow_list.push_front(Skip {
                    index: AtomicU32::new(skip.index.load(Relaxed)),
                    // w: AtomicF32::new(0.0),
                    length: AtomicU64::new(skip.length.load(Relaxed)),
                    next: AtomicU32::new(u32::MAX),
                });
                self.num_overflow_skips.fetch_add(1, Ordering::Relaxed);
                drop(overflow_list);
                skip.index.store(index, Relaxed);
                skip.length.store(0, Relaxed);
                self.put_to_head(skip_i);
                return;
            }
            let new_version = ((free_head >> 32) + 1) << 32;
            let free_head_next = self.list[(free_head as u32) as usize].next.load(Relaxed);
            let new_free_head = free_head_next as u64 | new_version;
            let ce_result = self.free_head.compare_exchange(free_head, new_free_head, Ordering::AcqRel, Relaxed);
            if ce_result.is_ok() {
                let skip = &self.list[(free_head as u32) as usize];
                skip.index.store(index, Relaxed);
                skip.length.store(0, Relaxed);
                self.put_to_head(free_head as u32);
                return;
            }
        }
    }

    /// Serializes the list of skips into a byte vector
    /// Only call when there's no other thread accessing the list
    /// Otherwise we cannot possibly serialize the entire list since it might change
    /// while we're serializing it
    /// We'll probably achieve this by using a RwLock over the entire 
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);
        let (index, w_next) = self.index_and_w_next.load(Relaxed);
        cursor.write_u32::<BigEndian>(index).unwrap();
        cursor.write_f32::<BigEndian>(w_next).unwrap();
        let mut next = self.head.load(Relaxed) as u32;
        while next != u32::MAX {
            let skip = &self.list[next as usize];
            cursor.write_u32::<BigEndian>(skip.index.load(Relaxed)).unwrap();
            cursor.write_u64::<BigEndian>(skip.length.load(Relaxed)).unwrap();
            next = skip.next.load(Relaxed);
        }
        let overflow_list = self.overflow_skips.lock();
        for skip in overflow_list.iter() {
            cursor.write_u32::<BigEndian>(skip.index.load(Relaxed)).unwrap();
            cursor.write_u64::<BigEndian>(skip.length.load(Relaxed)).unwrap();
        }
        return bytes;
    }

    pub fn from_bytes(bytes: &[u8], n_threads: u32, n_samples: u32, random: R) -> Self {
        let mut cursor = Cursor::new(bytes);
        let index = cursor.read_u32::<BigEndian>().unwrap();
        let w_next = cursor.read_f32::<BigEndian>().unwrap();
        let mut list: Vec<Skip> = Vec::new();
        while cursor.position() < bytes.len() as u64 && list.len() <= n_threads as usize {
            let index = cursor.read_u32::<BigEndian>().unwrap();
            let length = cursor.read_u64::<BigEndian>().unwrap();
            list.push(Skip {
                index: AtomicU32::new(index),
                // w: AtomicF32::new(0.0),
                length: AtomicU64::new(length),
                next: AtomicU32::new(list.len() as u32 + 1),
            });
        }
        list.last().unwrap().next.store(u32::MAX, Relaxed);
        let mut overflow_skips = LinkedList::new();
        while cursor.position() < bytes.len() as u64 {
            let index = cursor.read_u32::<BigEndian>().unwrap();
            let length = cursor.read_u64::<BigEndian>().unwrap();
            overflow_skips.push_back(Skip {
                index: AtomicU32::new(index),
                // w: AtomicF32::new(0.0),
                length: AtomicU64::new(length),
                next: AtomicU32::new(u32::MAX),
            });
        }
        let free_head = if list.len() <= n_threads as usize {
            list.len() as u64
        } else {
            u32::MAX as u64
        };
        while list.len() <= n_threads as usize {
            list.push(Skip {
                index: AtomicU32::new(u32::MAX),
                // w: AtomicF32::new(0.0),
                length: AtomicU64::new(0),
                next: AtomicU32::new(list.len() as u32 + 1),
            });
        }
        list.last().unwrap().next.store(u32::MAX, Relaxed);
        return Self {
            random,
            m: n_samples,
            list: list.into_boxed_slice(),
            head: AtomicU64::new(0),
            index_and_w_next: AtomicU32F32Tup::new((index, w_next)),
            free_head: AtomicU64::new(free_head),
            num_overflow_skips: AtomicUsize::new(0),
            overflow_skips: Mutex::new(overflow_skips),
        }
    }
}

#[derive(Debug)]
pub struct ReservoirSampler<R: Fn() -> f32 + Clone = fn() -> f32> {
    list_of_skips: ListOfSkips<R>,
    sample_size: u32,
    // We will probably have to increment a version counter on every modification. I think this is probably neccesary
    // for recovery once it is implemented to be able to see what has to be undone/redone.
    // We will then just log the version with any modification to the LoS to the WAL
    snapshot_latch: RwLock<()>,
}

impl ReservoirSampler<fn() -> f32> {
    pub fn new(sample_size: u32, n_threads: u32) -> ReservoirSampler<fn() -> f32> {
        ReservoirSampler::with_random(sample_size, n_threads, rand::random)
    }

    pub fn parse(raw: &[u8], n_threads: u32) -> ReservoirSampler<fn() -> f32> {
        ReservoirSampler::parse_with_random(raw, n_threads, rand::random)
    }
}

impl<'a, R: Fn() -> f32 + Clone> ReservoirSampler<R> {

    fn with_random(sample_size: u32, n_threads: u32, random: R) -> ReservoirSampler<R> {
        ReservoirSampler {
            list_of_skips: ListOfSkips::new(n_threads, sample_size, random),
            sample_size,
            snapshot_latch: RwLock::new(()),
        }
    }

    fn parse_with_random(raw: &[u8], n_threads: u32, random: R) -> ReservoirSampler<R> {
        let mut raw_cursor = Cursor::new(raw);
        let sample_size = raw_cursor.read_u32::<BigEndian>().unwrap();
        let los = ListOfSkips::from_bytes(&raw[4..], n_threads, sample_size, random.clone());  
        return ReservoirSampler {
            list_of_skips: los,
            sample_size,
            snapshot_latch: RwLock::new(()),
        }      
    }

    pub fn get_skip(&'a self) -> SkipGuard<'a, R> {
        let _snapshot_latch_guard = self.snapshot_latch.read();
        let (list_index, skip) = self.list_of_skips.get_skip();
        return SkipGuard {
            list_index,
            skip,
            sampler: &self,
            skip_count: skip.length.load(Relaxed),
            skip_index: skip.index.load(Relaxed),
            proccessed: 0,
            _snapshot_latch_guard
        }
    }

    pub fn delete_sample(&self, index: u32) {
        let _snapshot_latch_guard = self.snapshot_latch.read();
        self.list_of_skips.delete_sample(index);
    } 

    pub fn delete_skipped(&self, n: usize) {
        let _snapshot_latch_guard = self.snapshot_latch.read();
        // We successfully increased the number of skips
        // We can now delete the skipped tuple
        let mut skip = self.get_skip();
        skip.proccessed = -(n as i64);
    }

    pub fn snapshot(&self) -> Vec<u8> {
        let _snapshot_latch = self.snapshot_latch.write();
        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);
        cursor.write_u32::<BigEndian>(self.sample_size).unwrap();
        let los_bytes: Vec<u8> = self.list_of_skips.to_bytes();
        cursor.write(&los_bytes).unwrap();
        return bytes;
    }
}

// TODO(important): Finish implementing the reservoir sampler including preloading and
//                  serialization and deserialization of this into a byte vector
//                  So that we can save the state of the reservoir sampler.
//                  For preloading the simplest solution is likely, just giving out "0 skips"
//                  while adding 1 to the preload count before. Whenever the preload count goes
//                  below 0, we just throw away the list of skips and start over whenever we
//                  are finished preloading again.



#[cfg(test)]
mod test {
    use std::{cell::RefCell, sync::Arc, collections::{VecDeque, BTreeSet}};

    use parking_lot::Mutex;
    use rand::{thread_rng, Rng, SeedableRng, rngs::StdRng};

    use super::*;

    fn exp_rv(intensity: f64, rng: &mut StdRng) -> f64 {
        (-1.0 / intensity) * rng.gen::<f64>().ln()
    }

    fn gen_data() -> Vec<f64> {
        let mut rng = StdRng::seed_from_u64(1234);
        let intensity = 0.1;
        let mut data = Vec::with_capacity(200_000);
        for _ in 0..200_000 {
            data.push(exp_rv(intensity, &mut rng));
        }
        data.sort_by(|a, b| a.partial_cmp(b).unwrap());
        data
    }

    #[test]
    fn test_init_1() {
        let los = ListOfSkips::new(1, 1, || thread_rng().gen());
        assert_eq!(los.list.len(), 2);
    }

    #[test]
    fn test_init_2() {
        let los = ListOfSkips::new(2, 1, || thread_rng().gen());
        assert_eq!(los.list.len(), 3);
    }

    #[test]
    fn test_get_skip_1() {
        let rng = RefCell::new(rand::rngs::StdRng::seed_from_u64(0));
        let los = ListOfSkips::new(1, 1, || rng.borrow_mut().gen());
        let (list_index, skip) = los.get_skip();
        assert_eq!(list_index, 0);
        assert_eq!(skip.length.load(Relaxed), 0);
        assert_eq!(los.head.load(Relaxed), 1u64 << 32 | 1);
        assert_eq!(los.free_head.load(Relaxed), 1u64 << 32 | u32::MAX as u64);
    }

    #[test]
    fn test_get_skip_2() {
        let rng = RefCell::new(rand::rngs::StdRng::seed_from_u64(0));
        let los = ListOfSkips::new(1, 1, || rng.borrow_mut().gen());
        let (list_index, skip) = los.get_skip();
        assert_eq!(list_index, 0);
        assert_eq!(skip.length.load(Relaxed), 0);
        assert_eq!(los.head.load(Relaxed), 1u64 << 32 | 1);
        assert_eq!(los.free_head.load(Relaxed), 1u64 << 32 | u32::MAX as u64);
    }

    #[test]
    fn single_threaded_skip_test() {
        let rng = RefCell::new(rand::rngs::StdRng::seed_from_u64(4224123));
        let sampler = ReservoirSampler::with_random(1000, 1, || rng.borrow_mut().gen());
        let data = gen_data();
        let mut res = data[0..1000].to_vec();
        let mut skip = sampler.get_skip();
        let mut skips = VecDeque::new();
        for v in data.iter() {
            loop {
                if let Some(take) = skip.next() {
                    if take {
                        let ind = if skip.skip_index < 1000 {
                            skip.skip_index as usize
                        } else {
                            rng.borrow_mut().gen_range(0..1000)
                        };
                        res[ind] = *v;
                        skip = skip.get_new();
                        skips.push_back(skip.skip_count);
                    }
                    break;
                } else {
                    skip = skip.get_new();
                }
            }
        }
        let mean = res.iter().sum::<f64>() / res.len() as f64;
        assert!((1.0/mean - 0.1).abs() < 0.01);

        let rng = RefCell::new(rand::rngs::StdRng::seed_from_u64(4224123));
        let sampler = ReservoirSampler::with_random(1000, 200, || rng.borrow_mut().gen());
        let data = gen_data();
        let mut res = data[0..1000].to_vec();
        let mut skip = sampler.get_skip();
        for v in data.iter() {
            loop {
                if let Some(take) = skip.next() {
                    if take {
                        let ind = if skip.skip_index < 1000 {
                            skip.skip_index as usize
                        } else {
                            rng.borrow_mut().gen_range(0..1000)
                        };
                        res[ind] = *v;
                        skip = skip.get_new();
                        assert_eq!(skips.pop_front().unwrap(), skip.skip_count);
                    }
                    break;
                } else {
                    skip = skip.get_new();
                }
            }
        }
        let mean = res.iter().sum::<f64>() / res.len() as f64;
        assert!((1.0/mean - 0.1).abs() < 0.01);

    }

    #[test]
    // This test is not deterministic, but it should fail with a very low probability
    // It's more of a sanity check than a real test
    fn multithreaded_get_skip_test() {
        let mut success_counter = 0;
        let num_threads = 12;
        let n_samples = 1000;
        for _ in 0..3 {
            // Make sure only one thread at a time has a skip
            let t_s: Arc<Vec<Mutex<()>>>= Arc::new((0..=num_threads).map(|_| Mutex::new(())).collect());
            let sampler = Arc::new(ReservoirSampler::new(n_samples, num_threads));
            let mut threads = Vec::new();
            let data = gen_data();
            let res = Arc::new(Mutex::new(data[0..n_samples as usize].to_vec()));
            let data = Arc::new(data);
            for t in 0..num_threads as usize {
                let sampler = sampler.clone();
                let res = res.clone();
                let data = data.clone();
                let t_s = t_s.clone();
                threads.push(std::thread::spawn(move || {
                    let mut rng = rand::thread_rng();
                    let mut skip = sampler.get_skip();
                    let mut lock = t_s[skip.list_index as usize].try_lock().unwrap();
                    for v in data.iter().enumerate().filter(|(i, _)| i % num_threads as usize == t).map(|(_, v)| v) {
                        loop {
                            if let Some(take) = skip.next() {
                                if take {
                                    if skip.skip_index < n_samples {
                                        assert_eq!(skip.skip_count, 0);
                                        res.lock()[skip.skip_index as usize] = *v;
                                    } else {
                                        res.lock()[rng.gen_range(0..n_samples as usize)] = *v;
                                    }
                                    drop(lock);
                                    skip = skip.get_new();
                                    lock = t_s[skip.list_index as usize].try_lock().unwrap();
                                }
                                break;
                            } else {
                                drop(lock);
                                skip = skip.get_new();
                                lock = t_s[skip.list_index as usize].try_lock().unwrap();
                            }
                        }
                    }
                    drop(lock);
                    drop(skip);
                }));
            }
            for thread in threads {
                thread.join().unwrap();
            }
            let mean = res.lock().iter().sum::<f64>() / n_samples as f64;
            if (-0.025..0.025).contains(&(1.0/mean - 0.1)) {
                success_counter += 1;
            }
        }
        // This could sometimes fail (sth like < 1 in a 1000 runs) out of pure chance but should be very rare
        assert!(success_counter >= 2, "Success rate: {}", success_counter as f64 / 10.0);
    }

    #[test]
    fn test_binary_snapshot_simple() {
        let sampler = ReservoirSampler::with_random(1000, 10, || rand::thread_rng().gen());
        let mut skip = sampler.get_skip();
        let mut skips = VecDeque::new();
        for _ in 0..1000 {
            loop {
                if let Some(take) = skip.next() {
                    if take {
                        skip = skip.get_new();
                        skips.push_back(skip.skip_count);
                    }
                    break;
                } else {
                    skip = skip.get_new();
                }
            }
        }
        let skip_2 = sampler.get_skip();
        drop(skip);
        drop(skip_2);
        let binary = sampler.snapshot();
        let sampler_recons = ReservoirSampler::parse_with_random(&binary, 10, || rand::thread_rng().gen());
        let mut next = sampler.list_of_skips.head.load(Relaxed) as u32;
        let mut next_recons = sampler_recons.list_of_skips.head.load(Relaxed) as u32;
        let mut seen_list_indexes = BTreeSet::new();
        while next != u32::MAX && next_recons != u32::MAX {
            let skip = &sampler.list_of_skips.list[next as usize];
            let skip_recons = &sampler_recons.list_of_skips.list[next_recons as usize];
            assert_eq!(skip.index.load(Relaxed), skip_recons.index.load(Relaxed));
            assert_eq!(skip.length.load(Relaxed), skip_recons.length.load(Relaxed));
            seen_list_indexes.insert(next_recons);
            next = skip.next.load(Relaxed) as u32;
            next_recons = skip_recons.next.load(Relaxed) as u32;
        }
        assert_eq!(next, next_recons);
        // Check free list
        let mut next = sampler.list_of_skips.free_head.load(Relaxed) as u32;
        let mut next_recons = sampler_recons.list_of_skips.free_head.load(Relaxed) as u32;
        while next != u32::MAX && next_recons != u32::MAX {
            let skip = &sampler.list_of_skips.list[next as usize];
            let skip_recons = &sampler.list_of_skips.list[next as usize];
            assert!(!seen_list_indexes.contains(&next_recons));
            seen_list_indexes.insert(next_recons);
            next = skip.next.load(Relaxed) as u32;
            next_recons = skip_recons.next.load(Relaxed) as u32;
        }
        assert_eq!(seen_list_indexes.len(), sampler_recons.list_of_skips.list.len());
        assert_eq!(next, next_recons);
    }

    #[test]
    fn test_delete_skipped() {
        let sampler = ReservoirSampler::with_random(1000, 1, || rand::thread_rng().gen());
        sampler.delete_skipped(2);
        let skip = sampler.get_skip();
        assert_eq!(skip.skip_count, 2);
    }

    #[test]
    fn test_delete_sample() {
        let sampler = ReservoirSampler::with_random(1000, 1, || rand::thread_rng().gen());
        let mut skip = sampler.get_skip();
        skip.next();
        skip = skip.get_new();
        skip.next();
        drop(skip);
        sampler.delete_sample(0);
        let skip = sampler.get_skip();
        assert_eq!(skip.skip_count, 0);
        assert_eq!(skip.skip_index, 0);
    }

    #[test]
    fn test_overflow() {
        let sampler = ReservoirSampler::with_random(10, 1, || rand::thread_rng().gen());
        let mut skip = sampler.get_skip();
        for _ in 0..9 {
            skip.next();
            skip = skip.get_new();
        }
        skip.next();
        drop(skip);
        for i in 0..10 {
            sampler.delete_sample(i);
        }
        for i in (0..10).rev() {
            let mut skip = sampler.get_skip();
            assert_eq!(skip.skip_count, 0);
            assert_eq!(skip.skip_index, i);
            skip.next();
        }
        let skip = sampler.get_skip();
        assert_eq!(skip.skip_index, 10);
    }
}
