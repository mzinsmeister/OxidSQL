
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

    The original paper describes an approach for deletes that uses upcoming tuples for deleted
    ones that were included in the sample... This approach has the advantage of beeing very cheap 
    however I don't like it since for tuples in the sample it requires adding a Skip node to the
    List of skips. This means that the List of skips could grow as large as the sample if, for example,
    the entire table is deleted. Also if a significant portion of tuples is deleted you will end up
    just automatically including the next bunch of tuples which are likely to be rather related to
    each other and will therefore make the sample much less representative of the entire table.
    The strategy we will use instead is to just sample new tuples from the main segment.
    To do this we will use the following strategy since we have no way of finding the 'nth tuple'
    without scanning the entire table (we can only randomly pick pages): Randomly choose 3 pages. Now
    the probability of a specific tuple per page beeing picked is 1/P * 1/Tp where P is the number of
    pages and Tp is the number of tuples on the page. Therefore a tuple on a page with twice as many
    tuples will be picked with half the probability. To correct for this somewhat (only each tuple *on
    the 3 pages* will have the same probability, not each tuple in the whole segment), we will have to make
    the probability of a tuple on that page beeing picked twice as likely. To do this we think of the 3
    pages as a sequential store of tuples and pick a random tuple from them. If that tuple is already 
    in the sample (we will set a flag in the actual full segment) we just scan the relation until we 
    find one that's not in the sample. This can result in a full table scan if the relation is very 
    close to the sample size but this will only happen in very rare cases and even
    then it's not that expensive since the main segment will then be only slightly larger than the
    sample. We do this so that we can somewhat eliminate the bias towards larger tuples if we would
    do a two stage sampling (page->tuple on page).
    
    The samples will be stored in a regular transactional segment so
    we will just update the samples in place (single active writer will be enforced since it's enforced)
    on the main segment anyway. 
*/
use std::sync::atomic::{AtomicU64, AtomicU32};
use std::sync::atomic::Ordering::Relaxed;

use atomic::Ordering;
use rand::Rng;

// Quick and Hacky Atomic f32, u32 tuple
pub struct AtomicF32 {
    storage: AtomicU32,
}
impl AtomicF32 {
    pub fn new(value: f32) -> Self {
        let as_u32 = value.to_bits();
        Self { storage: AtomicU32::new(as_u32) }
    }
    pub fn store(&self, value: f32, ordering: Ordering) {
        let as_u32 = value.to_bits();
        self.storage.store(as_u32, ordering)
    }
    pub fn load(&self, ordering: Ordering) -> f32 {
        let as_u32 = self.storage.load(ordering);
        f32::from_bits(as_u32)
    }

    pub fn compare_exchange(&self, current: f32, new: f32, success: Ordering, failure: Ordering) -> Result<f32, f32> {
        let current_as_u32 = current.to_bits();
        let new_as_u32 = new.to_bits();
        match self.storage.compare_exchange(current_as_u32, new_as_u32, success, failure) {
            Ok(_) => Ok(current),
            Err(actual) => Err(f32::from_bits(actual))
        }
    }
}

// Since we don't use the original papers way of doing deletes
// we don't need to store the index.
// And I'm not quite sure why the 
struct Skip {
    length: AtomicU64,
    next: AtomicU32, // u32::MAX means end of list
}

struct ListOfSkips<R: Fn() -> f32> {
    // we will access this concurrently so it must be fixed size at runtime
    // we will therefore allocate all the skips we will need at the start
    w_next: AtomicF32,
    list: Box<[Skip]>,
    head: AtomicU64,
    free_head: AtomicU64,
    m: u32,
    random: R,
}

pub struct SkipGuard<'a, R: Fn() -> f32> {
    skip_count: u64,
    proccessed: u64,
    list_index: u32,
    skip: &'a Skip,
    list: &'a ListOfSkips<R>
}

impl<'a, R: Fn() -> f32> SkipGuard<'a, R> {
    pub fn next(&mut self) -> Option<bool> {
        if self.proccessed < self.skip_count {
            self.proccessed += 1;
            Some(false)
        } else if self.proccessed == self.skip_count {
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
        let list = self.list;
        drop(self);
        list.get_skip()
    }
}

impl<'a, R: Fn() -> f32> Drop for SkipGuard<'a, R> {
    fn drop(&mut self) {
        if self.proccessed > self.skip_count {
            loop {
                // We have processed the skip, return it to the free list
                let free_head = self.list.free_head.load(Relaxed);
                let new_version = ((free_head >> 32) + 1) << 32;
                let new_free_head = self.list_index as u64 | new_version;
                self.skip.next.store(free_head as u32, Relaxed);
                let ce_result = self.list.free_head.compare_exchange(free_head, new_free_head, Ordering::AcqRel, Relaxed);
                if ce_result.is_ok() {
                    return;
                }
            }
        } else {
            assert_eq!(self.skip_count, self.skip.length.load(Relaxed));
            // We have not completely processed the skip, return it to the list of skips
            loop {
                let head = self.list.head.load(Ordering::Acquire);
                let new_version = ((head >> 32) + 1) << 32;
                let new_head = (self.list_index as u64 & 0x00000000FFFFFFFF) | new_version;
                self.skip.next.store(head as u32, Relaxed);
                let ce_result = self.list.head.compare_exchange(head, new_head, Ordering::AcqRel, Relaxed);
                if ce_result.is_ok() {
                    return;
                }
            }
        }
    }
}

impl<R: Fn() -> f32> ListOfSkips<R> {
    pub fn new(n_threads: u32, n_samples: u32, random: R) -> Self {
        let mut list = Vec::with_capacity(n_threads as usize);
        let w = (((random)()).ln()/n_samples as f32).exp();
        let w_next = w * (((random)()).ln()/n_samples as f32).exp();
        list.push(Skip {
            // floor(log(random())/log(1 − Wi ))
            length: AtomicU64::new(((random)().log2()/(1.0 - w).log2()).floor() as u64),
            next: AtomicU32::new(u32::MAX),
        });
        for i in 1..=n_threads {
            list.push(Skip {
                length: AtomicU64::new(999321),
                next: AtomicU32::new(i+1),
            });
        }
        list.last_mut().unwrap().next.store(u32::MAX, Relaxed);
        Self {
            w_next: AtomicF32::new(w_next),
            list: list.into_boxed_slice(),
            head: AtomicU64::new(0),
            free_head: AtomicU64::new(1),
            m: n_samples,
            random,
        }
    }

    pub fn from_bytes(bytes: &[u8], random: R) -> Self {
        let len = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let mut list = Vec::with_capacity(len as usize);
        let head = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
        let free_head = u64::from_le_bytes(bytes[12..20].try_into().unwrap());
        let m = u32::from_le_bytes(bytes[20..24].try_into().unwrap());
        let w_next = f32::from_le_bytes(bytes[24..28].try_into().unwrap());
        for i in 0..len {
            let start_byte = i as usize * 12 + 28;
            let length = u64::from_le_bytes(bytes[start_byte..start_byte+8].try_into().unwrap());
            let next = u32::from_le_bytes(bytes[start_byte+8..start_byte+12].try_into().unwrap());
            list.push(Skip {
                length: AtomicU64::new(length),
                next: AtomicU32::new(next),
            });
        }
        Self {
            w_next: AtomicF32::new(0.0),
            list: list.into_boxed_slice(),
            head: AtomicU64::new(0),
            free_head: AtomicU64::new(1),
            m: 0,
            random,
        }
    }

    pub fn allocate_new_with_free(&self, free: u32) -> SkipGuard<R> {
        let mut w = self.w_next.load(Relaxed);
        loop {
            let w_next = w * ((((self.random)()).ln()/(self.m as f32)).exp());
            let cs_result = self.w_next.compare_exchange(w, w_next, Ordering::AcqRel, Relaxed);
            if let Err(w_actual) = cs_result {
                w = w_actual;
            } else {
                break;
            }
        }
        // We own this skip now
        let free_skip = &self.list[free as usize];
        let random = (self.random)();
        let new_skip = (random.log2()/(1.0-w).log2()) as u64;
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
                // We successfully put the new skip into the list
                return SkipGuard {
                    skip_count: head_skip.length.load(Relaxed),
                    proccessed: 0,
                    list_index: head as u32,
                    skip: head_skip,
                    list: self,
                }
            }
        }
    }

    pub fn allocate_new(&self) -> SkipGuard<R> {
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


    pub fn get_skip(&self) -> SkipGuard<R> {
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
                    return SkipGuard {
                        // We can just work with this now since the Skip is now "ours" until we return it
                        skip_count: head_skip.length.load(Relaxed),
                        proccessed: 0,
                        list_index: head as u32,
                        skip: head_skip,
                        list: self,
                    }
                }
            }
        }
    }

    
    // TODO: We could implement something like a "replace skip" here
    // That reuses the already reserved skip for a performance improvement

    fn delete_skipped(&self) {
        // Increase the skip of some node count by 1 
        // (pretend like the tuple was never there)
        let skip = self.get_skip();
        skip.skip.length.fetch_add(1, Relaxed);
    }
}

impl<R: Fn() -> f32> From<ListOfSkips<R>> for Vec<u8> {
    /// Serializes the list of skips into a byte vector
    /// Only call when there's no other thread accessing the list
    fn from(value: ListOfSkips<R>) -> Self {
        let mut vec = Vec::with_capacity(4 + 24 + value.list.len() * 12);
        vec.extend_from_slice(&(value.list.len() as u32).to_be_bytes());
        vec.extend_from_slice(&value.head.load(Relaxed).to_be_bytes());
        vec.extend_from_slice(&value.free_head.load(Relaxed).to_be_bytes());
        vec.extend_from_slice(&value.w_next.load(Relaxed).to_be_bytes());
        vec.extend_from_slice(&value.m.to_be_bytes());
        for skip in value.list.iter() {
            vec.extend_from_slice(&skip.next.load(Relaxed).to_be_bytes());
            vec.extend_from_slice(&skip.length.load(Relaxed).to_be_bytes());
        }
        vec
    }
}

pub struct ReservoirSampler<R: Fn() -> f32 + Clone = fn() -> f32> {
    random: R,
    list_of_skips: Option<ListOfSkips<R>>,
    preload__count: AtomicU32,
    sample_size: u32,
    n_threads: u32,
}

impl ReservoirSampler<fn() -> f32> {
    pub fn new(sample_size: u32, n_threads: u32) -> ReservoirSampler<fn() -> f32> {
        ReservoirSampler::with_random(sample_size, n_threads, || rand::random())
    }
}

impl<R: Fn() -> f32 + Clone> ReservoirSampler<R> {

    fn with_random(sample_size: u32, n_threads: u32, random: R) -> ReservoirSampler<R> {
        ReservoirSampler {
            random,
            list_of_skips: None,
            preload__count: AtomicU32::new(0),
            sample_size,
            n_threads,
        }
    }
}

// TODO(important): Finish implementing the reservoir sampler including preloading and
//                  serialization and deserialization of this into a byte vector
//                  So that we can save the state of the reservoir sampler.
//                  For preloading the simplest solution is likely, just giving out "0 skips"
//                  while adding 1 to the preload count before. Whenever the preload count goes
//                  below 0, we just throw away the list of skips and start over whenever we
//                  are finished preloading again.

impl<R: Fn() -> f32 + Clone> From<ReservoirSampler<R>> for Vec<u8> {
    /// Serializes the list of skips into a byte vector
    /// Only call when there's no other thread accessing the list
    fn from(value: ReservoirSampler<R>) -> Self {
        let mut vec = Vec::new();
        vec.extend_from_slice(&value.preload__count.load(Relaxed).to_be_bytes());
        vec.extend_from_slice(&value.sample_size.to_be_bytes());
        if let Some(los_bytes) = value.list_of_skips.map(|los| Vec::<u8>::from(los)) {
            vec.extend_from_slice(&los_bytes);
        }
        vec
    }
}


#[cfg(test)]
mod test {
    use std::{cell::RefCell, sync::Arc, collections::VecDeque};

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
        let skip = los.get_skip();
        assert_eq!(skip.skip_count, 0);
        assert_eq!(skip.proccessed, 0);
        assert_eq!(skip.list_index, 0);
        assert_eq!(skip.skip.length.load(Relaxed), 0);
        assert_eq!(skip.skip.next.load(Relaxed), u32::MAX);
        assert_eq!(los.head.load(Relaxed), 1u64 << 32 | 1);
        assert_eq!(los.free_head.load(Relaxed), 1u64 << 32 | u32::MAX as u64);
    }

    #[test]
    fn test_get_skip_2() {
        let rng = RefCell::new(rand::rngs::StdRng::seed_from_u64(0));
        let los = ListOfSkips::new(1, 1, || rng.borrow_mut().gen());
        let skip = los.get_skip();
        assert_eq!(skip.skip_count, 0);
        assert_eq!(skip.proccessed, 0);
        assert_eq!(skip.list_index, 0);
        assert_eq!(skip.skip.length.load(Relaxed), 0);
        assert_eq!(skip.skip.next.load(Relaxed), u32::MAX);
        assert_eq!(los.head.load(Relaxed), 1u64 << 32 | 1);
        assert_eq!(los.free_head.load(Relaxed), 1u64 << 32 | u32::MAX as u64);
    }

    #[test]
    fn single_threaded_skip_test() {
        let rng = RefCell::new(rand::rngs::StdRng::seed_from_u64(4224123));
        let los = ListOfSkips::new(1, 1000, || rng.borrow_mut().gen());
        let data = gen_data();
        let mut res = data[0..1000].to_vec();
        let mut skip = los.get_skip();
        let mut skips = VecDeque::new();
        for v in data.iter().skip(1000) {
            loop {
                if let Some(take) = skip.next() {
                    if take {
                        res[rng.borrow_mut().gen_range(0..1000)] = *v;
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
        let los = ListOfSkips::new(200, 1000, || rng.borrow_mut().gen());
        let data = gen_data();
        let mut res = data[0..1000].to_vec();
        let mut skip = los.get_skip();
        for v in data.iter().skip(1000) {
            loop {
                if let Some(take) = skip.next() {
                    if take {
                        res[rng.borrow_mut().gen_range(0..1000)] = *v;
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
            let los = Arc::new(ListOfSkips::new(num_threads, n_samples, || thread_rng().gen()));
            let mut threads = Vec::new();
            let data = gen_data();
            let res = Arc::new(Mutex::new(data[0..n_samples as usize].to_vec()));
            let data = Arc::new(data[n_samples as usize..].to_vec());
            for t in 0..num_threads as usize {
                let los = los.clone();
                let res = res.clone();
                let data = data.clone();
                let t_s = t_s.clone();
                threads.push(std::thread::spawn(move || {
                    let mut rng = rand::thread_rng();
                    let mut skip = los.get_skip();
                    let mut lock = t_s[skip.list_index as usize].try_lock().unwrap();
                    for v in data.iter().enumerate().filter(|(i, _)| i % num_threads as usize == t).map(|(_, v)| v) {
                        loop {
                            if let Some(take) = skip.next() {
                                if take {
                                    res.lock()[rng.gen_range(0..n_samples as usize)] = *v;
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
}

