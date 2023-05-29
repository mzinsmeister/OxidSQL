
/*
    Essentially an implementation of this paper: "Concurrent Online Sampling for All, for Free" 
    (https://altan.birler.co/static/papers/2020DAMON_ConcurrentOnlineSampling.pdf)
 */

// First we need a "List of skips" datastructure
// This is a linked list of skips, where each skip is a node in the list
// We assume a fixed (maximum) number of threads. This makes sense anyway since
// having a number of worker threads much larger than the number of CPU cores
// will usually not improve performance but rather degrade it due to context switching.
// The number of threads must be less than u32::MAX but for all practical purposes it will
// probably remain less than u32::MAX for at least the next 100 years or until the singularity, 
// whichever comes first.

use std::sync::atomic::{AtomicU64, AtomicU32};
use std::sync::atomic::Ordering::Relaxed;

use atomic::Ordering;

// Quick and Hacky Atomic f32, u32 tuple
pub struct AtomicF32U32 {
    storage: AtomicU64,
}
impl AtomicF32U32 {
    pub fn new(value: (f32, u32)) -> Self {
        let as_u32 = value.0.to_bits();
        Self { storage: AtomicU64::new((as_u32 as u64) << 32 | value.1 as u64) }
    }
    pub fn store(&self, value: (f32, u32), ordering: Ordering) {
        let as_u32 = value.0.to_bits();
        self.storage.store((as_u32 as u64) << 32 | value.1 as u64, ordering)
    }
    pub fn load(&self, ordering: Ordering) -> (f32, u32) {
        let as_u64 = self.storage.load(ordering);
        (f32::from_bits((as_u64 >> 32) as u32), as_u64 as u32)
    }

    pub fn compare_exchange(&self, current: (f32, u32), new: (f32, u32), success: Ordering, failure: Ordering) -> Result<(f32, u32), (f32, u32)> {
        let current_as_u64 = (current.0.to_bits() as u64) << 32 | current.1 as u64;
        let new_as_u64 = (new.0.to_bits() as u64) << 32 | new.1 as u64;
        match self.storage.compare_exchange(current_as_u64, new_as_u64, success, failure) {
            Ok(_) => Ok(current),
            Err(actual) => Err((f32::from_bits((actual >> 32) as u32), actual as u32))
        }
    }
}

struct Skip {
    index: AtomicU32,
    skip: AtomicU64,
    next: AtomicU32, // u32::MAX means end of list
}

struct ListOfSkips<R: Fn() -> f32> {
    // we will access this concurrently so it must be fixed size at runtime
    // we will therefore allocate all the skips we will need at the start
    wi_next: AtomicF32U32,
    list: Box<[Skip]>,
    head: AtomicU64,
    free_head: AtomicU64,
    m: usize,
    random: R
}

pub struct SkipGuard<'a, R: Fn() -> f32> {
    skip_count: u64,
    proccessed: u64,
    skip_index: u32,
    skip: &'a Skip,
    list: &'a ListOfSkips<R>,
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
}

impl<'a, R: Fn() -> f32> Drop for SkipGuard<'a, R> {
    fn drop(&mut self) {
        if self.proccessed > self.skip_count {
            loop {
                // We have processed the skip, return it to the free list
                let free_head = self.list.free_head.load(Relaxed);
                let new_version = ((free_head >> 32) + 1) << 32;
                let new_free_head = (self.skip_index as u64 & 0x00000000FFFFFFFF) | new_version;
                self.skip.next.store(free_head as u32, Relaxed);
                let ce_result = self.list.free_head.compare_exchange(free_head, new_free_head, Relaxed, Relaxed);
                if ce_result.is_ok() {
                    return;
                }
            }
        } else {
            // We have not completely processed the skip, return it to the list of skips
            self.skip.skip.store(self.skip_count - self.proccessed, Ordering::Release); // Not sure ordering is actually needed here but just to be safe
            loop {
                let head = self.list.head.load(Relaxed);
                let new_version = ((head >> 32) + 1) << 32;
                let new_head = (self.skip_index as u64 & 0x00000000FFFFFFFF) | new_version;
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
    pub fn new(n_threads: u32, n_samples: usize, random: R) -> Self {
        let mut list = Vec::with_capacity(n_threads as usize);
        let mut w_next = (((random)()).ln()/n_samples as f32).exp();
        list.push(Skip {
            index: AtomicU32::new(0),
            // floor(log(random())/log(1 − Wi ))
            skip: AtomicU64::new(((random)().ln()/(1.0-w_next).ln()) as u64),
            next: AtomicU32::new(u32::MAX),
        });
        for i in 1..=n_threads {
            w_next *= (((random)()).ln()/n_samples as f32).exp();
            list.push(Skip {
                index: AtomicU32::new(i),
                skip: AtomicU64::new(0),
                next: AtomicU32::new(i+1),
            });
        }
        w_next *= (((random)()).ln()/n_samples as f32).exp();
        list.last_mut().unwrap().next.store(u32::MAX, Relaxed);
        Self {
            wi_next: AtomicF32U32::new((w_next, 1)),
            list: list.into_boxed_slice(),
            head: AtomicU64::new(0),
            free_head: AtomicU64::new(1),
            m: n_samples,
            random,
        }
    }

    pub fn get_skip(&self) -> SkipGuard<R> {
        loop {
            let head = self.head.load(Relaxed);
            let skip = &self.list[head as usize];
            // Check whether it has a next, else we need to allocate a new skip
            // If someone else happens to do the same thing in parallel, we don't care
            let next = skip.next.load(Relaxed);
            if next == u32::MAX {
                // Allocate a new skip
                loop {
                    let free_head = self.free_head.load(Relaxed);
                    if free_head as u32 == u32::MAX {
                        // No free skips. This must not happen since we
                        // allocated enough skips for all threads at the start
                        panic!("No free skips");
                    }
                    let new_version = ((free_head >> 32) + 1) << 32;
                    let free_head_next = self.list[(free_head as u32) as usize].next.load(Relaxed);
                    let new_free_head = free_head_next as u64 | new_version;
                    let ce_result = self.free_head.compare_exchange(free_head, new_free_head, Relaxed, Relaxed);
                    if ce_result.is_ok() {
                        // We successfully allocated a new skip
                        // Put it into the list
                        // We don't have to worry about the previous head here

                        let (mut w, mut i) = self.wi_next.load(Relaxed);
                        loop {
                            let w_next = w * (((self.random)()).ln()/self.m as f32).exp();
                            let cs_result = self.wi_next.compare_exchange((w,i), (w_next, i+1), Relaxed, Relaxed);
                            if cs_result.is_ok() {
                                break;
                            } else {
                                (w, i) = self.wi_next.load(Relaxed);
                            }
                        }
                        let free_skip = &self.list[(free_head as u32) as usize];
                        free_skip.skip.store(((self.random)().ln()/(1.0-w)).exp() as u64, Relaxed);
                        loop {
                            let head = self.head.load(Relaxed);
                            let head_next = self.list[(head as u32) as usize].next.load(Relaxed);
                            let new_version = ((head >> 32) + 1) << 32;
                            free_skip.next.store(head_next, Relaxed);
                            let new_head = (free_head & 0x00000000FFFFFFFF) | new_version;
                            let ce_result = self.head.compare_exchange(head, new_head, Relaxed, Relaxed);
                            if ce_result.is_ok() {
                                // We successfully put the new skip into the list
                                return SkipGuard {
                                    skip_count: self.list[(head as u32) as usize].skip.load(Relaxed),
                                    proccessed: 0,
                                    skip_index: head as u32,
                                    skip: &self.list[(head as u32) as usize],
                                    list: self
                                }
                            }
                        }
                    }
                }
            } else {
                // We can just use the next skip
                let new_version = ((head >> 32) + 1) << 32;
                let new_head = (head & 0x00000000FFFFFFFF) | new_version;
                let ce_result = self.head.compare_exchange(head, new_head, Relaxed, Relaxed);
                if ce_result.is_ok() {
                    // We successfully got a new skip
                    return SkipGuard {
                        // We can just work with this now since the Skip is now "ours" until we return it
                        skip_count: skip.skip.load(Relaxed),
                        proccessed: 0,
                        skip_index: head as u32,
                        skip,
                        list: self
                    }
                }
            }
        }
    }

    // TODO: Implement update (nothing to do in LoS in think) and delete
}


#[cfg(test)]
mod test {
    use std::cell::RefCell;

    use rand::{thread_rng, Rng, SeedableRng};

    use super::*;

    #[test]
    fn test_init() {
        let los = ListOfSkips::new(1, 1, || thread_rng().gen());
        assert_eq!(los.list.len(), 2);
    }

    #[test]
    fn test_get_skip() {
        let rng = RefCell::new(rand::rngs::StdRng::seed_from_u64(0));
        let los = ListOfSkips::new(1, 1, || rng.borrow_mut().gen());
        let skip = los.get_skip();
        assert_eq!(skip.skip_count, 0);
        assert_eq!(skip.proccessed, 0);
        assert_eq!(skip.skip_index, 0);
        assert_eq!(skip.skip.index.load(Relaxed), 0);
        assert_eq!(skip.skip.skip.load(Relaxed), 0);
        assert_eq!(skip.skip.next.load(Relaxed), u32::MAX);
        assert_eq!(los.head.load(Relaxed), 1u64 << 32 | 1);
        assert_eq!(los.free_head.load(Relaxed), 1u64 << 32 | u32::MAX as u64);
    }
}

