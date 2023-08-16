

/*
    Byte-Serializable implementation of a "counting HyperLogLog" sketch. A HyperLogLog sketch is a probabilistic
    data structure that can be used to estimate the number of distinct values in a set. This implementation 
    is based on this paper: https://db.in.tum.de/~freitag/papers/p23-freitag-cidr19.pdf which is a modified
    version of the original HyperLogLog algorithm that allows for online updates and deletes.

    You have 64 buckets that you hash the values into using the first 5 bits of the hash. For each bucket
    you have an 8-bit counter per number of leading zeros in the hash of the value (59 per bucket).
    Up to 128 the counter represents the actual number of values with that many leading zeros. 
    For 129 and above the counter becomes a "probabilistic counter" that instead represents the interval
    [128 + 2^(v−129), 128 + 2^(v−128)] that the actual value is likely in. This is done to reduce the memory
    footprint of the sketch while still retaining the important lower numbers that can have a big impact
    on the estimate exactly. We therefore increase the counter with probability 1/2^(counter − 128) above 128.

 */

use std::sync::atomic::{Ordering::Relaxed, AtomicU8};

use itertools::Itertools;

use rand::Rng;

fn gen_bool_thread_rng(p: f64) -> bool {
    rand::thread_rng().gen_bool(p)
}

#[derive(Debug)]
pub struct CountingHyperLogLog<R: Fn(f64) -> bool = fn(f64) -> bool> {
    buckets: [[AtomicU8; 59]; 64],
    random: R
}

const M: f64 = 64.0;
const ALPHA_M: f64 = 0.709;

fn linear_counting(m: f64, v: u8) -> f64 {
    m * (m / v as f64).ln()
}

impl CountingHyperLogLog<fn(f64) -> bool> {
    pub fn new() -> CountingHyperLogLog<fn(f64) -> bool> {
        CountingHyperLogLog::with_random(gen_bool_thread_rng)
    }

    pub fn from_bytes(bytes: [u8; 59 * 64]) -> CountingHyperLogLog<fn(f64) -> bool> {
        CountingHyperLogLog::from_bytes_with_random(bytes, gen_bool_thread_rng)
    }
}

impl<R: Fn(f64) -> bool> CountingHyperLogLog<R> {
    fn with_random(random: R) -> CountingHyperLogLog<R> {
        let buckets = [(); 64].map(|_| [(); 59].map(|_| AtomicU8::new(0)));
        CountingHyperLogLog {
            buckets,
            random
        }
    }

    pub fn from_bytes_with_random(bytes: [u8; 59 * 64], random: R) -> CountingHyperLogLog<R> {
        let buckets: [[AtomicU8; 59]; 64] = [(); 64].map(|_| [(); 59].map(|_| AtomicU8::new(0)));
        for (i, val) in bytes.iter().enumerate() {
            let bucket = &buckets[i / 59];
            let counter = &bucket[i % 59];
            counter.store(*val, Relaxed);
        }
        CountingHyperLogLog {
            buckets,
            random
        }
    }


    pub fn add(&self, hash: u64) {
        let bucket = (hash >> 58) as usize;
        let rest_hash = (hash << 6) >> 6;
        let leading_zeros = rest_hash.leading_zeros() - 6;
        let bucket = &self.buckets[bucket];
        let counter = &bucket[leading_zeros as usize];
        let _ = counter.fetch_update(Relaxed, Relaxed, |c| {
            if c < 128 {
                Some(c + 1)
            } else {
                if c < u8::MAX && (self.random)(1.0 / (2.0f64.powi((c - 128) as i32))) {
                    Some(c + 1)
                } else {
                    None
                }
            }
        });    }

    pub fn delete(&self, hash: u64) {
        let bucket = (hash >> 58) as usize;
        let rest_hash = (hash << 6) >> 6;
        let leading_zeros = rest_hash.leading_zeros() - 6;
        let bucket = &self.buckets[bucket];
        let counter = &bucket[leading_zeros as usize];
        let _ = counter.fetch_update(Relaxed, Relaxed, |c| {
            if c <= 128 {
                Some(c - 1)
            } else {
                if c > 0 && (self.random)(1.0 / (2.0f64.powi((c - 128) as i32))) {
                    Some(c - 1)
                } else {
                    None
                }
            }
        });    }

    pub fn update(&self, old_hash: u64, new_hash:u64) {
        if old_hash == new_hash {
            return;
        } else {
            // This could possibly lead to 
            // a kind of race condition where the old_hash is deleted
            // then someone takes the estimate and then the new_hash is added
            // but since this is a probabilistic data structure it's not really
            // a problem and it's not updated transactionally anyway.
            self.delete(old_hash);
            self.add(new_hash);
        }
    }


    fn raw_estimate(&self) -> (f64, u8){
        let mut sum = 0.0;
        let mut zero_buckets = 0;
        for bucket in &self.buckets {
            let max_set =  bucket.iter()
                .enumerate()
                .rev()
                .find(|(_, c)| c.load(Relaxed) > 0)
                .map(|(i, _)| i + 1)
                .unwrap_or(0);
            if max_set == 0 {
                zero_buckets += 1;
            }
            sum += 2.0f64.powi(-(max_set as i32));
        }
        let estimate = ALPHA_M * M * M * 1.0/sum;
        (estimate, zero_buckets)
    }

    /// Algorithm as described in "HyperLogLog in Practice: Algorithmic Engineering of a 
    /// State of The Art Cardinality Estimation Algorithm"
    pub fn estimate(&self) -> f64 {
        let (estimate, v) = self.raw_estimate();
        // Bias correction
        if estimate <= 2.5 * M {
            if v != 0 {
                linear_counting(M, v)
            } else {
                estimate
            }
        } else if estimate <= (1u64 << 32) as f64 / 30.0  {
            estimate
        } else {
            -((1u64 << 32) as f64) * (1.0 - estimate / (1u64 << 32) as f64).ln()
        }
    }

    pub fn to_bytes(&self) -> [u8; 59 * 64] {
        self.into()
    }
    
    // TODO: Implement merge/union and intersection 
    //      (Not as trivial as for non-counting HLLs because we 
    //       have to deal with probabilistic counters)

}

/// Make sure you only call this when you're sure that the CountingHyperLogLog is not being updated
/// by another thread.
impl<R: Fn(f64) -> bool> From<&CountingHyperLogLog<R>> for [u8; 59 * 64] {
    fn from(chll: &CountingHyperLogLog<R>) -> [u8; 59 * 64] {
        chll.buckets.iter()
            .flat_map(|b| b.iter())
            .map(|c| c.load(Relaxed))
            .collect_vec()
            .try_into()
            .unwrap()
    }
}


#[cfg(test)]
mod test {
    // IMPORTANT: Make sure to seed every randomized component here
    //            to make sure the tests are deterministic.

    use std::{sync::atomic::AtomicUsize, cell::RefCell};
    
    use rand::{Rng, SeedableRng};

    use super::*;

    fn get_testee() -> CountingHyperLogLog<impl Fn(f64) -> bool> {
        let counter = AtomicUsize::new(0);
        CountingHyperLogLog::with_random(move |_| {
            counter.fetch_add(1, Relaxed) % 4 == 3
        })
    }
    
    const HASH_2_0: u64 = 0b0000101000000000000000000000000000000000000000000000000000000000;
    const HASH_3_0: u64 = 0b0000111000000000000000000000000000000000000000000000000000000000;

    #[test]
    fn test_add() {
        let testee = get_testee();
        testee.add(0);
        assert_eq!(testee.buckets[0][58].load(Relaxed), 1);
        testee.add(0);
        assert_eq!(testee.buckets[0][58].load(Relaxed), 2);
        testee.add(1);
        assert_eq!(testee.buckets[0][57].load(Relaxed), 1);
        testee.add(0b0000010000000000000000000000000000000000000000000000000000000000);
        assert_eq!(testee.buckets[1][58].load(Relaxed), 1);
        for i in 0..128 {
            assert_eq!(testee.buckets[2][0].load(Relaxed), i);
            testee.add(HASH_2_0);
        }
        for _ in 0..4 {
            assert_eq!(testee.buckets[2][0].load(Relaxed), 128);
            testee.add(HASH_2_0);
        }
        assert_eq!(testee.buckets[2][0].load(Relaxed), 129);
        for _ in 0..4 {
            assert_eq!(testee.buckets[2][0].load(Relaxed), 129);
            testee.add(HASH_2_0);
        }
        assert_eq!(testee.buckets[2][0].load(Relaxed), 130);
    }

    #[test]
    fn test_delete() {
        let testee = get_testee();
        for _ in 0..128 {
            testee.add(HASH_2_0);
            testee.add(HASH_3_0);
        }
        for _ in 0..8 {
            testee.add(HASH_2_0);
        }
        for _ in 0..4 {
            assert_eq!(testee.buckets[2][0].load(Relaxed), 130);
            testee.delete(HASH_2_0);
        }
        assert_eq!(testee.buckets[2][0].load(Relaxed), 129);
        for _ in 0..4 {
            assert_eq!(testee.buckets[2][0].load(Relaxed), 129);
            testee.delete(HASH_2_0);
        }
        assert_eq!(testee.buckets[2][0].load(Relaxed), 128);
        testee.delete(HASH_2_0)
    }

    #[test]
    fn test_update() {
        let testee = get_testee();
        testee.add(0);
        testee.update(0, HASH_2_0);
        assert_eq!(testee.buckets[0][58].load(Relaxed), 0);
        assert_eq!(testee.buckets[2][0].load(Relaxed), 1);
    }

    #[test]
    fn test_estimate() {
        let rng = RefCell::new(rand::rngs::StdRng::seed_from_u64(42));
        let testee = CountingHyperLogLog::with_random(|p| rng.borrow_mut().gen_bool(p));
        let hasher = ahash::RandomState::with_seeds(5432123, 456532, 123454321, 424242);
        assert_eq!(testee.estimate(), 0.0);
        testee.add(hasher.hash_one(0));
        assert!((0.5..2.0).contains(&testee.estimate()));
        testee.add(hasher.hash_one(0));
        assert!((0.5..2.0).contains(&testee.estimate()));
        testee.add(hasher.hash_one(1));
        assert!((1.0..3.0).contains(&testee.estimate()));
        testee.add(hasher.hash_one(1234324324262u64));
        testee.add(hasher.hash_one(1234324324262u64));
        testee.add(hasher.hash_one(1234324324262u64));
        testee.add(hasher.hash_one(1234324324262u64));
        assert!((2.0..4.0).contains(&testee.estimate()));
        for i in 0..200_000 {
            let hash = hasher.hash_one(i);
            testee.add(hash);
        }
        let estimate = testee.estimate();
        assert!((180_000.0..220_000.0).contains(&estimate), "estimate not within 20% bounds of 200_000: {}", estimate);
        for i in 200_000..1_200_000 {
            let hash = hasher.hash_one(i);
            testee.add(hash);
        }
        assert!((1_000_000.0..1_400_000.0).contains(&testee.estimate()));
    }
}
