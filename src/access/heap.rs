use std::borrow::BorrowMut;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use aes::cipher::BlockEncrypt;
use aes::cipher::KeyInit;
use aes::cipher::generic_array::GenericArray;
use bitvec::bitvec;
use bitvec::vec::BitVec;

use crate::catalog::Catalog;
use crate::catalog::TableDesc;
use crate::catalog::statistics::AttributeStatistics;
use crate::statistics::sampling::ReservoirSampler;
use crate::storage::buffer_manager::BufferManager;
use crate::types::RelationTID;
use crate::types::TupleValue;
use crate::types::TupleValueType;

use super::ScanFunction;
use super::SlottedPageScan;
use super::SlottedPageSegment;
use super::tuple::MutatingTupleParser;
use super::tuple::Tuple;
use super::tuple::TupleParser;

// TODO: Do this properly
const HASHER: ahash::RandomState = ahash::RandomState::with_seeds(5432123, 456532, 123454321, 424242);

/*
    A heap storage is a storage that only allows access through a tuple id or a scan.
 */
pub trait HeapStorage<B: BufferManager> {
    // Not sure FnMut is actually a good idea here. Makes parallelizing this impossible
    // but probably it's a better idea to hand out iterators starting and ending at different positions anyway
    type ScanIterator<P: FnMut(&Tuple) -> bool>: Iterator<Item = Result<(RelationTID, Tuple), B::BError>>;
    type MutatingScanIterator<'a, V: BorrowMut<Option<TupleValue>>, P: Fn(&[V]) -> bool>: Iterator<Item = Result<RelationTID, B::BError>> where V: 'a;

    fn get_tuple(&self, tid: RelationTID, attributes: &BitVec<usize>) -> Result<Option<Tuple>, B::BError>;
    fn get_tuple_all(&self, tid: RelationTID) -> Result<Option<Tuple>, B::BError>;
    /// The tuples values won't be modified however flags could be set (sampled for example)
    fn insert_tuples(&self, tuples: &mut [Tuple]) -> Result<Box<[RelationTID]>, B::BError>;
    fn update_tuple(&self, tid: RelationTID, tuple: Tuple) -> Result<(), B::BError>;
    fn delete_tuple(&self, tid: RelationTID) -> Result<(), B::BError>;
    fn scan<P: FnMut(&Tuple) -> bool>(&self, attributes: BitVec<usize>, predicate: P) -> Result<Self::ScanIterator<P>, B::BError>;
    // Convenience function that just gives us all attributes
    fn scan_all<P: FnMut(&Tuple) -> bool>(&self, predicate: P) -> Result<Self::ScanIterator<P>, B::BError>;
    fn scan_to_tuple<'a, V, P>(&self,  tuple: Tuple<V, &'a mut [V]>, 
                                attributes: BitVec<usize>,  
                                predicate: P) -> Result<Self::MutatingScanIterator<'a, V, P>, B::BError> 
        where V: BorrowMut<Option<TupleValue>> + 'a, P: Fn(&[V]) -> bool;
}

pub struct SlottedPageHeapStorage<B: BufferManager> {
    segment: SlottedPageSegment<B>,
    attributes: Vec<TupleValueType>
}

impl<B: BufferManager> SlottedPageHeapStorage<B> {
    pub fn new(segment: SlottedPageSegment<B>, attributes: Vec<TupleValueType>) -> SlottedPageHeapStorage<B> {
        SlottedPageHeapStorage {
            segment,
            attributes
        }
    }
}

impl<B: BufferManager> HeapStorage<B> for SlottedPageHeapStorage<B> {
    type ScanIterator<P: FnMut(&Tuple) -> bool> = SlottedPageHeapStorageScan<B, P>;

    type MutatingScanIterator<'a, V: BorrowMut<Option<TupleValue>>, P: Fn(&[V]) -> bool> = SlottedPageMutatingHeapStorageScan<'a, B, V, P> where V: 'a;

    fn get_tuple(&self, tid: RelationTID, attributes: &BitVec<usize>) -> Result<Option<Tuple>, <B as BufferManager>::BError> {
        self.segment.get_record(tid, |src| {
            Tuple::parse_binary_partial(&self.attributes, attributes, src)
        })
    }

    fn get_tuple_all(&self, tid: RelationTID) -> Result<Option<Tuple>, <B as BufferManager>::BError> {
        let parse_all = bitvec![1; self.attributes.len()];
        self.get_tuple(tid, &parse_all)
    }

    fn insert_tuples(&self, tuples: &mut [Tuple]) -> Result<Box<[RelationTID]>, <B as BufferManager>::BError> {
        let mut tids = Vec::with_capacity(tuples.len());
        for tuple in tuples {
            let binary = tuple.get_binary();
            tids.push(self.segment.insert_record(&binary)?);
        }
        Ok(tids.into_boxed_slice())
    }

    fn update_tuple(&self, tid: RelationTID, tuple: Tuple) -> Result<(), <B as BufferManager>::BError> {
        let binary = tuple.get_binary();
        self.segment.write_record(tid, &binary)
    }

    fn delete_tuple(&self, tid: RelationTID) -> Result<(), <B as BufferManager>::BError> {
        self.segment.erase_record(tid)
    }

    fn scan<P: FnMut(&Tuple) -> bool>(&self, attributes: BitVec<usize>, predicate: P) -> Result<Self::ScanIterator<P>, <B as BufferManager>::BError> {
        let parser = PredicateTupleParser(TupleParser::new(self.attributes.clone(), attributes), predicate);
        let scan = self.segment.clone().scan(parser);
        Ok(SlottedPageHeapStorageScan { scan })
    }

    fn scan_all<P: FnMut(&Tuple) -> bool>(&self, predicate: P) -> Result<Self::ScanIterator<P>, <B as BufferManager>::BError> {
        self.scan(bitvec![1;self.attributes.len()], predicate)
    }

    fn scan_to_tuple<'a, V, P>(&self, tuple: Tuple<V, &'a mut [V]>, 
                                attributes: BitVec<usize>,  
                                predicate: P) -> Result<Self::MutatingScanIterator<'a, V, P>, <B as BufferManager>::BError> 
        where V: BorrowMut<Option<TupleValue>> + 'a, P: Fn(&[V]) -> bool  {
        let parser = MutatingTupleParser::new(self.attributes.clone(), attributes, tuple);
        let predicate_parser = PredicateMutatingTupleParser(parser, predicate, PhantomData);
        let scan = self.segment.clone().scan(predicate_parser);
        Ok(SlottedPageMutatingHeapStorageScan { scan, _phantom: PhantomData })
    }
}

struct PredicateTupleParser<P: FnMut(&Tuple) -> bool>(TupleParser, P);

impl<P: FnMut(&Tuple) -> bool> ScanFunction<Tuple> for PredicateTupleParser<P> {

    fn apply(&mut self, val: &[u8]) -> Option<Tuple> {
        self.0.apply(val).filter(|t| (self.1)(t))
    }
}

pub struct SlottedPageHeapStorageScan<B: BufferManager, P: FnMut(&Tuple) -> bool> {
    scan: SlottedPageScan<B, Tuple, PredicateTupleParser<P>>
}

impl <B: BufferManager, P: FnMut(&Tuple) -> bool> Iterator for SlottedPageHeapStorageScan<B, P> {
    type Item = Result<(RelationTID, Tuple), B::BError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.scan.next()
    }
}

struct PredicateMutatingTupleParser<'a, V: BorrowMut<Option<TupleValue>>, P: Fn(&[V]) -> bool>(MutatingTupleParser<'a, V>, P, PhantomData<&'a V>);

impl<'a, V: BorrowMut<Option<TupleValue>>, P: Fn(&[V]) -> bool> ScanFunction<()> for PredicateMutatingTupleParser<'a, V, P> {

    fn apply(&mut self, val: &[u8]) -> Option<()> {
        self.0.apply(val);
        if (self.1)(self.0.values()) {
            Some(())
        } else {
            None
        }
    }
}
pub struct SlottedPageMutatingHeapStorageScan<'a, B: BufferManager, V: BorrowMut<Option<TupleValue>>, P: Fn(&[V]) -> bool> {
    scan: SlottedPageScan<B, (), PredicateMutatingTupleParser<'a, V, P>>,
    _phantom: PhantomData<V>
}

impl <'a, B: BufferManager, V: BorrowMut<Option<TupleValue>>, P: Fn(&[V]) -> bool> Iterator for SlottedPageMutatingHeapStorageScan<'a, B, V, P> {
    type Item = Result<RelationTID, B::BError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.scan.next().map(|r| r.map(|(tid, _)| tid))
    }
}

struct CounterRNG (aes::Aes128);

impl CounterRNG {
    fn new(key: [u8; 16]) -> Self {
        let key = GenericArray::from_slice(&key);
        CounterRNG(aes::Aes128::new(key))
    }

    fn counter_rng(&self, counter: u32) -> u32 {
        let mut block = GenericArray::from([0u8; 16]);
        block[12..16].copy_from_slice(&counter.to_be_bytes());
        
        // Encrypt block in-place
        self.0.encrypt_block(&mut block);
        
        u32::from_be_bytes(block[12..16].try_into().unwrap())
    }
}

// TODO: Implement this
/*
    This is how i imagine statistics collection to look short/medium term.
    The executor will make all its modifications to the storage through this wrapper.
    The wrapper will then update the statistics (Count distinct sketches (CountingHyperLogLog) 
    and a sample of the data (ReservoirSampler)). Thes should then be able to be used to make
    quite accurate estimates of the cardinality of the data in the storage and the selectivity
    of predicates. This should be enough to make the optimizer work quite well. Only after
    we have somewhat accurate statistics does it actually make sense to improve the optimizer
    or implement more complex query execution (optimizer can make queries faster by factors of 100s
    to 1000s).
 */
pub struct StatisticsCollectingSPHeapStorage<B: BufferManager> {
    storage: SlottedPageHeapStorage<B>,
    // One HyperLogLog for each column. 
    // Write lock is only needed for schema changes.
    attribute_statistics: Vec<AttributeStatistics>,
    reservoir_sampler: Arc<ReservoirSampler>,
    cardinality: Arc<AtomicU64>,
    sample_storage: SlottedPageHeapStorage<B>,
    sample_size: usize,
    crng: CounterRNG,
}

impl<B: BufferManager> StatisticsCollectingSPHeapStorage<B> {
    pub fn new(table: &TableDesc, buffer_manager: B, catalog: Catalog<B>, sample_size: usize) -> Result<Self, B::BError> {
        let segment = SlottedPageSegment::new(buffer_manager.clone(), table.segment_id, table.fsi_segment_id);
        let storage = SlottedPageHeapStorage::new(segment, table.attributes.iter().map(|a| a.data_type).collect());
        let sample_segment = SlottedPageSegment::new(buffer_manager.clone(), table.sample_segment_id, table.sample_fsi_segment_id);
        let sample_attributes = [TupleValueType::BigInt, TupleValueType::BigInt, TupleValueType::BigInt].iter().cloned().chain(table.attributes.iter().map(|a| a.data_type)).collect();
        let sample_storage = SlottedPageHeapStorage::new(sample_segment, sample_attributes);
        //let attributes_statistics = catalog
        let crng = CounterRNG::new([42; 16]);
        let statistics = catalog.get_statistics(table.id)?.expect("There should be statistics for every table");
        let attribute_statistics = statistics.attribute_statistics.clone();
        let reservoir_sampler = statistics.table_statistics.sampler.clone();
        let cardinality = statistics.table_statistics.cardinality.clone();
        Ok(Self { storage, attribute_statistics, reservoir_sampler, sample_storage, sample_size, crng, cardinality })
    }
}


impl<B: BufferManager> HeapStorage<B> for StatisticsCollectingSPHeapStorage<B> {
    type ScanIterator<P: FnMut(&Tuple) -> bool> = SlottedPageHeapStorageScan<B, P>;

    type MutatingScanIterator<'a, V: BorrowMut<Option<TupleValue>>, P: Fn(&[V]) -> bool> = SlottedPageMutatingHeapStorageScan<'a, B, V, P> where V: 'a;

    fn get_tuple(&self, tid: RelationTID, attributes: &BitVec<usize>) -> Result<Option<Tuple>, B::BError> {
        self.storage.get_tuple(tid, attributes)
    }

    fn get_tuple_all(&self, tid: RelationTID) -> Result<Option<Tuple>, B::BError> {
        self.storage.get_tuple_all(tid)
    }

    fn insert_tuples(&self, tuples: &mut [Tuple]) -> Result<Box<[RelationTID]>, B::BError> {
        let mut skip = self.reservoir_sampler.get_skip();
        let result = self.storage.insert_tuples(tuples)?;
        // Add to HyperLogLog sketches
        
        for tuple in tuples.iter() {
            for (i, value) in tuple.values.iter().enumerate() {
                if let Some(value) = value {
                    let hash = HASHER.hash_one(value);
                    self.attribute_statistics[i].counting_hyperloglog.add(hash);
                }
            }
        }
        // Add to sample
        for (tuple, tid) in tuples.iter_mut().zip(result.iter()) {
            match skip.next() {
                Some(true) => {
                    let mut sample_tuple = tuple.clone();
                    sample_tuple.values.insert(0, Some(TupleValue::BigInt(u64::from(tid) as i64)));
                    sample_tuple.values.insert(0, Some(TupleValue::BigInt(skip.skip_index as i64)));
                    let index = if (skip.skip_index as usize) < self.sample_size {
                        skip.skip_index
                    } else {
                        self.crng.counter_rng(skip.skip_index) % self.sample_size as u32
                    };
                    sample_tuple.values.insert(0, Some(TupleValue::BigInt(index as i64)));
                    let mut attributes = bitvec![0; self.sample_storage.attributes.len()];
                    attributes.set(0, true);
                    let mut res = self.sample_storage.scan(attributes, |t| {
                        t.values[0] == Some(TupleValue::BigInt(index as i64))
                    })?;
                    if let Some(r) = res.next() {
                        self.sample_storage.update_tuple(r?.0, sample_tuple)?;
                    } else {
                        self.sample_storage.insert_tuples(&mut [sample_tuple])?;
                    }
                    tuple.flags |= 1;
                    skip = skip.get_new();
                }
                None => {
                    skip = skip.get_new();
                }
                _ => {}
            }
        }
        self.cardinality.fetch_add(result.len() as u64, atomic::Ordering::Relaxed);
        Ok(result)
    }

    fn update_tuple(&self, tid: RelationTID, mut tuple: Tuple) -> Result<(), B::BError> {
        let tup = self.storage.get_tuple(tid, &bitvec![0; self.storage.attributes.len()])?.unwrap();
        // Update HyperLogLog

        for (i, value) in tup.values.iter().enumerate() {
            if let Some(value) = value {
                let hash = HASHER.hash_one(value);
                self.attribute_statistics[i].counting_hyperloglog.delete(hash);
            }
        }
        
        for (i, value) in tuple.values.iter().enumerate() {
            if let Some(value) = value {
                let hash = HASHER.hash_one(value);
                self.attribute_statistics[i].counting_hyperloglog.add(hash);
            }
        }

        // Update Sample
        if tup.flags & 1 == 1 {
            let mut attributes = bitvec![0; self.sample_storage.attributes.len()];
            attributes.set(0, true);
            attributes.set(1, true);
            attributes.set(2, true);
            let mut scan = self.sample_storage.scan(attributes, |t| {
                t.values[2] == Some(TupleValue::BigInt(u64::from(&tid) as i64))
            })?;
            if let Some(t) = scan.next() {
                let (sample_tid, prev_tup) = t?;
                let mut sample_tuple = tuple.clone();
                sample_tuple.values.insert(0, prev_tup.values[2].clone());
                sample_tuple.values.insert(0, prev_tup.values[1].clone());
                sample_tuple.values.insert(0, prev_tup.values[0].clone());
                self.sample_storage.update_tuple(sample_tid, sample_tuple)?;
                tuple.flags |= 1;
            } else {
                tuple.flags &= !1;
            }
        }
        self.storage.update_tuple(tid, tuple)
    }

    fn delete_tuple(&self, tid: RelationTID) -> Result<(), B::BError> {
        let tuple = self.storage.get_tuple(tid, &bitvec![0; self.storage.attributes.len()])?.unwrap();
        // Update HyperLogLog
    
        for (i, value) in tuple.values.iter().enumerate() {
            if let Some(value) = value {
                let hash = HASHER.hash_one(value);
                self.attribute_statistics[i].counting_hyperloglog.delete(hash);
            }
        }

        // Update Sample
        if tuple.flags & 1 == 1 {
            let mut attributes = bitvec![0; self.sample_storage.attributes.len()];
            attributes.set(0, true);
            attributes.set(1, true);
            attributes.set(2, true);
            let mut scan = self.sample_storage.scan(attributes, |t| {
                t.values[2] == Some(TupleValue::BigInt(u64::from(&tid) as i64))
            })?;
            if let Some(t) = scan.next() {
                let (sample_tid, tup) = t?;
                self.reservoir_sampler.delete_sample(tup.values[1].clone().unwrap().as_big_int() as u32);
                self.sample_storage.delete_tuple(sample_tid)?;
            } else {
                self.reservoir_sampler.delete_skipped(1);
            }
        } else {
            self.reservoir_sampler.delete_skipped(1);
        }
        self.cardinality.fetch_sub(1, atomic::Ordering::Relaxed);
        self.storage.delete_tuple(tid)
    }

    fn scan<P: FnMut(&Tuple) -> bool>(&self, attributes: BitVec<usize>, predicate: P) -> Result<Self::ScanIterator<P>, B::BError> {
        self.storage.scan(attributes, predicate)
    }

    fn scan_all<P: FnMut(&Tuple) -> bool>(&self, predicate: P) -> Result<Self::ScanIterator<P>, B::BError> {
        self.storage.scan_all(predicate)
    }

    fn scan_to_tuple<'a, V, P>(&self,  tuple: Tuple<V, &'a mut [V]>, 
                                attributes: BitVec<usize>,  
                                predicate: P) -> Result<Self::MutatingScanIterator<'a, V, P>, B::BError> 
        where V: BorrowMut<Option<TupleValue>> + 'a, P: Fn(&[V]) -> bool {
        self.storage.scan_to_tuple(tuple, attributes, predicate)
    }
}

#[cfg(test)]
mod test {
    // TODO: TEST (SPHeapStorage and StatisticsCollecting)
}
