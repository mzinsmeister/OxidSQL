use std::borrow::Borrow;
use std::borrow::BorrowMut;
use std::marker::PhantomData;
use std::sync::Arc;

use bitvec::bitvec;
use bitvec::vec::BitVec;

use crate::catalog::Catalog;
use crate::statistics::counting_hyperloglog::CountingHyperLogLog;
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

/*
    A heap storage is a storage that only allows access through a tuple id or a scan.
 */
pub trait HeapStorage<B: BufferManager> {
    type ScanIterator<P: Fn(&Tuple) -> bool>: Iterator<Item = Result<(RelationTID, Tuple), B::BError>>;
    type MutatingScanIterator<'a, V: BorrowMut<Option<TupleValue>>, P: Fn(&[V]) -> bool>: Iterator<Item = Result<RelationTID, B::BError>> where V: 'a;

    fn get_tuple(&self, tid: RelationTID, attributes: &BitVec<usize>) -> Result<Option<Tuple>, B::BError>;
    fn get_tuple_all(&self, tid: RelationTID) -> Result<Option<Tuple>, B::BError>;
    fn insert_tuple(&self, tuple: Tuple) -> Result<RelationTID, B::BError>;
    fn update_tuple(&self, tid: RelationTID, tuple: Tuple) -> Result<(), B::BError>;
    fn delete_tuple(&self, tid: RelationTID) -> Result<(), B::BError>;
    fn scan<P: Fn(&Tuple) -> bool>(&self, attributes: BitVec<usize>, predicate: P) -> Result<Self::ScanIterator<P>, B::BError>;
    // Convenience function that just gives us all attributes
    fn scan_all<P: Fn(&Tuple) -> bool>(&self, predicate: P) -> Result<Self::ScanIterator<P>, B::BError>;
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
    type ScanIterator<P: Fn(&Tuple) -> bool> = SlottedPageHeapStorageScan<B, P>;

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

    fn insert_tuple(&self, tuple: Tuple) -> Result<RelationTID, <B as BufferManager>::BError> {
        let binary = tuple.get_binary();
        self.segment.insert_record(&binary)
    }

    fn update_tuple(&self, tid: RelationTID, tuple: Tuple) -> Result<(), <B as BufferManager>::BError> {
        let binary = tuple.get_binary();
        self.segment.write_record(tid, &binary)
    }

    fn delete_tuple(&self, tid: RelationTID) -> Result<(), <B as BufferManager>::BError> {
        self.segment.erase_record(tid)
    }

    fn scan<P: Fn(&Tuple) -> bool>(&self, attributes: BitVec<usize>, predicate: P) -> Result<Self::ScanIterator<P>, <B as BufferManager>::BError> {
        let parser = PredicateTupleParser(TupleParser::new(self.attributes.clone(), attributes), predicate);
        let scan = self.segment.clone().scan(parser);
        Ok(SlottedPageHeapStorageScan { scan })
    }

    fn scan_all<P: Fn(&Tuple) -> bool>(&self, predicate: P) -> Result<Self::ScanIterator<P>, <B as BufferManager>::BError> {
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

struct PredicateTupleParser<P: Fn(&Tuple) -> bool>(TupleParser, P);

impl<P: Fn(&Tuple) -> bool> ScanFunction<Tuple> for PredicateTupleParser<P> {

    fn apply(&mut self, val: &[u8]) -> Option<Tuple> {
        self.0.apply(val).filter(|t| (self.1)(t))
    }
}

pub struct SlottedPageHeapStorageScan<B: BufferManager, P: Fn(&Tuple) -> bool> {
    scan: SlottedPageScan<B, Tuple, PredicateTupleParser<P>>
}

impl <B: BufferManager, P: Fn(&Tuple) -> bool> Iterator for SlottedPageHeapStorageScan<B, P> {
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
struct StatisticsCollectingSPHeapStorage<B: BufferManager> {
    storage: SlottedPageHeapStorage<B>,
    catalog: Catalog<B>,
    hyperloglog: Arc<CountingHyperLogLog>,
    reservoir_sampler: ReservoirSampler,
}
