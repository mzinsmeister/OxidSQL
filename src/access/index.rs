use crate::{storage::buffer_manager::BufferManager, types::{RelationTID, TupleValue}};



// A basic index doesn't need to support range scans (hash tables for example don't)
pub trait Index<B: BufferManager> {
    fn lookup(&self, key: &[Option<TupleValue>]) -> Result<Option<RelationTID>, B::BError>;
    fn insert(&self, key: &[Option<TupleValue>], tid: RelationTID) -> Result<(), B::BError>;
    // fn update(&self, tid: RelationTID, tuple: Tuple) -> Result<(), B::BError>; Update doesn't really make sense in a transactional context, you'll possibly want to access old and new values at the same time
    // fn delete(&self, tid: RelationTID) -> Result<(), B::BError>; No deletes for now. Deletes must be handled differently later with transactions anyway (GC)
}

pub trait OrderedIndex<B: BufferManager>: Index<B> {
    // Probably doesn't really make much sense to have a predicate here since we have
    // an Index
    type ScanIterator: Iterator<Item = Result<RelationTID, B::BError>>;

    fn scan(&self, from: &[Option<TupleValue>], to: &[Option<TupleValue>]) -> Result<Self::ScanIterator, B::BError>;
}