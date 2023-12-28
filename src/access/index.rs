use crate::{storage::buffer_manager::BufferManager, types::{RelationTID, TupleValue}};



// A basic index doesn't need to support range scans (hash tables for example don't)
// It will always be a unique index
pub trait Index<B: BufferManager> {
    fn lookup(&self, key: &[Option<TupleValue>]) -> Result<Option<RelationTID>, B::BError>;
    // The result is a bool indicating whether uniqueness was violated
    fn insert(&self, key: &[Option<TupleValue>], tid: RelationTID) -> Result<bool, B::BError>;
    // fn update(&self, tid: RelationTID, tuple: Tuple) -> Result<(), B::BError>; Update doesn't really make sense in a transactional context, you'll possibly want to access old and new values at the same time
    // fn delete(&self, tid: RelationTID) -> Result<(), B::BError>; No deletes for now. Deletes must be handled differently later with transactions anyway (GC)
}

pub trait OrderedIndex<B: BufferManager>: Index<B> {
    // Probably doesn't really make much sense to have a predicate here since we have
    // an Index
    type ScanIterator: Iterator<Item = Result<(Box<[Option<TupleValue>]>, RelationTID), B::BError>>;

    // Will scan from the specified to the specified value, both ends beeing inclusive
    fn scan(&self, from: Option<&[Option<TupleValue>]>, to: Option<&[Option<TupleValue>]>) -> Result<Self::ScanIterator, B::BError>;
}