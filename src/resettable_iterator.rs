use crate::database::TupleValue;

pub trait ResettableIterator: Iterator {
  fn reset(&mut self);
}

pub struct EmptyResettableIterator {}

impl Iterator for EmptyResettableIterator {
  type Item = Result<Vec<TupleValue>, sled::Error>;

  fn next(&mut self) -> Option<Self::Item> {
      return None
  }
}

impl ResettableIterator for EmptyResettableIterator {
  fn reset(&mut self) {
      // do nothing
  }
}

pub struct OnceResettableIterator<T: Clone> {
  item: T,
  shown: bool
}

impl<T: Clone> OnceResettableIterator<T> {
  pub fn new(item: T) -> OnceResettableIterator<T> {
      OnceResettableIterator {
          item: item,
          shown: false
      }
  }
}

impl<T: Clone> Iterator for OnceResettableIterator<T> {
  type Item = T;

  fn next(&mut self) -> Option<Self::Item> {
      if self.shown {
          None
      } else {
          self.shown = true;
          Some(self.item.clone())
      }
  }
}

impl<T: Clone> ResettableIterator for OnceResettableIterator<T> {
  fn reset(&mut self) {
      self.shown = false;
  }
}

