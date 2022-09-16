use std::option::Option;

pub trait Replacer {
  fn find_victim(&mut self) -> Option<u32>;
  fn use_page(&mut self, page_id: u32);
  fn load_page(&mut self, page_id: u32);
  fn free_page(&mut self, page_id: u32);
}