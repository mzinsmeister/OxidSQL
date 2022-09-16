use std::{fs::File, path::PathBuf, io::{Seek, Read, SeekFrom, Write}};

use super::page::{PAGE_SIZE, PageId, RelationIdType};

pub(super) struct DiskManager {
  data_folder: PathBuf
}

impl DiskManager {

  pub fn create_relation(&self, relation_id: RelationIdType) {
    File::create(self.data_folder.join(relation_id.to_string())).unwrap();
  }

  pub fn read_page(&self, page_id: PageId, buf: &mut[u8]) {
    let file = File::options().read(true).open(self.data_folder.join(page_id.relation_id.to_string())).unwrap();
    let offset = page_id.page_nr as u64 * PAGE_SIZE as u64;
    file.seek(SeekFrom::Start(offset));
    file.read(buf).unwrap();
  }

  pub fn write_page(&self, page_id: PageId, buf: &[u8]) {
    let file = File::options().write(true).open(self.data_folder.join(page_id.relation_id.to_string())).unwrap();
    let offset = page_id.page_nr as u64 * PAGE_SIZE as u64;
    file.seek(SeekFrom::Start(offset));
    file.write(buf).unwrap();
  }

  pub fn get_relation_size(&self, relation_id: RelationIdType) -> u64 {
    let file = File::options().open(self.data_folder.join(relation_id.to_string())).unwrap();
    file.metadata().unwrap().len() / PAGE_SIZE as u64
  }
}