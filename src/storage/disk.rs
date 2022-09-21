use std::{fs::File, path::PathBuf, io::{Seek, Read, SeekFrom, Write}, sync::{RwLock, Arc}};

use super::page::{PAGE_SIZE, PageId, RelationIdType};

pub(super) trait StorageManager: Sync + Send {
  fn create_relation(&self, relation_id: RelationIdType);
  fn read_page(&self, page_id: PageId, buf: &mut[u8]) -> Result<(), std::io::Error>;
  fn write_page(&self, page_id: PageId, buf: &[u8]) -> Result<(), std::io::Error>;
  fn get_relation_size(&self, relation_id: RelationIdType) -> u64;
}

pub(super) struct DiskManager {
  data_folder: Arc<RwLock<PathBuf>>
}

impl StorageManager for DiskManager {

  fn create_relation(&self, relation_id: RelationIdType) {
    File::create(self.data_folder.read().unwrap().join(relation_id.to_string())).unwrap();
  }

  fn read_page(&self, page_id: PageId, buf: &mut[u8]) -> Result<(), std::io::Error> {
    let mut file = File::options().read(true).open(self.data_folder.read().unwrap().join(page_id.relation_id.to_string())).unwrap();
    let offset = page_id.page_id as u64 * PAGE_SIZE as u64;
    file.seek(SeekFrom::Start(offset));
    file.read_exact(buf)?;
    Ok(())
  }

  fn write_page(&self, page_id: PageId, buf: &[u8]) -> Result<(), std::io::Error> {
    let mut file = File::options().write(true).open(self.data_folder.read().unwrap().join(page_id.relation_id.to_string())).unwrap();
    let offset = page_id.page_id as u64 * PAGE_SIZE as u64;
    file.seek(SeekFrom::Start(offset));
    file.write_all(buf)?;
    Ok(())
  }

  fn get_relation_size(&self, relation_id: RelationIdType) -> u64 {
    let file = File::options().open(self.data_folder.read().unwrap().join(relation_id.to_string())).unwrap();
    file.metadata().unwrap().len() / PAGE_SIZE as u64
  }
}