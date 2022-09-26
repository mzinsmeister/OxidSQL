use std::{fs::File, path::PathBuf, io::{Seek, Read, SeekFrom, Write}, sync::{RwLock, Arc}};

use super::page::{PAGE_SIZE, PageId, RelationIdType};

#[cfg(test)]
use mockall::{automock};


#[cfg_attr(test, automock)]
pub trait StorageManager: Sync + Send {
  fn create_relation(&self, relation_id: RelationIdType);
  fn read_page(&self, page_id: PageId, buf: &mut[u8]) -> Result<(), std::io::Error>;
  fn write_page(&self, page_id: PageId, buf: &[u8]) -> Result<(), std::io::Error>;
  fn get_relation_size(&self, relation_id: RelationIdType) -> u64;
}

pub(super) struct DiskManager {
  data_folder: Arc<RwLock<PathBuf>>
}

impl DiskManager {
  pub fn new(data_folder: PathBuf) -> DiskManager {
    DiskManager { data_folder: Arc::new(RwLock::new(data_folder)) }
  }
}

impl StorageManager for DiskManager {

  fn create_relation(&self, relation_id: RelationIdType) {
    File::create(self.data_folder.read().unwrap().join(relation_id.to_string())).unwrap();
  }

  fn read_page(&self, page_id: PageId, buf: &mut[u8]) -> Result<(), std::io::Error> {
    let mut file = File::options().read(true).open(self.data_folder.read().unwrap().join(page_id.relation_id.to_string())).unwrap();
    let offset = page_id.offset_id as u64 * PAGE_SIZE as u64;
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(buf)?;
    Ok(())
  }

  fn write_page(&self, page_id: PageId, buf: &[u8]) -> Result<(), std::io::Error> {
    // TODO: It's not ideal that we open the file new each time. Think about caching but then
    // we need to make sure that we don't have races between seeks and read/writes
    let mut file = File::options()
                                .create(true)
                                .write(true)
                                .open(self.data_folder.read().unwrap()
                                  .join(page_id.relation_id.to_string()))
                                .unwrap();
    let offset = page_id.offset_id as u64 * PAGE_SIZE as u64;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(buf)?;
    file.flush()?;
    Ok(())
  }

  fn get_relation_size(&self, relation_id: RelationIdType) -> u64 {
    let file = File::options().open(self.data_folder.read().unwrap().join(relation_id.to_string())).unwrap();
    file.metadata().unwrap().len() / PAGE_SIZE as u64
  }
}