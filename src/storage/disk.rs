use std::{fs::{File, metadata}, path::PathBuf, io::{Seek, Read, SeekFrom, Write}, sync::{RwLock, Arc}};

use super::page::{PAGE_SIZE, PageId, RelationIdType};

#[cfg(test)]
use mockall::{automock};


#[cfg_attr(test, automock)]
pub trait StorageManager: Sync + Send {
  fn create_relation(&self, segment_id: RelationIdType);
  fn read_page(&self, page_id: PageId, buf: &mut[u8]) -> Result<(), std::io::Error>;
  fn write_page(&self, page_id: PageId, buf: &[u8]) -> Result<(), std::io::Error>;
  fn get_relation_size(&self, segment_id: RelationIdType) -> u64;
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

  fn create_relation(&self, segment_id: RelationIdType) {
    File::create(self.data_folder.read().unwrap().join(segment_id.to_string())).unwrap();
  }

  fn read_page(&self, page_id: PageId, buf: &mut[u8]) -> Result<(), std::io::Error> {
    let mut file = File::options().read(true).open(self.data_folder.read().unwrap().join(page_id.segment_id.to_string())).unwrap();
    let offset = page_id.offset_id as u64 * PAGE_SIZE as u64;
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(buf)?;
    Ok(())
  }

  fn write_page(&self, page_id: PageId, buf: &[u8]) -> Result<(), std::io::Error> {
    // TODO: It's not ideal that we open the file new each time. Think about caching but then
    // we need to make sure that we don't have races between seeks and read/writes
    let mut file = File::options()
                                .write(true)
                                .create(true)
                                .open(self.data_folder.read().unwrap()
                                  .join(page_id.segment_id.to_string()))
                                .unwrap();
    let offset = page_id.offset_id as u64 * PAGE_SIZE as u64;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(buf)?;
    file.flush()?;
    Ok(())
  }

  fn get_relation_size(&self, segment_id: RelationIdType) -> u64 {
    //TODO: It would be better to return 0 if the file doesn't exist instead of creating it
    let path = self.data_folder.read().unwrap().join(segment_id.to_string());
    if path.is_file(){
      metadata(path.clone()).unwrap().len()
    } else {
      0
    }
  }
}