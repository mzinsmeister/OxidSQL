use std::{fs::{File, metadata}, path::PathBuf, sync::Arc, collections::HashMap, fmt::Debug};

use super::page::{PAGE_SIZE, PageId, SegmentId};

#[cfg(test)]
use mockall::{automock};
use parking_lot::RwLock;


#[cfg_attr(test, automock)]
pub trait StorageManager: Debug + Sync + Send {
  fn create_segment(&self, segment_id: SegmentId);
  fn read_page(&self, page_id: PageId, buf: &mut[u8]) -> Result<(), std::io::Error>;
  fn write_page(&self, page_id: PageId, buf: &[u8]) -> Result<(), std::io::Error>;
  fn get_relation_size(&self, segment_id: SegmentId) -> u64;
}


#[derive(Debug)]
pub(crate) struct DiskManager {
  data_folder: Arc<PathBuf>,
  #[cfg(unix)]
  file_cache: Arc<RwLock<HashMap<SegmentId, File>>>
}

impl DiskManager {
  pub fn new(data_folder: PathBuf) -> DiskManager {
    DiskManager {
      data_folder: Arc::new(data_folder),
      #[cfg(unix)]
      file_cache: Arc::new(RwLock::new(HashMap::new()))
    }
  }
}

#[cfg(unix)]
const O_DIRECT: i32 = 0o0040000;

#[derive(Debug, PartialEq, Eq)]
enum AccessType {
  Read,
  Write
}

impl DiskManager {
  #[cfg(unix)]
  fn open_file(&self, segment_id: SegmentId, _access: AccessType) -> File {
    use std::os::unix::prelude::OpenOptionsExt;

    let file = self.file_cache.read().get(&segment_id).map(|f| f.try_clone());

    if let Some(Ok(file)) = file {
      return file;
    }

    let path = self.data_folder.join(segment_id.to_string());

    if !path.is_file() {
      File::create(path.clone()).unwrap();
    }


    let file = File::options()
      .read(true)
      .write(true)
      .create(true)
      .custom_flags(O_DIRECT)
      .open(path)
      .unwrap();

    let file_clone = file.try_clone();
    if let Ok(file_clone) = file_clone {
      let mut file_cache = self.file_cache.write();
      file_cache.insert(segment_id, file_clone);
      if file_cache.len() > 100 {
        // For now just evict a random element. Better than nothing.
        // TODO: Implement a LRU cache or something like that
        let n = rand::random::<usize>() % file_cache.len();
        let nth_segment_id = *file_cache.keys().nth(n).unwrap();
        file_cache.remove(&nth_segment_id);
      }
    }
    file
  }

  #[cfg(not(unix))]
  fn open_file(&self, segment_id: RelationIdType, access: AccessType) -> File {
    let path = self.data_folder.read()
                                .join(segment_id.to_string());

    File::options()
      .read(access == AccessType::Read)
      .write(access == AccessType::Write)
      .create(access == AccessType::Write)
      .open(data_folder.join(segment_id.to_string()))
      .unwrap()
  }
}

impl StorageManager for DiskManager {

  fn create_segment(&self, segment_id: SegmentId) {
    File::create(self.data_folder.join(segment_id.to_string())).unwrap();
  }

  #[cfg(unix)]
  fn read_page(&self, page_id: PageId, buf: &mut[u8]) -> Result<(), std::io::Error> {
    use std::os::unix::prelude::FileExt;

    let file = self.open_file(page_id.segment_id, AccessType::Read);
    let offset = page_id.offset_id as u64 * PAGE_SIZE as u64;
    file.read_exact_at(buf, offset)?;
    Ok(())
  }

  #[cfg(unix)]
  fn write_page(&self, page_id: PageId, buf: &[u8]) -> Result<(), std::io::Error> {
    use std::os::unix::prelude::FileExt;

    let file = self.open_file(page_id.segment_id, AccessType::Write);
    let offset = page_id.offset_id as u64 * PAGE_SIZE as u64;
    file.write_all_at(buf, offset)?;
    Ok(())
  }

  #[cfg(not(unix))]
  fn read_page(&self, page_id: PageId, buf: &mut[u8]) -> Result<(), std::io::Error> {
    let mut file = self.open_file(page_id.segment_id, AccessType::Read);
    let f = file.try_clone().unwrap();
    let offset = page_id.offset_id as u64 * PAGE_SIZE as u64;
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(buf)?;
    Ok(())
  }

  #[cfg(not(unix))]
  fn write_page(&self, page_id: PageId, buf: &[u8]) -> Result<(), std::io::Error> {
    let mut file = self.open_file(page_id.segment_id, AccessType::Write);
    let offset = page_id.offset_id as u64 * PAGE_SIZE as u64;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(buf)?;
    file.flush()?;
    Ok(())
  }

  fn get_relation_size(&self, segment_id: SegmentId) -> u64 {
    //TODO: It would be better to return 0 if the file doesn't exist instead of creating it
    let path = self.data_folder.join(segment_id.to_string());
    if path.is_file(){
      metadata(path.clone()).unwrap().len()
    } else {
      0
    }
  }
}