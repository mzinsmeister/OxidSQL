use std::io::{Cursor};
use std::error::Error;
use std::convert::TryInto;
use std::fmt::{Display, Formatter};
use std::sync::{RwLock, Arc};
use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};

pub type RelationIdType = u16;

#[derive(Debug)]
pub enum PageError {
    NotEnoughSpace{ has: u16, need: usize }
}

impl Display for PageError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            PageError::NotEnoughSpace{ has, need } => 
                f.write_fmt(format_args!("Not enough space on page, has {}, needs {}", has, need))
        }
    }
}

impl Error for PageError{}

// Were using 16kb pages for now. 
// Thomas Neumann told us to go for at least this so we will do exactly that for now
pub const PAGE_SIZE: usize = 16384; 
pub const PAGE_HEADER_SIZE: u16 = 6;
pub const ENTRY_POINTER_SIZE: u16 = 4;

//Instead of Wrapping a Page in a Lock, maybe Wrap data in an RwLock, make dirty an Atomic Bool and wrap id in RwLock or something

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageId {
  pub page_id: u64,
  pub relation_id: u16
}

impl PageId {
    pub fn new(relation_id: u16, page_id: u64) -> PageId {
        Self { page_id, relation_id }
    }
}

impl From<u64> for PageId {
    fn from(input: u64) -> Self {
        Self { 
            page_id: input & ((1u64 << 48) - 1), 
            relation_id: (input >> 48) as u16 
        }
    }
}

impl Into<u64> for PageId {
    fn into(self) -> u64 {
        ((self.relation_id as u64) << 48) | self.page_id
    }
}

pub struct Page{
    pub data: Box<[u8]>,
    pub dirty: bool,
    pub id: Option<PageId>,
}

impl Page {
    pub fn new() -> Page {
        let mut new_page = Page {
            dirty: false,
            id: Option::None,
            data: Box::new([0; PAGE_SIZE])
        };
        new_page.set_free_end((PAGE_SIZE - 1) as u16);
        new_page.set_special_start(PAGE_SIZE as u16);
        new_page.set_free_start(6);
        new_page
    }

    pub fn reset(&mut self, special_size: u16) {
        self.set_free_end((PAGE_SIZE - 1) as u16 - special_size);
        self.set_special_start(PAGE_SIZE as u16 - special_size);
        self.set_free_start(6);
    }

    pub fn reset_entries(&mut self) {
        self.set_free_start(6);
        self.set_free_end(self.get_special_start() - 1);
        self.dirty = true;
    }

    pub fn get_u16(&self, pos: usize) -> u16 {
        u16::from_be_bytes(self.data[pos..pos+2].try_into().unwrap())
    }

    pub fn get_u32(&self, pos: usize) -> u32 {
        u32::from_be_bytes(self.data[pos..pos+4].try_into().unwrap())

    }

    pub fn get_u16_tuple(&self, pos: usize) -> (u16, u16) {
        let mut cursor = Cursor::new(&*self.data);
        cursor.set_position(pos as u64);
        let v1 = cursor.read_u16::<BigEndian>().unwrap();
        let v2 = cursor.read_u16::<BigEndian>().unwrap();
        return (v1, v2);
    }

    pub fn set_u16(&mut self, pos: usize, val: u16) {
        let mut bytes = Vec::with_capacity(2);
        bytes.write_u16::<BigEndian>(val).unwrap();
        self.data[pos] = bytes[0];
        self.data[pos + 1] = bytes[1];
        self.dirty = true;
    }

    pub fn set_u32(&mut self, pos: usize, val: u32) {
        let bytes = val.to_be_bytes();
        self.data[pos..pos+4].copy_from_slice(&bytes);
        self.dirty = true;
    }

    pub fn get_free_start(&self) -> u16 {
        return self.get_u16(0);
    }

    pub fn set_free_start(&mut self, value: u16) {
        self.set_u16(0, value);
    }

    pub fn get_free_end(&self) -> u16 {
        return self.get_u16(2);
    }

    pub fn set_free_end(&mut self, value: u16) {
        self.set_u16(2, value);
        self.dirty = true;
    }

    pub fn get_special_start(&self) -> u16 {
        return self.get_u16(4);
    }

    pub fn set_special_start(&mut self, value: u16) {
        self.set_u16(4, value);
        self.dirty = true;
    }

    pub fn len(&self) -> u16 {
        (self.get_free_start() - PAGE_HEADER_SIZE) / ENTRY_POINTER_SIZE
    }

    pub fn get_free_space(&self) -> u16 {
        let free_end = self.get_free_end();
        let free_start = self.get_free_start();
        if free_end < free_start {
            0
        } else {
            free_end - free_start + 1
        }
    }

    pub fn get_max_data_size(&self) -> u16 {
        let space = self.get_free_space();
        if space >= ENTRY_POINTER_SIZE {
            space as u16 - ENTRY_POINTER_SIZE
        } else {
            0
        }
    }

    pub fn insert_data(&mut self, data: &[u8]) -> Result<u16, PageError>{
        let free_start = self.get_free_start();
        let free_end = self.get_free_end();
        if (free_end as i32 - free_start as i32) < data.len() as i32 + 3 {
            return Err(PageError::NotEnoughSpace{ has: self.get_free_space(), need: data.len() + 4})
        }
        self.set_free_start(free_start + ENTRY_POINTER_SIZE);
        self.set_free_end(free_end - data.len() as u16);        
        let offset = free_end - data.len() as u16 + 1;
        self.data[offset as usize..(offset + data.len() as u16) as usize].copy_from_slice(data);
        self.set_u16(free_start as usize, offset);
        self.set_u16(free_start as usize + 2, data.len() as u16);
        self.dirty = true;
        Ok((free_start - 6) / ENTRY_POINTER_SIZE)
    }

    pub fn get_data(&self, index: u16) -> &[u8] {
        let (offset, length) = self.get_u16_tuple(PAGE_HEADER_SIZE as usize + (index * ENTRY_POINTER_SIZE) as usize);
        &self.data[offset as usize..(offset+length) as usize]
    }

    pub fn entry_iter(&self) -> EntryIterator {
        EntryIterator {
            page: self,
            index: 0
        }
    }
}

pub struct EntryIterator<'a> {
    pub page: &'a Page, //TODO: Remove pub
    pub index: u16
}

impl<'a> Iterator for EntryIterator<'a> {
    type Item = (u16, u16);

    fn next(&mut self) -> Option<Self::Item> {
        let next_offset = 6 + 4 * self.index;
        if next_offset < self.page.get_free_start() {
            let item = self.page.get_u16_tuple(next_offset as usize);
            self.index += 1;
            Some(item)
        } else {
            None
        }
    }
}
