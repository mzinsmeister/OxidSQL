use std::io::{Cursor};
use std::error::Error;
use std::convert::TryInto;
use std::fmt::{Display, Formatter};
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

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct PageId {
  pub relation_id: u16,
  pub offset_id: u64
}

impl PageId {
    pub fn new(relation_id: u16, offset_id: u64) -> PageId {
        Self { offset_id, relation_id }
    }
}

impl From<u64> for PageId {
    fn from(input: u64) -> Self {
        Self { 
            offset_id: input & ((1u64 << 48) - 1), 
            relation_id: (input >> 48) as u16 
        }
    }
}

impl Into<u64> for PageId {
    fn into(self) -> u64 {
        ((self.relation_id as u64) << 48) | self.offset_id
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
        new_page
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
}
