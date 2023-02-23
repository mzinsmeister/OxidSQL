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

//Instead of Wrapping a Page in a Lock, maybe Wrap data in an RwLock, make dirty an Atomic Bool and wrap id in RwLock or something

pub type SegmentId = u16;
pub type OffsetId = u64;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct PageId {
  pub segment_id: SegmentId,
  pub offset_id: OffsetId
}

impl PageId {
    pub fn new(segment_id: u16, offset_id: u64) -> PageId {
        Self { offset_id, segment_id }
    }
}

impl From<u64> for PageId {
    fn from(input: u64) -> Self {
        Self { 
            offset_id: input & ((1u64 << 48) - 1), 
            segment_id: (input >> 48) as u16 
        }
    }
}

impl Into<u64> for PageId {
    fn into(self) -> u64 {
        ((self.segment_id as u64) << 48) | self.offset_id
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum PageState {
    NEW,
    DIRTY,
    CLEAN
}

impl PageState {
    pub fn get_dirty_repr(&self) -> PageState {
        match self {
            PageState::NEW => PageState::NEW,
            PageState::DIRTY => PageState::DIRTY,
            PageState::CLEAN => PageState::DIRTY
        }
    }

    pub fn is_dirtyish(&self) -> bool {
        match self {
            PageState::NEW => true,
            PageState::DIRTY => true,
            PageState::CLEAN => false
        }
    }
}

pub struct Page{
    pub data: Box<[u8]>,
    pub state: PageState,
    pub id: PageId,
}

impl Page {
    pub fn new(page_id: PageId) -> Page {
        Page {
            state: PageState::NEW,
            id: page_id,
            data: Box::new([]) // This doesn't actually do any allocation
        }
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
        self.state = self.state.get_dirty_repr();
    }

    pub fn set_u32(&mut self, pos: usize, val: u32) {
        let bytes = val.to_be_bytes();
        self.data[pos..pos+4].copy_from_slice(&bytes);
        self.state = self.state.get_dirty_repr();
    }

    pub fn make_dirty(&mut self) {
        self.state = self.state.get_dirty_repr();
    }
}
