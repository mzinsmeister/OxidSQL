
// TODO: Implement a B+Tree using (optimistic) latch crabbing
// We will implement right-links on the leaf pages for now but not left-links bc. of deadlocks
// Could also implement a B-Link tree using latching approach described in Lehmans & Yao paper

use std::{mem::size_of, cmp::Ordering, ops::Deref};

use byteorder::{BigEndian, NativeEndian};
use zerocopy::{AsBytes, FromBytes, LayoutVerified, ByteSlice, ByteSliceMut, Unaligned, U16, U32, U64};

use crate::{storage::{buffer_manager::{BufferManager, BMArcReadGuard, BMArc}, page::{SegmentId, PageId, Page}}, types::{TupleValue, RelationTID, TupleValueType}};

use super::{index::{OrderedIndex, Index}, tuple::Tuple};

pub struct BTreeSegment<B: BufferManager> {
    bm: B,
    segment_id: SegmentId,
    key_attributes: Vec<TupleValueType>
}

impl<B: BufferManager> BTreeSegment<B> {
    pub fn new(bm: B, segment_id: SegmentId, key_attributes: Vec<TupleValueType>) -> Self {
        Self {
            bm: bm.clone(),
            segment_id,
            key_attributes
        }
    }

    fn lookup_leaf_page<T: Deref<Target=Page>>(&self, key: &[Option<TupleValue>], lock: fn(BMArc<B>) -> T)-> Result<(T,T), B::BError> {
        let mut parent_page = lock(self.bm.fix_page(PageId::new(self.segment_id, 0))?);
        let root_page: u64 = u64::from_be_bytes(parent_page[0..8].try_into().unwrap());
        let root_page = self.bm.fix_page(PageId::new(self.segment_id, root_page))?;
        let mut current_page = lock(root_page);
        loop {
            if current_page[1] & 1 == 1 {
                // Leaf page
                return Ok((parent_page, current_page));
            }
            drop(parent_page);
            // Still inner nodes
            let current_bt_page = BTreeInnerPage::parse(current_page.as_ref()).unwrap();
            let next_page_id = current_bt_page.binary_search(|val| {
                let tuple = Tuple::parse_binary_all(&self.key_attributes, val);
                key.partial_cmp(tuple.values.as_slice()).unwrap()
            });
            let next_page = self.bm.fix_page(PageId::new(self.segment_id, next_page_id))?;
            parent_page = current_page;
            current_page = lock(next_page);
        }
    }
}

impl<B: BufferManager> Index<B> for BTreeSegment<B> {
    fn lookup(&self, key: &[Option<TupleValue>]) -> Result<Option<RelationTID>, B::BError> {
        let (_, leaf_page_raw) = self.lookup_leaf_page(key, |b| b.read_owning())?;
        let leaf_page = BTreeLeafPage::parse(leaf_page_raw.as_ref()).unwrap();
        Ok(leaf_page.binary_search(|val| {
            let tuple = Tuple::parse_binary_all(&self.key_attributes, val);
            key.partial_cmp(tuple.values.as_slice()).unwrap()
        }))
    }

    fn insert(&self, key: &[Option<TupleValue>], tid: RelationTID) -> Result<(), B::BError> {
        // We first try the happy path: Taking read latches all the way down and only write locking the leaf page
        // if we need to split the leaf page, we will retry and take a stack of write latches all the way down
        // Since we have variable length keys (potentially, but we won't optimize the fixed-size case for now)
        // we cannot do eager splitting and will instead have to split on demand
        let (parent_page, page) = self.lookup_leaf_page(key, |r| r.upgradeable_read_owning())?;
        let leaf_page = BTreeLeafPage::parse(page.as_ref()).unwrap();
        let key_binary = Tuple::new(key).get_binary();
        if key_binary.len() + 8 + 8 <= leaf_page.header.free_space.get() as usize {
            // don't need to split -> just insert
            let mut upgraded_page = page.upgrade();
            let mut leaf_page = BTreeLeafPage::parse(upgraded_page.as_mut()).unwrap();
            leaf_page.insert(|val| {
                let tuple = Tuple::parse_binary_all(&self.key_attributes, val);
                key.partial_cmp(tuple.values.as_slice()).unwrap()
            }, &key_binary, tid).unwrap();
            Ok(())
        } else {
            // need to split -> does the parent have enough space? if yes upgrade parent latch and split
            // else take write-latches all the way down and keep them
            drop(leaf_page);
            drop(page);
            let mut latch_stack = Vec::new();
            let mut parent_page = self.bm.fix_page(PageId::new(self.segment_id, 0))?.write_owning();
            let root_page: u64 = u64::from_be_bytes(parent_page[0..8].try_into().unwrap());
            let root_page = self.bm.fix_page(PageId::new(self.segment_id, root_page))?;
            let mut current_page = root_page.write_owning();
            loop {
                if current_page[1] & 1 == 1 {
                    // Leaf page
                    let mut leaf_page = BTreeLeafPage::parse(current_page.as_mut()).unwrap();
                    if key_binary.len() + 8 + 8 <= leaf_page.header.free_space.get() as usize {
                        // don't need to split -> just insert
                        leaf_page.insert(|val| {
                            let tuple = Tuple::parse_binary_all(&self.key_attributes, val);
                            key.partial_cmp(tuple.values.as_slice()).unwrap()
                        }, &key_binary, tid).unwrap();
                        return Ok(());
                    } else {

                    }
                }
                latch_stack.push(parent_page);
                // Still inner nodes
                let current_bt_page = BTreeInnerPage::parse(current_page.as_ref()).unwrap();
                let next_page_id = current_bt_page.binary_search(|val| {
                    let tuple = Tuple::parse_binary_all(&self.key_attributes, val);
                    key.partial_cmp(tuple.values.as_slice()).unwrap()
                });
                let next_page = self.bm.fix_page(PageId::new(self.segment_id, next_page_id))?;
                parent_page = current_page;
                current_page = next_page.write_owning();
            }
        }
    }
}

impl<B: BufferManager> OrderedIndex<B> for BTreeSegment<B> {

    type ScanIterator = BTreeScan<B>;

    fn scan(&self, from: &[Option<TupleValue>], to: &[Option<TupleValue>]) -> Result<Self::ScanIterator, <B as BufferManager>::BError> {
        todo!()
    }
}

// 16 bit slot count, 16 bit first free slot, 32 bit data start, 32 bit free space
#[repr(C)]
#[derive(Copy, Clone, Debug, AsBytes, FromBytes, Unaligned)]
struct BTreePageInnerHeader {
    flags: U16<BigEndian>,
    slot_count: U16<NativeEndian>,
    data_start: U32<NativeEndian>,
    free_space: U32<NativeEndian>,
    last_pointer: U64<NativeEndian>
}

/*
    The inner page contains the header after which the slots follow. 
    The values contain a variable sized tuple (the key) followed by a 64 bit page id.
 */

#[repr(C)]
#[derive(FromBytes)]
struct BTreeInnerPage<B: ByteSlice> {
    header: LayoutVerified<B, BTreePageInnerHeader>,
    data: B
}

#[repr(C)]
#[derive(FromBytes, Unaligned, AsBytes)]
struct Slot {
    offset: U32<NativeEndian>,
    length: U32<NativeEndian>
}

impl<B: ByteSlice> BTreeInnerPage<B> {
    fn parse(bytes: B) -> Option<BTreeInnerPage<B>> {
        let (header, data) = LayoutVerified::new_unaligned_from_prefix(bytes)?;
        Some(BTreeInnerPage { header, data })
    }

    fn get_slots(&self) -> &[Slot] {
        let slot_count = self.header.slot_count.get() as usize;
        let slot_size = size_of::<Slot>();
        let slots = &self.data.as_ref()[0..slot_count * slot_size];
        let layout: LayoutVerified<&[u8], [Slot]> = LayoutVerified::new_slice(slots).unwrap();
        layout.into_slice()
    }

    fn binary_search<C: Fn(&[u8]) -> Ordering>(&self, comp: C) -> u64 {
        let slots = self.get_slots();
        let mut left = 0;
        let mut right = slots.len();
        while left < right {
            let mid = (left + right) / 2;
            let slot = &slots[mid];
            let data = &self.data.as_ref()[slot.offset.get() as usize..(slot.offset.get() + slot.length.get()) as usize];
            match comp(&data[..data.len() - 8]) {
                Ordering::Less => {
                    right = mid;
                },
                Ordering::Equal => {
                    return u64::from_be_bytes(data[data.len() - 8..].try_into().unwrap());
                },
                Ordering::Greater => {
                    left = mid + 1;
                }
            }
        }
        if right == slots.len() {
            self.header.last_pointer.get()
        } else {
            let slot = &slots[right];
            let data = &self.data.as_ref()[slot.offset.get() as usize..(slot.offset.get() + slot.length.get()) as usize];
            u64::from_be_bytes(data[data.len() - 8..].try_into().unwrap())
        }
    }
    
}

impl<B: ByteSliceMut> BTreeInnerPage<B> {
    
}

// 16 bit slot count, 16 bit first free slot, 32 bit data start, 32 bit free space
#[repr(C)]
#[derive(Copy, Clone, Debug, AsBytes, FromBytes, Unaligned)]
struct BTreePageLeafHeader {
    flags: U16<BigEndian>,
    slot_count: U16<NativeEndian>,
    data_start: U32<NativeEndian>, // Not really necessary. It's just the offset of the first slot
    free_space: U32<NativeEndian>, // Not really necessary. It's just the data length minus data_start
}

#[repr(C)]
#[derive(FromBytes)]
struct BTreeLeafPage<B: ByteSlice> {
    header: LayoutVerified<B, BTreePageLeafHeader>,
    data: B
}

impl<B: ByteSlice> BTreeLeafPage<B> {
    pub fn parse(bytes: B) -> Option<BTreeLeafPage<B>> {
        let (header, data) = LayoutVerified::new_unaligned_from_prefix(bytes)?;
        Some(BTreeLeafPage { header, data })
    }

    fn get_slots(&self) -> &[Slot] {
        let slot_count = self.header.slot_count.get() as usize;
        let slot_size = size_of::<Slot>();
        let slots = &self.data.as_ref()[0..slot_count * slot_size];
        let layout: LayoutVerified<&[u8], [Slot]> = LayoutVerified::new_slice(slots).unwrap();
        layout.into_slice()
    }

    fn binary_search_slot<C: Fn(&[u8]) -> Ordering>(&self, comp: C) -> Option<(usize, bool)> {
        let slots = self.get_slots();
        let mut left = 0;
        let mut right = slots.len();
        while left < right {
            let mid = (left + right) / 2;
            let slot = &slots[mid];
            let data = &self.data.as_ref()[slot.offset.get() as usize..(slot.offset.get() + slot.length.get()) as usize];
            match comp(&data[..data.len() - 8]) {
                Ordering::Less => {
                    right = mid;
                },
                Ordering::Equal => {
                    return Some((mid, true));
                },
                Ordering::Greater => {
                    left = mid + 1;
                }
            }
        }
        if right < slots.len() {
            let slot = &slots[right];
            let data = &self.data.as_ref()[slot.offset.get() as usize..(slot.offset.get() + slot.length.get()) as usize];
            if comp(&data[..data.len() - 8]) == Ordering::Equal {
                return Some((right, true));
            }
        }
        None
    }

    fn binary_search<C: Fn(&[u8]) -> Ordering>(&self, comp: C) -> Option<RelationTID> {
        self.binary_search_slot(comp).map(|(s, exact_match)| {
            let slot = &self.get_slots()[s];
            let value_end = (slot.offset.get() + slot.length.get()) as usize;
            let value_start = value_end - 8;
            // Check whether the key actually matches
            if !exact_match {
                return None;
            }
            Some(RelationTID::from(u64::from_be_bytes(self.data[value_start..value_end].try_into().unwrap())))
        }
        ).flatten()
    }
}

impl<B: ByteSliceMut> BTreeLeafPage<B> {

    fn insert<C: Fn(&[u8]) -> Ordering>(&mut self, comp: C, key: &[u8], value: RelationTID) -> Result<(), ()> {
        let slot = self.binary_search_slot(comp).to_owned();
        let (slot_index, move_end) = if let Some((slot_index, exact_match)) = slot {
            if exact_match {
                return Err(());
            }
            let slot: &Slot = &self.get_slots()[slot_index];
            (slot_index, (slot.offset.get() + slot.length.get()) as usize)
        } else {
            // Insert at the end
            (self.header.slot_count.get() as usize, self.data.len())
        };
        // Move greaters slots and insert
        self.data.copy_within(self.header.slot_count.get() as usize * size_of::<Slot>()..move_end, self.header.slot_count.get() as usize * size_of::<Slot>() - size_of::<Slot>());
        // Move smaller elements and insert
        let insert_len = key.len() + 8;
        self.data.copy_within(self.header.data_start.get() as usize..move_end, self.header.data_start.get() as usize - insert_len);
        self.data[move_end-insert_len..move_end-8].copy_from_slice(key);
        self.data[move_end-8..move_end].copy_from_slice(&u64::from(&value).to_be_bytes());
        // Update header
        let slots = self.header.slot_count.get();
        self.header.slot_count.set(slots + 1);
        let data_start = self.header.data_start.get();
        self.header.data_start.set(data_start - insert_len as u32);
        let free_space = self.header.free_space.get();
        self.header.free_space.set(free_space - insert_len as u32);
        Ok(())
    }

    fn split_into(&mut self, other: &mut Self) {
        let mut split_index = 0;
        let mut cumulative_size = 0;
        let slots = self.get_slots();
        // We want that both pages have at least (PAGE_SIZE - HEADER_SIZE) / 2 space after the split
        while ((cumulative_size + slots[split_index].length.get()) as usize)  < self.data.len() / 2 {
            let slot = &slots[split_index];
            cumulative_size += slot.length.get();
            split_index += 1;
        }
        let split_key = self.data.as_ref()[slots[split_index].offset.get() as usize..(slots[split_index].offset.get() + slots[split_index].length.get() - 8) as usize].to_owned();
        let old_data_start = self.header.data_start.get() as usize;
        let new_other_size = self.data.len() - old_data_start - cumulative_size as usize - split_key.len();
        let other_data_start = other.data.len() - new_other_size;
        let other_data_end = other.data.len();
        let data_to_copy = &self.data[self.data.len() - new_other_size..self.data.len()];
        other.data[other_data_start..other_data_end].copy_from_slice(data_to_copy);
        let data_end = self.data.len();
        let new_data_start = data_end - cumulative_size as usize;
        self.data.copy_within(old_data_start..data_end, new_data_start);
        // Update slotsenumerate
        let mut data_start = other.data.len() as u32;
        let slots = self.get_slots();
        for slot_index in (0..split_index).rev() {
            let length = U32::<NativeEndian>::from(slots[slot_index].length.get());
            let offset = U32::<NativeEndian>::from(data_start - length.get());
            other.data[slot_index*size_of::<Slot>()..(slot_index+1)*size_of::<Slot>()].copy_from_slice(Slot{ length, offset }.as_bytes());
            data_start -= length.get();
        }
        // Update self headers
        self.header.data_start.set(new_data_start as u32);
        self.header.free_space.set((self.data.len() - new_data_start) as u32);
        let new_slot_count = self.header.slot_count.get() - (split_index - 1) as u16;
        self.header.slot_count.set(new_slot_count);
        // Update other headers
        other.header.data_start.set(other_data_start as u32);
        other.header.free_space.set((other_data_end - other_data_start) as u32);
        other.header.slot_count.set(split_index as u16);
    }
}

pub struct BTreeScan<B: BufferManager> {
    bm: B
}

impl<B: BufferManager> Iterator for BTreeScan<B> {
    type Item = Result<RelationTID, B::BError>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}