
// TODO: Implement a B+Tree using (optimistic) latch crabbing
// We will implement right-links on the leaf pages for now but not left-links bc. of deadlocks
// Could also implement a B-Link tree using latching approach described in Lehmans & Yao paper

use std::{mem::size_of, cmp::Ordering, ops::Deref};

use byteorder::{BigEndian, NativeEndian};
use zerocopy::{AsBytes, FromBytes, ByteSlice, ByteSliceMut, Unaligned, U16, U32, U64, FromZeroes, Ref};

use crate::{storage::{buffer_manager::{BufferManager, BMArc, BMArcUpgradeableReadGuard, BMArcReadGuard}, page::{SegmentId, PageId, Page}}, types::{TupleValue, RelationTID, TupleValueType}};

use super::{index::{OrderedIndex, Index}, tuple::Tuple};

// A B+Tree. It will always be unique. If we want an index with duplicates, we append the TID to the key.

#[derive(Clone, Debug)]
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

    pub fn init(&self) {
        let metadata_page = self.bm.fix_page(PageId::new(self.segment_id, 0)).unwrap();
        let mut metadata_write = metadata_page.write();
        metadata_write[0..8].copy_from_slice(&1u64.to_be_bytes());
        let root_page = self.bm.fix_page(PageId::new(self.segment_id, 1)).unwrap();
        let mut root_write = root_page.write();
        let mut root_leaf = BTreePage::parse(root_write.as_mut()).unwrap();
        root_leaf.init(true);
        root_leaf.header.flags.set(1); // Leaf
    }

    pub fn lower_bound(self, key: &[Option<TupleValue>]) -> Result<BTreeScan<B>, B::BError> {
        let (_, leaf_page_guard) = self.lookup_leaf_page(key, |b| b.read_owning())?;
        let leaf_page = BTreePage::parse(leaf_page_guard.as_ref()).unwrap();

        if let Some((slot_id, _)) = leaf_page.binary_search_slot(|v| {
                let tuple = Tuple::parse_binary_all(&self.key_attributes, v);
                key.partial_cmp(tuple.values.as_slice()).unwrap()
            }) {
            Ok(BTreeScan {
                segment: self,
                slot_id: slot_id as u16,
                page_guard: Some(leaf_page_guard)
            })
        } else {
            return Ok(BTreeScan { segment: self, page_guard: None, slot_id: 0 });
        }
    }

    fn lookup_leaf_page<T: Deref<Target=Page>>(&self, key: &[Option<TupleValue>], lock: fn(BMArc<B>) -> T)-> Result<(T,T), B::BError> {
        let mut parent_page = lock(self.bm.fix_page(PageId::new(self.segment_id, 0))?);
        let root_page_offset: u64 = u64::from_be_bytes(parent_page[0..8].try_into().unwrap());
        let root_page = self.bm.fix_page(PageId::new(self.segment_id, root_page_offset))?;
        let mut current_page_offset = root_page_offset;
        let mut current_page = lock(root_page);
        loop {
            if current_page[1] & 1 == 1 {
                // Leaf page
                return Ok((parent_page, current_page));
            }
            drop(parent_page);
            // Still inner nodes
            let current_bt_page = BTreePage::parse(current_page.as_ref()).unwrap();
            let next_page_offset =  current_bt_page.search_inner(|val| {
                        let tuple = Tuple::parse_binary_all(&self.key_attributes, val);
                        key.partial_cmp(tuple.values.as_slice()).unwrap()
                    });
            assert_ne!(next_page_offset, u64::MAX, "BTree corrupted");
            assert_ne!(next_page_offset, 0, "BTree corrupted");
            assert_ne!(next_page_offset, current_page_offset, "BTree corrupted");
            let next_page = self.bm.fix_page(PageId::new(self.segment_id, next_page_offset))?;
            parent_page = current_page;
            current_page = lock(next_page);
            current_page_offset = next_page_offset;
        }
    }
}

impl<B: BufferManager> Index<B> for BTreeSegment<B> {
    fn lookup(&self, key: &[Option<TupleValue>]) -> Result<Option<RelationTID>, B::BError> {
        let (_, leaf_page_raw) = self.lookup_leaf_page(key, |b| b.read_owning())?;
        let leaf_page = BTreePage::parse(leaf_page_raw.as_ref()).unwrap();
        Ok(leaf_page.binary_search(|val| {
            let tuple = Tuple::parse_binary_all(&self.key_attributes, val);
            key.partial_cmp(tuple.values.as_slice()).unwrap()
        }).map(|tid| RelationTID::from(tid)))
    }

    fn insert(&self, key: &[Option<TupleValue>], tid: RelationTID) -> Result<bool, B::BError> {
        // We first try the happy path: Taking read latches all the way down and only write locking the leaf page
        // if we need to split the leaf page, we will retry and take a stack of write latches all the way down
        // Since we have variable length keys (potentially, but we won't optimize the fixed-size case for now)
        // we cannot do eager splitting and will instead have to split on demand
        let (parent_page, page) = self.lookup_leaf_page(key, |r| r.upgradeable_read_owning())?;
        let leaf_page = BTreePage::parse(page.as_ref()).unwrap();
        let key_binary = Tuple::new(key).get_binary();
        let insert_cmp = |val: &[u8]| {
            let tuple = Tuple::parse_binary_all(&self.key_attributes, val);
            key.partial_cmp(tuple.values.as_slice()).unwrap()
        };
        if key_binary.len() + 8 + 8 <= leaf_page.header.free_space.get() as usize {
            // don't need to split -> just insert
            let mut upgraded_page = page.upgrade();
            let mut leaf_page = BTreePage::parse(upgraded_page.as_mut()).unwrap();
            let res = leaf_page.insert(insert_cmp, &key_binary, u64::from(&tid));
            Ok(res.is_ok())
        } else {
            // TODO: need to split -> does the parent have enough space? if yes upgrade parent latch and split
            // else take write-latches all the way down and keep them
            drop(leaf_page);
            drop(page);
            drop(parent_page);
            let mut latch_stack: Vec<BMArcUpgradeableReadGuard<B>> = Vec::new();
            let mut parent_page = self.bm.fix_page(PageId::new(self.segment_id, 0))?.upgradeable_read_owning();
            let root_page_id: u64 = u64::from_be_bytes(parent_page[0..8].try_into().unwrap());
            let root_page = self.bm.fix_page(PageId::new(self.segment_id, root_page_id))?;
            let mut current_page = root_page.upgradeable_read_owning();
            loop {
                if current_page[1] & 1 == 1 {
                    // Leaf page
                    let mut upgraded_page = current_page.upgrade();
                    let mut leaf_page = BTreePage::parse(upgraded_page.as_mut()).unwrap();
                    let free_space = leaf_page.header.free_space.get() as usize;
                    if key_binary.len() + 8 + 8 <= free_space {
                        // don't need to split -> just insert
                        leaf_page.insert(insert_cmp, &key_binary, u64::from(&tid)).unwrap();
                        return Ok(true);
                    } else {
                        latch_stack.push(parent_page);
                        // split
                        let mut new_pid = self.bm.allocate_page(self.segment_id);
                        let mut split_page_write = self.bm.fix_page(new_pid)?.write_owning();
                        let mut split_bt_page = BTreePage::parse(split_page_write.as_mut()).unwrap();
                        let mut split_key = leaf_page.leaf_split_into(&mut split_bt_page, new_pid.offset_id);
                        let mut split_key_tuple = Tuple::parse_binary_all(&self.key_attributes, &split_key);
                        // insert the actual tuple
                        if key.partial_cmp(split_key_tuple.values.as_slice()).unwrap() != Ordering::Greater {
                            // Insert into original page
                            let res = leaf_page.insert(insert_cmp, &key_binary, u64::from(&tid));
                            if res.is_err() {
                                return Ok(false);
                            }
                        } else {
                            // Insert into new page
                            let res = split_bt_page.insert(insert_cmp, &key_binary, u64::from(&tid));
                            if res.is_err() {
                                return Ok(false);
                            }
                        }
                        while latch_stack.len() > 1{
                            let mut parent_page = latch_stack.pop().unwrap().upgrade();
                            let mut parent_bt_page = BTreePage::parse(parent_page.as_mut()).unwrap();
                            if parent_bt_page.header.free_space.get() as usize >= split_key.len() + 8 + 8 {
                                // don't need to split -> just insert
                                let res = parent_bt_page.insert(|val| {
                                    let tuple = Tuple::parse_binary_all(&self.key_attributes, val);
                                    split_key_tuple.values.as_slice().partial_cmp(tuple.values.as_slice()).unwrap()
                                }, &split_key, new_pid.offset_id);
                                return Ok(res.is_ok());
                            } else {
                                // split
                                new_pid = self.bm.allocate_page(self.segment_id);
                                split_page_write = self.bm.fix_page(new_pid)?.write_owning();
                                let mut split_bt_page = BTreePage::parse(split_page_write.as_mut()).unwrap();
                                let split_key_new = parent_bt_page.inner_split_into(&mut split_bt_page);
                                let split_key_tuple_new = Tuple::parse_binary_all(&self.key_attributes, &split_key);
                                let insert_page = if key.partial_cmp(split_key_tuple.values.as_slice()).unwrap() == Ordering::Less {
                                    // Insert into original page
                                    &mut parent_bt_page
                                } else {
                                    // Insert into new page
                                    &mut split_bt_page
                                };
                                let res = insert_page.insert(|val| {
                                    let tuple = Tuple::parse_binary_all(&self.key_attributes, val);
                                    split_key_tuple.values.as_slice().partial_cmp(tuple.values.as_slice()).unwrap()
                                }, &split_key, new_pid.offset_id); // TODO: This could still fail with large keys
                                if res.is_err() {
                                    return Ok(false);
                                }
                                split_key = split_key_new;
                                split_key_tuple = split_key_tuple_new;
                            }
                        }
                        let meta_page_lock = latch_stack.pop().unwrap();
                        let mut meta_page = meta_page_lock.upgrade();
                        let new_root_pid = self.bm.allocate_page(self.segment_id);
                        let new_root_page = self.bm.fix_page(new_root_pid)?;
                        let mut new_root_page_write = new_root_page.write();
                        let mut new_root_bt_page = BTreePage::parse(new_root_page_write.as_mut()).unwrap();
                        new_root_bt_page.init(false);
                        new_root_bt_page.header.first_next_pointer.set(root_page_id);
                        new_root_bt_page.insert(|val| {
                            let tuple = Tuple::parse_binary_all(&self.key_attributes, val);
                            split_key_tuple.values.as_slice().partial_cmp(tuple.values.as_slice()).unwrap()
                        }, &split_key, new_pid.offset_id).unwrap();
                        meta_page[0..8].copy_from_slice(&new_root_pid.offset_id.to_be_bytes());
                        return Ok(true);
                    }
                } else {
                    latch_stack.push(parent_page);
                    // Still inner nodes
                    let current_bt_page = BTreePage::parse(current_page.as_ref()).unwrap();
                    let next_page_id = current_bt_page.search_inner(|val| {
                                let tuple = Tuple::parse_binary_all(&self.key_attributes, val);
                                key.partial_cmp(tuple.values.as_slice()).unwrap()
                            });
                    let next_page = self.bm.fix_page(PageId::new(self.segment_id, next_page_id))?;
                    parent_page = current_page;
                    current_page = next_page.upgradeable_read_owning();
                }
            }
        }
    }
}

impl<B: BufferManager> OrderedIndex<B> for BTreeSegment<B> {

    type ScanIterator = BTreeScan<B>;

    fn scan(&self, _from: Option<&[Option<TupleValue>]>, _to: Option<&[Option<TupleValue>]>) -> Result<Self::ScanIterator, <B as BufferManager>::BError> {
        todo!()
    }
}

#[repr(C)]
#[derive(FromBytes, FromZeroes, Unaligned, AsBytes)]
struct Slot {
    offset: U32<NativeEndian>,
    length: U32<NativeEndian>
}

impl Slot {
    fn new(offset: u32, length: u32) -> Self {
        Self {
            offset: U32::<NativeEndian>::new(offset),
            length: U32::<NativeEndian>::new(length)
        }
    }
}

#[repr(C)]
#[derive(Copy, Clone, Debug, AsBytes, FromBytes, FromZeroes, Unaligned)]
struct BTreePageHeader {
    flags: U16<BigEndian>,
    slot_count: U16<NativeEndian>,
    data_start: U32<NativeEndian>, // Not really necessary. It's just the offset of the first slot
    free_space: U32<NativeEndian>, // Not really necessary. It's just the data length minus data_start
    first_next_pointer: U64<NativeEndian> // First (less than first key) Pointer for inner pages, next pointer for leaf pages
}

#[repr(C)]
#[derive(FromBytes, FromZeroes)]
struct BTreePage<B: ByteSlice> {
    header: Ref<B, BTreePageHeader>,
    data: B
}

impl<B: ByteSlice> BTreePage<B> {
    #[allow(dead_code)]
    fn is_inner(&self) -> bool {
        self.header.flags.get() & 1 == 0
    }

    pub fn parse(bytes: B) -> Option<BTreePage<B>> {
        let (header, data) = Ref::new_unaligned_from_prefix(bytes)?;
        Some(BTreePage { header, data })
    }

    fn get_slots(&self) -> &[Slot] {
        let slot_count = self.header.slot_count.get() as usize;
        let slot_size = size_of::<Slot>();
        let slots = &self.data.as_ref()[0..slot_count * slot_size];
        let layout: Ref<&[u8], [Slot]> = Ref::new_slice(slots).unwrap();
        layout.into_slice()
    }

    fn get_item(&self, slot_id: u16) -> &[u8] {
        let slot = &self.get_slots()[slot_id as usize];
        &self.data.as_ref()[slot.offset.get() as usize..(slot.offset.get() + slot.length.get()) as usize]
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
            } else {
                return Some((right, false));
            }
        }
        None
    }

    fn lower_bound<C: Fn(&[u8]) -> Ordering>(&self, comp: C) -> Option<u64> {
        self.binary_search_slot(comp).map(|(s, _)| {
            let slot = &self.get_slots()[s];
            let value_end = (slot.offset.get() + slot.length.get()) as usize;
            let value_start = value_end - 8;
            // Don't check whether the key actually matches
            Some(u64::from_be_bytes(self.data[value_start..value_end].try_into().unwrap()))
        }
        ).flatten()
    }

    fn binary_search<C: Fn(&[u8]) -> Ordering>(&self, comp: C) -> Option<u64> {
        self.binary_search_slot(comp).map(|(s, exact_match)| {
            let slot = &self.get_slots()[s];
            let value_end = (slot.offset.get() + slot.length.get()) as usize;
            let value_start = value_end - 8;
            // Check whether the key actually matches
            if !exact_match {
                return None;
            }
            Some(u64::from_be_bytes(self.data[value_start..value_end].try_into().unwrap()))
        }
        ).flatten()
    }

    fn search_inner<C: Fn(&[u8]) -> Ordering>(&self, comp: C) -> u64 {
        let mut slot_id = self.binary_search_slot(comp)
                    .map(|(s, _)| s)
                    .unwrap_or(self.header.slot_count.get() as usize);
        if slot_id == 0 {
            return self.header.first_next_pointer.get();
        }
        slot_id -= 1;
        let slot = &self.get_slots()[slot_id];
        let value_end = (slot.offset.get() + slot.length.get()) as usize;
        let value_start = value_end - 8;
        // Don't check whether the key actually matches
        u64::from_be_bytes(self.data[value_start..value_end].try_into().unwrap())
    }
}

impl<B: ByteSliceMut> BTreePage<B> {

    fn init(&mut self, is_leaf: bool) {
        self.header.free_space.set(self.data.len() as u32);
        self.header.data_start.set(self.data.len() as u32);
        self.header.slot_count.set(0);
        self.header.first_next_pointer.set(u64::MAX);
        let flags_value = if is_leaf { 1 } else { 0 };
        self.header.flags.set(flags_value);
    }

    fn get_slots_mut(&mut self) -> &mut [Slot] {
        let slot_count = self.header.slot_count.get() as usize;
        let slot_size = size_of::<Slot>();
        let slots = &mut self.data.as_mut()[0..slot_count * slot_size];
        let layout: Ref<&mut [u8], [Slot]> = Ref::new_slice(slots).unwrap();
        layout.into_mut_slice()
    }

    fn insert<C: Fn(&[u8]) -> Ordering>(&mut self, comp: C, key: &[u8], value: u64) -> Result<usize, ()> {
        let insert_len = key.len() + 8;
        let slot = self.binary_search_slot(comp).to_owned();
        let (slot_index, move_end) = if let Some((slot_index, exact_match)) = slot {
            if exact_match {
                // We don't want actual duplicates on this level since this will destroy worst case O(log n) performance
                // Insert with a tie breaker key if you want duplicates
                return Err(());
            }
            let slot: &Slot = &self.get_slots()[slot_index];
            (slot_index, (slot.offset.get() + slot.length.get()) as usize)
        } else {
            // Insert at the end
            (self.header.slot_count.get() as usize, self.header.data_start.get() as usize)
        };
        // Move greater slots and insert
        self.data.copy_within(slot_index * size_of::<Slot>()..self.header.slot_count.get() as usize * size_of::<Slot>(), slot_index * size_of::<Slot>() + size_of::<Slot>());
        let new_slot_count = self.header.slot_count.get() + 1;
        self.header.slot_count.set(new_slot_count);
        let slots = self.get_slots_mut();
        slots[slot_index] = Slot::new(move_end as u32 - insert_len as u32, insert_len as u32);
        for slot in &mut slots[slot_index+1..] {
            slot.offset.set(slot.offset.get() - insert_len as u32);
        }
        // Move larger elements and insert
        self.data.copy_within(self.header.data_start.get() as usize..move_end, self.header.data_start.get() as usize - insert_len);
        self.data[move_end-insert_len..move_end-8].copy_from_slice(key);
        self.data[move_end-8..move_end].copy_from_slice(&value.to_be_bytes());
        // Update header
        self.header.data_start -= (insert_len as u32).into();
        self.header.free_space -= (insert_len as u32 + 8).into();
        Ok(slot_index)
    }

    fn inner_split_into(&mut self, other: &mut Self) -> Box<[u8]> {
        let mut split_index = 0;
        let mut cumulative_size = 0;
        let slots = self.get_slots();
        // We want that both pages have at least (PAGE_SIZE - HEADER_SIZE) / 2 space after the split
        while ((cumulative_size + slots[split_index].length.get()) as usize + 8 * (split_index + 1))  < self.data.len() / 2 {
            let slot = &slots[split_index];
            cumulative_size += slot.length.get();
            split_index += 1;
        }
        let split_key_data = self.get_item(split_index as u16);
        let split_key = split_key_data[..split_key_data.len() - 8].to_owned();
        let split_key_value = u64::from_be_bytes(split_key_data[split_key_data.len() - 8..].try_into().unwrap());
        let old_data_start = self.header.data_start.get() as usize;
        let data_end = self.data.len();
        let new_data_start = data_end - cumulative_size as usize;
        let other_size = new_data_start - old_data_start - split_key_data.len();
        let other_data_start = other.data.len() - other_size;
        let other_data_end = other.data.len();
        let data_to_copy = &self.data[old_data_start..new_data_start];
        other.data[other_data_start..other_data_end].copy_from_slice(data_to_copy);
        // Update slots
        let slots = self.get_slots_mut();
        let mut other_data_start = other.data.len() as u32;
        for slot_index in split_index + 1..slots.len() {
            let length = U32::<NativeEndian>::from(slots[slot_index].length.get());
            let offset = U32::<NativeEndian>::from(other_data_start - length.get());
            let new_slot_index = slot_index - split_index - 1;
            other.data[new_slot_index*size_of::<Slot>()..(new_slot_index+1)*size_of::<Slot>()].copy_from_slice(Slot{ length, offset }.as_bytes());
            other_data_start -= length.get();
        }
        // Update self headers
        self.header.data_start.set(new_data_start as u32);
        self.header.free_space.set(new_data_start as u32 - ((split_index)*8) as u32);
        let new_slot_count = split_index as u16;
        let old_slot_count = self.header.slot_count.get();
        self.header.slot_count.set(new_slot_count);
        // Update other headers
        other.header.data_start.set(other_data_start as u32);
        let other_slot_count = old_slot_count - (split_index as u16 + 1);
        other.header.free_space.set(other_data_end as u32 - other_data_start - other_slot_count as u32*8);
        other.header.slot_count.set(other_slot_count);
        other.header.first_next_pointer.set(split_key_value);
        split_key.into_boxed_slice()
    }

    fn leaf_split_into(&mut self, other: &mut Self, other_pid_offset: u64) -> Box<[u8]> {
        let mut split_index = 0;
        let mut cumulative_size = 0;
        let slots = self.get_slots();
        // We want that both pages have at least (PAGE_SIZE - HEADER_SIZE) / 2 space after the split
        while ((cumulative_size + slots[split_index].length.get()) as usize + 8 * (split_index + 1))  < self.data.len() / 2 {
            let slot = &slots[split_index];
            cumulative_size += slot.length.get();
            split_index += 1;
        }
        let split_key_data = self.get_item(split_index as u16);
        let split_key = split_key_data[..split_key_data.len() - 8].to_owned();
        let old_data_start = self.header.data_start.get() as usize;
        let data_end = self.data.len();
        let new_data_start = data_end - cumulative_size as usize - split_key_data.len();
        let new_other_size = new_data_start - old_data_start;
        let other_data_start = other.data.len() - new_other_size;
        let other_data_end = other.data.len();
        let data_to_copy = &self.data[old_data_start..new_data_start];
        other.data[other_data_start..other_data_end].copy_from_slice(data_to_copy);
        // Update slots
        let slots = self.get_slots_mut();
        let mut other_data_start = other.data.len() as u32;
        for slot_index in split_index+1..slots.len() {
            let length = U32::<NativeEndian>::from(slots[slot_index].length.get());
            let offset = U32::<NativeEndian>::from(other_data_start - length.get());
            let new_slot_index = slot_index - split_index - 1;
            other.data[new_slot_index*size_of::<Slot>()..(new_slot_index+1)*size_of::<Slot>()].copy_from_slice(Slot{ length, offset }.as_bytes());
            other_data_start -= length.get();
        }
        // Update self headers
        self.header.data_start.set(new_data_start as u32);
        let new_slot_count = split_index as u16 + 1;
        self.header.free_space.set(new_data_start as u32 - (new_slot_count*8) as u32);
        let old_slot_count = self.header.slot_count.get();
        self.header.slot_count.set(new_slot_count);
        // Update other headers
        other.header.data_start.set(other_data_start as u32);
        let other_slot_count = old_slot_count - (split_index as u16 + 1);
        other.header.free_space.set(other_data_start - (other_slot_count*8) as u32);
        other.header.slot_count.set(other_slot_count);
        other.header.first_next_pointer.set(self.header.first_next_pointer.get());
        other.header.flags = self.header.flags;
        self.header.first_next_pointer.set(other_pid_offset);
        split_key.into_boxed_slice()
    }
}

pub struct BTreeScan<B: BufferManager> {
    segment: BTreeSegment<B>,
    slot_id: u16,
    page_guard: Option<BMArcReadGuard<B>>,
}

impl<B: BufferManager> Iterator for BTreeScan<B> {
    type Item = Result<(Box<[Option<TupleValue>]>, RelationTID), B::BError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(page_read) = &self.page_guard {
            let leaf = BTreePage::parse(page_read.as_ref()).unwrap();
            let item = leaf.get_item(self.slot_id);
            let tuple = Tuple::parse_binary_all(&self.segment.key_attributes, item[..item.len() - 8].as_ref());
            let tid = RelationTID::from(u64::from_be_bytes(item[item.len() - 8..].try_into().unwrap()));
            if self.slot_id + 1 < leaf.header.slot_count.get() {
                self.slot_id += 1;
            } else if leaf.header.first_next_pointer.get() == u64::MAX {
                self.page_guard = None;
            } else {
                self.page_guard = Some(self.segment.bm.fix_page(PageId::new(self.segment.segment_id, leaf.header.first_next_pointer.get())).unwrap().read_owning());
                self.slot_id = 0;
            }
            Some(Ok((tuple.values.into_boxed_slice(), tid)))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use rand::{seq::SliceRandom, SeedableRng};

    use crate::{storage::page::PAGE_SIZE, types::{TupleValueType, TupleValue, RelationTID}};
    use super::super::index::*;


    #[test]
    fn test_single_value() {
        let buffer_manager = crate::storage::buffer_manager::mock::MockBufferManager::new(PAGE_SIZE);

        let segment = super::BTreeSegment::new(buffer_manager, 0, vec![TupleValueType::Int]);
        segment.init();
        segment.insert(&[Some(1.into())], 1.into()).unwrap();
        let mut scan = segment.lower_bound(&[Some(1.into())]).unwrap();
        let (k, v) = scan.next().unwrap().unwrap();
        assert_eq!(k, vec![Some(1.into())].into_boxed_slice());
        assert_eq!(v, 1.into());
        assert!(scan.next().is_none());
    }

    #[test]
    fn test_two_values() {
        let buffer_manager = crate::storage::buffer_manager::mock::MockBufferManager::new(PAGE_SIZE);

        let segment = super::BTreeSegment::new(buffer_manager, 0, vec![TupleValueType::Int]);
        segment.init();
        segment.insert(&[Some(1.into())], 11.into()).unwrap();
        segment.insert(&[Some(2.into())], 22.into()).unwrap();
        let mut scan = segment.lower_bound(&[Some(1.into())]).unwrap();
        let (k, v) = scan.next().unwrap().unwrap();
        assert_eq!(k, vec![Some(1.into())].into_boxed_slice());
        assert_eq!(v, 11.into());
        let (k, v) = scan.next().unwrap().unwrap();
        assert_eq!(k, vec![Some(2.into())].into_boxed_slice());
        assert_eq!(v, 22.into());
        assert!(scan.next().is_none());
    }

    #[test]
    fn test_two_values_non_ordered() {
        let buffer_manager = crate::storage::buffer_manager::mock::MockBufferManager::new(PAGE_SIZE);

        let segment = super::BTreeSegment::new(buffer_manager, 0, vec![TupleValueType::Int]);
        segment.init();
        segment.insert(&[Some(2.into())], 22.into()).unwrap();
        segment.insert(&[Some(1.into())], 11.into()).unwrap();
        let mut scan = segment.lower_bound(&[Some(1.into())]).unwrap();
        let (k, v) = scan.next().unwrap().unwrap();
        assert_eq!(k, vec![Some(1.into())].into_boxed_slice());
        assert_eq!(v, 11.into());
        let (k, v) = scan.next().unwrap().unwrap();
        assert_eq!(k, vec![Some(2.into())].into_boxed_slice());
        assert_eq!(v, 22.into());
        assert!(scan.next().is_none());
    }

    #[test]
    fn test_three_values_middle() {
        let buffer_manager = crate::storage::buffer_manager::mock::MockBufferManager::new(PAGE_SIZE);

        let segment = super::BTreeSegment::new(buffer_manager, 0, vec![TupleValueType::Int]);
        segment.init();
        segment.insert(&[Some(3.into())], 33.into()).unwrap();
        segment.insert(&[Some(1.into())], 11.into()).unwrap();
        segment.insert(&[Some(2.into())], 22.into()).unwrap();
        let mut scan = segment.lower_bound(&[Some(1.into())]).unwrap();
        let (k, v) = scan.next().unwrap().unwrap();
        assert_eq!(k, vec![Some(1.into())].into_boxed_slice());
        assert_eq!(v, 11.into());
        let (k, v) = scan.next().unwrap().unwrap();
        assert_eq!(k, vec![Some(2.into())].into_boxed_slice());
        assert_eq!(v, 22.into());
        let (k, v) = scan.next().unwrap().unwrap();
        assert_eq!(k, vec![Some(3.into())].into_boxed_slice());
        assert_eq!(v, 33.into());
        assert!(scan.next().is_none());
    }

    #[test]
    fn test_three_values_lower_bound_middle() {
        let buffer_manager = crate::storage::buffer_manager::mock::MockBufferManager::new(PAGE_SIZE);

        let segment = super::BTreeSegment::new(buffer_manager, 0, vec![TupleValueType::Int]);
        segment.init();
        segment.insert(&[Some(3.into())], 33.into()).unwrap();
        segment.insert(&[Some(1.into())], 11.into()).unwrap();
        segment.insert(&[Some(2.into())], 22.into()).unwrap();
        let mut scan = segment.lower_bound(&[Some(1.into())]).unwrap();
        let (k, v) = scan.next().unwrap().unwrap();
        assert_eq!(k, vec![Some(1.into())].into_boxed_slice());
        assert_eq!(v, 11.into());
        let (k, v) = scan.next().unwrap().unwrap();
        assert_eq!(k, vec![Some(2.into())].into_boxed_slice());
        assert_eq!(v, 22.into());
        let (k, v) = scan.next().unwrap().unwrap();
        assert_eq!(k, vec![Some(3.into())].into_boxed_slice());
        assert_eq!(v, 33.into());
        assert!(scan.next().is_none());
    }


    #[test]
    fn test_three_values_lower_bound_end() {
        let buffer_manager = crate::storage::buffer_manager::mock::MockBufferManager::new(PAGE_SIZE);

        let segment = super::BTreeSegment::new(buffer_manager, 0, vec![TupleValueType::Int]);
        segment.init();
        segment.insert(&[Some(3.into())], 33.into()).unwrap();
        segment.insert(&[Some(1.into())], 11.into()).unwrap();
        segment.insert(&[Some(2.into())], 22.into()).unwrap();
        let mut scan = segment.lower_bound(&[Some(4.into())]).unwrap();
        assert!(scan.next().is_none());
    }

    #[test]
    fn test_three_values_lower_bound_non_exact_match() {
        let buffer_manager = crate::storage::buffer_manager::mock::MockBufferManager::new(PAGE_SIZE);

        let segment = super::BTreeSegment::new(buffer_manager, 0, vec![TupleValueType::Int]);
        segment.init();
        segment.insert(&[Some(4.into())], 33.into()).unwrap();
        segment.insert(&[Some(1.into())], 11.into()).unwrap();
        segment.insert(&[Some(3.into())], 22.into()).unwrap();
        let mut scan = segment.lower_bound(&[Some(2.into())]).unwrap();
        let (k, v) = scan.next().unwrap().unwrap();
        assert_eq!(k, vec![Some(3.into())].into_boxed_slice());
        assert_eq!(v, 22.into());
        let (k, v) = scan.next().unwrap().unwrap();
        assert_eq!(k, vec![Some(4.into())].into_boxed_slice());
        assert_eq!(v, 33.into());
        assert!(scan.next().is_none());
    }

    fn test_with_splits(mut key_values: Vec<(Option<TupleValue>, RelationTID)>) {
        let buffer_manager = crate::storage::buffer_manager::mock::MockBufferManager::new(PAGE_SIZE);

        let segment = super::BTreeSegment::new(buffer_manager.clone(), 0, vec![TupleValueType::Int]);
        segment.init();

        for (k, v) in key_values.iter() {
            segment.insert(&[k.clone()], v.clone()).unwrap();
        }
        key_values.sort_by(|(k1, _), (k2, _)| k1.partial_cmp(k2).unwrap());
        let mut scan = segment.lower_bound(&[Some(0.into())]).unwrap();
        for (k, v) in key_values.iter() {
            let (k2, v2) = scan.next().unwrap().unwrap();
            assert_eq!(k, &k2[0]);
            assert_eq!(v, &v2);
        }
        assert!(scan.next().is_none());
    }

    #[test]
    fn test_with_splits_random() {
        let num_values = PAGE_SIZE as i32;
        let mut key_values: Vec<(Option<TupleValue>, RelationTID)> = Vec::new();
        for i in 0i32..num_values {
            key_values.push((Some(TupleValue::Int(i)), (i as u64).into()));
        }
        test_with_splits(key_values);
    }

    #[test]
    fn test_with_splits_sequential() {
        let num_values = PAGE_SIZE as i32;
        let mut key_values: Vec<(Option<TupleValue>, RelationTID)> = Vec::new();
        for i in 0i32..num_values {
            key_values.push((Some(TupleValue::Int(i)), (i as u64).into()));
        }
        test_with_splits(key_values);
    }

    #[test]
    fn test_with_splits_reverse_sequential() {
        let num_values = PAGE_SIZE as i32;
        let mut key_values: Vec<(Option<TupleValue>, RelationTID)> = Vec::new();
        for i in (0i32..num_values).rev() {
            key_values.push((Some(TupleValue::Int(i)), (i as u64).into()));
        }
        test_with_splits(key_values);
    }

    #[test]
    fn test_with_splits_lower_bound() {
        let buffer_manager = crate::storage::buffer_manager::mock::MockBufferManager::new(PAGE_SIZE);

        let segment = super::BTreeSegment::new(buffer_manager, 0, vec![TupleValueType::Int]);
        segment.init();

        let num_values = PAGE_SIZE as i32;
        let mut key_values: Vec<(Option<TupleValue>, RelationTID)> = Vec::new();
        for i in 0i32..num_values {
            key_values.push((Some(TupleValue::Int(i)), (i as u64).into()));
        }
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        key_values.shuffle(&mut rng);
        for (k, v) in key_values.iter() {
            segment.insert(&[k.clone()], v.clone()).unwrap();
        }
        for i in 0i32..num_values {
            let mut scan = segment.clone().lower_bound(&[Some(i.into())]).unwrap();
            let (k, v) = scan.next().unwrap().unwrap();
            assert_eq!(k, vec![Some(i.into())].into_boxed_slice());
            assert_eq!(v, (i as u64).into());
        }
    }

    #[test]
    fn test_with_splits_lower_bound_non_exact() {
        let buffer_manager = crate::storage::buffer_manager::mock::MockBufferManager::new(PAGE_SIZE);

        let segment = super::BTreeSegment::new(buffer_manager, 0, vec![TupleValueType::Int]);
        segment.init();

        let num_values = PAGE_SIZE as i32;
        let mut key_values: Vec<(Option<TupleValue>, RelationTID)> = Vec::new();
        for i in (0i32..num_values).step_by(10) {
            key_values.push((Some(TupleValue::Int(i)), (i as u64).into()));
        }
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        key_values.shuffle(&mut rng);
        for (k, v) in key_values.iter() {
            segment.insert(&[k.clone()], v.clone()).unwrap();
        }
        let mut scan = segment.clone().lower_bound(&[Some(5.into())]).unwrap();
        for i in (10i32..num_values).step_by(10) {
            let (k, v) = scan.next().unwrap().unwrap();
            assert_eq!(k, vec![Some(i.into())].into_boxed_slice());
            assert_eq!(v, (i as u64).into());
        }
        assert!(scan.next().is_none());
    }

}