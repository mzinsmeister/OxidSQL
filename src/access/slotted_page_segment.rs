use std::{collections::BTreeMap, ops::{Deref, DerefMut}, marker::PhantomData, time::Duration, borrow::BorrowMut};

use crate::{storage::{buffer_manager::{BufferManager, BMArc}, page::{Page, PAGE_SIZE, PageId, SegmentId}}, types::{RelationTID, TupleValue}};

use super::{free_space_inventory::FreeSpaceSegment, tuple::{Tuple, TupleParser, MutatingTupleParser}};

// We implement a safe variant for now. Transmuting stuff like the header will likely lead to more
// readable code but would need unsafe

// 16 bit slot count, 16 bit first free slot, 32 bit data start, 32 bit free space
const HEADER_SIZE: usize = 2 * 4 + 2 * 2; 
const SLOT_SIZE: usize = 8;

const REDIRECT_TARGET_LOCK_TIMEOUT_MS: Duration = Duration::from_secs(1);

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
enum Slot {
    Slot { offset: u32, length: u32 },
    RedirectTarget { offset: u32, length: u32 },
    Redirect { tid: RelationTID },
    Free
}

impl Slot {

    fn new(offset: u32, length: u32) -> Slot {
        Slot::Slot { offset, length }
    }
    
    #[allow(dead_code)]
    fn new_reference(tid: RelationTID) -> Slot {
        Slot::Redirect { tid }
    }

    fn new_redirect_target(offset: u32, length: u32) -> Slot {
        Slot::RedirectTarget { offset, length }
    }

    #[allow(dead_code)]
    fn get_data_offset_length(&self) -> (u32, u32) {
        match self {
            Slot::Slot { offset, length } => (*offset, *length),
            Slot::RedirectTarget { offset, length } => (*offset + 8, *length - 8),
            Slot::Redirect { .. } => panic!("Tried to get data offset and length of redirect slot"),
            Slot::Free => panic!("Tried to get data offset of free slot")
        }
    }
}

impl From<&[u8]> for Slot {
    
    fn from(source: &[u8]) -> Self {
        if source[0] & (1 << 6) != 0 && source[0] & (1 << 7) != 0 {
            let offset = u32::from_be_bytes(source[0..4].try_into().unwrap()) & !(1 << 31 | 1 << 30);
            return Slot::RedirectTarget {
                offset,
                length: u32::from_be_bytes(source[4..8].try_into().unwrap())
            };
        }
        if source[0] & (1 << 7) != 0 {
            let tid_num = u64::from_be_bytes(source.try_into().unwrap()) & !(1 << 63);
            return Slot::Redirect { tid: RelationTID::from(tid_num) };
        }
        if source[0] & (1 << 6) != 0 {
            return Slot::Free;
        }
        Slot::Slot {
            offset: u32::from_be_bytes(source[0..4].try_into().unwrap()),
            length: u32::from_be_bytes(source[4..8].try_into().unwrap()),
        }
    }

}

impl From<&Slot> for [u8; 8] {
    fn from(source: &Slot) -> [u8; 8] {
        match source {
            Slot::Slot { offset, length } => {
                let mut bytes = [0u8; 8];
                bytes[0..4].copy_from_slice(&offset.to_be_bytes());
                bytes[4..8].copy_from_slice(&length.to_be_bytes());
                bytes
            },
            Slot::Redirect { tid } => {
                let mut bytes = u64::from(tid).to_be_bytes();
                bytes[0] |= 1 << 7;
                bytes
            },
            Slot::RedirectTarget { offset, length } => {
                let mut bytes = [0u8; 8];
                bytes[0..4].copy_from_slice(&offset.to_be_bytes());
                bytes[4..8].copy_from_slice(&length.to_be_bytes());
                bytes[0] |= 1 << 6;
                bytes[0] |= 1 << 7;
                bytes
            },
            Slot::Free => {
                let mut bytes = [0u8; 8];
                bytes[0] |= 1 << 6;
                bytes
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct SlottedPageSegment<B: BufferManager> {
    bm: B,
    segment_id: SegmentId,
    free_space_segment: FreeSpaceSegment<B>
}

impl<B: BufferManager> SlottedPageSegment<B> {
    fn max_useable_space(&self) -> usize {
        PAGE_SIZE - HEADER_SIZE - SLOT_SIZE
    }

    pub fn new(bm: B, segment_id: SegmentId, free_space_segment_id: SegmentId) -> SlottedPageSegment<B> {
        SlottedPageSegment {
            bm: bm.clone(),
            segment_id,
            free_space_segment: FreeSpaceSegment::new(free_space_segment_id, PAGE_SIZE - HEADER_SIZE, bm)
        }
    }

    #[allow(dead_code)]
    pub fn get_max_record_size() -> usize {
        PAGE_SIZE - HEADER_SIZE - SLOT_SIZE
    }

    /// Applies the given function to the tuple at the given tid. The function is passed a slice of the tuple data
    /// and can then either use that to set some data in captured variables or return some transformed version of the tuple
    /// or just a copy of the data. Its result cannot reference the data in the page though.
    /// The operation should be idempotent as it's not guaranteed that in the future there won't be some optimistic optimization.
    // This should be the most flexible way to implement this
    pub fn get_record<T, F: Fn(&[u8]) -> T>(&self, tid: RelationTID, operation: F) -> Result<Option<T>, B::BError> {
        loop {
            let page = self.bm.fix_page(PageId::new(self.segment_id, tid.page_id))?;
            let page_read = page.read();
            let slotted_page = SlottedPage::new(page_read);
            let slot = slotted_page.get_slot(tid.slot_id);
            match slot {
                Slot::Redirect { tid: redirect_tid } => {
                    assert!(redirect_tid.page_id != tid.page_id, "Redirect to same page (would lead to deadlock)");
                    let page = self.bm.fix_page(PageId::new(self.segment_id, redirect_tid.page_id))?;
                    // Deadlock avoidance if we have a redirect from A to B and one from B to A, otherwise just restart
                    let page_read = page.try_read_for(REDIRECT_TARGET_LOCK_TIMEOUT_MS);
                    if let Some(page_read) = page_read {
                        let slotted_page = SlottedPage::new(page_read);
                        let slot = slotted_page.get_slot(redirect_tid.slot_id);
                        if let Slot::RedirectTarget { offset, length } = slot {
                            return Ok(Some(operation(slotted_page.get_record(offset + 8, length - 8))));
                        }
                    }
                },
                Slot::RedirectTarget { offset, length } => {
                    return Ok(Some(operation(slotted_page.get_record(offset + 8, length - 8))))
                }
                Slot::Free => return Ok(None),
                Slot::Slot { offset, length } => {
                    return Ok(Some(operation(slotted_page.get_record(offset, length))))
                }
            }
        }
    }

    // Operation takes a mutable reference to the page, the slot_id and the offset and length of the new record
    fn allocate_and_do<F: FnMut(&mut SlottedPage<&mut Page>, RelationTID, u32, u32) -> Result<(), B::BError>,
                        C: Fn(u64) -> bool>(&self, size: usize, mut operation: F, page_id_check: C, lock_timeout: Option<Duration>) -> Result<RelationTID, B::BError> {
        loop {
            if size > PAGE_SIZE - HEADER_SIZE {
                panic!("Data too large for page"); // Caller is responsible for ensuring this
            }
            loop {
                let page_id = self.free_space_segment.find_page(size as u32 + 8)?;
                if !page_id_check(page_id) {
                    continue;
                }
                let page = self.bm.fix_page(PageId::new(self.segment_id, page_id))?;
                // Here would be the only place where they could still potentially deadlock
                // but it's hardest to tackle here
                // This technically avoids deadlocks as long as we assume other transactions do any inserts/updates/deletes
                // in the meantime which is often a reasonable assumption also with the way the
                // free space inventory currently works, it's almost impossible to deadlock at this exact position
                // still, better safe than sorry
                let mut page_write = if let Some(duration) = lock_timeout {
                    if let Some(w) = page.try_write_for(duration) {
                        w
                    } else {
                        continue;
                    }
                } else {
                    page.write()
                };
                let mut slotted_page: SlottedPage<&mut Page> = SlottedPage::new(&mut page_write);
                if slotted_page.get_slot_count() == 0 {
                    slotted_page.initialize();
                }
                let alloc_result = slotted_page.allocate(size);
                if let Some(slot_id) = alloc_result {
                    if let Slot::Slot { offset, length } = slotted_page.get_slot(slot_id) {
                        let new_tid = RelationTID{ page_id: page_id, slot_id: slot_id };
                        operation(&mut slotted_page, new_tid, offset, length)?;
                        self.free_space_segment.update_page_size(page_id, slotted_page.get_free_space())?;
                        drop(slotted_page);
                        return Ok(new_tid);
                    } else {
                        panic!("allocation result should be a normal slot")
                    }
                } else {
                    self.free_space_segment.update_page_size(page_id, slotted_page.get_free_space())?;
                }
            }
        }
    }

    pub fn insert_record(&self, data: &[u8]) -> Result<RelationTID, B::BError> {
        self.allocate_and_do(data.len(), |page, _, offset, length| {
            page.get_record_mut(offset, length).copy_from_slice(data);
            Ok(())
        }, |_| true, None)
    }

    #[allow(dead_code)]
    pub fn allocate(&self, size: u16) -> Result<RelationTID, B::BError> {
        self.allocate_and_do(size as usize, |_, _, _ , _| {Ok(())}, |_| true, None)
    }

    fn resize_and_do_redirect<F: Fn(&mut [u8]), P: DerefMut<Target=Page>>(&self, tid: RelationTID, redirect_tid: RelationTID, slotted_root_page: &mut SlottedPage<P>, 
                                                    size: usize, copy_previous: bool, operation: F) -> Result<bool, B::BError> {
        let redirect_target_page = self.bm.fix_page(PageId::new(self.segment_id, redirect_tid.page_id))?;
        let redirect_target_page_write = if let Some(write) = redirect_target_page.try_write_for(REDIRECT_TARGET_LOCK_TIMEOUT_MS) {
            write
        } else {
            return Ok(false);
        };
        let mut slotted_redirect_target_page = SlottedPage::new(redirect_target_page_write);
        let orig_slot = slotted_redirect_target_page.get_slot(redirect_tid.slot_id);
        if let Slot::RedirectTarget { offset: _, length } = orig_slot {
            let root_relocate_result = slotted_root_page.relocate(tid.slot_id, size);
            if root_relocate_result {
                if let Slot::Slot { offset, length } = slotted_root_page.get_slot(tid.slot_id) {
                    operation(slotted_root_page.get_record_mut(offset, length));
                    self.free_space_segment.update_page_size(tid.page_id, slotted_root_page.get_free_space())?;
                    drop(slotted_root_page);
                    slotted_redirect_target_page.erase(redirect_tid.slot_id);
                    self.free_space_segment.update_page_size(redirect_tid.page_id, slotted_redirect_target_page.get_free_space())?;
                    return Ok(true);
                } else {
                    panic!("Successful relocate should have kept it a slot");
                }
            }
            let orig_relocate_result = slotted_redirect_target_page.relocate(redirect_tid.slot_id, size + 8);
            if orig_relocate_result {
                if let Slot::RedirectTarget { offset, length } = slotted_redirect_target_page.get_slot(redirect_tid.slot_id) {
                    drop(slotted_root_page);
                    operation(slotted_redirect_target_page.get_record_mut(offset + 8, length - 8));
                    self.free_space_segment.update_page_size(redirect_tid.page_id, slotted_redirect_target_page.get_free_space())?;
                    return Ok(true);
                } else {
                    panic!("Successful relocate should have kept it a redirect target");
                }
            }
            self.allocate_and_do(size + 8, |slotted_alloc_page: &mut SlottedPage<&mut Page>, new_tid: RelationTID, new_offset, new_length| -> Result<(), B::BError> {
                slotted_root_page.write_slot(tid.slot_id, &Slot::Redirect { tid: RelationTID { page_id: new_tid.page_id, slot_id: new_tid.slot_id } });
                slotted_alloc_page.write_slot(new_tid.slot_id, &Slot::RedirectTarget{ offset: new_offset, length: new_length });
                slotted_alloc_page.get_record_mut(new_offset, 8).copy_from_slice(u64::from(&tid).to_be_bytes().as_ref());
                if copy_previous {
                    let copy_size = length.min(size as u32);
                    slotted_alloc_page.get_record_mut(new_offset + 8, copy_size)
                            .copy_from_slice(slotted_redirect_target_page.get_record(new_offset + 8, copy_size));
                }
                slotted_redirect_target_page.erase(redirect_tid.slot_id);
                self.free_space_segment.update_page_size(redirect_tid.page_id, slotted_redirect_target_page.get_free_space())?;
                self.free_space_segment.update_page_size(new_tid.page_id, slotted_alloc_page.get_free_space())?;
                operation(slotted_alloc_page.get_record_mut(new_offset + 8, new_length - 8));
                Ok(())
            }, |page_id| page_id != tid.page_id && page_id != redirect_tid.page_id, Some(REDIRECT_TARGET_LOCK_TIMEOUT_MS))?;
            Ok(true)
        } else {
            panic!("Redirect slot should point to a redirect target");
        }
    }

    fn resize_and_do<F: Fn(&mut [u8]) + Clone>(&self, tid: RelationTID, size: usize, copy_previous: bool, operation: F) -> Result<(), B::BError> {
        if size > self.max_useable_space() {
            panic!("Data too large for page"); // Caller is responsible for 
        }
        // We have at most three pages at play here:
        // Root page: the page we were given by the user. We assume we are never given a relocate target
        // Orig(inal) page: the page that contained the actual data before (might be the same as root)
        // Alloc page: the page that might contain the data afterwards
        //             (will never be root or orig because we try to relocate there first)
        loop {
            let root_page_id = tid.page_id;
            let root_page = self.bm.fix_page(PageId::new(self.segment_id, root_page_id))?;
            let mut slotted_root_page = SlottedPage::new(root_page.write());
            let root_slot = slotted_root_page.get_slot(tid.slot_id);
            match root_slot {
                Slot::Slot { offset: old_offset, length: old_length } => {
                    let relocate_result = slotted_root_page.relocate(tid.slot_id, size);
                    if relocate_result {
                        if let Slot::Slot { offset, length } = slotted_root_page.get_slot(tid.slot_id) {
                            operation(slotted_root_page.get_record_mut(offset, length));
                            self.free_space_segment.update_page_size(root_page_id, slotted_root_page.get_free_space())?;
                            return Ok(());
                        } else {
                            panic!("Successful relocate should have kept it a slot");
                        }
                    }
                    // We now need a redirect
                    self.allocate_and_do(size + 8, |slotted_alloc_page: &mut SlottedPage<&mut Page>, new_tid: RelationTID, offset , length| {
                        slotted_alloc_page.write_slot(new_tid.slot_id, &Slot::RedirectTarget{ offset, length });
                        slotted_alloc_page.get_record_mut(offset, 8).copy_from_slice(u64::from(&tid).to_be_bytes().as_ref());
                        if copy_previous {
                            let copy_size = old_length.min(size as u32);
                            slotted_alloc_page.get_record_mut(offset + 8, copy_size)
                                    .copy_from_slice(slotted_root_page.get_record(old_offset, copy_size));
                        }
                        slotted_root_page.erase_for_redirect(tid.slot_id, new_tid);
                        self.free_space_segment.update_page_size(tid.page_id, slotted_root_page.get_free_space())?;
                        self.free_space_segment.update_page_size(new_tid.page_id, slotted_alloc_page.get_free_space())?;
                        operation(slotted_alloc_page.get_record_mut(offset + 8, length - 8));
                        Ok(())
                    }, |page_id| page_id != tid.page_id, Some(REDIRECT_TARGET_LOCK_TIMEOUT_MS))?;
                    return Ok(());
                },
                Slot::Redirect { tid: redirect_tid } => {
                    if self.resize_and_do_redirect(tid, redirect_tid, &mut slotted_root_page, size, copy_previous, operation.clone())? {
                        return Ok(());
                    }
                },
                // TODO: Implement this one. We will probably want this for updates while sequentially scanning
                Slot::RedirectTarget { offset: _, length: _ } => panic!("tried to resize a redirect target"),
                Slot::Free => panic!("tried to resize a free slot"),
            }
        }
    }

    pub fn write_record(&self, tid: RelationTID, data: &[u8]) -> Result<(), B::BError> {
        self.resize_and_do(tid, data.len(), false, |record| record.copy_from_slice(data))
    }

    #[allow(dead_code)]
    pub fn resize_record(&self, tid: RelationTID, size: usize) -> Result<(), B::BError> {
        self.resize_and_do(tid, size, true, |_| {})
    }

    pub fn get_size(&self) -> u64 {
        self.bm.segment_size(self.segment_id) as u64
    }

    pub fn erase_record(&self, tid: RelationTID) -> Result<(), B::BError> {
        loop {
            let page = self.bm.fix_page(PageId::new(self.segment_id, tid.page_id))?;
            let mut slotted_page = SlottedPage::new(page.write());
            let slot = slotted_page.get_slot(tid.slot_id);
            slotted_page.erase(tid.slot_id);
    
            if let Slot::Redirect { tid: redirect_tid } = slot {
                let redirect_page = self.bm.fix_page(PageId::new(self.segment_id, redirect_tid.page_id))?;
                let page_write = redirect_page.try_write_for(REDIRECT_TARGET_LOCK_TIMEOUT_MS);
                if let Some(page_write) = page_write {
                    let mut slotted_redirect_page = SlottedPage::new(page_write);
                    slotted_redirect_page.erase(redirect_tid.slot_id);
                    self.free_space_segment.update_page_size(redirect_tid.page_id, slotted_redirect_page.get_free_space())?;
                } else {
                    continue;
                }
            }
            self.free_space_segment.update_page_size(tid.page_id, slotted_page.get_free_space())?;
            return Ok(());
        }
    }

    // A scan operator allows us to pass a transformation that returns an Option so that the scan
    // only returns the records that are not None. This way filters and tuple parsing can for example
    // directly be pushed into the scan.
    pub fn scan<'a, T, F: ScanFunction<T>>(self, transformation: F) -> SlottedPageScan<B, T, F> {
        SlottedPageScan {
            segment: self,
            page: None,
            slot_id: 0,
            page_id: 0,
            transformation,
            _phantom_data: PhantomData
        }
    }
}

pub trait ScanFunction<T> {
    fn apply(&mut self, val: &[u8]) -> Option<T>;
}

impl<T, F: FnMut(&[u8]) -> Option<T>> ScanFunction<T> for F {
    #[inline(always)]
    fn apply(&mut self, val: &[u8]) -> Option<T> {
        self(val)
    }
}

impl ScanFunction<Tuple> for TupleParser {
    #[inline(always)]
    fn apply(&mut self, val: &[u8]) -> Option<Tuple> {
        Some(self.parse(val))
    }
}

impl<'a, V: BorrowMut<Option<TupleValue>> + 'a> ScanFunction<()> for MutatingTupleParser<'a, V> {
    #[inline(always)]
    fn apply(&mut self, val: &[u8]) -> Option<()> {
        self.parse(val);
        Some(())
    }
}

pub struct SlottedPageScan<B: BufferManager, T, F: ScanFunction<T>> {
    segment: SlottedPageSegment<B>,
    page: Option<BMArc<B>>,
    slot_id: u16,
    page_id: u64,
    transformation: F,
    _phantom_data: PhantomData<T>
}

impl<'a, B: BufferManager, T, F: ScanFunction<T>> Iterator for SlottedPageScan<B, T, F> {
    type Item = Result<(RelationTID, T), B::BError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.page.is_none() {
            if self.page_id < self.segment.get_size() {
                self.page = Some(self.segment.bm.fix_page(PageId::new(self.segment.segment_id, self.page_id)).unwrap());
            } else {
                return None;
            }
        }
        loop {
            let page = self.page.as_ref().unwrap();
            let slotted_page = SlottedPage::new(page.read());
            loop {
                if self.slot_id >= slotted_page.get_slot_count() {
                    if self.page_id + 1 >= self.segment.get_size() {
                        return None;
                    }
                    self.page_id += 1;
                    drop(slotted_page);
                    let new_page = self.segment.bm.fix_page(PageId::new(self.segment.segment_id, self.page_id)).unwrap();
                    self.page = Some(new_page);
                    self.slot_id = 0;
                    break;
                }
                let slot = slotted_page.get_slot(self.slot_id);
                match slot {
                    Slot::Free | Slot::Redirect { tid: _ } => { /* Do nothing and continue scanning */ },
                    Slot::RedirectTarget { offset, length } => {
                        let tid = RelationTID::from(u64::from_be_bytes(slotted_page.get_record(offset, 8).try_into().unwrap()));
                        let transform_result = self.transformation.apply(slotted_page.get_record(offset + 8, length - 8));
                        if let Some(transform_result) = transform_result {
                            self.slot_id += 1;
                            return Some(Ok((tid, transform_result)));
                        }
                    },
                    Slot::Slot { offset, length } => {
                        let tid = RelationTID::new(self.page_id, self.slot_id);
                        let transform_result = self.transformation.apply(slotted_page.get_record(offset, length));
                        if let Some(transform_result) = transform_result {
                            self.slot_id += 1;
                            return Some(Ok((tid, transform_result)));
                        }
                    }
                }
                self.slot_id += 1;
            }
        }
    }
}

struct SlottedPage<A: Deref<Target = Page>> {
    page: A
}

fn get_slot_from_page(page: &Page, slot_id: u16) -> Slot {
    let slot_offset = HEADER_SIZE + slot_id as usize * 8;
    (&page[slot_offset..slot_offset + 8]).into()
}

#[allow(dead_code)]
fn get_data_from_page(page: &Page, data_start: usize) -> &[u8] {
    &page[data_start as usize..]
}

fn get_record_from_page(page: &Page, offset: u32, length: u32) -> &[u8] {
    &page[offset as usize .. offset as usize + length as usize]
}

impl<A: Deref<Target = Page>> SlottedPage<A> {

    fn new(page: A) -> SlottedPage<A> {
        SlottedPage { page }
    }

    fn get_slot(&self, slot_id: u16) -> Slot {
        get_slot_from_page(&self.page, slot_id)
    }

    fn get_slot_count(&self) -> u16 {
        self.page.get_u16(0)
    }

    fn get_first_free_slot(&self) -> u16 {
        self.page.get_u16(2)
    }

    fn get_data_start(&self) -> u32 {
        self.page.get_u32(4)
    }

    fn get_free_space(&self) -> u32 {
        self.page.get_u32(8)
    }

    fn get_fragmented_free_space(&self) -> usize {
        let data_start = self.get_data_start() as usize;
        let slot_count = self.get_slot_count() as usize;
        data_start as usize - HEADER_SIZE - 8 * slot_count
    }

    #[allow(dead_code)]
    fn get_data(&self) -> &[u8] {
        get_data_from_page(&self.page, self.get_data_start() as usize)
    }

    #[allow(dead_code)]
    fn get_data_length(&self) -> u32 {
        self.page.len() as u32 - self.get_data_start()
    }

    fn get_record(&self, offset: u32, length: u32) -> &[u8] {
        get_record_from_page(&self.page, offset, length)
    }

    #[allow(dead_code)]
    fn read_record(&self, slot_id: u16) -> Option<&[u8]> {
        match self.get_slot(slot_id) {
            Slot::Slot { offset, length } => Some(self.get_record(offset, length)),
            Slot::Redirect { tid: _ } => None,
            Slot::RedirectTarget { offset, length } => Some(self.get_record(offset + 8, length - 8)),
            Slot::Free => None,
        }
    }
}

impl<A: Deref<Target = Page> + DerefMut<Target = Page>> SlottedPage<A> {
    fn initialize(&mut self) {
        self.set_slot_count(0);
        self.set_first_free_slot(0);
        self.set_data_start(PAGE_SIZE as u32);
        self.set_free_space(PAGE_SIZE as u32 - HEADER_SIZE as u32);
    }

    fn write_slot(&mut self, slot_id: u16, slot: &Slot) {
        let slot_offset = HEADER_SIZE + slot_id as usize * 8;
        let slot_binary: [u8; 8] = slot.into();
        self.page[slot_offset..slot_offset + 8].copy_from_slice(slot_binary.as_ref());
        #[cfg(debug_assertions)]
        {
            let read_binary = &self.page[slot_offset..slot_offset + 8];
            assert_eq!(&Slot::from(read_binary), slot);
        }
    }

    fn set_slot_count(&mut self, slot_count: u16) {
        self.page[0..2].copy_from_slice(&slot_count.to_be_bytes());
    }

    fn set_first_free_slot(&mut self, first_free_slot: u16) {
        self.page[2..4].copy_from_slice(&first_free_slot.to_be_bytes());
    }

    fn set_free_space(&mut self, free_space: u32) {
        self.page[8..12].copy_from_slice(&free_space.to_be_bytes());
    }

    fn set_data_start(&mut self, data_start: u32) {
        self.page[4..8].copy_from_slice(&data_start.to_be_bytes());
    }

    fn get_record_mut(&mut self, offset: u32, length: u32) -> &mut [u8] {
        &mut self.page[offset as usize .. offset as usize + length as usize]
    }

    fn allocate(&mut self, size: usize) -> Option<u16> {
        let mut free_space = self.get_free_space();
        let first_free_slot = self.get_first_free_slot();
        let slot_count = self.get_slot_count();
        if first_free_slot == slot_count {
            free_space -= 8;
        }
        if free_space < size as u32 {
            return None;
        }
        if self.get_fragmented_free_space() < size {
            self.compactify();
        }
        assert!(first_free_slot <= slot_count);
        if first_free_slot == slot_count {
            self.set_first_free_slot(slot_count + 1);
            self.set_slot_count(slot_count + 1);
        } else {
            let mut next_free_slot = first_free_slot + 1;
            while next_free_slot < slot_count {
                let next_slot = self.get_slot(next_free_slot);
                if next_slot == Slot::Free {
                    break;
                }
                next_free_slot += 1;
            }
            self.set_first_free_slot(next_free_slot);
        }
        let new_data_start = self.get_data_start() - size as u32;
        self.write_slot(first_free_slot, &Slot::new(new_data_start, size as u32));
        free_space -= size as u32;
        self.set_data_start(new_data_start);
        self.set_free_space(free_space);
        assert!(self.get_slot(first_free_slot) == Slot::new(new_data_start, size as u32));
        Some(first_free_slot)
    }

    fn erase_for_redirect(&mut self, slot_id: u16, redirect_tid: RelationTID) {
        let slot = self.get_slot(slot_id);
        match slot {
            Slot::Redirect { tid: _ } => self.write_slot(slot_id, &Slot::Redirect { tid: redirect_tid }),
            Slot::Slot { offset, length } => {
                self.set_free_space(self.get_free_space() + length);
                self.write_slot(slot_id, &Slot::Redirect { tid: redirect_tid });
                if offset == self.get_data_start() {
                    self.set_data_start(self.get_data_start() + length);
                }
            },
            _ => {}
        }
    }

    fn erase(&mut self, slot_id: u16) {
        let slot = self.get_slot(slot_id);
        match slot {
            Slot::Redirect { tid: _ } => self.write_slot(slot_id, &Slot::Free),
            Slot::Slot { offset, length } => {
                self.set_free_space(self.get_free_space() + length);
                self.write_slot(slot_id, &Slot::Free);
                if offset == self.get_data_start() {
                    self.set_data_start(self.get_data_start() + length);
                }
            },
            _ => {}
        }
        if slot_id < self.get_first_free_slot() {
            self.set_first_free_slot(slot_id);
        }
        let mut slot_count = self.get_slot_count();
        if slot_id + 1 == slot_count {
            let mut free_space = self.get_free_space();
            let mut first_free_slot = self.get_first_free_slot();
            for i in (0..slot_id).rev() {
                let slot = self.get_slot(i);
                if slot == Slot::Free {
                    slot_count -= 1;
                    free_space += 8;
                    if i < first_free_slot {
                        first_free_slot = i;
                    }
                } else {
                    break;
                }
            }
            self.set_slot_count(slot_count);
            self.set_first_free_slot(first_free_slot);
            self.set_free_space(free_space);
        }
     }     

    fn relocate(&mut self, slot_id: u16, size: usize) -> bool {
        let slot = self.get_slot(slot_id);
        match slot {
            Slot::Free => return false,
            Slot::Slot { offset, length } | Slot::RedirectTarget { offset, length } => {
                if length as usize > size {
                    self.set_free_space(self.get_free_space() + (length as usize - size) as u32);
                    self.write_slot(slot_id, &Slot::new(offset, size as u32));
                    return true;
                }
                if length < size as u32  {
                    if size as u32 - length > self.get_free_space() {
                        return false;
                    }
                    let buffer = self.get_record(offset, (length).min(size as u32)).to_vec();
                    self.write_slot(slot_id, &Slot::Free);
                    if self.get_fragmented_free_space() < size {
                        self.compactify();
                    }
                    let new_data_start = self.get_data_start() - size as u32;
                    match slot {
                        Slot::Slot { offset: _, length: _ } => 
                            self.write_slot(slot_id, &Slot::new(new_data_start, size as u32)),
                        Slot::RedirectTarget { offset: _, length: _ } => 
                            self.write_slot(slot_id, &Slot::new_redirect_target(new_data_start, size as u32)),
                        _ => unreachable!()
                    }
                    let tuple_mut = self.get_record_mut(new_data_start, (size as u32).min(length));
                    tuple_mut.copy_from_slice(buffer.as_slice());
                    self.set_data_start(new_data_start);
                    self.set_free_space(self.get_free_space() + (size - length as usize) as u32);
                }
            }
            Slot::Redirect{ tid: _ } => {
                if self.get_free_space() < size as u32 {
                    return false;
                }
                if self.get_fragmented_free_space() < size {
                    self.compactify();
                }
                let new_data_start = self.get_data_start() - size as u32;
                let new_slot = Slot::new(new_data_start, size as u32);
                self.write_slot(slot_id, &new_slot);
                return true;
            }
        }
        true
    }

    /// Compactify the page by moving all data to the end of the page and removing any free space in between.
    fn compactify(&mut self) {
        struct SlotIdLength {
            slot_id: u16, length: u32
        }
        let mut offset_slot_map: BTreeMap<usize, SlotIdLength> = BTreeMap::new();
        for slot_id in 0..self.get_slot_count() {
            let slot = self.get_slot(slot_id);
            if let Slot::Slot { offset, length } = slot {
                offset_slot_map.insert(offset as usize, SlotIdLength { slot_id, length });
            }
        }
        let mut new_data_start = self.page.len() as u32;
        for (offset, slot) in offset_slot_map.iter().rev() {
            new_data_start = new_data_start - slot.length;
            self.page.copy_within(*offset..*offset + slot.length as usize, new_data_start as usize);
            let new_slot = Slot::new(new_data_start, slot.length);
            self.write_slot(slot.slot_id, &new_slot);
        }
        self.set_data_start(new_data_start);
    }
}

#[cfg(test)]
mod test {

    use crate::{access::{slotted_page_segment::HEADER_SIZE, SlottedPageSegment}, storage::{buffer_manager::{mock::MockBufferManager, BufferManager, HashTableBufferManager}, clock_replacer::ClockReplacer, disk::DiskManager, page::{Page, PageId, PageState, PAGE_SIZE}}, types::RelationTID, util::align::{alligned_slice, AlignedSlice}};

    use super::{Slot, SlottedPage};

    
    #[test]
    fn allocate_succeeds() {
        let bm = MockBufferManager::new(PAGE_SIZE);
        let testee = SlottedPageSegment::new(bm, 1, 0);
        assert!(testee.allocate(100).is_ok());
    }

    #[test]
    fn allocate_allocates() {
        let bm = MockBufferManager::new(PAGE_SIZE);
        let testee = SlottedPageSegment::new(bm, 1, 0);
        let tid = testee.allocate(100).unwrap();
        let record = testee.get_record(tid, |record| {Vec::from(record)}).unwrap();
        assert_eq!(record.unwrap().len(), 100);
    }

    #[test]
    fn write_writes() {
        let bm = MockBufferManager::new(PAGE_SIZE);
        let testee = SlottedPageSegment::new(bm, 1, 0);
        let mut data = vec![0u8; 500];
        data[0] = 1;
        data[499] = 255;
        data[200] = 100;
        let tid = testee.insert_record(&data).unwrap();
        let record = testee.get_record(tid, |record| {Vec::from(record)}).unwrap().unwrap();
        assert_eq!(record.len(), 500);
        assert_eq!(record[0], 1);
        assert_eq!(record[200], 100);
        assert_eq!(record[499], 255);
    }

    #[test]
    fn write_dirties_page() {
        let bm = MockBufferManager::new(PAGE_SIZE);
        let testee = SlottedPageSegment::new(bm.clone(), 1, 0);
        let data = vec![0u8; 500];
        let tid = testee.insert_record(&data).unwrap();
        let page = bm.fix_page(PageId::new(1, tid.page_id)).unwrap();
        assert!(page.try_read().unwrap().state.is_dirtyish());
    }

    #[test]
    fn write_twice_gives_different_tids() {
        let bm = MockBufferManager::new(PAGE_SIZE);
        let testee = SlottedPageSegment::new(bm, 1, 0);
        let mut data = vec![0u8; 500];
        data[0] = 1;
        data[499] = 255;
        data[200] = 100;
        let tid1 = testee.insert_record(&data).unwrap();
        let mut data = vec![0u8; 200];
        data[0] = 4;
        data[199] = 2;
        data[100] = 3;
        let tid2 = testee.insert_record(&data).unwrap();
        assert_ne!(tid1, tid2);
    }

    #[test]
    fn write_twice_doesnt_overwrite() {
        let bm = MockBufferManager::new(PAGE_SIZE);
        let testee = SlottedPageSegment::new(bm, 1, 0);
        let mut data = vec![0u8; 500];
        data[0] = 1;
        data[499] = 255;
        data[200] = 100;
        let tid1 = testee.insert_record(&data).unwrap();
        let mut data = vec![0u8; 200];
        data[0] = 4;
        data[199] = 2;
        data[100] = 3;
        testee.insert_record(&data).unwrap();
        let record = testee.get_record(tid1, |record| {Vec::from(record)}).unwrap().unwrap();
        assert_eq!(record.len(), 500);
        assert_eq!(record[0], 1);
        assert_eq!(record[200], 100);
        assert_eq!(record[499], 255);
    }

    #[test]
    fn write_twice_writes_second() {
        let bm = MockBufferManager::new(PAGE_SIZE);
        let testee = SlottedPageSegment::new(bm, 1, 0);
        let mut data = vec![0u8; 500];
        data[0] = 1;
        data[499] = 255;
        data[200] = 100;
        testee.insert_record(&data).unwrap();
        let mut data = vec![0u8; 200];
        data[0] = 4;
        data[199] = 2;
        data[100] = 3;
        let tid2 = testee.insert_record(&data).unwrap();
        let record = testee.get_record(tid2, |record| {Vec::from(record)}).unwrap().unwrap();
        assert_eq!(record.len(), 200);
        assert_eq!(record[0], 4);
        assert_eq!(record[199], 2);
        assert_eq!(record[100], 3);
    }
    
    #[test]
    fn allocate_max() {
        let bm = MockBufferManager::new(PAGE_SIZE);
        let testee = SlottedPageSegment::new(bm, 1, 0);
        let size = PAGE_SIZE - HEADER_SIZE - 8;
        let mut data = vec![0u8; size];
        let max_i = size - 1;
        data[0] = 1;
        data[max_i] = 255;
        data[200] = 100;
        let tid = testee.insert_record(&data).unwrap();
        let record = testee.get_record(tid, |record| {Vec::from(record)}).unwrap().unwrap();
        assert_eq!(record.len(), size);
        assert_eq!(record[0], 1);
        assert_eq!(record[200], 100);
        assert_eq!(record[max_i], 255);
    }

    #[test]
    fn allocate_max_twice_gives_differnt_pages() {
        let bm = MockBufferManager::new(PAGE_SIZE);
        let testee = SlottedPageSegment::new(bm, 1, 0);
        let size = PAGE_SIZE - HEADER_SIZE - 8;
        let data = vec![0u8; size];
        let tid1 = testee.insert_record(&data).unwrap();
        let tid2 = testee.insert_record(&data).unwrap();
        assert_ne!(tid1.page_id, tid2.page_id);
    }

    #[test]
    fn allocate_max_twice_dirties_only_second() {
        let bm = MockBufferManager::new(PAGE_SIZE);
        let testee = SlottedPageSegment::new(bm.clone(), 1, 0);
        let size = PAGE_SIZE - HEADER_SIZE - 8;
        let data = vec![0u8; size];
        let tid1 = testee.insert_record(&data).unwrap();
        bm.fix_page(PageId::new(1, tid1.page_id)).unwrap().try_write().unwrap().state = PageState::CLEAN;
        let tid2 = testee.insert_record(&data).unwrap();
        assert!(!bm.fix_page(PageId::new(1, tid1.page_id)).unwrap().try_write().unwrap().state.is_dirtyish());
        assert!(bm.fix_page(PageId::new(1, tid2.page_id)).unwrap().try_write().unwrap().state.is_dirtyish());
    }

    #[test]
    fn allocate_max_second_gives_differnt_pages() {
        let bm = MockBufferManager::new(PAGE_SIZE);
        let testee = SlottedPageSegment::new(bm, 1, 0);
        let size = PAGE_SIZE - HEADER_SIZE - 8;
        let data = vec![1u8; 1];
        let tid1 = testee.insert_record(&data).unwrap();
        let data = vec![1u8; size];
        let tid2 = testee.insert_record(&data).unwrap();
        assert_ne!(tid1.page_id, tid2.page_id);
    }

    #[test]
    fn update_with_redirect() {
        let bm = MockBufferManager::new(PAGE_SIZE);
        let testee = SlottedPageSegment::new(bm, 1, 0);
        let data = vec![10u8; PAGE_SIZE - 2000];
        testee.insert_record(&data).unwrap();
        let mut data = vec![0u8; 500];
        data[0] = 1;
        data[499] = 2;
        data[200] = 3;
        let tid = testee.insert_record(&data).unwrap();
        let data = vec![50u8; 5000];
        testee.write_record(tid, &data).unwrap();

        let record = testee.get_record(tid, |record| {Vec::from(record)}).unwrap().unwrap();
        assert_eq!(record.len(), 5000);
        assert_eq!(record[0], 50);
        assert_eq!(record[200], 50);
        assert_eq!(record[4999], 50);
    }
    
    #[test]
    fn update_with_redirect_dirties_both() {
        let bm = MockBufferManager::new(PAGE_SIZE);
        let testee = SlottedPageSegment::new(bm.clone(), 1, 0);
        let data = vec![10u8; PAGE_SIZE - 2000];
        let tid1 = testee.insert_record(&data).unwrap();
        bm.fix_page(PageId::new(1, tid1.page_id)).unwrap().try_write().unwrap().state = PageState::CLEAN;
        testee.insert_record(&data).unwrap();
        let data = vec![50u8; 5000];
        testee.write_record(tid1, &data).unwrap();
        assert!(bm.fix_page(PageId::new(1, tid1.page_id)).unwrap().try_write().unwrap().state.is_dirtyish());
        assert!(bm.fix_page(PageId::new(1, tid1.page_id + 1)).unwrap().try_write().unwrap().state.is_dirtyish());
    }

    #[test]
    fn update_with_redirect_back_to_root() {
        let bm = MockBufferManager::new(PAGE_SIZE);
        let testee = SlottedPageSegment::new(bm, 1, 0);
        let data = vec![10u8; PAGE_SIZE - 2000];
        testee.insert_record(&data).unwrap();
        let mut data = vec![0u8; 500];
        data[0] = 1;
        data[499] = 2;
        data[200] = 3;
        let tid = testee.insert_record(&data).unwrap();
        let data = vec![50u8; 5000];
        testee.write_record(tid, &data).unwrap();
        let data = vec![40u8; 50];
        testee.write_record(tid, &data).unwrap();

        let record = testee.get_record(tid, |record| {Vec::from(record)}).unwrap().unwrap();
        assert_eq!(record.len(), 50);
        assert_eq!(record[0], 40);
        assert_eq!(record[20], 40);
        assert_eq!(record[49], 40);
    }

   #[test]
    fn update_with_redirect_back_to_root_dirties_both() {
        let bm = MockBufferManager::new(PAGE_SIZE);
        let testee = SlottedPageSegment::new(bm.clone(), 1, 0);
        let data = vec![10u8; PAGE_SIZE - 2000];
        let tid1 = testee.insert_record(&data).unwrap();
        let data = vec![0u8; 500];
        let tid2 = testee.insert_record(&data).unwrap();
        let data = vec![50u8; 5000];
        testee.write_record(tid2, &data).unwrap();
        bm.fix_page(PageId::new(1, tid1.page_id)).unwrap().try_write().unwrap().state = PageState::CLEAN;
        bm.fix_page(PageId::new(1, tid1.page_id + 1)).unwrap().try_write().unwrap().state = PageState::CLEAN;
        let data = vec![40u8; 50];
        testee.write_record(tid2, &data).unwrap();
        assert!(bm.fix_page(PageId::new(1, tid2.page_id)).unwrap().try_write().unwrap().state.is_dirtyish());
        assert!(bm.fix_page(PageId::new(1, tid2.page_id + 1)).unwrap().try_write().unwrap().state.is_dirtyish());
    }

    #[test]
    fn update_with_relocate_on_redirect_page() {
        let bm = MockBufferManager::new(PAGE_SIZE);
        let testee = SlottedPageSegment::new(bm, 1, 0);
        let data = vec![10u8; PAGE_SIZE - 2000];
        testee.insert_record(&data).unwrap();
        let mut data = vec![0u8; 500];
        data[0] = 1;
        data[499] = 2;
        data[200] = 3;
        let tid = testee.insert_record(&data).unwrap();
        let data = vec![50u8; 5000];
        testee.write_record(tid, &data).unwrap();
        let data = vec![40u8; 8000];
        testee.write_record(tid, &data).unwrap();

        let record = testee.get_record(tid, |record| {Vec::from(record)}).unwrap().unwrap();
        assert_eq!(record.len(), 8000);
        assert_eq!(record[0], 40);
        assert_eq!(record[2000], 40);
        assert_eq!(record[7999], 40);
    }

    #[test]
    fn update_with_relocate_on_redirect_page_dirties_only_relocate_page() {
        let bm = MockBufferManager::new(PAGE_SIZE);
        let testee = SlottedPageSegment::new(bm.clone(), 1, 0);
        let data = vec![10u8; PAGE_SIZE - 2000];
        testee.insert_record(&data).unwrap();
        let data = vec![0u8; 500];
        let tid = testee.insert_record(&data).unwrap();
        let data = vec![50u8; 5000];
        testee.write_record(tid, &data).unwrap();
        bm.fix_page(PageId::new(1, tid.page_id)).unwrap().try_write().unwrap().state = PageState::CLEAN;
        let data = vec![40u8; 8000];
        testee.write_record(tid, &data).unwrap();
        assert!(!bm.fix_page(PageId::new(1, tid.page_id)).unwrap().try_write().unwrap().state.is_dirtyish());
        assert!(bm.fix_page(PageId::new(1, tid.page_id + 1)).unwrap().try_write().unwrap().state.is_dirtyish());
    }

    #[test]
    fn update_with_relocate_from_redirect_to_different_redirect() {
        let bm = MockBufferManager::new(PAGE_SIZE);
        let testee = SlottedPageSegment::new(bm, 1, 0);
        let data = vec![10u8; PAGE_SIZE - 2000];
        testee.insert_record(&data).unwrap();
        let data = vec![0u8; 500];
        let tid = testee.insert_record(&data).unwrap();
        let data = vec![50u8; 5000];
        testee.write_record(tid, &data).unwrap();
        let data = vec![100u8; 5000];
        testee.insert_record(&data).unwrap();
        let data = vec![40u8; PAGE_SIZE - 2000];
        testee.write_record(tid, &data).unwrap();

        let record = testee.get_record(tid, |record| {Vec::from(record)}).unwrap().unwrap();
        assert_eq!(record.len(), PAGE_SIZE - 2000);
        assert_eq!(record[0], 40);
        assert_eq!(record[2000], 40);
        assert_eq!(record[7999], 40);
    }

    #[test]
    fn update_with_relocate_from_redirect_to_different_redirect_dirties_all_three() {
        let bm = MockBufferManager::new(PAGE_SIZE);
        let testee = SlottedPageSegment::new(bm.clone(), 1, 0);
        let data = vec![10u8; PAGE_SIZE - 2000];
        testee.insert_record(&data).unwrap();
        let data = vec![0u8; 500];
        let tid = testee.insert_record(&data).unwrap();
        let data = vec![50u8; 5000];
        testee.write_record(tid, &data).unwrap();
        let data = vec![100u8; 5000];
        testee.insert_record(&data).unwrap();
        bm.fix_page(PageId::new(1, tid.page_id)).unwrap().try_write().unwrap().state = PageState::CLEAN;
        bm.fix_page(PageId::new(1, tid.page_id + 1)).unwrap().try_write().unwrap().state = PageState::CLEAN;
        let data = vec![40u8; PAGE_SIZE - 2000];
        testee.write_record(tid, &data).unwrap();

        assert!(bm.fix_page(PageId::new(1, tid.page_id)).unwrap().try_write().unwrap().state.is_dirtyish());
        assert!(bm.fix_page(PageId::new(1, tid.page_id + 1)).unwrap().try_write().unwrap().state.is_dirtyish());
        assert!(bm.fix_page(PageId::new(1, tid.page_id + 2)).unwrap().try_write().unwrap().state.is_dirtyish());
    }

    #[test]
    fn update_with_relocate_from_redirect_to_different_redirect_integration_test() {
        let datadir = tempfile::tempdir().unwrap();
        let datadir_path = datadir.into_path();
        let disk_manager = DiskManager::new(datadir_path.clone());
        let replacer = ClockReplacer::new();
        let bm = HashTableBufferManager::new(disk_manager, replacer, PAGE_SIZE);
        let testee = SlottedPageSegment::new(bm, 1, 0);
        let data = vec![10u8; PAGE_SIZE - 2000];
        testee.insert_record(&data).unwrap();
        let data = vec![0u8; 500];
        let tid = testee.insert_record(&data).unwrap();
        let data = vec![50u8; 5000];
        testee.write_record(tid, &data).unwrap();
        let data = vec![100u8; 5000];
        testee.insert_record(&data).unwrap();
        let data = vec![40u8; PAGE_SIZE - 2000];
        testee.write_record(tid, &data).unwrap();
        drop(testee);

        let disk_manager = DiskManager::new(datadir_path);
        let replacer = ClockReplacer::new();
        let bm = HashTableBufferManager::new(disk_manager, replacer, PAGE_SIZE);
        let testee = SlottedPageSegment::new(bm, 1, 0);

        let record = testee.get_record(tid, |record| {Vec::from(record)}).unwrap().unwrap();
        assert_eq!(record.len(), PAGE_SIZE - 2000);
        assert_eq!(record[0], 40);
        assert_eq!(record[2000], 40);
        assert_eq!(record[7999], 40);
    }

    #[test]
    fn test_erase_simple() {
        let bm = MockBufferManager::new(PAGE_SIZE);
        let testee = SlottedPageSegment::new(bm, 1, 0);
        let data = vec![10u8; PAGE_SIZE - 2000];
        let tid = testee.insert_record(&data).unwrap();
        testee.erase_record(tid).unwrap();
        assert_eq!(testee.get_record(tid, |_| {panic!()}).unwrap(), None);
    }

    #[test]
    fn test_compactify() {
        let aligned_slice = alligned_slice(PAGE_SIZE, 1);
        let mut page = Page::new_from(PageId::new(1,1), aligned_slice);
        let mut slotted_page = SlottedPage::new(&mut page);
        slotted_page.initialize();
        let useable_size = PAGE_SIZE - HEADER_SIZE;
        let num_records = useable_size / 16;
        for i in 0..num_records {
            let slot_id = slotted_page.allocate(8).unwrap();
            let slot = slotted_page.get_slot(slot_id);
            if let Slot::Slot { offset, length } = slot {
                slotted_page.get_record_mut(offset, length).copy_from_slice(&i.to_be_bytes());
            } else {
                panic!();
            }
        }

        slotted_page.erase(0);
        slotted_page.erase(1);

        slotted_page.compactify();

        for i in 0..2 {
            let slot = slotted_page.get_slot(i);
            assert_eq!(Slot::Free, slot);
        }

        for i in 2..num_records as u16 {
            let slot = slotted_page.get_slot(i);
            if let Slot::Slot { offset, length } = slot {
                let record = slotted_page.get_record(offset, length);
                assert_eq!((i as u64).to_be_bytes(), record);
            } else {
                panic!();
            }
        }
    }

    #[test]
    fn test_scan() {
        let bm = MockBufferManager::new(PAGE_SIZE);
        let testee = SlottedPageSegment::new(bm, 1, 0);
        let records = vec![
            vec![10u8; PAGE_SIZE - 2000],
            vec![0u8; 500],
            vec![50u8; 5000],
            vec![100u8; 5000],
            vec![40u8; PAGE_SIZE - 2000],
        ];
        for record in &records {
            testee.insert_record(&record).unwrap();
        }

        fn get_vec(record: &[u8]) -> Option<Vec<u8>> {
            Some(Vec::from(record))
        }

        let mut scan = testee.scan( get_vec);
        assert_eq!(scan.next().unwrap().unwrap(), (RelationTID::new(0, 0), records[0].clone()));
        assert_eq!(scan.next().unwrap().unwrap(), (RelationTID::new(0, 1), records[1].clone()));
        assert_eq!(scan.next().unwrap().unwrap(), (RelationTID::new(1, 0), records[2].clone()));
        assert_eq!(scan.next().unwrap().unwrap(), (RelationTID::new(1, 1), records[3].clone()));
        assert_eq!(scan.next().unwrap().unwrap(), (RelationTID::new(2, 0), records[4].clone()));
        assert!(scan.next().is_none());
    }

    #[test]
    fn test_scan_with_relocates() {
        let bm = MockBufferManager::new(PAGE_SIZE);
        let testee = SlottedPageSegment::new(bm.clone(), 1, 0);
        let records = vec![
            vec![10u8; PAGE_SIZE - 2000],
            vec![0u8; 500],
            vec![50u8; 5000],
            vec![100u8; 5000],
            vec![40u8; PAGE_SIZE - 2000],
        ];
        for record in &records {
            testee.insert_record(&record).unwrap();
        }
        let data = vec![40u8; PAGE_SIZE - 2000];
        testee.write_record(RelationTID::new(0, 1), &data).unwrap();

        fn get_vec(record: &[u8]) -> Option<Vec<u8>> {
            Some(Vec::from(record))
        }

        let mut scan = testee.scan(get_vec);
        assert_eq!(scan.next().unwrap().unwrap(), (RelationTID::new(0, 0), records[0].clone()));
        assert_eq!(scan.next().unwrap().unwrap(), (RelationTID::new(1, 0), records[2].clone()));
        assert_eq!(scan.next().unwrap().unwrap(), (RelationTID::new(1, 1), records[3].clone()));
        assert_eq!(scan.next().unwrap().unwrap(), (RelationTID::new(2, 0), records[4].clone()));
        assert_eq!(scan.next().unwrap().unwrap(), (RelationTID::new(0, 1), data.clone()));
        assert!(scan.next().is_none());
    }

    #[test]
    fn test_empty_scan() {
        let bm = MockBufferManager::new(PAGE_SIZE);
        let testee = SlottedPageSegment::new(bm, 1, 0);

        let get_vec = |record: &[u8]| -> Option<Vec<u8>> {
            Some(Vec::from(record))
        };

        let mut scan = testee.scan(get_vec);
        assert!(scan.next().is_none());
    }
    // TODO: Integration tests with actual disk writes
}
