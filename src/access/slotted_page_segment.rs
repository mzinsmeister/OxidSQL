use std::{sync::Arc, collections::BTreeMap};
use crate::{storage::{buffer_manager::BufferManager, page::Page}, types::RelationTID};

// We implement a safe variant for now. Transmuting stuff like the header will likely lead to more
// readable code but would need unsafe

// 16 bit slot count, 16 bit first free slot, 32 bit data start, 32 bit free space
const HEADER_SIZE: usize = 2 * 4 + 2 * 2; 

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
enum Slot {
    Slot { offset: u32, length: u32 },
    Redirect { tid: RelationTID },
    Free
}

impl Slot {

    fn new(offset: u32, length: u32) -> Slot {
        Slot::Slot { offset, length }
    }

    fn new_reference(tid: RelationTID) -> Slot {
        Slot::Redirect { tid }
    }
}

impl From<&[u8]> for Slot {
    
    fn from(source: &[u8]) -> Self {
        if source[0] & (1 << 7) != 0 {
            return Slot::Redirect { tid: RelationTID::from(u64::from_be_bytes(source.try_into().unwrap())) };
        }
        if source[0] & (1 << 6) != 0 {
            return Slot::Free;
        }
        Slot::Slot {
            offset: u32::from_le_bytes(source[0..4].try_into().unwrap()),
            length: u32::from_le_bytes(source[4..8].try_into().unwrap()),
        }
    }

}

impl From<&Slot> for [u8; 8] {
    fn from(source: &Slot) -> [u8; 8] {
        match source {
            Slot::Slot { offset, length } => {
                let mut bytes = [0u8; 8];
                bytes[0..4].copy_from_slice(&offset.to_le_bytes());
                bytes[4..8].copy_from_slice(&length.to_le_bytes());
                bytes
            },
            Slot::Redirect { tid } => {
                let mut bytes = u64::from(tid).to_be_bytes();
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

pub struct SlottedPageSegment {
    bm: Arc<BufferManager>,
    segment_id: u16
}

trait SlottedPageRead {
    fn get_slot(&self, slot_id: u16) -> Slot;

    fn get_slot_count(&self) -> u16;

    fn get_first_free_slot(&self) -> u16;

    fn get_data_start(&self) -> u32;

    fn get_free_space(&self) -> u32;

    fn get_fragmented_free_space(&self) -> usize;

    fn get_data(&self) -> &[u8];

    fn get_data_length(&self) -> u32;

    fn get_tuple(&self, offset: u32, length: u32) -> &[u8];
}

struct SlottedPage<'a> {
    page: &'a Page
}

impl SlottedPage<'_> {
    fn new(page: &Page) -> SlottedPage {
        SlottedPage { page }
    }

}

fn get_slot_from_page(page: &Page, slot_id: u16) -> Slot {
    let slot_offset = HEADER_SIZE + slot_id as usize * 8;
    (&page.data[slot_offset..slot_offset + 8]).into()
}

fn get_data_from_page(page: &Page, data_start: usize) -> &[u8] {
    &page.data[data_start as usize..]
}

fn get_tuple_from_page(page: &Page, data_start: u32, offset: u32, length: u32) -> &[u8] {
    &get_data_from_page(page, data_start as usize)[offset as usize .. offset as usize + length as usize]
}

impl SlottedPageRead for SlottedPage<'_> {

    fn get_slot(&self, slot_id: u16) -> Slot {
        get_slot_from_page(self.page, slot_id)
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
        self.page.data.len() - self.get_data_start() as usize - HEADER_SIZE - 8 * self.get_slot_count() as usize
    }

    fn get_data(&self) -> &[u8] {
        get_data_from_page(self.page, self.get_data_start() as usize)
    }

    fn get_data_length(&self) -> u32 {
        self.page.data.len() as u32 - self.get_data_start()
    }

    fn get_tuple(&self, offset: u32, length: u32) -> &[u8] {
        get_tuple_from_page(&self.page, self.get_data_start(), offset, length)
    }
}

struct SlottedPageMut<'a> {
    page: &'a mut Page
}

impl SlottedPageRead for SlottedPageMut<'_> {

    fn get_slot(&self, slot_id: u16) -> Slot {
        SlottedPage { page: self.page }.get_slot(slot_id)
    }

    fn get_slot_count(&self) -> u16 {
        SlottedPage { page: self.page }.get_slot_count()
    }

    fn get_first_free_slot(&self) -> u16 {
        SlottedPage { page: self.page }.get_first_free_slot()
    }

    fn get_data_start(&self) -> u32 {
        SlottedPage { page: self.page }.get_data_start()
    }

    fn get_free_space(&self) -> u32 {
        SlottedPage { page: self.page }.get_free_space()
    }

    fn get_fragmented_free_space(&self) -> usize {
        SlottedPage { page: self.page }.get_fragmented_free_space()
    }

    fn get_data(&self) -> &[u8] {
        get_data_from_page(self.page, self.get_data_start() as usize)
    }

    fn get_data_length(&self) -> u32 {
        SlottedPage { page: self.page }.get_data_length()
    }

    fn get_tuple(&self, offset: u32, length: u32) -> &[u8] {
        get_tuple_from_page(&self.page, self.get_data_start(), offset, length)
    }
}

impl SlottedPageMut<'_> {
    pub fn new(page: &mut Page) -> SlottedPageMut {
        SlottedPageMut { page }
    }

    fn write_slot(&mut self, slot_id: u16, slot: &Slot) {
        let slot_offset = HEADER_SIZE + slot_id as usize * 8;
        let slot_binary: [u8; 8] = slot.into();
        self.page.data[slot_offset..slot_offset + 8].copy_from_slice(slot_binary.as_ref());
    }

    fn set_slot_count(&mut self, slot_count: u16) {
        self.page.data[0..2].copy_from_slice(&slot_count.to_be_bytes());
    }

    fn set_first_free_slot(&mut self, first_free_slot: u16) {
        self.page.data[2..4].copy_from_slice(&first_free_slot.to_be_bytes());
    }

    fn set_free_space(&mut self, free_space: u32) {
        self.page.data[8..12].copy_from_slice(&free_space.to_be_bytes());
    }

    fn set_data_start(&mut self, data_start: u32) {
        self.page.data[4..8].copy_from_slice(&data_start.to_be_bytes());
    }

    fn get_tuple_mut(&mut self, offset: u32, length: u32) -> &mut [u8] {
        let data_start = self.get_data_start();
        &mut self.page.data[data_start as usize + offset as usize .. data_start as usize + offset as usize + length as usize]
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
        None
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
            Slot::Slot { offset, length } => {
                if length as usize > size {
                    self.set_free_space(self.get_free_space() + (length as usize - size) as u32);
                    self.write_slot(slot_id, &Slot::new(offset, size as u32));
                    return true;
                }
                if length < size as u32  {
                    if size as u32 - length > self.get_free_space() {
                        return false;
                    }
                    let buffer = self.get_tuple(offset, size as u32).to_vec();
                    self.write_slot(slot_id, &Slot::Free);
                    if self.get_fragmented_free_space() < size {
                        self.compactify();
                    }
                    let new_data_start = self.get_data_start() - size as u32;
                    self.write_slot(slot_id, &Slot::new(new_data_start, size as u32));
                    let tuple_mut = self.get_tuple_mut(new_data_start, size as u32);
                    tuple_mut.copy_from_slice(buffer.as_slice());
                    self.set_data_start(new_data_start);
                    self.set_free_space(self.get_free_space() + (size - length as usize) as u32);
                }
            }
            Slot::Redirect{ tid: _ } => {
                if self.get_free_space() < size as u32 {
                    return false;
                }
                let new_data_start = self.get_data_start() - size as u32;
                let new_slot = Slot::new(new_data_start, size as u32);
                self.write_slot(slot_id, &new_slot);
            }
        }
        true
    }

    /// Compactify the page by moving all data to the end of the page and removing any free space in between.
    fn compactify(&mut self) {
        struct OffsetLengthSlot {
            slot_id: u16, offset: u32, length: u32
        }
        let mut offset_slot_map: BTreeMap<usize, OffsetLengthSlot> = BTreeMap::new();
        for slot_id in 0..self.get_slot_count() {
            let slot = self.get_slot(slot_id);
            if let Slot::Slot { offset, length } = slot {
                offset_slot_map.insert(offset as usize, OffsetLengthSlot { slot_id, offset, length });
            }
        }
        let mut new_data_start = self.page.data.len() as u32;
        for (offset, slot) in offset_slot_map.iter() {
            new_data_start = new_data_start - slot.length;
            self.page.data[new_data_start as usize..new_data_start as usize + slot.length as usize].copy_from_slice(&self.page.data[*offset..*offset + slot.length as usize]);
            let new_slot = Slot::new(new_data_start, slot.length);
            self.write_slot(slot.slot_id, &new_slot);
        }
        self.set_data_start(new_data_start);
    }
}
