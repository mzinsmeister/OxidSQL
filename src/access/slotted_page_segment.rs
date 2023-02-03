use crate::{storage::{buffer_manager::BufferManager, page::Page}, types::TID};

// We implement a safe variant for now. Transmuting stuff like the header will likely lead to more
// readable code but would need unsafe

// 16 bit slot count, 16 bit first free slot, 32 bit data start, 32 bit free space
const HEADER_SIZE: usize = 2 * 4 + 2 * 2; 

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
enum Slot {
    Slot { offset: u32, length: u32 },
    Reference { tid: TID }
}

impl Slot {

    fn new(offset: u32, length: u32) {

    }

}

impl From<&[u8]> for Slot {
    
    fn from(source: &[u8]) -> Self {
        Slot::Slot {
            offset: u32::from_le_bytes(source[0..4].try_into().unwrap()),
            length: u32::from_le_bytes(source[4..8].try_into().unwrap()),
        }
    }

}

impl Into<[u8; 8]> for Slot {
    fn into(self) -> [u8; 8] {
        let mut bytes = [0u8; 8];
        bytes[0..4].copy_from_slice(&self.offset.to_le_bytes());
        bytes[4..8].copy_from_slice(&self.length.to_le_bytes());
        bytes
    }
}

pub struct SlottedPageSegment {
    bm: Arc<BufferManager>
}

struct SlottedPage<'a> {
    page: &'a Page
}

impl SlottedPage<'_> {
    fn new(page: &Page) -> SlottedPage {
        SlottedPage { page }
    }

    fn get_slot(&self, slot_id: u32) -> Slot {
        Self::get_slot_from_page(self.page, slot_id)
    }

    fn get_slot_from_page(page: &Page, slot_id: u32) -> Slot {
        let slot_offset = HEADER_SIZE + slot_id as usize * 8;
        (&page.data[slot_offset..slot_offset + 8]).into()
    }
}

struct SlottedPageMut<'a> {
    page: &'a mut Page
}

impl SlottedPageMut<'_> {
    pub fn new(page: &mut Page) -> SlottedPageMut {
        SlottedPageMut { page }
    }

    fn get_slot(&self, slot_id: u32) -> Slot {
        SlottedPageRead::get_slot_from_page(self.page, slot_id)
    }

    fn write_slot(&mut self, slot_id: u32, slot: Slot) {
        let slot_offset = HEADER_SIZE + slot_id as usize * 8;
        self.page.data[slot_offset..slot_offset + 8].copy_from_slice(&slot.into::<[u8; 8]>());
    }
}
