use crate::storage::page::PageId;


#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TID {
    page_id: PageId,
    slot_id: u16
}