
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RelationTID {
    pub page_id: u64, // only current segment (48 bits max)
    pub slot_id: u16
}

impl From<u64> for RelationTID {
    fn from(input: u64) -> Self {
        Self { 
            page_id: input >> 16,
            slot_id: (input & ((1u64 << 16) - 1)) as u16
        }
    }
}

impl From<&RelationTID> for u64 {
    fn from(input: &RelationTID) -> u64 {
        (input.page_id << 16) | (input.slot_id as u64)
    }
}

pub enum TupleValue {
    Int(i64), // Only ints for now
    // Float(f64),
    // String(String),
    // Bool(bool),
    // Null,
}
