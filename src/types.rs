#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RelationTID {
    pub page_id: u64, // only current segment (48 bits max)
    pub slot_id: u16
}

impl RelationTID {
    pub fn new(page_id: u64, slot_id: u16) -> Self {
        Self { page_id, slot_id }
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum TupleValueType {
    BigInt = 0,
    VarChar(u16) = 1,
    Int = 2,
    SmallInt = 3
}

impl TupleValueType {
    pub fn get_size(&self) -> Option<usize> {
        match self {
            TupleValueType::BigInt => Some(8),
            TupleValueType::VarChar(_) => None,
            TupleValueType::Int => Some(4),
            TupleValueType::SmallInt => Some(2)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TupleValue {
    BigInt(i64),
    Int(i32),
    SmallInt(i16),
    String(String),
    // Float(f64),
    // Bool(bool),
    // Null,
}

impl TupleValue {
    pub fn as_big_int(&self) -> i64 {
        match self {
            TupleValue::BigInt(value) => *value,
            _ => unreachable!(),
        }
    }

    pub fn as_int(&self) -> i32 {
        match self {
            TupleValue::Int(value) => *value,
            _ => unreachable!(),
        }
    }

    pub fn as_small_int(&self) -> i16 {
        match self {
            TupleValue::SmallInt(value) => *value,
            _ => unreachable!(),
        }
    }

    pub fn as_varchar(&self) -> &str {
        match self {
            TupleValue::String(value) => value,
            _ => unreachable!(),
        }
    }
}
