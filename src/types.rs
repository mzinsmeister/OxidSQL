use std::fmt::Display;

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

#[derive(Debug, Clone, Hash)]
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

impl PartialEq for TupleValue {
    fn eq(&self, other: &Self) -> bool {
        // For numeric types also allow comparisons between different types (like bigint with smallint)
        match (self, other) {
            (TupleValue::BigInt(a), TupleValue::BigInt(b)) => a == b,
            (TupleValue::Int(a), TupleValue::Int(b)) => a == b,
            (TupleValue::SmallInt(a), TupleValue::SmallInt(b)) => a == b,
            (TupleValue::String(a), TupleValue::String(b)) => a == b,
            (TupleValue::BigInt(a), TupleValue::Int(b)) => *a == *b as i64,
            (TupleValue::BigInt(a), TupleValue::SmallInt(b)) => *a == *b as i64,
            (TupleValue::Int(a), TupleValue::BigInt(b)) => *a as i64 == *b,
            (TupleValue::Int(a), TupleValue::SmallInt(b)) => *a == *b as i32,
            (TupleValue::SmallInt(a), TupleValue::BigInt(b)) => *a as i64 == *b,
            (TupleValue::SmallInt(a), TupleValue::Int(b)) => *a as i32 == *b,
            _ => false
        }
    }
}

impl Eq for TupleValue {}

impl PartialOrd for TupleValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // For numeric types also allow comparisons between different types (like bigint with smallint)
        match (self, other) {
            (TupleValue::BigInt(a), TupleValue::BigInt(b)) => a.partial_cmp(b),
            (TupleValue::Int(a), TupleValue::Int(b)) => a.partial_cmp(b),
            (TupleValue::SmallInt(a), TupleValue::SmallInt(b)) => a.partial_cmp(b),
            (TupleValue::String(a), TupleValue::String(b)) => a.partial_cmp(b),
            (TupleValue::BigInt(a), TupleValue::Int(b)) => a.partial_cmp(&(*b as i64)),
            (TupleValue::BigInt(a), TupleValue::SmallInt(b)) => a.partial_cmp(&(*b as i64)),
            (TupleValue::Int(a), TupleValue::BigInt(b)) => (*a as i64).partial_cmp(b),
            (TupleValue::Int(a), TupleValue::SmallInt(b)) => a.partial_cmp(&(*b as i32)),
            (TupleValue::SmallInt(a), TupleValue::BigInt(b)) => (*a as i64).partial_cmp(b),
            (TupleValue::SmallInt(a), TupleValue::Int(b)) => (*a as i32).partial_cmp(b),
            _ => None
        }
    }
}

impl Display for TupleValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TupleValue::BigInt(i) => write!(f, "{}", i),
            TupleValue::Int(i) => write!(f, "{}", i),
            TupleValue::SmallInt(i) => write!(f, "{}", i),
            TupleValue::String(s) => write!(f, "\"{}\"", s.escape_debug()),
        }
    }
}
