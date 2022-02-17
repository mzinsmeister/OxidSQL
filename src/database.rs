#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub enum TupleValue {
    Long(i64),
    UnsignedInt(u32),
    String(String),
    Bytes(Vec<u8>)
}

impl TupleValue {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            TupleValue::Long(l) => Vec::from(l.to_be_bytes()),
            TupleValue::UnsignedInt(u) => Vec::from(u.to_be_bytes()),
            TupleValue::String(s) => s.as_bytes().to_vec(),
            TupleValue::Bytes(b) => b.to_vec()
        }
    }

    pub fn get_as_string(self) -> String {
        if let Self::String(str) = self {
            str
        } else {
            panic!("Not a string");
        }
    }

    pub fn get_as_bytes(self) -> Vec<u8> {
        if let Self::Bytes(bytes) = self {
            bytes
        } else {
            panic!("Not a byte array");
        }
    }

    pub fn get_as_unsigned_int(self) -> u32 {
        if let Self::UnsignedInt(uint) = self {
            uint
        } else {
            panic!("Not an unsigned int");
        }
    }
}
