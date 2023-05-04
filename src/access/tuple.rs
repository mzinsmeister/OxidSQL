use std::{io::{Cursor, Read, Write}};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use crate::types::{TupleValue, TupleValueType};

/*
We use a null bitmap to indicate which attributes are null. The null bitmap is stored in the first n bytes of the tuple
where n is the number of attributes in the tuple divided by 8 plus 1 (for the remainder). The null bitmap is stored
in big endian order. The first bit of the first byte indicates whether the first attribute is null, the second bit
indicates whether the second attribute is null and so on. The first bit of the second byte indicates whether the
ninth attribute is null, the second bit indicates whether the tenth attribute is null and so on.
The benefit of this approach is that we don't have to store the values of null attributes and thus save space.
The downside is that we have to parse the null bitmap before we can parse the actual values and that we don't have
constant time access to the values (we have to parse the null bitmap first).
 */
pub struct Tuple {
    pub values: Vec<Option<TupleValue>>
}

impl Tuple {

    #[inline]
    pub fn parse_binary(attributes: Vec<TupleValueType>, src: &[u8]) -> Tuple { 
        let num_null_bytes = attributes.len() / 8 + 1;
        let num_null_values: u32 = src.iter().take(num_null_bytes).map(|b| b.count_ones()).sum();
        let mut values = Vec::with_capacity(attributes.len() - num_null_values as usize);
        let mut cursor = Cursor::new(src);
        cursor.set_position(num_null_bytes as u64);
        let mut var_atts = Vec::new();
        for (i, attribute_type) in attributes.iter().enumerate() {
            let null_byte = src[i / 8];
            let null_bit_mask = 1 <<  (7 - (i % 8));
            if null_byte & null_bit_mask == 0 {
                match attribute_type {
                    TupleValueType::BigInt => {
                        values.push(Some(TupleValue::BigInt(cursor.read_i64::<BigEndian>().unwrap())));
                    },
                    TupleValueType::String => {
                        var_atts.push((values.len(), attribute_type, cursor.read_u16::<BigEndian>().unwrap()));
                        values.push(None);
                    }
                    TupleValueType::Int => {
                        values.push(Some(TupleValue::Int(cursor.read_i32::<BigEndian>().unwrap())));
                    },
                    TupleValueType::SmallInt => {
                        values.push(Some(TupleValue::SmallInt(cursor.read_i16::<BigEndian>().unwrap())));
                    },
                }
            } else {
                values.push(None);
            }
        }
        // now read the variable length attributes
        for (index, attribute_type, length) in var_atts {
            match attribute_type {
                TupleValueType::String => {
                    let mut string = String::with_capacity(length as usize);
                    cursor.read_to_string(&mut string).unwrap();
                    values[index] = Some(TupleValue::String(string));
                },
                _ => unreachable!() // No other variable sized types
            }
        }
        Tuple { values }
    }

    #[inline]
    pub fn calculate_binary_length(&self) -> usize {
        let num_null_bytes = self.values.len() / 8 + 1;
        let mut length = num_null_bytes;
        for value in self.values.iter() {
            if let Some(value) = value {
                match value {
                    TupleValue::BigInt(_) => {
                        length += 8;
                    },
                    TupleValue::String(value) => {
                        length += value.len() + 2;
                    }
                    TupleValue::Int(_) => {
                        length += 4;
                    },
                    TupleValue::SmallInt(_) => {
                        length += 2;
                    },
                }
            }
        }
        length
    }

    #[inline]
    pub fn write_binary(&self, buffer: &mut [u8]) {
        let num_null_bytes = self.values.len() / 8 + 1;
        let mut null_bytes = vec![0u8; num_null_bytes];
        let mut var_atts = Vec::new();
        let mut cursor = Cursor::new(buffer);
        // first write a placeholder for the null bitmap
        cursor.write(&null_bytes).unwrap();
        for (i, value) in self.values.iter().enumerate() {
            if let Some(value) = value {
                match value {
                    TupleValue::BigInt(value) => {
                        cursor.write_i64::<BigEndian>(*value).unwrap();
                    },
                    TupleValue::String(value) => {
                        var_atts.push((cursor.position(), value));
                        cursor.write_u16::<BigEndian>(0).unwrap(); // Write 0 as placeholder
                    }
                    TupleValue::Int(value) => {
                        cursor.write_i32::<BigEndian>(*value).unwrap();
                    },
                    TupleValue::SmallInt(value) => {
                        cursor.write_i16::<BigEndian>(*value).unwrap();
                    },
                }
            } else {
                let null_byte = &mut null_bytes[i / 8];
                let null_bit_mask = 1 <<  (7 - (i % 8));
                *null_byte |= null_bit_mask;
            }
        }
        // now write the variable length attributes
        for (index, value) in var_atts {
            cursor.write(value.as_bytes()).unwrap();
            cursor.set_position(index as u64);
            let position = cursor.position();
            cursor.set_position(index);
            cursor.write_u16::<BigEndian>(value.len() as u16).unwrap();
            cursor.set_position(position);
        }
        cursor.set_position(0);
        cursor.write(&null_bytes).unwrap();
    }
}

#[cfg(test)]
mod test {

    use super::*;

    const TEST_BINARY: &[u8] = &[0b10000000, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x02, 'a' as u8, 'b' as u8];

    #[test]
    fn test_parse_binary() {
        let attributes = vec![
            TupleValueType::Int,
            TupleValueType::String,
            TupleValueType::Int,
            TupleValueType::BigInt,
            TupleValueType::SmallInt];
        let tuple = Tuple::parse_binary(attributes.clone(), TEST_BINARY);
        assert_eq!(tuple.values.len(), 5);
        assert_eq!(tuple.values[0], None);
        assert_eq!(tuple.values[1], Some(TupleValue::String("ab".to_string())));
        assert_eq!(tuple.values[2], Some(TupleValue::Int(1)));
        assert_eq!(tuple.values[3], Some(TupleValue::BigInt(8)));
        assert_eq!(tuple.values[4], Some(TupleValue::SmallInt(2)));
    }

    #[test]
    fn test_write_binary() {
        let tuple = Tuple {
            values: vec![
                None,
                Some(TupleValue::String("ab".to_string())),
                Some(TupleValue::Int(1)),
                Some(TupleValue::BigInt(8)),
                Some(TupleValue::SmallInt(2))
            ]
        };
        let mut buffer = vec![0u8; tuple.calculate_binary_length()];
        tuple.write_binary(&mut buffer);
        assert_eq!(buffer, TEST_BINARY)
    }

    #[test]
    fn test_write_parse() {
        let tuple = Tuple {
            values: vec![
                None,
                Some(TupleValue::String("ab".to_string())),
                Some(TupleValue::Int(1))
            ]
        };
        let mut buffer = vec![0u8; tuple.calculate_binary_length()];
        tuple.write_binary(&mut buffer);
        let tuple = Tuple::parse_binary(vec![
            TupleValueType::Int,
            TupleValueType::String,
            TupleValueType::Int], &buffer);
        assert_eq!(tuple.values.len(), 3);
        assert_eq!(tuple.values[0], None);
        assert_eq!(tuple.values[1], Some(TupleValue::String("ab".to_string())));
        assert_eq!(tuple.values[2], Some(TupleValue::Int(1)));
    }
}