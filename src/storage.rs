use {
    byteorder::BigEndian,
};
use sled::IVec;
use byteorder::ReadBytesExt;
use std::io::{Cursor, BufRead};
use crate::catalog::{TableMeta, Type};
use crate::database::TupleValue;
use zerocopy::AsBytes;
use std::collections::BTreeMap;
use nom::number::Endianness::Big;
use std::convert::TryInto;

#[derive(Copy, Clone)]
struct DbRowKey<'a> {
    table_id: u32,
    index_id: u32,
    index_values: &'a Vec<TupleValue>,
}

pub const STRING_TERMINATOR: u8 = 0xff;

impl DbRowKey<'_> {
    fn calculate_as_bytes_len(&self) -> usize {
        8 + self.index_values.iter()
            .map(|v| match v {
                TupleValue::Long(_) => 8usize,
                TupleValue::UnsignedInt(_) => 4usize,
                TupleValue::String(s) => s.len(),
                TupleValue::Bytes(b) => b.len()
            })
            .sum::<usize>()
    }

    fn as_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::<u8>::new();
        self.append_as_bytes(&mut buffer);
        buffer
    }

    fn append_as_bytes(&self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&self.table_id.to_be_bytes());
        buffer.extend_from_slice(&self.index_id.to_be_bytes());
        append_serialized_primary_key_values(buffer, self.index_values);
    }
}

struct DbSingleValueKey<'a> {
    db_row_key: DbRowKey<'a>,
    column_id: u32
}

impl DbSingleValueKey<'_> {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.db_row_key.calculate_as_bytes_len() + 4);
        self.db_row_key.append_as_bytes(&mut bytes);
        bytes.extend_from_slice(&self.column_id.to_be_bytes());
        bytes
    }
}

pub struct ResultRowIterator {
    sled_iter: sled::Iter,
    table_meta: TableMeta
}

impl Iterator for ResultRowIterator {
    type Item = Result<Vec<TupleValue>, sled::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(dummy_row) = self.sled_iter.next() {
            let (dummy_key, _) = dummy_row.unwrap();
            assert_eq!(dummy_key[(dummy_key.len() - 4)..].as_bytes().read_u32::<BigEndian>().unwrap(), 0);
            let mut data_row: Vec<TupleValue> = Vec::with_capacity(self.table_meta.columns.len());
            let parsed_pk =
                deserialize_primary_key_values(&dummy_key[8..(dummy_key.len() - 4)], &self.table_meta);
            for column in self.table_meta.columns.values() {
                if let Some(pk_position) =
                self.table_meta.primary_key_column_ids.iter().enumerate()
                    .find(|(_, &c)| c == column.id)
                    .map(|(i, _)| i) {
                    data_row.push(parsed_pk[pk_position].to_owned());
                } else {
                    let (key, value) = self.sled_iter.next().unwrap().unwrap();
                    assert_eq!(key[(key.len() - 4)..].as_bytes().read_u32::<BigEndian>().unwrap(), column.id);
                    let tuple_value = match column.column_type {
                        Type::Long => TupleValue::Long(value.as_bytes().read_i64::<BigEndian>().unwrap()),
                        Type::UnsignedInteger =>
                            TupleValue::UnsignedInt(value.as_bytes().read_u32::<BigEndian>().unwrap()),
                        Type::Varchar(_) =>
                            TupleValue::String(String::from_utf8(value.to_vec()).unwrap()),
                        Type::Bytes(_) => TupleValue::Bytes(value.to_vec())
                    };
                    data_row.push(tuple_value);
                }
            }
            Some(Ok(data_row))
        } else {
            None
        }
    }
}

fn append_serialized_primary_key_values(buffer: &mut Vec<u8>, values: &Vec<TupleValue>) {
    for index_value in values {
        match index_value {
            TupleValue::Long(value) =>
                buffer.extend_from_slice(&value.to_be_bytes()),
            TupleValue::UnsignedInt(value) =>
                buffer.extend_from_slice(&value.to_be_bytes()),
            TupleValue::String(value) => {
                buffer.extend_from_slice(value.as_bytes());
                buffer.push(STRING_TERMINATOR);
            }
            TupleValue::Bytes(_) => panic!("BYTES Type currently not supported in PK")
        }
    }
}

fn deserialize_primary_key_values(bytes: &[u8], table_meta: &TableMeta) -> Vec<TupleValue> {
    let mut parsed_pk: Vec<TupleValue> =
        Vec::with_capacity(table_meta.primary_key_column_ids.len());
    let mut pk_cursor = Cursor::new(bytes);
    for pk_column_id in table_meta.primary_key_column_ids.iter() {
        let value = match table_meta.columns[pk_column_id].column_type {
            Type::Long => TupleValue::Long(pk_cursor.read_i64::<BigEndian>().unwrap()),
            Type::Varchar(_) => {
                let mut buffer = Vec::new();
                pk_cursor.read_until(0xff, &mut buffer).unwrap();
                buffer.pop();
                TupleValue::String(String::from_utf8(buffer).unwrap())
            }
            Type::UnsignedInteger =>
                TupleValue::UnsignedInt(pk_cursor.read_u32::<BigEndian>().unwrap()),
            Type::Bytes(_) => panic!("BYTES Type can't be used in PK at the moment"),
        };
        parsed_pk.push(value);
    }
    parsed_pk
}

pub struct SledStorageEngine {
    sled_db: sled::Db
}

impl SledStorageEngine {
    pub fn new() -> SledStorageEngine {
        let sled_db = sled::open("sled_test/testdb").expect("Database file could not be opened");
        SledStorageEngine { sled_db }
    }

    pub fn whole_row_primary_key_lookup(&self, table: TableMeta, index_values: &Vec<TupleValue>) -> Option<Result<Vec<TupleValue>, sled::Error>> {
        self.primary_key_scan(table, index_values).next()
    }

    pub fn primary_key_scan(&self, table: TableMeta, index_values: &Vec<TupleValue>) -> ResultRowIterator {
        let db_key = DbRowKey { table_id: table.id, index_id: 0, index_values };
        let db_key_bytes = db_key.as_bytes();
        ResultRowIterator {
            sled_iter: self.sled_db.scan_prefix(&db_key_bytes),
            table_meta: table
        }
    }

    pub fn primary_key_lookup(&self,
                          table_id: u32,
                          index_values: &Vec<TupleValue>,
                          columns: Vec<u32>) -> Vec<IVec> {
        let db_key = DbRowKey { table_id, index_id: 0, index_values };
        columns.iter()
            .map(|c_id| {
                let key = DbSingleValueKey {
                    db_row_key: db_key,
                    column_id: *c_id
                };
                self.sled_db.get(key.as_bytes()).unwrap().unwrap()
            })
            .collect()
    }

    pub fn secondary_unique_index_lookup(&self,
                              table: TableMeta,
                              index_id: u32,
                              index_values: &Vec<TupleValue>) -> Option<Vec<TupleValue>> {
        let key = DbRowKey { table_id: table.id, index_id, index_values };
        self.sled_db
            .get(key.as_bytes())
            .unwrap()
            .map(|b| deserialize_primary_key_values(b.as_bytes(), &table))
    }

    pub fn scan_table(&self, table: TableMeta) -> ResultRowIterator {
        let db_key: u64 = (table.id as u64) << 32;
        ResultRowIterator {
            sled_iter: self.sled_db.scan_prefix(&db_key.to_be_bytes()),
            table_meta: table
        }
    }

    pub fn write_row(&self, table: TableMeta, values: &Vec<TupleValue>) -> Result<(), sled::Error> {
        let mut column_values_map: BTreeMap<u32, &TupleValue> = BTreeMap::new();
        for (i, (c_id, _)) in table.columns.iter().enumerate() {
            column_values_map.insert(*c_id, &values[i]);
        }
        let pk_values: Vec<TupleValue> = table.primary_key_column_ids.iter()
            .map(|c_id| column_values_map[c_id].to_owned())
            .collect();
        let row_key = DbRowKey{
            table_id: table.id,
            index_id: 0,
            index_values: &pk_values
        };
        let mut batch = sled::Batch::default();
        let dummy_key = DbSingleValueKey{ db_row_key: row_key, column_id: 0 };
        batch.insert(dummy_key.as_bytes(), &[]);
        for (column_meta, non_pk_value) in table.columns.values()
            .filter(|c| !table.primary_key_column_ids.contains(&c.id))
            .map(|c| (c, column_values_map[&c.id])) {
            let key = DbSingleValueKey{ db_row_key: row_key, column_id: column_meta.id };
            batch.insert(key.as_bytes(), non_pk_value.to_bytes());
        }
        self.sled_db.apply_batch(batch)?;
        Ok(())
    }

    pub fn write_secondary_unique_index(&self, table_id: u32,
                                         index_id: u32,
                                         index_values: &Vec<TupleValue>,
                                         primary_key_value: &Vec<TupleValue>) {
        let row_key = DbRowKey{
            table_id,
            index_id,
            index_values
        };
        let mut value = Vec::<u8>::new();
        append_serialized_primary_key_values(&mut value, primary_key_value);
        self.sled_db.insert(row_key.as_bytes(), value);
    }

    pub fn get_and_increment_u32(&self,
                                 table_id: u32,
                                 column_id: u32,
                                 index_values: &Vec<TupleValue>) -> Option<u32> {
        let key = DbSingleValueKey{ db_row_key: DbRowKey {
            table_id,
            index_id: 0,
            index_values
        }, column_id };
        self.sled_db.fetch_and_update(key.as_bytes(), increment_u32_value).unwrap()
            .map(|v| Cursor::new(v.as_bytes()).read_u32::<BigEndian>().unwrap())
    }

    pub fn flush(&self) {
        self.sled_db.flush();
    }

    pub fn is_uninitialized(&self) -> bool {
        self.sled_db.is_empty()
    }
}

fn increment_u32_value(old: Option<&[u8]>) -> Option<Vec<u8>> {
    match old {
        Some(bytes) => {
            let array: [u8; 4] = bytes.try_into().unwrap();
            let number = u32::from_be_bytes(array);
            Some((number + 1).to_be_bytes().to_vec())
        }
        None => None,
    }
}
