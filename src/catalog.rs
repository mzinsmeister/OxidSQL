use crate::storage::SledStorageEngine;
use zerocopy::AsBytes;
use byteorder::{ReadBytesExt, BigEndian};
use std::io::Cursor;
use nom::lib::std::collections::BTreeMap;
use crate::database::TupleValue;
use crate::analyze::TableDefinition;

pub struct Catalog {
    storage_engine: SledStorageEngine,
    system_tables_meta: BTreeMap<u32, TableMeta>
}

impl Catalog {
    pub fn new(storage_engine: SledStorageEngine) -> Catalog {
        Catalog {
            storage_engine,
            system_tables_meta: make_system_tables_meta_map()
        }
    }

    pub fn init_catalog(&self) {
        if self.storage_engine.is_uninitialized() {
            // Init Sequence for Table IDs
            let values = vec![TupleValue::UnsignedInt(0), TupleValue::UnsignedInt(1000)];
            self.storage_engine.write_row(self.system_tables_meta[&2].to_owned(), &values);
        }
    }

    pub fn lookup_table_id_by_name(&self, name: &str) -> Option<u32> {
        self.storage_engine
            .secondary_unique_index_lookup(self.system_tables_meta[&0].to_owned(), 1,
                                           &vec![TupleValue::String(name.to_uppercase())])
            .map(|e| e[0].to_owned().get_as_unsigned_int())
    }

    pub fn lookup_column_id_by_name(&self, table_id: u32, column_name: &str) -> Option<u32> {
        let key_vec = vec![
            TupleValue::UnsignedInt(table_id),
            TupleValue::String(column_name.to_uppercase())
        ];
        let found_id =
            self.storage_engine.secondary_unique_index_lookup(self.system_tables_meta[&1].to_owned(),
                                                              1, &key_vec);
        found_id.map(|e| e[1].to_owned().get_as_unsigned_int())
    }

    pub fn find_table_by_id(&self, id: u32) -> Option<TableMeta> {
        let result = self.storage_engine
            .whole_row_primary_key_lookup(self.system_tables_meta[&0].to_owned(),
                                          &vec![TupleValue::UnsignedInt(id)]);
        if result.is_none() {
            return None
        }
        let columns = self.find_all_columns_for_table(id);
        let column_name_index: BTreeMap<String, u32> = columns.iter()
            .map(|(id, c)| (c.name.to_owned(), *id))
            .collect();
        let mut result_vec = result.unwrap().unwrap();
        result_vec.pop(); // we don't need the table id
        Some(TableMeta {
            id,
            primary_key_column_ids: read_primary_key_ids(&result_vec.pop().unwrap().get_as_bytes()),
            name: result_vec.pop().unwrap().get_as_string(),
            columns,
            column_name_index
        })
    }

    fn find_all_columns_for_table(&self, table_id: u32) -> BTreeMap<u32, ColumnMeta> {
        let result = self.storage_engine
            .primary_key_scan(self.system_tables_meta[&1].to_owned(), &vec![TupleValue::UnsignedInt(table_id)]);
        let mut result_map = BTreeMap::new();
        for row_result in result {
            let mut row = row_result.unwrap();
            let column_id = row.pop().unwrap().get_as_unsigned_int();
            row.pop(); // Table ID which we don't need
            let column_meta = ColumnMeta {
                id: column_id,
                column_type: Type::from_bytes(&row.pop().unwrap().get_as_bytes()),
                name: row.pop().unwrap().get_as_string()
            };
            result_map.insert(column_id, column_meta);
        }
        result_map
    }

    pub fn find_column_by_id(&self, table_id: u32, column_id: u32) -> Option<ColumnMeta> {
        let result = self.storage_engine
            .whole_row_primary_key_lookup(self.system_tables_meta[&1].to_owned(),
                                          &vec![
                                              TupleValue::UnsignedInt(table_id),
                                              TupleValue::UnsignedInt(column_id)]);
        if result.is_none() {
            return None;
        }
        let mut result = result.unwrap().unwrap();
        // We don't need column and table id
        result.pop();
        result.pop();
        Some(ColumnMeta {
            id: column_id,
            column_type: Type::from_bytes(&result.pop().unwrap().get_as_bytes()),
            name: result.pop().unwrap().get_as_string()
        })
    }

    pub fn create_table(&self, table_definition: &TableDefinition) {
        let table_id = self.generate_table_id();
        //TODO: Do this atomically
        // (maybe... Because we insert columns first it shouldn't be a problem at the moment)
        let mut current_column_id = 1;
        let mut pk_definition: Vec<u8> = Vec::new();
        for (i, column) in table_definition.columns.iter().enumerate() {
            let row = vec![TupleValue::String(column.name.to_uppercase()),
                           TupleValue::Bytes(column.data_type.as_bytes()),
                           TupleValue::UnsignedInt(table_id),
                           TupleValue::UnsignedInt(current_column_id)];
            self.storage_engine.write_row(self.system_tables_meta[&1].to_owned(), &row);
            let secondary_index_key = vec![TupleValue::UnsignedInt(table_id),
                                           TupleValue::String(column.name.to_uppercase())];
            let primary_key_value = vec![TupleValue::UnsignedInt(table_id),
                                             TupleValue::UnsignedInt(current_column_id)];
            self.storage_engine.write_secondary_unique_index(
                1, 1, &secondary_index_key, &primary_key_value);
            if table_definition.primary_key.contains(&i) {
                pk_definition.extend_from_slice(current_column_id.to_be_bytes().as_bytes());
            }
            current_column_id += 1;
        }
        let table_row = vec![TupleValue::String(table_definition.name.to_uppercase()),
                             TupleValue::Bytes(pk_definition),
                             TupleValue::UnsignedInt(table_id)];
        self.storage_engine.write_row(self.system_tables_meta[&0].to_owned(), &table_row);
        self.storage_engine.write_secondary_unique_index(
            0, 1, &vec![TupleValue::String(table_definition.name.to_uppercase())],
            &vec![TupleValue::UnsignedInt(table_id)]);
    }

    fn generate_table_id(&self) -> u32 {
        self.storage_engine.get_and_increment_u32(
            2, 2, &vec![TupleValue::UnsignedInt(0)]).unwrap()
    }
}

fn make_system_tables_meta_map() -> BTreeMap<u32, TableMeta> {
    let tables_table_meta: TableMeta = TableMeta {
        id: 0,
        name: String::from("TABLE"),
        columns: {
            let mut columns_map = BTreeMap::new();
            columns_map.insert(1, ColumnMeta {
                id: 1,
                name: String::from("NAME"),
                column_type: Type::Varchar(u16::MAX)
            });
            columns_map.insert(2, ColumnMeta {
                id: 2,
                name: String::from("PRIMARY_KEY_COLUMN_IDS"),
                column_type: Type::Bytes(u16::MAX)
            });
            columns_map.insert(3, ColumnMeta {
                id: 3,
                name: String::from("ID"),
                column_type: Type::UnsignedInteger
            });
            columns_map
        },
        // We don't need to lookup anything by name here currently
        column_name_index: BTreeMap::new(),
        primary_key_column_ids: vec![3]
    };
    let columns_table_meta: TableMeta = TableMeta {
        id: 1,
        name: String::from("COLUMN"),
        columns: {
            let mut columns_map = BTreeMap::new();
            columns_map.insert(1, ColumnMeta {
                id: 1,
                name: String::from("NAME"),
                column_type: Type::Varchar(u16::MAX)
            });
            columns_map.insert(2, ColumnMeta {
                id: 2,
                name: String::from("TYPE"),
                column_type: Type::Bytes(u16::MAX)
            });
            columns_map.insert(3, ColumnMeta {
                id: 3,
                name: String::from("TABLE_ID"),
                column_type: Type::UnsignedInteger
            });
            columns_map.insert(4, ColumnMeta {
                id: 4,
                name: String::from("ID"),
                column_type: Type::UnsignedInteger
            });
            /*columns_map.insert(1, ColumnMeta {
                id: 4,
                name: String::from("IS_NULLABLE"),
                column_type: Type::BYTES(1)
            });*/
            columns_map
        },
        // We don't need to lookup anything by name here currently
        column_name_index: BTreeMap::new(),
        primary_key_column_ids: vec![3, 4]
    };
    let sequences_table_meta: TableMeta = TableMeta {
        id: 2,
        name: String::from("INT_SEQUENCE"),
        columns: {
            let mut columns_map = BTreeMap::new();
            columns_map.insert(1, ColumnMeta {
                id: 1,
                name: String::from("ID"),
                column_type: Type::UnsignedInteger
            });
            columns_map.insert(2, ColumnMeta {
                id: 2,
                name: String::from("VALUE"),
                column_type: Type::UnsignedInteger
            });
            columns_map
        },
        // We don't need to lookup anything by name here currently
        column_name_index: BTreeMap::new(),
        primary_key_column_ids: vec![1]
    };
    let mut system_tables_meta = BTreeMap::new();
    system_tables_meta.insert(0, tables_table_meta);
    system_tables_meta.insert(1, columns_table_meta);
    system_tables_meta.insert(2, sequences_table_meta);
    system_tables_meta
}

fn read_primary_key_ids(bytes: &[u8]) -> Vec<u32> {
    let number_of_ints = bytes.len() / 4;
    let mut vec = Vec::with_capacity(number_of_ints);
    for i in 0usize..number_of_ints {
        vec.push(Cursor::new(&bytes[(i*4)..((i+1)*4)]).read_u32::<BigEndian>().unwrap())
    }
    vec
}

#[derive(Debug, Clone)]
pub struct TableMeta {
    pub id: u32,
    pub name: String,
    pub columns: BTreeMap<u32, ColumnMeta>,
    pub column_name_index: BTreeMap<String, u32>,
    pub primary_key_column_ids: Vec<u32>
}

#[derive(Debug, Clone)]
pub struct ColumnMeta {
    pub id: u32,
    pub name: String,
    pub column_type: Type,
}

// Column Types. UnsignedInteger and BYTES are currently only for system catalog tables
#[derive(Debug, Clone)]
pub enum Type {
    Long,
    UnsignedInteger,
    Varchar(u16),
    Bytes(u16)
}

impl Type {
    fn as_bytes(&self) -> Vec<u8> {
        match self {
            Type::Long => Vec::from(1u16.to_be_bytes()),
            Type::Varchar(len) => {
                let mut vec = Vec::from(2u16.to_be_bytes());
                vec.extend_from_slice(len.to_be_bytes().as_bytes());
                vec
            }
            Type::UnsignedInteger => Vec::from(3u16.to_be_bytes()),
            Type::Bytes(len) => {
                let mut vec = Vec::from(4u16.to_be_bytes());
                vec.extend_from_slice(len.to_be_bytes().as_bytes());
                vec
            }
        }
    }

    fn from_bytes(bytes: &[u8]) -> Type {
        let type_code: u16 = Cursor::new(&bytes[0..2]).read_u16::<BigEndian>().unwrap();
        match type_code {
            1 => Type::Long,
            2 => Type::Varchar(Cursor::new(&bytes[2..4]).read_u16::<BigEndian>().unwrap()),
            _ => panic!("Type {} unknown", type_code)
        }
    }
}
