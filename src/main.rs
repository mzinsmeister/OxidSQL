mod storage;

use std::{fs::{create_dir_all, OpenOptions}, path::Path};

fn main() {
    let datafile_path = Path::new("./data/test.data");
    let prefix = datafile_path.parent().unwrap();
    create_dir_all(prefix).unwrap();
    // TODO: Enable Direct I/O (platform dependant flag)
    let mut db_file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(datafile_path)
        .unwrap();
}
