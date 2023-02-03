mod types;
mod storage;
mod access;

use std::{fs::create_dir_all, path::Path};

fn main() {
    let datafile_path = Path::new("./data/test.data");
    let prefix = datafile_path.parent().unwrap();
    create_dir_all(prefix).unwrap();
}
