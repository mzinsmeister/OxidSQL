use std::{fs::{create_dir_all, OpenOptions}, path::Path, io::Write};

fn main() {
    let datafile_path = Path::new("./data/test.data");
    let prefix = datafile_path.parent().unwrap();
    create_dir_all(prefix).unwrap();
    let mut db_file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(datafile_path)
        .unwrap();

    db_file.write("Hello W0rld".as_bytes()).unwrap();
}
