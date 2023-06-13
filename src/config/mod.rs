

pub struct DbConfig {
    pub n_threads: u32
}

impl DbConfig {
    pub fn new() -> DbConfig {
        // Read environment variable 'OXIDSQL_NTHREADS'
        // If not set, use 4 as default
        let n_threads = match std::env::var("OXIDSQL_NTHREADS") {
            Ok(val) => val.parse::<u32>().unwrap_or(4),
            Err(_) => 4
        };
        DbConfig {
            n_threads
        }
    }
}
