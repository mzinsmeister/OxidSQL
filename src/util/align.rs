
#[cfg(unix)]
// Sadly the Rust standard library doesn't provide a way to get alligned memory
// We could overallocate and then use a subslice which wastes memory and isn't nice either
pub fn alligned_slice(size: usize, align: usize) -> Box<[u8]> {
    use libc::c_void;

    unsafe {
        let mut ptr: *mut c_void = std::ptr::null_mut();
        libc::posix_memalign(std::ptr::addr_of_mut!(ptr), align, size);
        Box::<[u8]>::from_raw(std::slice::from_raw_parts_mut(ptr as *mut u8, size))
    }
}