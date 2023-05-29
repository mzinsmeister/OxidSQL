use std::ops::{Deref, DerefMut};

// TODO: Try playing around with #[repr(align(512))] to see whether that could
// achieve the result too.

#[cfg(any(not(unix), feature="allow-unsafe"))]
pub type AlignedSlice = Box<[u8]>;

#[cfg(all(unix, feature="allow-unsafe"))]
// Sadly the Rust standard library doesn't provide a way to get a standard way to get
// aligned memory so we have to use posix_memalign from libc. :-(
// We could overallocate and then use a subslice which wastes memory and isn't nice either.
pub fn alligned_slice(size: usize, align: usize) -> Box<[u8]> {
    use libc::c_void;

    unsafe {
        let mut ptr: *mut c_void = std::ptr::null_mut();
        libc::posix_memalign(std::ptr::addr_of_mut!(ptr), align, size);
        Box::<[u8]>::from_raw(std::slice::from_raw_parts_mut(ptr as *mut u8, size))
    }
}


/// Safe representation of an aligned slice.
/// Likely uses more memory (will waste something like 3% for 16kb pages)
#[cfg(all(unix, not(feature="allow-unsafe")))]
#[derive(Debug, Clone)]
pub struct AlignedSlice{
    padded_slice: Box<[u8]>,
    start_padding: usize,
    len: usize,
}

#[cfg(all(unix, not(feature="allow-unsafe")))]
impl Deref for AlignedSlice {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.padded_slice[self.start_padding..self.start_padding + self.len]
    }
}

#[cfg(all(unix, not(feature="allow-unsafe")))]
impl DerefMut for AlignedSlice {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.padded_slice[self.start_padding..self.start_padding + self.len]
    }
}

pub trait EmptyAlignedSlice {
    fn new_empty() -> Self;
}

impl EmptyAlignedSlice for AlignedSlice {
    #[cfg(all(unix, not(feature="allow-unsafe")))]
    fn new_empty() -> Self {
        AlignedSlice {
            padded_slice: Box::new([]),
            start_padding: 0,
            len: 0,
        }
    }

    #[cfg(any(not(unix), feature="allow-unsafe"))]
    fn new_empty() -> AlignedSlice {
        Box::new([]) // This doesn't actually do any allocation
    }
}

#[cfg(all(unix, not(feature="allow-unsafe")))]
// Safe variant of the above function. Likely uses more memory.
// We could overallocate and then use a subslice which wastes memory and isn't nice either.
pub fn alligned_slice(size: usize, align: usize) -> AlignedSlice {
    let mut padded_slice = vec![0u8; size + align - 1].into_boxed_slice();
    let start_padding = (align - ((padded_slice.as_ptr() as usize) % align)) % align;
    AlignedSlice {
        padded_slice,
        start_padding,
        len: size,
    }
}
