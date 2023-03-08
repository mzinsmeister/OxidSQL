use std::sync::Arc;

use crate::storage::{buffer_manager::{BufferManager, BufferManagerError}, page::{PAGE_SIZE, PageId, Page, OffsetId}};

/*
Free Space Segment:

Free space segments are used to store free space information for other segments.
Each page is represented by a nibble (half a byte) which represents the amount of free space on the page.
The encoding of the free space is actually more of an ecoding of the used space. This way every unused page
by default is represented by a 0 as an empty page. The encoding isn't just linear, it is exponential. 
Because of this the pages will be able to put small tuples on pages with little space way better. And be sure
that that the pages will have at most 2-3% of their space wasted.

I decided to use the function min(15, ceil(2^(x / (MAXSIZE / 4)))) for now as the encoding function. The min is
just there in case the function would return something > 15 for values close to MAXSIZE because of numerics.

The way the the free space segment works is quite simple. We start off with a small header on the first page that
acts as a cache. For each size class we store the first page that has space for that size class. This way we can
quickly find a page (8 byte page number) that has space for a tuple and once we update a size we will just linearly
search through the pages until we find a new page for all the ones that were updated. This will usually not have
to read a lot of pages. Even in the unlikely case that we have to actually read through the whole inventory because
let's say a tuple on the very first page of a relation was deleted we will only have to read through 2 pages per
gigabyte of relation data because every page is just 4 bits. This could be improved either by implementing the
technique that postgres uses or by inserting a header every 100 pages or so that stores the smallest size class
for the next 100 pages so that you could probably skip most pages most of the times. In my opinion this should even
be enough for terrabytes of data since you will likely only read a few hundred at most in the worst case. In the
good/average case you will likely have all your pages towards the end of the segment so that you will only have to
read a handful of pages at most. Linear search is very cache friendly and therefore very fast anyway. We won't have
to do any calculations in the loop that does the linear search because the encoding function is monotonic.

If this ever gets too slow or we get problems with too many inserts getting contention on the free space inventory
or the pages they get we could group pages by mod 100 for example and go around them clockwise with every request
coming in.
*/
pub struct FreeSpaceSegment<B: BufferManager> {
    bm: Arc<B>,
    segment_id: u16,
    max_useable_space: usize
}

impl<B: BufferManager> FreeSpaceSegment<B> {
    pub fn new(segment_id: u16, max_useable_space: usize, bm: Arc<B>) -> Self {
        Self { bm, segment_id, max_useable_space }
    }

    fn write_cache_for_size_class(page: &mut Page, size_class: u8, page_nr: u64) {
        // Every page has at most size_class 15
        if size_class < 15 {
            page.data[size_class as usize * 8..(size_class as usize + 1) * 8]
            .copy_from_slice(&page_nr.to_le_bytes());
        }
    }

    fn read_cache_for_size_class(page: &Page, size_class: u8) -> u64 {
        if size_class >= 15 {
            return 0;
        }
        u64::from_le_bytes(page.data[size_class as usize * 8..(size_class as usize + 1) * 8].try_into().unwrap())
    }

    fn write_nibble(page: &mut Page, nibble_id: u64, value: u8) {
        let byte_id = nibble_id / 2;
        let nibble_offset = nibble_id % 2;
        if nibble_offset == 0 {
            page.data[byte_id as usize] = (page.data[byte_id as usize] & 0xF0) | value;
        } else {
            page.data[byte_id as usize] = (page.data[byte_id as usize] & 0x0F) | (value << 4);
        }
    }

    fn read_nibble(page: &Page, nibble_id: u64) -> u8 {
        let byte_id = nibble_id / 2;
        let nibble_offset = nibble_id % 2;
        if nibble_offset == 0 {
            page.data[byte_id as usize] & 0x0F
        } else {
            (page.data[byte_id as usize] & 0xF0) >> 4
        }
    }

    pub fn find_page(&self, size: u32) -> Result<OffsetId, BufferManagerError> {
        // Page must have at most one less than this size class except for empty pages
        let encoded = self.encode(self.max_useable_space - size as usize).max(1) - 1; 
        let page = self.bm.fix_page(PageId::new(self.segment_id, 0))?;
        let page_read = page.read().unwrap();
        Ok(Self::read_cache_for_size_class(&page_read, encoded))
    }

    pub fn update_page_size(&self, page_nr: u64, size: u32) -> Result<(), BufferManagerError> {
        // Page must have at most this size class
        let size_nibble = self.encode(self.max_useable_space - size as usize);
        let fsi_page = (page_nr + 15*16) / (PAGE_SIZE as u64 * 2);
        let nibble_id = (page_nr + 15*16) % (PAGE_SIZE as u64 * 2);
        let page = self.bm.fix_page(PageId::new(self.segment_id, 0))?; 
        let mut page_write = page.write().unwrap();
        let previous_size = Self::read_nibble(&page_write, nibble_id);
        Self::write_nibble(&mut page_write, nibble_id, size_nibble);
        if previous_size != size_nibble {
            page_write.make_dirty();
        }
        drop(page_write); // Fsi segment isn't guaranteed to actually give you up to date information anyway so we can unlock here
        drop(page);
        if previous_size != size_nibble && previous_size < 15 {
            let cache_page = self.bm.fix_page(PageId::new(self.segment_id, 0))?;
            let mut cache_page_write = cache_page.write().unwrap();
            let old_class_cached = Self::read_cache_for_size_class(&cache_page_write, previous_size);
            let new_class_cached = Self::read_cache_for_size_class(&cache_page_write, size_nibble);
            if old_class_cached == page_nr || new_class_cached > page_nr {
                // Need to update the cache by finding new pages for [previous_size, upper_bound)
                let mut upper_bound = previous_size;
                for i in previous_size..15 {
                    if Self::read_cache_for_size_class(&cache_page_write, i) == page_nr && i < size_nibble{
                        upper_bound = i + 1;
                    } else {
                        break;
                    }
                }
                if fsi_page == 0 {
                    for nibble in nibble_id + 1..(PAGE_SIZE as u64 * 2) {
                        let size = Self::read_nibble(&cache_page_write, nibble);
                        for current_size in (size.max(previous_size)..upper_bound).rev() {
                            upper_bound = current_size;
                            Self::write_cache_for_size_class(&mut cache_page_write, current_size, nibble as u64 - 15 * 16);
                            cache_page_write.make_dirty();
                        }
                        if upper_bound <= previous_size {
                            break;
                        }
                    }
                }

                let mut page_id = fsi_page.max(1);
                while upper_bound > previous_size {
                    let page = self.bm.fix_page(PageId::new(self.segment_id, page_id))?;
                    let page_read = page.read().unwrap();
                    for nibble in nibble_id + 1..PAGE_SIZE as u64 * 2 {
                        let size = Self::read_nibble(&page_read, nibble);
                        for current_size in (size.max(previous_size)..upper_bound).rev() {
                            upper_bound = current_size;
                            Self::write_cache_for_size_class(&mut cache_page_write, current_size, page_id * PAGE_SIZE as u64 * 2 + nibble as u64 - 15 * 16);
                            cache_page_write.make_dirty();
                        }
                        if upper_bound <= previous_size {
                            break;
                        }
                    }
                    page_id += 1;
                }
            }
            for i in size_nibble..15 {
                let cached_page = Self::read_cache_for_size_class(&cache_page_write, i);
                if cached_page > page_nr {
                    Self::write_cache_for_size_class(&mut cache_page_write, i, page_nr);
                    cache_page_write.make_dirty();
                } else {
                    break;
                }
            }
        }
        Ok(())
    }

    fn encode(&self, size: usize) -> u8 {
        let size = size as f64;
        let max_size = self.max_useable_space as f64;
        // min(15, ceil(2^(x / (MAXSIZE / 4))))
        let encoded = (2f64.powf(size / (max_size / 4f64))).ceil() as u8;
        encoded.min(15)
    }

    fn decode(&self, encoded: u8) -> usize {
        let encoded = encoded as f64;
        let max_size = self.max_useable_space as f64;
        // (MAXSIZE / 4) * log2(x)
        let decoded = (max_size / 4f64) * encoded.log2();
        decoded as usize
    }
}


#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::storage::{buffer_manager::mock::MockBufferManager, page::PAGE_SIZE};

    use super::FreeSpaceSegment;
    
    #[test]
    fn first_cache() {
        let bm = Arc::new(MockBufferManager::new(PAGE_SIZE));
        let testee = FreeSpaceSegment::new(0, 1000, bm);
        assert_eq!(testee.find_page(1000).unwrap(), 0);
        assert_eq!(testee.find_page(10).unwrap(), 0);
        assert_eq!(testee.find_page(200).unwrap(), 0);
        assert_eq!(testee.find_page(500).unwrap(), 0);
    }

    #[test]
    fn update_cache_completely() {
        let bm = Arc::new(MockBufferManager::new(PAGE_SIZE));
        let testee = FreeSpaceSegment::new(0, 1000, bm);
        testee.update_page_size(0, 0);
        assert_eq!(testee.find_page(1000).unwrap(), 1);
        assert_eq!(testee.find_page(10).unwrap(), 1);
        assert_eq!(testee.find_page(200).unwrap(), 1);
        assert_eq!(testee.find_page(500).unwrap(), 1);
    }

    #[test]
    fn update_cache_partly() {
        let bm = Arc::new(MockBufferManager::new(PAGE_SIZE));
        let testee = FreeSpaceSegment::new(0, 1000, bm);
        testee.update_page_size(0, 500);
        assert_eq!(testee.find_page(1000).unwrap(), 1);
        assert_eq!(testee.find_page(10).unwrap(), 0);
        assert_eq!(testee.find_page(200).unwrap(), 0);
        assert_eq!(testee.find_page(501).unwrap(), 1);
    }
}
