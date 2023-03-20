//! Segment Validity Table (SVT).
use super::BitMap;
use crate::prelude::*;
use crate::util::DiskShadow;
use crate::util::Byte;
use errno::return_errno;

use std::convert::TryInto;
use std::fmt::{self, Debug};

/// Segment Validity Table.
/// Manage allocation/deallocation of data/index segments.
pub struct SVT {
    region_addr: Hba,
    num_segments: usize,
    segment_size: usize,
    num_allocated: usize,
    // A bitmap where each bit indicates whether a segment is valid
    bitmap: BitMap,
    disk_array: DiskArray<Byte>
}

impl SVT {
    pub fn new(
        region_addr: Hba,
        num_segments: usize,
        segment_size: usize,
        svt_boundary: HbaRange,
        disk: DiskView,
        key: Key
    ) -> Self {
        Self {
            region_addr,
            num_segments,
            segment_size,
            num_allocated: 0,
            bitmap: BitMap::repeat(true, num_segments),
            disk_array: DiskArray::new(DiskShadow::new(svt_boundary, disk), key)
        }
    }

    pub fn pick_avail_seg(&mut self) -> Result<Hba> {
        let avail_seg = self.find_avail_seg()?;
        self.invalidate_seg(avail_seg);

        self.num_allocated = self.num_allocated.saturating_add(1);
        Ok(avail_seg)
    }

    pub fn validate_seg(&mut self, seg_addr: Hba) {
        let idx = self.calc_bitmap_idx(seg_addr);
        self.bitmap.set(idx, true);

        self.num_allocated = self.num_allocated.saturating_sub(1);
    }

    pub fn num_segments(&self) -> usize {
        self.num_segments
    }

    pub fn num_allocated(&self) -> usize {
        self.num_allocated
    }

    fn find_avail_seg(&self) -> Result<Hba> {
        for (idx, bit) in self.bitmap.iter().enumerate() {
            if *bit {
                return Ok(Hba::from_byte_offset_aligned(idx * self.segment_size)?
                    + self.region_addr.to_raw());
            }
        }

        return_errno!(ENOMEM, "no available memory for segment");
    }

    fn invalidate_seg(&mut self, seg_addr: Hba) {
        let idx = self.calc_bitmap_idx(seg_addr);
        self.bitmap.set(idx, false);
    }

    fn calc_bitmap_idx(&self, seg_addr: Hba) -> usize {
        debug_assert!((seg_addr - self.region_addr.to_raw()).to_offset() % self.segment_size == 0);

        (seg_addr - self.region_addr.to_raw()).to_offset() / self.segment_size
    }

    fn calc_diskarray_offset(&self, bitmap_idx: usize) -> usize {
        bitmap_idx / BITMAP_UNIT
    }

    /// Calculate SVT blocks without shadow block
    pub fn calc_svt_blocks(num_segments: usize) -> usize {
        let nr_bits = num_segments;
        let nr_units = align_up(nr_bits, BITMAP_UNIT) / BITMAP_UNIT;
        DiskArray::<Byte>::total_blocks(nr_units)
    }

    /// Calculate space cost in bytes (with shadow blocks) on disk.
    pub fn calc_size_on_disk(num_segments: usize) -> usize {
        let nr_bits = num_segments;
        let nr_units = align_up(nr_bits, BITMAP_UNIT) / BITMAP_UNIT;
        let total_blocks = DiskArray::<Byte>::total_blocks_with_shadow(nr_units);
        total_blocks * BLOCK_SIZE
    }
}

impl SVT {
    pub async fn persist(&self) -> Result<()> {
        // TODO: write to disk_array
        self.disk_array.persist().await
    }

    pub async fn load(
        region_addr: Hba,
        num_segments: usize,
        segment_size: usize,
        svt_boundary: HbaRange,
        disk: DiskView,
        key: Key,
        shadow: bool
    ) -> Result<Self> {
        let disk_shadow = DiskShadow::load(svt_boundary, disk, shadow).await?;
        let mut disk_array = DiskArray::new(disk_shadow, key);

        let nr_bit_aligned = align_up(num_segments, BITMAP_UNIT);
        let nr_units = nr_bit_aligned / BITMAP_UNIT;
        let mut bitmap = BitMap::with_capacity(nr_bit_aligned);
        for offset in 0..nr_units {
            let value = disk_array.get(offset).await.ok_or(
                errno!(EIO, "svt disk_array read error")
            )?;
            bitmap.extend_from_raw_slice(&[value]);
        }
        bitmap.resize(num_segments, false);
        let num_allocated = bitmap.count_zeros();

        Ok(Self {
            region_addr,
            num_segments,
            segment_size,
            num_allocated,
            bitmap,
            disk_array
        })
    }

    pub async fn checkpoint(&mut self) -> Result<bool> {
        for offset in 0..self.bitmap.as_raw_slice().len() {
            let value = self.bitmap.as_raw_slice()[offset];
            self.disk_array.set(offset, value).await?;
        }
        self.disk_array.checkpoint().await
    }
}

impl Debug for SVT {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Checkpoint::SVT (Segment Validity Table)")
            .field("region_addr", &self.region_addr)
            .field("num_segments", &self.num_segments)
            .field("segment_size", &self.segment_size)
            .field("num_allocated", &self.num_allocated)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use block_device::mem_disk::MemDisk;

    #[test]
    fn test_svt_fns() {
        let disk_blocks = 64 * MiB / BLOCK_SIZE;
        let disk = Arc::new(MemDisk::new(disk_blocks).unwrap());
        let disk = DiskView::new_unchecked(disk);

        let region_addr = Hba::new(0);
        let num_segments = 8usize;
        let svt_blocks = SVT::calc_svt_blocks(num_segments);
        let svt_blocks_with_shadow = SVT::calc_size_on_disk(num_segments) / BLOCK_SIZE;
        assert_eq!(svt_blocks, 1);
        assert_eq!(svt_blocks_with_shadow, 4);

        let data_svt_start = region_addr + (NUM_BLOCKS_PER_SEGMENT * num_segments) as _;
        let data_svt_end = data_svt_start + (svt_blocks as _);
        let data_svt_boundary = HbaRange::new(data_svt_start..data_svt_end);
        let key = DefaultCryptor::gen_random_key();
        let mut data_svt = SVT::new(
            region_addr,
            num_segments,
            SEGMENT_SIZE,
            data_svt_boundary,
            disk.clone(),
            key.clone()
        );
        assert_eq!(data_svt.pick_avail_seg().unwrap(), region_addr);
        assert_eq!(
            data_svt.pick_avail_seg().unwrap(),
            region_addr + Hba::from_byte_offset(1 * SEGMENT_SIZE).to_raw()
        );

        let index_svt_start = data_svt_end;
        let index_svt_end = index_svt_start + 1;
        let index_svt_boundary = HbaRange::new(index_svt_start..index_svt_end);
        let mut index_svt = SVT::new(
            region_addr,
            num_segments,
            INDEX_SEGMENT_SIZE,
            index_svt_boundary,
            disk.clone(),
            key.clone()
        );

        let index_seg_hba = Hba::from_byte_offset(1 * INDEX_SEGMENT_SIZE).to_raw();
        assert_eq!(index_svt.pick_avail_seg().unwrap(), region_addr);
        assert_eq!(
            index_svt.pick_avail_seg().unwrap(),
            region_addr + index_seg_hba
        );
        index_svt.validate_seg(region_addr + index_seg_hba);
        assert_eq!(
            index_svt.pick_avail_seg().unwrap(),
            region_addr + index_seg_hba
        );
    }

    #[test]
    fn test_svt_persist_load() -> Result<()> {
        async_rt::task::block_on(async move {
            let disk_blocks = 64 * MiB / BLOCK_SIZE;
            let disk = Arc::new(MemDisk::new(disk_blocks).unwrap());
            let disk = DiskView::new_unchecked(disk);

            let region_addr = Hba::new(0);
            let num_segments = 8usize;
            let svt_addr = region_addr + (NUM_BLOCKS_PER_SEGMENT * num_segments) as _;
            let svt_blocks = SVT::calc_svt_blocks(num_segments);
            let boundary = HbaRange::new(
                svt_addr..svt_addr + (svt_blocks as _)
            );
            let key = DefaultCryptor::gen_random_key();

            let mut svt = SVT::new(
                region_addr,
                num_segments,
                SEGMENT_SIZE,
                boundary.clone(),
                disk.clone(),
                key.clone()
            );

            svt.invalidate_seg(region_addr);
            assert_eq!(svt.bitmap[0], false);

            svt.checkpoint().await?;

            let mut loaded_svt = SVT::load(
                region_addr,
                num_segments,
                SEGMENT_SIZE,
                boundary.clone(),
                disk.clone(),
                key.clone(),
                true
            ).await?;
            assert_eq!(
                loaded_svt.pick_avail_seg().unwrap(),
                region_addr + Hba::from_byte_offset(1 * SEGMENT_SIZE).to_raw()
            );
            Ok(())
        })
    }
}
