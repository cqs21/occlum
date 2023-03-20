//! Data Segment Table (DST).
use super::BitMap;
use crate::prelude::*;
use crate::util::DiskShadow;
use crate::util::Byte;

use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::fmt::{self, Debug};

/// Data Segment Table.
/// Manage per-segment metadata of data segments (valid block bitmap).
pub struct DST {
    region_addr: Hba,
    num_segments: usize,
    // K: Data segment start hba
    // V: (A bitmap where each bit indicates whether a block within is valid, Number of valid blocks)
    bitmaps: HashMap<Hba, (BitMap, usize)>,
    // Idx: Number of valid blocks
    // V: Set of data segment start hba
    validity_tracker: [HashSet<Hba>; NUM_BLOCKS_PER_SEGMENT + 1],
    disk_array: DiskArray<Byte>
}
// TODO: Adapt threaded logging

impl DST {
    pub fn new(
        data_region_addr: Hba,
        num_data_segments: usize,
        dst_boundary: HbaRange,
        disk: DiskView,
        key: Key
    ) -> Self {
        Self {
            region_addr: data_region_addr,
            num_segments: num_data_segments,
            bitmaps: HashMap::with_capacity(num_data_segments),
            validity_tracker: vec![HashSet::new(); NUM_BLOCKS_PER_SEGMENT + 1]
                .try_into()
                .unwrap(),
            disk_array: DiskArray::new(
                DiskShadow::new(dst_boundary, disk),
                key
            )
        }
    }

    pub fn update_validity(&mut self, blocks: &[Hba], is_valid: bool) {
        blocks.iter().for_each(|&block_addr| {
            // Get segment addr according to block addr
            let seg_addr = self.calc_seg_addr(block_addr);
            // Get per-segment bitmap
            let (block_bitmap, num_valid) = self.bitmaps.get_mut(&seg_addr).unwrap();

            // Invalid the block, update bitmap and validity counter
            let idx = (block_addr - seg_addr.to_raw()).to_raw() as usize;
            debug_assert!(block_bitmap[idx] != is_valid);
            block_bitmap.set(idx, is_valid);
            self.validity_tracker[*num_valid].remove(&seg_addr);
            *num_valid = {
                if is_valid {
                    *num_valid + 1
                } else {
                    *num_valid - 1
                }
            };
            self.validity_tracker[*num_valid].insert(seg_addr);
        })
    }

    pub fn validate_or_insert(&mut self, segment_addr: Hba) {
        let seg_cap = NUM_BLOCKS_PER_SEGMENT;
        let replaced = self
            .bitmaps
            .insert(segment_addr, (BitMap::repeat(true, seg_cap), seg_cap));
        if let Some((_, num_valid)) = replaced {
            self.validity_tracker[num_valid].remove(&segment_addr);
        }
        self.validity_tracker[seg_cap].insert(segment_addr);
    }

    /// Pick a victim segment.
    pub fn pick_victim(&self) -> Option<VictimSegment> {
        // Pick the victim which has most invalid blocks
        for (num_valid, seg_set) in self.validity_tracker.iter().enumerate() {
            if !seg_set.is_empty() {
                for seg_addr in seg_set {
                    let (block_bitmap, valid_cnt) = self.bitmaps.get(seg_addr).unwrap();
                    debug_assert!(num_valid == *valid_cnt);
                    return Some(VictimSegment::new(
                        *seg_addr,
                        Self::collect_blocks(*seg_addr, block_bitmap, true),
                    ));
                }
            }
        }
        None
    }

    #[allow(unused)]
    pub fn alloc_blocks<const N: usize>(&mut self) -> Result<[Hba; N]> {
        let mut block_vec = Vec::with_capacity(N);
        let mut updated_segs = vec![];
        let mut updated_blocks = vec![];

        'outer: for seg_set in self.validity_tracker.iter() {
            if !seg_set.is_empty() {
                for seg_addr in seg_set.iter() {
                    let (block_bitmap, valid_cnt) = self.bitmaps.get(seg_addr).unwrap();
                    if block_vec.len() + (NUM_BLOCKS_PER_SEGMENT - valid_cnt) <= N {
                        block_vec.extend_from_slice(&Self::collect_blocks(
                            *seg_addr,
                            block_bitmap,
                            false,
                        ));
                        updated_segs.push(*seg_addr);
                    } else {
                        let invalid_blocks = &Self::collect_blocks(*seg_addr, block_bitmap, false)
                            [..N - block_vec.len()];
                        block_vec.extend_from_slice(invalid_blocks);
                        updated_blocks.extend_from_slice(invalid_blocks);
                        break 'outer;
                    }
                }
            }
        }
        updated_segs
            .iter()
            .for_each(|seg| self.validate_or_insert(*seg));
        self.update_validity(&updated_blocks, true);

        debug_assert!(block_vec.len() == N);
        block_vec.sort();
        Ok(block_vec.try_into().map_err(|_| EINVAL)?)
    }

    fn count_num_blocks(block_bitmap: &BitMap, is_valid: bool) -> usize {
        block_bitmap.iter().filter(|bit| *bit == is_valid).count()
    }

    fn collect_blocks(segment_addr: Hba, block_bitmap: &BitMap, is_valid: bool) -> Vec<Hba> {
        let mut invalid_blocks = Vec::new();
        block_bitmap.iter().enumerate().for_each(|(idx, bit)| {
            if *bit == is_valid {
                invalid_blocks.push(segment_addr + idx as _)
            }
        });
        invalid_blocks
    }

    fn from(
        region_addr: Hba,
        num_segments: usize,
        bitmaps: HashMap<Hba, BitMap>,
        disk_array: DiskArray<Byte>
    ) -> Self {
        let mut validity_tracker: [HashSet<Hba>; NUM_BLOCKS_PER_SEGMENT + 1] =
            vec![HashSet::new(); NUM_BLOCKS_PER_SEGMENT + 1]
                .try_into()
                .unwrap();
        let bitmaps = bitmaps
            .into_iter()
            .map(|(seg_addr, block_bitmap)| {
                let num_valid = Self::count_num_blocks(&block_bitmap, true);
                validity_tracker[num_valid].insert(seg_addr);
                (seg_addr, (block_bitmap, num_valid))
            })
            .collect();
        Self {
            region_addr,
            num_segments,
            bitmaps,
            validity_tracker,
            disk_array
        }
    }

    fn calc_seg_addr(&self, block_addr: Hba) -> Hba {
        Hba::new(align_down(
            (block_addr - self.region_addr.to_raw()).to_raw() as _,
            NUM_BLOCKS_PER_SEGMENT,
        ) as _)
            + self.region_addr.to_raw()
    }

    fn calc_diskarray_offset(&self, block_addr: Hba) -> usize {
        let block_offset = (block_addr.to_raw() - self.region_addr.to_raw()) as usize;
        block_offset / BITMAP_UNIT
    }

    /// Calculate DST blocks without shadow block
    pub fn calc_dst_blocks(num_data_segments: usize) -> usize {
        let nr_bits = num_data_segments * NUM_BLOCKS_PER_SEGMENT;
        let nr_units = align_up(nr_bits, BITMAP_UNIT) / BITMAP_UNIT;
        DiskArray::<Byte>::total_blocks(nr_units)
    }

    /// Calculate space cost in bytes (with shadow blocks) on disk.
    pub fn calc_size_on_disk(num_data_segments: usize) -> usize {
        let nr_bits = num_data_segments * NUM_BLOCKS_PER_SEGMENT;
        let nr_units = align_up(nr_bits, BITMAP_UNIT) / BITMAP_UNIT;
        let total_blocks = DiskArray::<Byte>::total_blocks_with_shadow(nr_units);
        total_blocks * BLOCK_SIZE
    }
}

/// Victim segment.
pub struct VictimSegment {
    segment_addr: Hba,
    valid_blocks: Vec<Hba>,
}

impl VictimSegment {
    pub fn new(segment_addr: Hba, valid_blocks: Vec<Hba>) -> Self {
        Self {
            segment_addr,
            valid_blocks,
        }
    }

    pub fn segment_addr(&self) -> Hba {
        self.segment_addr
    }

    pub fn valid_blocks(&self) -> &Vec<Hba> {
        &self.valid_blocks
    }
}

impl DST {
    pub async fn persist(&self) -> Result<()> {
        self.disk_array.persist().await
    }

    pub async fn load(
        data_region_addr: Hba,
        num_data_segments: usize,
        dst_boundary: HbaRange,
        disk: DiskView,
        key: Key,
        shadow: bool
    ) -> Result<Self> {
        let disk_shadow = DiskShadow::load(dst_boundary, disk, shadow).await?;
        let mut disk_array = DiskArray::new(disk_shadow, key);

        let mut bitmaps = HashMap::with_capacity(num_data_segments);
        for i in 0..num_data_segments {
            let seg_addr = data_region_addr + (i as _);
            let mut bitmap = BitMap::with_capacity(NUM_BLOCKS_PER_SEGMENT);
            for j in 0..(NUM_BLOCKS_PER_SEGMENT / BITMAP_UNIT) {
                let offset = i * (NUM_BLOCKS_PER_SEGMENT / BITMAP_UNIT) + j;
                let value = disk_array.get(offset).await.ok_or(
                    errno!(EIO, "dst disk_array read error")
                )?;
                bitmap.extend_from_raw_slice(&[value]);
            }
            bitmaps.insert(seg_addr, bitmap);
        }

        Ok(DST::from(
            data_region_addr,
            num_data_segments,
            bitmaps,
            disk_array
        ))
    }

    pub async fn checkpoint(&mut self) -> Result<bool> {
        let bitmaps: HashMap<Hba, BitMap> = self.bitmaps
                .iter()
                .map(|(seg_addr, (bitmap, _))|(*seg_addr, bitmap.clone()))
                .collect();
        for (seg_addr, bitmap) in bitmaps.iter() {
            for i in 0..bitmap.as_raw_slice().len() {
                let block_addr = *seg_addr + ((i * BITMAP_UNIT) as _);
                let offset = self.calc_diskarray_offset(block_addr);
                let value = bitmap.as_raw_slice()[i];
                self.disk_array.set(offset, value).await;
            }
        }
        self.disk_array.checkpoint().await
    }
}

impl Debug for DST {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Checkpoint::DST (Data Segment Table)")
            .field("region_addr", &self.region_addr)
            .field("num_segments", &self.num_segments)
            .field("bitmaps_capacity", &self.bitmaps.capacity())
            .finish()
    }
}

impl Debug for VictimSegment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VictimSegment")
            .field("segment_addr", &self.segment_addr)
            .field("num_valid_blocks", &self.valid_blocks.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use block_device::mem_disk::MemDisk;

    #[test]
    fn test_dst_fns() {
        let disk_blocks = 64 * MiB / BLOCK_SIZE;
        let disk = Arc::new(MemDisk::new(disk_blocks).unwrap());
        let disk = DiskView::new_unchecked(disk);

        let region_addr = Hba::new(0);
        let num_segments = 10usize;
        let dst_blocks = DST::calc_dst_blocks(num_segments);
        let dst_blocks_with_shadow = DST::calc_size_on_disk(num_segments) / BLOCK_SIZE;
        assert_eq!(dst_blocks, 1);
        assert_eq!(dst_blocks_with_shadow, 4);

        let dst_start = region_addr + (NUM_BLOCKS_PER_SEGMENT * num_segments) as _;
        let dst_end = dst_start + (dst_blocks as _);
        let dst_boundary = HbaRange::new(dst_start..dst_end);
        let key = DefaultCryptor::gen_random_key();
        let mut dst = DST::new(
            region_addr,
            num_segments,
            dst_boundary,
            disk,
            key
        );

        let seg1 = Hba::new(0);
        let seg2 = Hba::from_byte_offset(1 * SEGMENT_SIZE);
        dst.validate_or_insert(seg1);
        dst.validate_or_insert(seg2);

        let invalid_blocks = [seg1, seg1 + 1 as _];
        dst.update_validity(&invalid_blocks, false);
        dst.update_validity(&[seg2], false);

        let victim = dst.pick_victim().unwrap();
        assert_eq!(victim.segment_addr, seg1);
        assert_eq!(victim.valid_blocks.len(), NUM_BLOCKS_PER_SEGMENT - 2);

        dst.update_validity(&[seg2 + 1 as _, seg2 + 5 as _], false);

        let victim = dst.pick_victim().unwrap();
        assert_eq!(victim.segment_addr, seg2);
        assert_eq!(victim.valid_blocks[0], seg2 + 2 as _);

        let alloc_blocks = dst.alloc_blocks::<5>().unwrap();
        assert_eq!(alloc_blocks[0], seg1);
        assert_eq!(alloc_blocks[4], seg2 + 5 as _);
    }

    #[test]
    fn test_dst_persist_load() -> Result<()> {
        async_rt::task::block_on(async move {
            let disk_blocks = 64 * MiB / BLOCK_SIZE;
            let disk = Arc::new(MemDisk::new(disk_blocks).unwrap());
            let disk = DiskView::new_unchecked(disk);

            let region_addr = Hba::new(0);
            let num_segments = 10usize;
            let dst_start = region_addr + (NUM_BLOCKS_PER_SEGMENT * num_segments) as _;
            let dst_end = dst_start + (DST::calc_dst_blocks(num_segments) as _);
            let key = DefaultCryptor::gen_random_key();
            let mut dst = DST::new(
                region_addr,
                num_segments,
                HbaRange::new(dst_start..dst_end),
                disk.clone(),
                key.clone()
            );

            let blocks = [region_addr, region_addr + 1 as _];
            dst.validate_or_insert(region_addr);
            dst.update_validity(&blocks, false);

            let (bitmap, num_valid) = dst.bitmaps.get(&region_addr).unwrap();
            let value = bitmap.as_raw_slice()[0];
            assert_eq!(value, 0b1111_1100);
            assert_eq!(*num_valid, 1022usize);

            dst.checkpoint().await?;

            let loaded_dst = DST::load(
                region_addr,
                num_segments,
                HbaRange::new(dst_start..dst_end),
                disk,
                key,
                true
            ).await?;

            let (bitmap, num_valid) = loaded_dst.bitmaps.get(&region_addr).unwrap();
            assert_eq!(value, 0b1111_1100);
            assert_eq!(*num_valid, 1022usize);
            Ok(())
        })
    }
}
