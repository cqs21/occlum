//! Block Index Table Catalog (BITC).
use crate::index::bit::disk_bit::DiskBit;
use crate::index::bit::{Bit, BitId, BIT_SIZE};
use crate::index::LsmLevel;
use crate::prelude::*;
use crate::util::DiskShadow;
use super::SVT;

use std::convert::TryInto;
use std::fmt::{self, Debug};

/// Block Index Table Catalog.
/// Manage all BITs in index (lsm tree).
pub struct BITC {
    max_bit_version: u32,
    l0_bit: Option<Bit>,
    l1_bits: Vec<Bit>,
    index_region_addr: Hba,
    disk_array: DiskArray<DiskBit>
}

impl BITC {
    pub fn new(
        index_region_addr: Hba,
        bitc_boundary: HbaRange,
        disk: DiskView,
        key: Key
    ) -> Self {
        Self {
            max_bit_version: 0,
            l0_bit: None,
            l1_bits: Vec::new(),
            index_region_addr,
            disk_array: DiskArray::new(DiskShadow::new(bitc_boundary, disk), key)
        }
    }

    /// Assign a version to BIT (monotonic increased)
    pub fn assign_version(&mut self) -> u32 {
        self.max_bit_version += 1;
        self.max_bit_version
    }

    pub fn insert_bit(&mut self, bit: Bit, level: LsmLevel) -> Option<Bit> {
        let old_l0_bit = match level {
            0 => {
                let old_bit = self.l0_bit.take();
                let _ = self.l0_bit.insert(bit);
                old_bit
            }
            1 => {
                self.l1_bits.push(bit);
                None
            }
            _ => panic!("illegal lsm level"),
        };
        old_l0_bit
    }

    pub fn l0_bit(&self) -> Option<Bit> {
        self.l0_bit.as_ref().map(|bit| bit.clone())
    }

    pub fn remove_bit(&mut self, bit_id: &BitId, level: LsmLevel) {
        match level {
            0 => {
                if let Some(l0_bit) = &self.l0_bit {
                    debug_assert!(l0_bit.id() == bit_id);
                    let _ = self.l0_bit.take();
                }
            }
            1 => {
                self.l1_bits.drain_filter(|l1_bit| l1_bit.id() == bit_id);
            }
            _ => panic!("illegal lsm level"),
        }
    }

    pub fn find_bit_by_lba(&self, target_lba: Lba, level: LsmLevel) -> Option<&Bit> {
        match level {
            // Find level 0
            0 => {
                if let Some(l0_bit) = &self.l0_bit {
                    if l0_bit.lba_range().is_within_range(target_lba) {
                        return Some(l0_bit);
                    }
                }
            }
            // Find level 1
            1 => {
                for bit in &self.l1_bits {
                    if bit.lba_range().is_within_range(target_lba) {
                        return Some(bit);
                    }
                }
            }
            _ => panic!("illegal lsm level"),
        }
        None
    }

    /// Find all `Bit`s which have overlapped lba range with the target range.
    pub fn find_bit_by_lba_range(&self, target_range: &LbaRange, level: LsmLevel) -> Vec<Bit> {
        match level {
            // Find level 0
            0 => {
                if let Some(l0_bit) = &self.l0_bit {
                    if l0_bit.lba_range().is_overlapped(target_range) {
                        return vec![l0_bit.clone()];
                    }
                }
            }
            // Find level 1
            1 => {
                return self
                    .l1_bits
                    .iter()
                    .filter(|l1_bit| l1_bit.lba_range().is_overlapped(target_range))
                    .map(|bit| bit.clone())
                    .collect();
            }
            _ => panic!("illegal lsm level"),
        }
        vec![]
    }

    /// Initialize all BIT node caches.
    pub async fn init_bit_caches(&self, disk: &DiskView) -> Result<()> {
        if self.l0_bit.is_none() {
            return Ok(());
        }
        self.l0_bit.as_ref().unwrap().init_cache(disk).await?;
        for bit in &self.l1_bits {
            bit.init_cache(disk).await?;
        }
        Ok(())
    }

    fn calc_diskarray_offset(&self, bit_addr: Hba) -> usize {
        (bit_addr.to_offset() - self.index_region_addr.to_offset()) / INDEX_SEGMENT_SIZE
    }

    /// Calculate BITC blocks without shadow block
    pub fn calc_bitc_blocks(num_index_segments: usize) -> usize {
        DiskArray::<DiskBit>::total_blocks(num_index_segments)
    }

    /// Calculate space cost (with shadow blocks) on disk.
    pub fn calc_size_on_disk(num_index_segments: usize) -> usize {
        let total_blocks = DiskArray::<DiskBit>::total_blocks_with_shadow(num_index_segments);
        total_blocks * BLOCK_SIZE
    }
}

impl BITC {
    pub async fn persist(&self) -> Result<()> {
        // TODO: write to disk_array
        self.disk_array.persist().await
    }

    pub async fn load(
        index_svt: &SVT,
        bitc_boundary: HbaRange,
        disk: DiskView,
        key: Key,
        shadow: bool
    ) -> Result<Self> {
        let disk_shadow = DiskShadow::load(bitc_boundary, disk, shadow).await?;
        let mut disk_array = DiskArray::new(disk_shadow, key);

        let mut max_bit_version = 0;
        let mut l0_bit = None;
        let mut l1_bits = Vec::new();
        for offset in index_svt.bitmap().iter_zeros() {
            let bit: DiskBit = disk_array.get(offset).await.ok_or(
                errno!(EIO, "bitc disk_array read error")
            )?;
            if max_bit_version < *bit.version() {
                max_bit_version = *bit.version();
            }
            match *bit.level() {
                0 => {
                    l0_bit = Some(Bit::new(bit))
                },
                1 => {
                    l1_bits.push(Bit::new(bit));
                },
                _ => {
                    return_errno!(EINVAL, "bitc load: illegal LsmLevel")
                }
            }
        }
        Ok(Self {
            max_bit_version,
            l0_bit,
            l1_bits,
            index_region_addr: index_svt.region_addr(),
            disk_array
        })
    }

    pub async fn checkpoint(&mut self) -> Result<bool> {
        if let Some(bit) = &self.l0_bit {
            let offset = self.calc_diskarray_offset(bit.addr());
            let disk_bit = bit.bit().clone();
            self.disk_array.set(offset, disk_bit).await?;
        }
        for bit in &self.l1_bits {
            let offset = self.calc_diskarray_offset(bit.addr());
            let disk_bit = bit.bit().clone();
            self.disk_array.set(offset, disk_bit).await?
        }
        self.disk_array.checkpoint().await
    }
}

impl Debug for BITC {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Checkpoint::BITC (Block Index Table Catalog)")
            .field("max_bit_version", &self.max_bit_version)
            .field("level_0_bit", &self.l0_bit)
            .field("level_1_bits", &self.l1_bits)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use crate::index::record::RootRecord;

    use super::*;
    use block_device::mem_disk::MemDisk;

    #[test]
    fn test_bitc_fns() {
        let disk_blocks = 256 * MiB / BLOCK_SIZE;
        let disk = Arc::new(MemDisk::new(disk_blocks).unwrap());
        let disk = DiskView::new_unchecked(disk);

        let index_region_addr = Hba::new(0);
        let num_index_segments = 57usize;
        let bitc_blocks = BITC::calc_bitc_blocks(num_index_segments);
        let bitc_blocks_with_shadow = BITC::calc_size_on_disk(num_index_segments) / BLOCK_SIZE;
        // size_of::<DiskBit>() -> 72B, (4096 - 16) / 72 = 56
        assert_eq!(bitc_blocks, 2);
        assert_eq!(bitc_blocks_with_shadow, 6);

        let bitc_start = index_region_addr + (NUM_BLOCKS_PER_SEGMENT * num_index_segments) as _;
        let bitc_end = bitc_start + (bitc_blocks as _);
        let bitc_boundary = HbaRange::new(bitc_start..bitc_end);
        let key = DefaultCryptor::gen_random_key();
        let mut bitc = BITC::new(
            index_region_addr,
            bitc_boundary,
            disk,
            key
        );

        let level0 = 0 as LsmLevel;
        let level1 = 1 as LsmLevel;

        let searched_bit = bitc.find_bit_by_lba(Lba::new(0), level0);
        assert!(searched_bit.is_none());

        let old_l0_bit = bitc.insert_bit(
            Bit::new_unchecked(BitId::new(0), LbaRange::new(Lba::new(0)..Lba::new(9))),
            level0,
        );

        assert!(old_l0_bit.is_none());

        let id = BitId::from_byte_offset(2 * INDEX_SEGMENT_SIZE);
        let old_l0_bit = bitc.insert_bit(
            Bit::new_unchecked(id, LbaRange::new(Lba::new(10)..Lba::new(19))),
            level0,
        );

        assert!(old_l0_bit.is_some());

        let searched_bit = bitc.find_bit_by_lba(Lba::new(15), level0);
        assert!(*searched_bit.unwrap().id() == id);

        let id = BitId::from_byte_offset(5 * INDEX_SEGMENT_SIZE);
        let old_l0_bit = bitc.insert_bit(
            Bit::new_unchecked(id, LbaRange::new(Lba::new(20)..Lba::new(29))),
            level1,
        );

        assert!(old_l0_bit.is_none());

        let searched_bit = bitc.find_bit_by_lba(Lba::new(25), level1);
        assert!(*searched_bit.unwrap().id() == id);

        let searched_bit =
            bitc.find_bit_by_lba_range(&LbaRange::new(Lba::new(20)..Lba::new(25)), level1);
        assert!(*searched_bit[0].id() == id);

        assert!(
            *bitc.find_bit_by_lba_range(&LbaRange::new(Lba::new(15)..Lba::new(25)), level1)[0].id()
                == id
        );
        assert!(bitc
            .find_bit_by_lba_range(&LbaRange::new(Lba::new(10)..Lba::new(15)), level1)
            .is_empty());
    }

    #[test]
    fn test_bitc_persist_load() -> Result<()> {
        async_rt::task::block_on(async move {
            let disk_blocks = 256 * MiB / BLOCK_SIZE;
            let disk = Arc::new(MemDisk::new(disk_blocks).unwrap());
            let disk = DiskView::new_unchecked(disk);

            let index_region_addr = Hba::new(0);
            let num_index_segments = 57usize;
            let bitc_blocks = BITC::calc_bitc_blocks(num_index_segments);
            let bitc_start = index_region_addr + (NUM_BLOCKS_PER_SEGMENT * num_index_segments) as _;
            let bitc_end = bitc_start + (bitc_blocks as _);
            let bitc_boundary = HbaRange::new(bitc_start..bitc_end);
            let key = DefaultCryptor::gen_random_key();

            let mut bitc = BITC::new(
                index_region_addr,
                bitc_boundary.clone(),
                disk.clone(),
                key.clone()
            );

            let mut fake_svt = SVT::new(
                index_region_addr,
                num_index_segments,
                INDEX_SEGMENT_SIZE,
                HbaRange::new(bitc_end..bitc_end + 1u64),
                disk.clone(),
                key.clone()
            );

            let bit_addr = Hba::from_byte_offset(5 * INDEX_SEGMENT_SIZE);
            fake_svt.invalidate_seg(bit_addr);
            let version = bitc.assign_version();
            bitc.insert_bit(
                Bit::new(DiskBit::new(
                    bit_addr,
                    version,
                    0,
                    RootRecord::new(
                        LbaRange::new(Lba::new(20)..Lba::new(29)),
                        Hba::new(0),
                        CipherMeta::new_uninit()
                    ),
                    DefaultCryptor::gen_random_key()
                )),
                0,
            );
            let bit_addr = Hba::from_byte_offset(2 * INDEX_SEGMENT_SIZE);
            fake_svt.invalidate_seg(bit_addr);
            let version = bitc.assign_version();
            bitc.insert_bit(
                Bit::new(DiskBit::new(
                    bit_addr,
                    version,
                    1,
                    RootRecord::new(
                        LbaRange::new(Lba::new(10)..Lba::new(19)),
                        Hba::new(0),
                        CipherMeta::new_uninit()
                    ),
                    DefaultCryptor::gen_random_key()
                )),
                1,
            );

            bitc.checkpoint().await?;

            let loaded_bitc = BITC::load(
                &fake_svt,
                bitc_boundary.clone(),
                disk.clone(),
                key.clone(),
                true
            ).await?;

            assert_eq!(format!("{:?}", bitc), format!("{:?}", loaded_bitc));
            Ok(())
        })
    }
}
