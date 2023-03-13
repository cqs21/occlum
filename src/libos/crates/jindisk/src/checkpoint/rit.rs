//! Reverse Index Table (RIT).
use crate::prelude::*;
use crate::util::DiskShadow;

use std::fmt::{self, Debug};

/// Reverse Index Table.
/// Manage reverse mapping (hba => lba).
pub struct RIT {
    data_region_addr: Hba,
    disk_array: DiskArray<Lba>,
}

impl RIT {
    pub fn new(data_region_addr: Hba, boundary: HbaRange, disk: DiskView) -> Self {
        let disk_shadow = DiskShadow::new(boundary, disk);
        Self {
            data_region_addr,
            disk_array: DiskArray::new(disk_shadow),
        }
    }

    pub async fn insert(&mut self, hba: Hba, lba: Lba) -> Result<()> {
        self.disk_array.set(self.offset(hba), lba).await
    }

    pub async fn find_lba(&mut self, hba: Hba) -> Option<Lba> {
        self.disk_array.get(self.offset(hba)).await
    }

    pub async fn find_and_invalidate(&mut self, hba: Hba) -> Result<Lba> {
        let existed_lba = self.find_lba(hba).await.unwrap();
        self.insert(hba, NEGATIVE_LBA).await?;
        Ok(existed_lba)
    }

    pub async fn check_valid(&mut self, hba: Hba, lba: Lba) -> bool {
        self.find_lba(hba).await.unwrap() == lba
    }

    pub fn nr_cached_blocks(&self) -> usize {
        self.disk_array.cache_size()
    }

    fn offset(&self, hba: Hba) -> usize {
        (hba - self.data_region_addr.to_raw()).to_raw() as _
    }

    /// Calculate RIT blocks without shadow block
    pub fn calc_rit_blocks(num_data_segments: usize) -> usize {
        let nr_units = num_data_segments * NUM_BLOCKS_PER_SEGMENT;
        DiskArray::<Lba>::total_blocks(nr_units)
    }

    /// Calculate space cost in bytes (with shadow blocks) on disk.
    pub fn calc_size_on_disk(num_data_segments: usize) -> usize {
        let nr_units = num_data_segments * NUM_BLOCKS_PER_SEGMENT;
        let total_blocks = DiskArray::<Lba>::total_blocks_with_shadow(nr_units);
        total_blocks * BLOCK_SIZE
    }
}

impl RIT {
    pub async fn persist(&self, key: &Key) -> Result<()> {
        self.disk_array.persist(key).await
    }

    pub async fn load(
        data_region_addr: Hba,
        boundary: HbaRange,
        disk: DiskView,
        shadow: bool
    ) -> Result<Self> {
        let disk_shadow = DiskShadow::load(boundary, disk, shadow).await?;
        Ok(Self {
            data_region_addr,
            disk_array: DiskArray::new(disk_shadow)
        })
    }

    pub async fn checkpoint(&mut self, key: &Key) -> Result<bool> {
        self.disk_array.checkpoint(key).await
    }
}

impl Debug for RIT {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Checkpoint::RIT (Reverse Index Table)")
            .field(" nr_cached_blocks", &self.nr_cached_blocks())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use block_device::mem_disk::MemDisk;

    #[test]
    fn test_rit_fns() -> Result<()> {
        async_rt::task::block_on(async move {
            let disk_blocks = 64 * MiB / BLOCK_SIZE;
            let disk = Arc::new(MemDisk::new(disk_blocks).unwrap());
            let disk = DiskView::new_unchecked(disk);

            let data_region_addr = Hba::new(0);
            let num_data_segments = 8usize;
            let rit_blocks = RIT::calc_rit_blocks(num_data_segments);
            let rit_blocks_with_shadow = RIT::calc_size_on_disk(num_data_segments) / BLOCK_SIZE;
            // data_blocks: (((32MiB / 4KiB) * 8B) / 4KiB) = 16, bitmap_blocks: 1
            assert_eq!(rit_blocks, 16);
            assert_eq!(rit_blocks_with_shadow, 34);

            let rit_start = data_region_addr + (NUM_BLOCKS_PER_SEGMENT * num_data_segments) as u64;
            let rit_end = rit_start + (rit_blocks as u64);
            let boundary = HbaRange::new(rit_start..rit_end);

            let mut rit = RIT::new(data_region_addr, boundary.clone(), disk.clone());

            let kv1 = (Hba::new(1), Lba::new(2));
            let kv2 = (Hba::new(1025), Lba::new(5));

            rit.insert(kv1.0, kv1.1).await?;
            rit.insert(kv2.0, kv2.1).await?;

            assert_eq!(rit.find_lba(kv1.0).await.unwrap(), kv1.1);
            assert_eq!(rit.find_lba(kv2.0).await.unwrap(), kv2.1);

            assert_eq!(rit.find_and_invalidate(kv2.0).await.unwrap(), kv2.1);
            assert_eq!(rit.check_valid(kv2.0, kv2.1).await, false);

            // illegal Hba: 8192
            let kv3 = (Hba::new(8192), Lba::new(0));
            match rit.insert(kv3.0, kv3.1).await {
                Ok(_) => unreachable!(),
                Err(e) => {
                    assert_eq!(e.errno(), EINVAL);
                    assert!(e.to_string().contains("requested Hba is illegal in DiskShadow"));
                }
            }

            let key = DefaultCryptor::gen_random_key();
            rit.persist(&key).await?;
            rit.checkpoint(&key).await?;

            let mut loaded_rit = RIT::load(data_region_addr, boundary, disk, true).await?;
            assert_eq!(loaded_rit.find_lba(kv1.0).await.unwrap(), kv1.1);
            assert_eq!(loaded_rit.check_valid(kv2.0, kv2.1).await, false);
            Ok(())
        })
    }
}
