//! Cryption Key Table for data segments.
use crate::prelude::*;
use crate::util::DiskShadow;

use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt::{self, Debug};

/// Cryption key table.
/// Manage per-segment cryption keys.
pub struct KeyTable {
    data_region_addr: Hba,
    num_data_segments: usize,
    table: HashMap<Hba, Key>,
    disk_array: DiskArray<Key>
}
// TODO: Support on-demand loading using `DiskArray<_>`
// TODO: Adapt threaded logging

impl KeyTable {
    pub fn new(
        data_region_addr: Hba,
        num_data_segments: usize,
        keytable_boundary: HbaRange,
        disk: DiskView,
        key: Key
    ) -> Self {
        Self {
            data_region_addr,
            num_data_segments,
            table: HashMap::new(),
            disk_array: DiskArray::new(DiskShadow::new(keytable_boundary, disk), key)
        }
    }

    pub fn get_or_insert(&mut self, block_addr: Hba) -> Key {
        self.table
            .entry(Hba::new(align_down(
                (block_addr - self.data_region_addr.to_raw()).to_raw() as _,
                NUM_BLOCKS_PER_SEGMENT,
            ) as _))
            .or_insert(DefaultCryptor::gen_random_key())
            .clone()
    }

    pub fn size(&self) -> usize {
        self.table.len()
    }

    /// Calculate KeyTable blocks without shadow block
    pub fn calc_keytable_blocks(num_data_segments: usize) -> usize {
        DiskArray::<Key>::total_blocks(num_data_segments)
    }

    /// Calculate space cost (with shadow blocks) on disk.
    pub fn calc_size_on_disk(num_data_segments: usize) -> usize {
        let total_blocks = DiskArray::<Key>::total_blocks_with_shadow(num_data_segments);
        total_blocks * BLOCK_SIZE
    }
}

crate::impl_default_serialize! {Key, AUTH_ENC_KEY_SIZE}

impl KeyTable {
    pub async fn persist(&self) -> Result<()> {
        // TODO: write to disk_array
        self.disk_array.persist().await
    }

    pub async fn load(
        data_region_addr: Hba,
        num_data_segments: usize,
        keytable_boundary: HbaRange,
        disk: DiskView,
        key: Key,
        shadow: bool
    ) -> Result<Self> {
        let disk_shadow = DiskShadow::load(keytable_boundary, disk, shadow).await?;
        let mut disk_array = DiskArray::new(disk_shadow, key);

        let mut table = HashMap::new();
        for i in 0..num_data_segments {
            let seg_addr = data_region_addr + ((i * NUM_BLOCKS_PER_SEGMENT) as _);
            let value: Key = disk_array.get(i).await.ok_or(
                errno!(EIO, "keytable disk_array read error")
            )?;
            if value.ne(&[0u8; AUTH_ENC_KEY_SIZE]) {
                table.insert(seg_addr, value);
            }
        }

        Ok(Self {
            data_region_addr,
            num_data_segments,
            table,
            disk_array
        })
    }

    pub async fn checkpoint(&mut self) -> Result<bool> {
        for (seg_addr, key) in self.table.iter() {
            let offset = (seg_addr.to_raw() as usize)/ NUM_BLOCKS_PER_SEGMENT;
            let value = key.clone();
            self.disk_array.set(offset, value).await?;
        }
        self.disk_array.checkpoint().await
    }
}

impl Debug for KeyTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Checkpoint::KeyTable")
            .field("data_region_addr", &self.data_region_addr)
            .field("num_data_segments", &self.num_data_segments)
            .field("table_size", &self.size())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use block_device::mem_disk::MemDisk;

    #[test]
    fn test_keytable_fns() {
        let disk_blocks = 64 * MiB / BLOCK_SIZE;
        let disk = Arc::new(MemDisk::new(disk_blocks).unwrap());
        let disk = DiskView::new_unchecked(disk);

        let data_region_addr = Hba::new(0);
        let num_data_segments = 8usize;
        let keytable_blocks = KeyTable::calc_keytable_blocks(num_data_segments);
        let keytable_blocks_with_shadow = KeyTable::calc_size_on_disk(num_data_segments) / BLOCK_SIZE;
        assert_eq!(keytable_blocks, 1);
        assert_eq!(keytable_blocks_with_shadow, 4);

        let keytable_start = data_region_addr + (NUM_BLOCKS_PER_SEGMENT * num_data_segments) as _;
        let keytable_end = keytable_start + (keytable_blocks as _);
        let keytable_boundary = HbaRange::new(keytable_start..keytable_end);
        let key = DefaultCryptor::gen_random_key();
        let mut keytable = KeyTable::new(
            data_region_addr,
            num_data_segments,
            keytable_boundary,
            disk,
            key
        );

        let k0 = keytable.get_or_insert(data_region_addr);
        let b1 = data_region_addr + 1 as _;
        let k1 = keytable.get_or_insert(b1);
        let b2 = data_region_addr + NUM_BLOCKS_PER_SEGMENT as _;
        let k2 = keytable.get_or_insert(b2);

        assert_eq!(k0, k1);
        assert_ne!(k0, k2);
        assert_eq!(keytable.size(), 2);
    }

    #[test]
    fn test_keytable_persist_load() -> Result<()> {
        async_rt::task::block_on(async move {
            let disk_blocks = 64 * MiB / BLOCK_SIZE;
            let disk = Arc::new(MemDisk::new(disk_blocks).unwrap());
            let disk = DiskView::new_unchecked(disk);

            let data_region_addr = Hba::new(0);
            let num_data_segments = 8usize;
            let keytable_addr = data_region_addr + (NUM_BLOCKS_PER_SEGMENT * num_data_segments) as _;
            let keytable_blocks = KeyTable::calc_keytable_blocks(num_data_segments);
            let keytable_boundary = HbaRange::new(
                keytable_addr..keytable_addr + (keytable_blocks as _)
            );
            let key = DefaultCryptor::gen_random_key();

            let mut keytable = KeyTable::new(
                data_region_addr,
                num_data_segments,
                keytable_boundary.clone(),
                disk.clone(),
                key.clone()
            );

            keytable.get_or_insert(data_region_addr);

            keytable.checkpoint().await?;

            let loaded_keytable = KeyTable::load(
                data_region_addr,
                num_data_segments,
                keytable_boundary,
                disk,
                key,
                true
            ).await?;

            assert_eq!(format!("{:?}", keytable), format!("{:?}", loaded_keytable));
            Ok(())
        })
    }
}
