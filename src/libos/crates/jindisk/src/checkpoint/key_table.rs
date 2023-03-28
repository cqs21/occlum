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
    disk_array: DiskArray<Key>,
}
// TODO: Adapt threaded logging

const KEYTABLE_ENCRYPTED_WITH_HMAC: bool = true;

impl KeyTable {
    pub fn new(
        data_region_addr: Hba,
        num_data_segments: usize,
        keytable_boundary: HbaRange,
        disk: DiskView,
        key: Key,
    ) -> Self {
        Self {
            data_region_addr,
            disk_array: DiskArray::new(
                num_data_segments,
                DiskShadow::new(keytable_boundary, disk),
                key,
                KEYTABLE_ENCRYPTED_WITH_HMAC,
            ),
        }
    }

    fn offset(&self, hba: Hba) -> usize {
        let offset = (hba.to_raw() - self.data_region_addr.to_raw()) as usize;
        offset / NUM_BLOCKS_PER_SEGMENT
    }

    pub async fn get_or_insert(&mut self, hba: Hba) -> Result<Key> {
        let offset = self.offset(hba);
        let mut key = self.disk_array.get(offset).await?;
        // Warning: gen_random_key() should not generate all-zero key
        if key.eq(&[0u8; AUTH_ENC_KEY_SIZE]) {
            key = DefaultCryptor::gen_random_key();
            self.disk_array.set(offset, key).await?;
        }
        Ok(key)
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
    pub async fn persist(&mut self, checkpoint: bool) -> Result<bool> {
        self.disk_array.persist(checkpoint).await
    }

    pub async fn load(
        data_region_addr: Hba,
        num_data_segments: usize,
        keytable_boundary: HbaRange,
        disk: DiskView,
        key: Key,
        shadow: bool,
    ) -> Result<Self> {
        let disk_shadow = DiskShadow::load(keytable_boundary, disk, shadow).await?;
        Ok(Self {
            data_region_addr,
            disk_array: DiskArray::new(
                num_data_segments,
                disk_shadow,
                key,
                KEYTABLE_ENCRYPTED_WITH_HMAC,
            ),
        })
    }
}

impl Debug for KeyTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Checkpoint::KeyTable")
            .field("data_region_addr", &self.data_region_addr)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use block_device::mem_disk::MemDisk;

    #[test]
    fn test_keytable_fns() -> Result<()> {
        async_rt::task::block_on(async move {
            let disk_blocks = 64 * MiB / BLOCK_SIZE;
            let disk = Arc::new(MemDisk::new(disk_blocks).unwrap());
            let disk = DiskView::new_unchecked(disk);

            let data_region_addr = Hba::new(0);
            let num_data_segments = 8usize;
            let keytable_blocks = KeyTable::calc_keytable_blocks(num_data_segments);
            let keytable_blocks_with_shadow =
                KeyTable::calc_size_on_disk(num_data_segments) / BLOCK_SIZE;
            assert_eq!(keytable_blocks, 1);
            assert_eq!(keytable_blocks_with_shadow, 4);

            let keytable_start =
                data_region_addr + (NUM_BLOCKS_PER_SEGMENT * num_data_segments) as _;
            let keytable_end = keytable_start + (keytable_blocks as _);
            let keytable_boundary = HbaRange::new(keytable_start..keytable_end);
            let key = DefaultCryptor::gen_random_key();
            let mut keytable = KeyTable::new(
                data_region_addr,
                num_data_segments,
                keytable_boundary,
                disk,
                key,
            );

            let k0 = keytable.get_or_insert(data_region_addr).await?;
            let b1 = data_region_addr + 1 as _;
            let k1 = keytable.get_or_insert(b1).await?;
            let b2 = data_region_addr + NUM_BLOCKS_PER_SEGMENT as _;
            let k2 = keytable.get_or_insert(b2).await?;

            assert_eq!(k0, k1);
            assert_ne!(k0, k2);
            Ok(())
        })
    }

    #[test]
    fn test_keytable_persist_load() -> Result<()> {
        async_rt::task::block_on(async move {
            let disk_blocks = 64 * MiB / BLOCK_SIZE;
            let disk = Arc::new(MemDisk::new(disk_blocks).unwrap());
            let disk = DiskView::new_unchecked(disk);

            let data_region_addr = Hba::new(0);
            let num_data_segments = 8usize;
            let keytable_addr =
                data_region_addr + (NUM_BLOCKS_PER_SEGMENT * num_data_segments) as _;
            let keytable_blocks = KeyTable::calc_keytable_blocks(num_data_segments);
            let keytable_boundary =
                HbaRange::new(keytable_addr..keytable_addr + (keytable_blocks as _));
            let key = DefaultCryptor::gen_random_key();

            let mut keytable = KeyTable::new(
                data_region_addr,
                num_data_segments,
                keytable_boundary.clone(),
                disk.clone(),
                key.clone(),
            );

            let k0 = keytable.get_or_insert(data_region_addr).await?;

            let shadow = keytable.persist(true).await?;
            assert_eq!(shadow, true);

            let mut loaded_keytable = KeyTable::load(
                data_region_addr,
                num_data_segments,
                keytable_boundary,
                disk,
                key,
                shadow,
            )
            .await?;

            let k1 = loaded_keytable.get_or_insert(data_region_addr).await?;
            assert_eq!(k0, k1);
            Ok(())
        })
    }
}
