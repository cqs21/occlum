//! Disk array.
use crate::data::DataBlock;
use crate::prelude::*;
use super::disk_shadow::DiskShadow;

use std::collections::HashMap;
use std::marker::PhantomData;
use std::mem::size_of;

/// Disk array that manages on-disk structures.
/// +------+-------------+------------+
/// | hmac | (unit, ...) | padding(0) |
/// +------+-------------+------------+
pub struct DiskArray<T> {
    cache: HashMap<Hba, DataBlock>,
    disk: DiskShadow,
    key: Key,
    phantom: PhantomData<T>,
}

impl<T: Serialize> DiskArray<T> {
    pub fn new(disk: DiskShadow, key: Key) -> Self {
        Self {
            cache: HashMap::new(),
            disk,
            key,
            phantom: PhantomData,
        }
    }

    pub async fn get(&mut self, offset: usize) -> Option<T> {
        let (hba, inner_offset) = self.hba_and_inner_offset(offset);
        let data_block = self.load_block(hba).await.ok()?;

        T::decode(&data_block.as_slice()[inner_offset..(inner_offset + Self::unit_size())]).ok()
    }

    pub async fn set(&mut self, offset: usize, unit: T) -> Result<()> {
        let (hba, inner_offset) = self.hba_and_inner_offset(offset);
        let data_block = self.load_block(hba).await?;

        let mut buf = Vec::with_capacity(Self::unit_size());
        unit.encode(&mut buf)?;
        data_block.as_slice_mut()[inner_offset..(inner_offset + Self::unit_size())]
            .copy_from_slice(&buf);
        self.disk.mark_dirty(hba);
        Ok(())
    }

    fn hba_and_inner_offset(&self, offset: usize) -> (Hba, usize) {
        let unit_per_block = Self::unit_per_block();
        let mut block_offset = (offset / unit_per_block) as _;
        (
            self.disk.boundary().start() + block_offset,
            AUTH_ENC_MAC_SIZE + (offset % unit_per_block) * Self::unit_size(),
        )
    }

    async fn load_block(&mut self, hba: Hba) -> Result<&mut DataBlock> {
        if !self.cache.contains_key(&hba) {
            let mut data_block = DataBlock::new_uninit();
            self.disk.read(hba, data_block.as_slice_mut()).await?;
            // TODO: decrypt block with self.key
            self.cache.insert(hba, data_block);
        }
        Ok(self.cache.get_mut(&hba).unwrap())
    }

    fn unit_size() -> usize {
        let size = size_of::<T>();
        debug_assert!(size > 0 && size <= BLOCK_SIZE - AUTH_ENC_MAC_SIZE);
        size
    }

    fn unit_per_block() -> usize {
        (BLOCK_SIZE - AUTH_ENC_MAC_SIZE) / Self::unit_size()
    }

    pub fn total_blocks(nr_units: usize) -> usize {
        let mut nr_blocks = 0;
        if nr_units != 0 {
            nr_blocks = (nr_units - 1) / Self::unit_per_block() + 1;
        }
        nr_blocks
    }

    pub fn total_blocks_with_shadow(nr_units: usize) -> usize {
        let nr_blocks = Self::total_blocks(nr_units);
        DiskShadow::total_blocks_with_shadow(nr_blocks)
    }

    pub fn cache_size(&self) -> usize {
        self.cache.len()
    }

    pub async fn persist(&self) -> Result<()> {
        // TODO: encrypt block with self.key
        for (hba, block) in self.cache.iter() {
            self.disk.write(*hba, block.as_slice()).await?;
        }
        Ok(())
    }

    pub async fn checkpoint(&mut self) -> Result<bool> {
        self.persist().await?;
        self.disk.checkpoint().await
    }
}
