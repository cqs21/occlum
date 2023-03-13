//! Disk array.
use crate::data::DataBlock;
use crate::prelude::*;
use super::disk_shadow::DiskShadow;

use std::collections::HashMap;
use std::marker::PhantomData;
use std::mem::size_of;

/// Disk array that manages on-disk structures.
pub struct DiskArray<T> {
    cache: HashMap<Hba, DataBlock>,
    disk: DiskShadow,
    phantom: PhantomData<T>,
}

impl<T: Serialize> DiskArray<T> {
    pub fn new(disk: DiskShadow) -> Self {
        Self {
            cache: HashMap::new(),
            disk,
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
        Ok(())
    }

    fn hba_and_inner_offset(&self, offset: usize) -> (Hba, usize) {
        let unit_per_block = Self::unit_per_block();
        let mut block_offset = (offset / unit_per_block) as u64;
        (
            self.disk.boundary().start() + block_offset,
            (offset % unit_per_block) * Self::unit_size(),
        )
    }

    async fn load_block(&mut self, hba: Hba) -> Result<&mut DataBlock> {
        if !self.cache.contains_key(&hba) {
            let mut data_block = DataBlock::new_uninit();
            self.disk.read(hba, data_block.as_slice_mut()).await?;
            self.cache.insert(hba, data_block);
        }
        Ok(self.cache.get_mut(&hba).unwrap())
    }

    fn unit_size() -> usize {
        let size = size_of::<T>();
        debug_assert!(size > 0 && size <= BLOCK_SIZE);
        size
    }

    fn unit_per_block() -> usize {
        BLOCK_SIZE / Self::unit_size()
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

    pub async fn persist(&self, _key: &Key) -> Result<()> {
        // TODO: Add encryption(symmetric) for blocks
        for (hba, block) in self.cache.iter() {
            self.disk.write(*hba, block.as_slice()).await?;
        }
        Ok(())
    }

    pub async fn checkpoint(&mut self, _key: &Key) -> Result<bool> {
        // TODO: Add encryption(symmetric) for blocks
        for (hba, block) in self.cache.iter() {
            self.disk.write(*hba, block.as_slice()).await?;
            self.disk.mark_dirty(*hba);
        }
        self.disk.checkpoint().await
    }
}
