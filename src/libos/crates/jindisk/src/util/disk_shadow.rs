//! Shadow paging DiskView.
use crate::prelude::*;

// DiskShadow manages IO for HbaRange [start, end) and its backup. When
// user issues an operation on HbaRange [start, end), DiskShadow will
// map the requested `hba` to `hba` or `hba - start + end`, depending
// on the current bitmap.
// Typical layout of DiskShadow:
// start           end     (2 * end - start)
//   +--------------+--------------+---------------+---------------+
//   |    block     | block_shadow |    bitmap     | bitmap_shadow |
//   +--------------+--------------+---------------+---------------+
#[derive(Debug)]
pub struct DiskShadow {
    boundary: HbaRange,
    current_block: BitMap,
    dirty_block: BitMap,
    current_bitmap: bool,
    nr_bitmap_blocks: usize,
    disk: DiskView,
}

impl DiskShadow {
    pub fn new(boundary: HbaRange, disk: DiskView) -> Self {
        let nr_blocks = boundary.num_covered_blocks();
        let nr_bitmap_bytes = (nr_blocks + BITMAP_UNIT - 1) / BITMAP_UNIT;
        let nr_bitmap_blocks = (nr_bitmap_bytes + BLOCK_SIZE - 1) / BLOCK_SIZE;
        let total_blocks = (nr_blocks + nr_bitmap_blocks) * 2;
        let end =  boundary.start() + (total_blocks as u64);
        debug_assert!(
            (!boundary.is_empty() && end <= disk.boundary().end()),
            "DiskShadow check boundary failed: {:?}",
            boundary
        );

        let current_block = BitMap::repeat(false,nr_blocks);
        let dirty_block = BitMap::repeat(false, nr_blocks);
        let current_bitmap = false;
        Self {
            boundary,
            current_block,
            dirty_block,
            current_bitmap,
            nr_bitmap_blocks,
            disk
        }
    }

    pub async fn load(boundary: HbaRange, disk: DiskView, shadow: bool) -> Result<Self> {
        let nr_blocks = boundary.num_covered_blocks();
        let nr_bitmap_bytes = (nr_blocks + BITMAP_UNIT - 1) / BITMAP_UNIT;
        let nr_bitmap_blocks = (nr_bitmap_bytes + BLOCK_SIZE - 1) / BLOCK_SIZE;
        let total_blocks = (nr_blocks + nr_bitmap_blocks) * 2;
        let end =  boundary.start() + (total_blocks as u64);
        debug_assert!(
            (!boundary.is_empty() && end <= disk.boundary().end()),
            "DiskShadow check boundary failed: {:?}",
            boundary
        );

        let mut bitmap_addr = boundary.start() + (2 * nr_blocks as u64);
        if shadow {
            bitmap_addr = bitmap_addr + (nr_bitmap_blocks as u64);
        }
        let mut buffer = Vec::<u8>::with_capacity(nr_bitmap_blocks * BLOCK_SIZE);
        buffer.resize(nr_bitmap_blocks * BLOCK_SIZE, 0);
        disk.read(bitmap_addr, buffer.as_mut_slice()).await?;

        buffer.resize(nr_bitmap_bytes, 0);
        let mut current_block = BitMap::from_vec(buffer);
        let dirty_block = BitMap::repeat(false, nr_blocks);
        let current_bitmap = shadow;
        Ok(Self {
            boundary,
            current_block,
            current_bitmap,
            dirty_block,
            nr_bitmap_blocks,
            disk
        })
    }

    pub fn total_blocks_with_shadow(nr_blocks: usize) -> usize {
        let nr_bitmap_bytes = (nr_blocks + BITMAP_UNIT - 1) / BITMAP_UNIT;
        let nr_bitmap_blocks = (nr_bitmap_bytes + BLOCK_SIZE - 1) / BLOCK_SIZE;
        (nr_blocks + nr_bitmap_blocks) * 2
    }

    pub fn boundary(&self) -> &HbaRange {
        &self.boundary
    }

    fn check_boundary(&self, hba: Hba) -> Result<()> {
        if !self.boundary.is_within_range(hba) {
            return_errno!(EINVAL, "requested Hba is illegal in DiskShadow");
        }
        Ok(())
    }

    fn index(&self, hba: Hba) -> usize {
        (hba.to_raw() - self.boundary.start().to_raw()) as usize
    }

    fn shadow(&self, hba: Hba) -> bool {
        let index = self.index(hba);
        self.current_block[index]
    }

    pub fn mark_dirty(&mut self, hba: Hba) {
        let index = self.index(hba);
        if !self.dirty_block[index] {
            self.dirty_block.set(index, true);
            let shadow = self.current_block[index];
            self.current_block.set(index, !shadow);
        }
    }

    fn block_addr(&self, hba: Hba, shadow: bool) -> Hba {
        if shadow {
            hba - self.boundary.start().to_raw() + self.boundary.end().to_raw()
        } else {
            hba
        }
    }

    fn bitmap_addr(&self, shadow: bool) -> Hba {
        let nr_blocks = self.boundary.num_covered_blocks();
        let start = self.boundary.start() + (2 * nr_blocks as u64);
        let nr_bitmap_blocks = self.nr_bitmap_blocks as u64;
        if shadow {
            start + nr_bitmap_blocks
        } else {
            start
        }
    }

    pub async fn read(&self, hba: Hba, buf: &mut [u8]) -> Result<usize> {
        debug_assert!(buf.len() <= BLOCK_SIZE);
        self.check_boundary(hba)?;

        let shadow = self.shadow(hba);
        self.disk.read(self.block_addr(hba, shadow), buf).await
    }

    pub async fn write(&self, hba: Hba, buf: &[u8]) -> Result<usize> {
        debug_assert!(buf.len() <= BLOCK_SIZE);
        self.check_boundary(hba)?;

        let index = self.index(hba);
        let mut shadow = self.shadow(hba);
        if !self.dirty_block[index] {
            shadow = !shadow;
        }
        let result = self.disk.write(self.block_addr(hba, shadow), buf).await?;
        Ok(result)
    }

    pub async fn sync(&self) -> Result<()> {
        self.disk.sync().await
    }

    pub async fn checkpoint(&mut self) -> Result<bool> {
        self.current_bitmap = !self.current_bitmap;
        let bitmap_addr = self.bitmap_addr(self.current_bitmap);

        let len = self.current_block.as_raw_slice().len();
        let mut buffer = Vec::<u8>::with_capacity(align_up(len, BLOCK_SIZE));
        buffer.resize(align_up(len, BLOCK_SIZE), 0);
        buffer[..len].copy_from_slice(self.current_block.as_raw_slice());

        self.disk.write(bitmap_addr, buffer.as_slice()).await?;
        self.dirty_block.fill(false);
        Ok(self.current_bitmap)
    }
}

mod tests {
    use super::*;
    use block_device::mem_disk::MemDisk;

    #[allow(unused)]
    fn create_disk_view() -> DiskView {
        let total_blocks = 4 * MiB / BLOCK_SIZE;
        let disk = Arc::new(MemDisk::new(total_blocks).unwrap());
        let range = HbaRange::new(Hba::new(0)..Hba::new(total_blocks as u64));
        DiskView::new(range, disk)
    }

    #[allow(unused)]
    fn disk_shadow_new() -> DiskShadow {
        let boundary = HbaRange::new(
            Hba::new(0)..Hba::new(4)
        );
        let disk = create_disk_view();
        let disk = DiskShadow::new(boundary, disk);
        assert_eq!(disk.nr_bitmap_blocks, 1);
        assert_eq!(disk.current_bitmap, false);
        disk
    }
    #[test]
    fn disk_shadow_load() {
        async_rt::task::block_on(async move {
            let disk_view = create_disk_view();
            let boundary = HbaRange::new(
                Hba::new(0)..Hba::new(4)
            );
            let mut disk = DiskShadow::new(boundary.clone(), disk_view.clone());
            disk.mark_dirty(Hba::new(0));
            disk.checkpoint().await;
            let disk = DiskShadow::load(boundary.clone(), disk_view.clone(), false).await.unwrap();
            assert_eq!(disk.current_bitmap, false);
            assert_eq!(disk.current_block[0], false);
            let disk = DiskShadow::load(boundary, disk_view, true).await.unwrap();
            assert_eq!(disk.current_bitmap, true);
            assert_eq!(disk.current_block[0], true);
        });
    }
    #[test]
    fn disk_shadow_write_and_read() {
        async_rt::task::block_on(async move {
            let mut disk = disk_shadow_new();
            let mut buffer = Vec::<u8>::with_capacity(BLOCK_SIZE);
            buffer.resize(BLOCK_SIZE, 1);
            let hba = Hba::new(0);
            disk.write(hba, buffer.as_slice()).await;
            disk.mark_dirty(hba);
            assert_eq!(disk.current_block[0], true);
            assert_eq!(disk.dirty_block[0], true);
            buffer.fill(0);
            disk.read(hba, buffer.as_mut_slice()).await;
            assert_eq!(buffer, vec![1u8; 4096]);
        });
    }
    #[test]
    fn disk_shadow_checkpoint() {
        async_rt::task::block_on(async move {
            let mut disk = disk_shadow_new();
            let mut buffer = Vec::<u8>::with_capacity(BLOCK_SIZE);
            buffer.resize(BLOCK_SIZE, 1);
            let hba = Hba::new(0);
            disk.write(hba, buffer.as_slice()).await;
            disk.mark_dirty(hba);
            assert_eq!(disk.current_bitmap, false);
            disk.checkpoint().await;
            assert_eq!(disk.current_block[0], true);
            assert_eq!(disk.dirty_block[0], false);
            assert_eq!(disk.current_bitmap, true);
        });
    }
}
