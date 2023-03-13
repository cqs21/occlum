//! Journal region.
//! A subsystem to ensure crash consistency and flush atomicity.
mod journaling;
mod data_log;

use crate::prelude::*;
use std::mem::size_of;
use std::convert::TryInto;
use journaling::{JournalBlock, RECORD_OFFSET};

pub type TypeId = [u8; 4];

impl Serialize for TypeId {
    fn encode(&self, encoder: &mut impl Encoder) -> Result<()> {
        encoder.write_bytes(&self[..])
    }
    fn decode(buf: &[u8]) -> Result<Self>
        where
            Self: Sized {
        let mut typeid = [0u8; 4];
        typeid.copy_from_slice(&buf[..size_of::<TypeId>()]);
        Ok(typeid)
    }
    fn bytes_len(&self) -> Option<usize> {
        Some(size_of::<TypeId>())
    }
}

pub trait JournalRecord {
    fn typeid(&self) -> TypeId;
    fn data(&self) -> Vec<u8>;
    /// size = typeid + data
    fn size(&self) -> usize;
    /// if success, return number of bytes consumed by recover
    fn recover(data: &[u8]) -> Result<usize> where Self: Sized;
}

pub struct Journal {
    disk: DiskView,
    root_key: Key,
    metadata: JournalMeta,
    current_block: Mutex<JournalBlock>,
}

struct JournalMeta {
    hba: Hba,
    block_range: HbaRange,
    first_block: Hba,
    last_block: Hba,
    block_init_mac: Mac,
}
crate::impl_default_serialize! {JournalMeta, size_of::<JournalMeta>()}

const JOURNALMETA_OFFSET: u64 = 0;
const JOURNALBLOCK_OFFSET: u64 = 1;

impl JournalMeta {
    pub fn new(region: &HbaRange) -> Self {
        assert!(region.num_covered_blocks() > 2);
        let meta_addr = region.start() + JOURNALMETA_OFFSET;
        let block_start = meta_addr + JOURNALBLOCK_OFFSET;
        Self {
            hba: meta_addr,
            block_range: HbaRange::new(block_start..region.end()),
            first_block: block_start,
            last_block: block_start,
            block_init_mac: [0u8; AUTH_ENC_MAC_SIZE],
        }
    }
    pub async fn open(disk: &DiskView, hba: Hba, key: &Key) -> Result<Self> {
        let mut block = JournalBlock::new(hba);
        block.load(disk, key).await?;
        let buf = block.record(RECORD_OFFSET, size_of::<JournalMeta>()).unwrap();
        JournalMeta::decode(buf)
    }
    pub async fn persist(&self, disk: &DiskView, key: &Key) -> Result<Mac>{
        let mut block = JournalBlock::new(self.hba);
        self.encode(&mut block)?;
        block.persist(disk, key).await?;
        Ok(block.current_mac())
    }
    pub fn first_block(&self) -> Hba {
        self.first_block
    }
    pub fn set_first_block(&mut self, hba: Hba) {
        self.first_block = hba;
    }
    pub fn last_block(&self) -> Hba {
        self.last_block
    }
    pub fn set_last_block(&mut self, hba: Hba) {
        self.last_block = hba;
    }
    pub fn block_init_mac(&self) -> &Mac {
        &self.block_init_mac
    }
    pub fn set_block_init_mac(&mut self, mac: &Mac) {
        self.block_init_mac.copy_from_slice(mac);
    }
    pub fn next_block(&self, hba: Hba) -> Result<Hba> {
        let start = self.block_range.start().to_raw();
        let total = self.block_range.num_covered_blocks() as u64;
        let mut offset = hba.to_raw() - start;
        offset = (offset + 1) % total;
        if start + offset == self.first_block.to_raw() {
            return_errno!(EINVAL, "no space for new journal block")
        }
        Ok(Hba::new(start + offset))
    }
}

impl Journal {
    pub fn new(disk: Arc<dyn BlockDevice>, range: HbaRange, root_key: Key) -> Self {
        let metadata = JournalMeta::new(&range);
        let block_addr = metadata.first_block();
        let current_block = Mutex::new(JournalBlock::new(block_addr));
        let disk = DiskView::new(range, disk);
        Self {
            disk,
            root_key,
            metadata,
            current_block
        }
    }
    pub async fn open(disk: Arc<dyn BlockDevice>, range: HbaRange, root_key: Key) -> Result<Self> {
        let meta_addr = range.start() + JOURNALMETA_OFFSET;
        let disk = DiskView::new(range, disk);
        let metadata = JournalMeta::open(&disk, meta_addr, &root_key).await?;

        /// TODO: recovery

        let last_block = metadata.last_block();
        let block_addr = metadata.next_block(last_block)?;
        let current_block = Mutex::new(JournalBlock::new(block_addr));

        Ok(Self {
            disk,
            root_key,
            metadata,
            current_block
        })
    }
    pub async fn add_record(&mut self, record: Box<dyn JournalRecord + Send>) -> Result<()> {
        let mut block = self.current_block.lock();
        if !block.can_hold(record.as_ref()) {
            block.persist(&self.disk, &self.root_key).await?;
            self.metadata.set_last_block(block.hba());
            let next_addr = self.metadata.next_block(block.hba())?;
            let previous_mac = block.current_mac();
            block.reset(next_addr);
            block.set_previous_mac(&previous_mac);
        }
        block.add_record(record.as_ref())
    }
    pub async fn sync(&mut self) -> Result<()> {
        let mut block = self.current_block.lock();
        block.persist(&self.disk, &self.root_key).await?;
        self.metadata.set_last_block(block.hba());
        let next_addr = self.metadata.next_block(block.hba())?;
        let previous_mac = block.current_mac();
        block.reset(next_addr);
        block.set_previous_mac(&previous_mac);
        self.metadata.persist(&self.disk, &self.root_key).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::data_log::*;
    use block_device::mem_disk::MemDisk;

    #[allow(unused)]
    fn create_journal_region() -> Journal {
        let total_blocks = 64 * MiB / BLOCK_SIZE;
        let disk = Arc::new(MemDisk::new(total_blocks).unwrap());
        let range = HbaRange::new(Hba::new(0)..Hba::new(total_blocks as u64));
        let root_key = DefaultCryptor::gen_random_key();
        Journal::new(disk, range, root_key)
    }

    #[test]
    fn journal_add_record() -> Result<()> {
        let mut journal = create_journal_region();
        match async_rt::task::block_on(async move {
            let record = random_datalog();
            match journal.add_record(Box::new(record)).await {
                Ok(_) => {
                    let block = journal.current_block.lock();
                    Ok((block.hba(), block.record_count(), block.offset()))
                },
                Err(_) => Err(()),
            }
        }) {
            Ok((hba, record_count, offset)) => {
                assert_eq!(hba, Hba::new(1));
                assert_eq!(record_count, 1);
                assert_eq!(offset, RECORD_OFFSET + 64);
                Ok(())
            },
            Err(_) => unreachable!(),
        }
    }
    #[test]
    fn journal_sync() -> Result<()> {
        let mut journal = create_journal_region();
        match async_rt::task::block_on(async move {
            let record = Box::new(random_datalog());
            journal.add_record(record).await;
            journal.sync().await;
            let record = Box::new(random_datalog());
            journal.add_record(record).await;
            match journal.sync().await {
                Ok(_) => {
                    Ok((
                        journal.metadata.first_block(),
                        journal.metadata.last_block(),
                        journal.current_block.lock().hba(),
                    ))
                },
                Err(_) => Err(())
            }
        }) {
            Ok((fisrt_block, last_block, current_hba)) => {
                assert_eq!(fisrt_block, Hba::new(1));
                assert_eq!(last_block,Hba::new(2));
                assert_eq!(current_hba, Hba::new(3));
                Ok(())
            },
            Err(_) => unreachable!()
        }
    }
}
