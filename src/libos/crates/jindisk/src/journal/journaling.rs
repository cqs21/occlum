use crate::prelude::*;
use super::*;
use std::convert::TryInto;
use std::mem::size_of;

// Disk layout of a journal block(4K):
//   +-------------+--------------+--------------+---------+------------+
//   | current mac | previous mac | record count | records | padding(0) |
//   +-------------+--------------+--------------+---------+------------+
// JounalMeta reuse this struct to persist its own data.
pub struct JournalBlock {
    hba: Hba,
    buffer: [u8; BLOCK_SIZE],
    offset: usize,
}
pub const RECORD_OFFSET: usize = AUTH_ENC_MAC_SIZE * 2 + size_of::<usize>();

impl Encoder for JournalBlock {
    fn write_bytes(&mut self, buf: &[u8]) -> Result<()> {
        let offset = self.offset;
        if offset + buf.len() > BLOCK_SIZE {
            return_errno!(EINVAL, "journal block buffer overflow");
        }
        self.buffer[offset..offset + buf.len()].copy_from_slice(buf);
        self.offset += buf.len();
        Ok(())
    }
}

impl JournalBlock {
    pub fn new(hba: Hba) -> Self {
        Self {
            hba,
            buffer: [0u8; BLOCK_SIZE],
            offset: RECORD_OFFSET,
        }
    }
    pub fn hba(&self) -> Hba {
        self.hba
    }
    pub fn offset(&self) -> usize {
        self.offset
    }
    pub fn current_mac(&self) -> Mac {
        let mut mac = [0u8; AUTH_ENC_MAC_SIZE];
        mac.copy_from_slice(&self.buffer[0..AUTH_ENC_MAC_SIZE]);
        mac
    }
    pub fn set_current_mac(&mut self, mac: &Mac) {
        self.buffer[0..AUTH_ENC_MAC_SIZE].copy_from_slice(mac);
    }
    pub fn previous_mac(&self) -> Mac {
        let mut mac = [0u8; AUTH_ENC_MAC_SIZE];
        mac.copy_from_slice(&self.buffer[AUTH_ENC_MAC_SIZE..AUTH_ENC_MAC_SIZE * 2]);
        mac
    }
    pub fn set_previous_mac(&mut self, mac: &Mac) {
        self.buffer[AUTH_ENC_MAC_SIZE..AUTH_ENC_MAC_SIZE * 2].copy_from_slice(mac);
    }
    pub fn record_count(&self) -> usize {
        let offset = AUTH_ENC_MAC_SIZE * 2;
        usize::from_le_bytes(self.buffer[offset..offset + size_of::<usize>()]
                            .try_into().unwrap())
    }
    pub fn set_record_count(&mut self, count: usize) {
        let offset = AUTH_ENC_MAC_SIZE * 2;
        self.buffer[offset..offset + size_of::<usize>()].copy_from_slice(&count.to_le_bytes());
    }
    pub fn record(&self, offset: usize, len: usize) -> Result<&[u8]> {
        if offset >= BLOCK_SIZE || len == 0 || (offset + len) > BLOCK_SIZE {
            return_errno!(EINVAL, "journal block cannot retrieve a valid buffer")
        }
        Ok(&self.buffer[offset..offset + len])
    }
    pub fn can_hold(&self, record: &dyn JournalRecord) -> bool {
        self.offset + record.size() <= BLOCK_SIZE
    }
    pub fn add_record(&mut self, record: &dyn JournalRecord) -> Result<()>{
        self.write_bytes(&record.typeid())?;
        self.write_bytes(&record.data())?;
        let count = self.record_count();
        self.set_record_count(count + 1);
        Ok(())
    }
    pub async fn persist(&mut self, disk: &DiskView, key: &Key) -> Result<()> {
        let mut ciphertext = [0u8; BLOCK_SIZE];
        let offset = AUTH_ENC_MAC_SIZE;
        let cipher_meta = DefaultCryptor::encrypt_arbitrary(&self.buffer[offset..], &mut ciphertext[offset..], key);
        ciphertext[..offset].copy_from_slice(cipher_meta.mac());
        disk.write(self.hba, &ciphertext).await?;
        self.set_current_mac(cipher_meta.mac());
        Ok(())
    }
    pub async fn load(&mut self, disk: &DiskView, key: &Key) -> Result<()> {
        let mut ciphertext = [0u8; BLOCK_SIZE];
        disk.read(self.hba, &mut ciphertext).await?;

        let mut offset = AUTH_ENC_MAC_SIZE;
        let mut cipher_mac = [0u8; AUTH_ENC_MAC_SIZE];
        cipher_mac.copy_from_slice(&ciphertext[..offset]);
        let cipher_meta = CipherMeta::new(cipher_mac);
        DefaultCryptor::decrypt_arbitrary(&ciphertext[offset..], &mut self.buffer[offset..], key, &cipher_meta)?;

        self.set_current_mac(cipher_meta.mac());
        self.offset = RECORD_OFFSET;
        Ok(())
    }
    pub fn reset(&mut self, new: Hba) {
        self.hba = new;
        self.buffer.fill(0);
        self.offset = RECORD_OFFSET;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::data_log::*;
    use block_device::mem_disk::MemDisk;

    #[allow(unused)]
    fn default_diskview() -> DiskView {
        let total_blocks = 64 * MiB / BLOCK_SIZE;
        let disk = Arc::new(MemDisk::new(total_blocks).unwrap());
        DiskView::new_unchecked(disk)
    }

    #[test]
    fn journal_block_new() {
        let hba = Hba::new(21);
        let block = JournalBlock::new(hba);
        assert_eq!(block.hba(), hba);
        assert_eq!(block.offset(), RECORD_OFFSET);
        assert_eq!(block.record_count(), 0);
    }
    #[test]
    fn journal_block_can_hold() {
        let hba = Hba::new(0);
        let mut block = JournalBlock::new(hba);
        let record = random_datalog();
        assert_eq!(record.size(), 64);
        assert!(block.can_hold(&record));
        let record = Box::new(record);
        block.offset = BLOCK_SIZE - record.size() + 1;
        assert!(!block.can_hold(record.as_ref()));
    }
    #[test]
    fn journal_block_add_record() {
        let hba = Hba::new(0);
        let mut block = JournalBlock::new(hba);
        let record = random_datalog();
        let size = record.size();
        let count = (BLOCK_SIZE - block.offset()) / size;
        while block.record_count() < count {
            block.add_record(&record);
        }
        assert_eq!(block.record_count(), count);
        assert_eq!(block.offset(), RECORD_OFFSET + size * count);
        match block.add_record(&record) {
            Ok(_) => unreachable!(),
            Err(e) => {
                assert_eq!(EINVAL, e.errno());
                assert!(e.to_string().contains("journal block buffer overflow"));
            }
        }
    }
    #[test]
    fn journal_block_persist_and_load() {
        let disk = default_diskview();
        let hba = Hba::new(0);
        let mut block = JournalBlock::new(hba);
        let record = random_datalog();
        let size = record.size();
        let count = (BLOCK_SIZE - block.offset()) / size;
        while block.record_count() < count {
            block.add_record(&record);
        }
        let key = DefaultCryptor::gen_random_key();
        match async_rt::task::block_on(async move {
            match block.persist(&disk, &key).await {
                Ok(_) => {
                    block.reset(hba);
                    match block.load(&disk, &key).await {
                        Ok(_) => Ok((block.offset(), block.record_count())),
                        Err(_) => Err(()),
                    }
                },
                Err(_) => Err(()),
            }
        }) {
            Ok((offset, record_count)) => {
                assert_eq!(offset, RECORD_OFFSET);
                assert_eq!(record_count, count);
            },
            Err(_) => unreachable!(),
        };
    }
    #[test]
    fn journal_block_reset() {
        let hba = Hba::new(0);
        let mut block = JournalBlock::new(hba);
        let record = random_datalog();
        block.add_record(&record);
        block.reset(hba + 1);
        assert_eq!(block.hba(), hba + 1);
        assert_eq!(block.offset(), RECORD_OFFSET);
        assert_eq!(block.record_count(), 0);
    }
}
