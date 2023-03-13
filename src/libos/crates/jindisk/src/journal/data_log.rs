use crate::prelude::*;
use super::*;
use openssl::rand::rand_bytes;

pub const DATALOG_TYPEID: TypeId = [0u8; 4];

#[derive(Debug)]
pub struct DataLog {
    lba: Lba,
    hba: Hba,
    key: Key,
    iv: Iv,
    mac: Mac
}

impl DataLog {
    pub fn new(lba: Lba, hba: Hba, key: Key, iv: Iv, mac: Mac) -> Self {
        Self { lba, hba, key, iv, mac }
    }
}

impl JournalRecord for DataLog {
    fn typeid(&self) -> TypeId {
        DATALOG_TYPEID
    }
    fn data(&self) -> Vec<u8> {
        let mut buf = Vec::<u8>::new();
        buf.extend_from_slice(&self.lba.to_raw().to_le_bytes());
        buf.extend_from_slice(&self.hba.to_raw().to_le_bytes());
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.iv);
        buf.extend_from_slice(&self.mac);
        buf
    }
    fn size(&self) -> usize {
        size_of::<TypeId>() +
        size_of::<Lba>() +
        size_of::<Hba>() +
        AUTH_ENC_KEY_SIZE +
        AUTH_ENC_IV_SIZE +
        AUTH_ENC_MAC_SIZE
    }
    fn recover(data: &[u8]) -> Result<usize> where Self: Sized {
        todo!()
    }
}

#[cfg(test)]
pub fn random_datalog() -> DataLog {
    let mut lba = [0u8; 8];
    rand_bytes(&mut lba);
    let lba = Lba::decode(&lba).unwrap();
    let mut hba = [0u8; 8];
    rand_bytes(&mut hba);
    let hba = Hba::decode(&hba).unwrap();
    let mut key = [0u8; AUTH_ENC_KEY_SIZE];
    rand_bytes(&mut key);
    let mut iv = [0u8; AUTH_ENC_IV_SIZE];
    rand_bytes(&mut iv);
    let mut mac = [0u8; AUTH_ENC_MAC_SIZE];
    rand_bytes(&mut mac);
    let dl = DataLog::new(lba, hba, key, iv, mac);
    println!("{:?}", dl);
    return dl;
}
