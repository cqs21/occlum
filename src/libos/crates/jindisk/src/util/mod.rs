//! Utility.
pub mod cryption;
mod disk_array;
mod disk_range;
mod disk_view;
mod disk_shadow;
mod range_query_ctx;
pub mod serialize;

use std::convert::TryInto;
use std::mem::size_of;

pub(crate) use disk_array::DiskArray;
pub(crate) use disk_range::{HbaRange, LbaRange};
pub(crate) use disk_view::DiskView;
pub(crate) use disk_shadow::DiskShadow;
pub(crate) use range_query_ctx::RangeQueryCtx;

pub type BitMap = bitvec::prelude::BitVec<Byte, bitvec::prelude::Lsb0>;

pub type Byte = u8;

impl serialize::Serialize for Byte {
    fn encode(&self, encoder: &mut impl serialize::Encoder) -> errno::Result<()> {
        encoder.write_bytes(&self.to_le_bytes());
        Ok(())
    }
    fn decode(buf: &[u8]) -> errno::Result<Self>
        where
            Self: Sized {
        Ok(buf[0])
    }
    fn bytes_len(&self) -> Option<usize> {
        Some(size_of::<Byte>())
    }
}

pub(crate) const fn align_down(x: usize, align: usize) -> usize {
    (x / align) * align
}

pub(crate) const fn align_up(x: usize, align: usize) -> usize {
    ((x + align - 1) / align) * align
}
