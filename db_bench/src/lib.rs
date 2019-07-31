use std::convert::TryInto;
use std::mem::size_of;
pub use store::*;

#[derive(Debug)]
pub struct Entry {
    pub value: Hash256,
    // pub state_root: Hash256,
}

#[derive(Debug)]
pub struct Entries {
    pub vec: Vec<Entry>,
}

pub const CHUNK_COUNT: usize = 8;

#[derive(Default)]
pub struct Chunk {
    pub values: [Hash256; CHUNK_COUNT],
}

#[derive(Debug)]
pub struct ChunkedEntry<T> {
    pub id: Hash256,
    pub values: Vec<T>,
    // pub state_root: Hash256,
}

#[derive(Debug)]
pub struct ChunkedEntries<T> {
    pub entries: Vec<ChunkedEntry<T>>,
}

impl<T: StoreItem> StoreItem for ChunkedEntry<T> {
    fn db_column() -> DBColumn {
        DBColumn::BeaconState
    }

    fn as_store_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![];
        bytes.extend_from_slice(self.id.as_bytes());
        bytes.push(
            self.values
                .len()
                .try_into()
                .expect("chunk length too large to fit in a byte"),
        );
        for value in &self.values {
            bytes.extend(value.as_store_bytes());
        }
        bytes
    }

    fn from_store_bytes(bytes: &mut [u8]) -> Result<(Self, usize), Error> {
        let id = Hash256::from_slice(&bytes[..32]);
        let length = usize::from(bytes[32]);

        let mut offset = 33;
        let mut values = vec![];

        for _ in 0..length {
            let (value, size) = T::from_store_bytes(&mut bytes[offset..])?;
            values.push(value);
            offset += size;
        }

        Ok((ChunkedEntry { id, values }, offset))
    }
}

impl<T: StoreItem> StoreItem for ChunkedEntries<T> {
    fn db_column() -> DBColumn {
        DBColumn::BeaconState
    }

    fn as_store_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![];
        for chunked_entry in &self.entries {
            bytes.extend(chunked_entry.as_store_bytes());
        }
        bytes
    }

    fn from_store_bytes(bytes: &mut [u8]) -> Result<(Self, usize), Error> {
        let mut offset = 0;
        let mut result = ChunkedEntries { entries: vec![] };
        // This is a bit dodgy
        while offset < bytes.len() {
            let (chunked_entry, size) = ChunkedEntry::from_store_bytes(&mut bytes[offset..])?;
            result.entries.push(chunked_entry);
            offset += size;
        }
        Ok((result, offset))
    }
}

impl StoreItem for Entries {
    fn db_column() -> DBColumn {
        DBColumn::BeaconState
    }

    fn as_store_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![];
        for item in &self.vec {
            bytes.extend_from_slice(item.value.as_bytes());
            // bytes.extend_from_slice(item.state_root.as_bytes());
        }
        bytes
    }

    fn from_store_bytes(bytes: &mut [u8]) -> Result<(Self, usize), Error> {
        Ok((
            Entries {
                vec: bytes
                    .chunks(size_of::<Entry>())
                    .map(|chunk| Entry {
                        value: Hash256::from_slice(&chunk[..32]),
                        // state_root: Hash256::from_slice(&chunk[32..]),
                    })
                    .collect(),
            },
            bytes.len(),
        ))
    }
}

impl StoreItem for Entry {
    fn db_column() -> DBColumn {
        DBColumn::BeaconState
    }

    fn as_store_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![];
        bytes.extend_from_slice(self.value.as_bytes());
        // bytes.extend_from_slice(self.state_root.as_bytes());
        bytes
    }

    fn from_store_bytes(bytes: &mut [u8]) -> Result<(Self, usize), Error> {
        Ok((
            Entry {
                value: Hash256::from_slice(&bytes[..32]),
                // state_root: Hash256::from_slice(&bytes[32..]),
            },
            bytes.len(),
        ))
    }
}

/*
impl StoreItem for Entries {
    fn db_column() -> DBColumn {
        DBColumn::BeaconState
    }

    fn as_store_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![];
        for item in &self.vec {
            bytes.extend_from_slice(item.value.as_bytes());
            // bytes.extend_from_slice(item.state_root.as_bytes());
        }
        bytes
    }

    fn from_store_bytes(bytes: &mut [u8]) -> Result<(Self, usize), Error> {
        Ok(Entries {
            vec: bytes
                .chunks(size_of::<Entry>())
                .map(|chunk| Entry {
                    value: Hash256::from_slice(&chunk[..32]),
                    // state_root: Hash256::from_slice(&chunk[32..]),
                })
                .collect(),
        })
    }
}
*/

impl StoreItem for Chunk {
    fn db_column() -> DBColumn {
        DBColumn::BeaconState
    }

    fn as_store_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![];
        for item in &self.values {
            bytes.extend_from_slice(item.as_bytes());
        }
        bytes
    }

    fn from_store_bytes(bytes: &mut [u8]) -> Result<(Self, usize), Error> {
        let values_vec = bytes
            .chunks(32)
            .map(|b| Hash256::from_slice(b))
            .collect::<Vec<_>>();
        let mut values = [Hash256::zero(); CHUNK_COUNT];
        values.copy_from_slice(&values_vec);
        Ok((Chunk { values }, bytes.len()))
    }
}

pub fn int_key(i: u64) -> Hash256 {
    Hash256::from_low_u64_be(i)
}

pub fn int_keyb(i: u64) -> BytesKey {
    BytesKey {
        key: int_key(i).as_bytes().to_vec(),
    }
}
