pub use store::*;

#[derive(Debug)]
pub struct Entry {
    pub value: Hash256,
    pub state_root: Hash256,
}

#[derive(Debug)]
pub struct Entries {
    pub vec: Vec<Entry>,
}

pub const CHUNK_COUNT: usize = 16;

#[derive(Default)]
pub struct Chunk {
    pub values: [Hash256; CHUNK_COUNT],
}

impl StoreItem for Entry {
    fn db_column() -> DBColumn {
        DBColumn::BeaconState
    }

    fn as_store_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![];
        bytes.extend_from_slice(self.value.as_bytes());
        bytes.extend_from_slice(self.state_root.as_bytes());
        bytes
    }

    fn from_store_bytes(bytes: &mut [u8]) -> Result<Self, Error> {
        Ok(Entry {
            value: Hash256::from_slice(&bytes[..32]),
            state_root: Hash256::from_slice(&bytes[32..]),
        })
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
            bytes.extend_from_slice(item.state_root.as_bytes());
        }
        bytes
    }

    fn from_store_bytes(bytes: &mut [u8]) -> Result<Self, Error> {
        Ok(Entries {
            vec: bytes
                .chunks(64)
                .map(|chunk| Entry {
                    value: Hash256::from_slice(&chunk[..32]),
                    state_root: Hash256::from_slice(&chunk[32..]),
                })
                .collect(),
        })
    }
}

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

    fn from_store_bytes(bytes: &mut [u8]) -> Result<Self, Error> {
        let values_vec = bytes
            .chunks(32)
            .map(|b| Hash256::from_slice(b))
            .collect::<Vec<_>>();
        let mut values = [Hash256::zero(); CHUNK_COUNT];
        values.copy_from_slice(&values_vec);
        Ok(Chunk { values })
    }
}

pub fn int_key(i: u64) -> Hash256 {
    Hash256::from_low_u64_be(i)
}

pub fn int_keyb(i: u64) -> BytesKey {
    BytesKey {
        key: Hash256::from_low_u64_be(i).as_bytes().to_vec(),
    }
}
