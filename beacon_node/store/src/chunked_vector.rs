use crate::*;
use ssz::{Decode, Encode};
use std::convert::TryFrom;
use std::marker::PhantomData;
use typenum::Unsigned;

use self::UpdatePattern::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdatePattern {
    OncePerSlot,
    OncePerEpoch,
}

pub trait Field {
    /// The `Default` impl will be used to fill extra vector entries.
    type Value: Decode + Encode + Default;
    type Length: Unsigned;

    fn update_pattern() -> UpdatePattern;

    fn column() -> DBColumn;

    // TODO: tweak chunk size as appropriate
    fn chunk_size() -> u8 {
        8
    }

    /// Get the most recently updated element of this vector from the state.
    fn get_latest_value<E: EthSpec>(
        state: &BeaconState<E>,
        spec: &ChainSpec,
    ) -> Result<Self::Value, BeaconStateError>;
}

/// Implemented for `StateRoots` and `BlockRoots`.
pub trait RootField {
    fn column() -> DBColumn;

    // TODO: tweak chunk size as appropriate
    fn chunk_size() -> u8 {
        8
    }

    /// Get the most recently updated element of this vector from the state.
    fn get_latest_value<E: EthSpec>(
        state: &BeaconState<E>,
        spec: &ChainSpec,
    ) -> Result<Hash256, BeaconStateError>;
}

pub struct BlockRoots<T: EthSpec>(PhantomData<T>);
pub struct StateRoots<T: EthSpec>(PhantomData<T>);
pub struct RandaoMixes<T: EthSpec>(PhantomData<T>);
pub struct ActiveIndexRoots<T: EthSpec>(PhantomData<T>);
pub struct CompactCommitteesRoots<T: EthSpec>(PhantomData<T>);

// TODO: impls for all the fields
impl<T> Field for ActiveIndexRoots<T>
where
    T: EthSpec,
{
    type Value = Hash256;
    type Length = T::EpochsPerHistoricalVector;

    fn update_pattern() -> UpdatePattern {
        OncePerSlot
    }

    fn column() -> DBColumn {
        DBColumn::BeaconActiveIndexRoots
    }

    fn get_latest_value<E: EthSpec>(
        state: &BeaconState<E>,
        spec: &ChainSpec,
    ) -> Result<Self::Value, BeaconStateError> {
        state.get_active_index_root(state.current_epoch(), spec)
    }
}

impl<T> RootField for StateRoots<T>
where
    T: EthSpec,
{
    fn column() -> DBColumn {
        DBColumn::BeaconStateRoots
    }

    fn get_latest_value<E: EthSpec>(
        state: &BeaconState<E>,
        spec: &ChainSpec,
    ) -> Result<Hash256, BeaconStateError> {
        state.get_state_root(state.slot).map(|x| *x)
    }
}

pub fn store_updated_vector_entry<F: Field, E: EthSpec, S: Store>(
    store: &S,
    state_root: &Hash256,
    state: &BeaconState<E>,
    spec: &ChainSpec,
) -> Result<(), Error> {
    // Only store fields that are updated once-per-epoch when the state is from an epoch boundary.
    if F::update_pattern() == OncePerEpoch && state.slot.as_u64() % E::slots_per_epoch() != 0 {
        return Ok(());
    }

    let chunk_size = F::chunk_size();

    let table_index = match F::update_pattern() {
        OncePerSlot => state.slot.as_u64() / u64::from(chunk_size),
        OncePerEpoch => state.current_epoch().as_u64() / u64::from(chunk_size),
    };

    // NOTE: using shorter 64-bit keys rather than 32 byte keys like everywhere else
    let chunk_key = &integer_key(table_index)[..];

    // Look up existing chunks
    let mut chunks = Chunks::load(store, F::column(), chunk_key)?.unwrap_or_else(Chunks::default);

    // Find the chunk stored for the previous slot/epoch
    let prev_chunk_id = match F::update_pattern() {
        // Updated once per slot: we should have an entry for the previous state root.
        OncePerSlot => *state.get_state_root(state.slot - 1)?,
        // Update once per epoch: we should have an entry for the state root from the start
        // of the previous epoch.
        OncePerEpoch => {
            *state.get_state_root(state.previous_epoch().start_slot(E::slots_per_epoch()))?
        }
    };

    let chunk_id = *state_root;

    let vector_value = F::get_latest_value(state, spec)?;

    match chunks.find_chunk_by_id(prev_chunk_id) {
        Some(existing_chunk) => {
            existing_chunk.id = chunk_id;
            existing_chunk.values.push(vector_value);
        }
        // At the chunk boundary, create the chunk
        None if state.slot % u64::from(chunk_size) == 0 => {
            chunks.chunks.push(Chunk::new(chunk_id, vec![vector_value]));
        }
        None => {
            return Err(Error::from(ChunkError::MissingChunk));
        }
    }

    // Store the updated chunks.
    chunks.store(store, F::column(), chunk_key)?;

    Ok(())
}

// FIXME: move
fn integer_key(index: u64) -> [u8; 8] {
    index.to_be_bytes()
}

// TODO: could be more efficient with RocksDB, and an iterator that streams in reverse order
// Chunks at the end index are included.
fn range_query<S: Store, T: Decode + Encode>(
    store: &S,
    column: DBColumn,
    start_index: u64,
    end_index: u64,
) -> Result<Vec<Chunks<T>>, Error> {
    let mut result = vec![];

    for table_index in start_index..=end_index {
        let key = &integer_key(table_index)[..];
        let chunks = Chunks::load(store, column, key)?.ok_or(ChunkError::MissingChunk)?;
        result.push(chunks);
    }

    Ok(result)
}

fn stitch(
    all_chunks: Vec<Chunks<Hash256>>,
    latest_state_root: Hash256,
    start_slot: Slot,
    end_slot: Slot,
    chunk_size: usize,
    length: usize,
) -> Result<Vec<Hash256>, ChunkError> {
    // We include both the start and end slot, so check we won't have too many values
    if (end_slot - start_slot).as_usize() >= length {
        return Err(ChunkError::SlotIntervalTooLarge);
    }

    let start_index = start_slot.as_usize() / chunk_size;
    let end_index = end_slot.as_usize() / chunk_size;

    let mut result = vec![Hash256::zero(); length];

    let mut head = latest_state_root;

    for (chunk_index, chunks) in (start_index..end_index + 1)
        .zip(all_chunks.into_iter())
        .rev()
    {
        // Select the matching chunk
        let chunk = chunks
            .find_chunk_by_element(&head)
            .ok_or(ChunkError::MissingChunk)?;

        // All chunks but the last chunk must be full-sized
        if chunk_index != end_index && chunk.values.len() != chunk_size {
            return Err(ChunkError::InvalidChunkSize);
        }

        // Copy the chunk entries into the result vector
        for (i, value) in chunk.values.iter().enumerate() {
            let slot = chunk_index * chunk_size + i;

            if slot >= start_slot.as_usize() && slot <= end_slot.as_usize() {
                result[slot % length] = *value;
            }
        }

        // Backtrack via the previous pointer located in the chunk's ID
        head = chunk.id;
    }

    Ok(result)
}

pub fn load_state_roots_from_db<F: RootField, E: EthSpec, S: Store>(
    store: &S,
    state_root: &Hash256,
    slot: Slot,
) -> Result<FixedVector<Hash256, E::SlotsPerHistoricalRoot>, Error> {
    // Do a range query
    let chunk_size = u64::from(F::chunk_size());
    let start_slot = slot - (E::SlotsPerHistoricalRoot::to_u64() - 1);
    let start_index = start_slot.as_u64() / chunk_size;
    let end_index = slot.as_u64() / chunk_size;
    let all_chunks = range_query(store, F::column(), start_index, end_index)?;

    // Stitch together the right vector by backtracking through the previous pointers
    let result = stitch(
        all_chunks,
        *state_root,
        start_slot,
        slot,
        usize::from(F::chunk_size()),
        E::SlotsPerHistoricalRoot::to_usize(),
    )?;

    Ok(result.into())
}

// TODO: load block roots from DB (reuse implementation above)

pub fn load_vector_from_db<F: Field, E: EthSpec, S: Store>(
    store: &S,
    state_roots: &FixedVector<Hash256, E::SlotsPerHistoricalRoot>,
) -> Result<FixedVector<F::Value, F::Length>, Error> {
    // Do a range query
    // Select the right fork
    // Build a vector (doing the right modulo junk)
    panic!()
}

#[derive(Debug, Clone)]
pub struct Chunks<T: Decode + Encode> {
    chunks: Vec<Chunk<T>>,
}

/// A chunk of a fixed-size vector from the `BeaconState`, stored in the database.
#[derive(Debug, Clone)]
pub struct Chunk<T: Decode + Encode> {
    /// Metadata that allows us to distinguish different short-lived forks, or backtrack.
    ///
    /// Usually set to the state root of the state that the last entry was sourced from.
    pub id: Hash256,
    /// A vector of up-to `chunk_size` values.
    pub values: Vec<T>,
}

impl<T> Default for Chunks<T>
where
    T: Decode + Encode,
{
    fn default() -> Self {
        Chunks { chunks: vec![] }
    }
}

impl<T> Chunks<T>
where
    T: Decode + Encode,
{
    pub fn new(chunks: Vec<Chunk<T>>) -> Self {
        Chunks { chunks }
    }

    pub fn load<S: Store>(store: &S, column: DBColumn, key: &[u8]) -> Result<Option<Self>, Error> {
        store
            .get_bytes(column.into(), key)?
            .map(|bytes| Self::decode(bytes))
            .transpose()
    }

    pub fn store<S: Store>(&self, store: &S, column: DBColumn, key: &[u8]) -> Result<(), Error> {
        store.put_bytes(column.into(), key, &self.encode()?)?;
        Ok(())
    }

    pub fn decode(bytes: Vec<u8>) -> Result<Self, Error> {
        let mut offset = 0;
        let mut result = Chunks::default();
        while offset < bytes.len() {
            let (chunk, size) = Chunk::decode(&bytes[offset..])?;
            result.chunks.push(chunk);
            offset += size;
        }
        Ok(result)
    }

    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut result = Vec::with_capacity(self.chunks.first().map_or(0, |c| c.encoded_size()));
        for chunk in &self.chunks {
            result.extend(chunk.encode()?);
        }
        Ok(result)
    }

    /// Find a chunk with a given ID.
    pub fn find_chunk_by_id(&mut self, id: Hash256) -> Option<&mut Chunk<T>> {
        for chunk in &mut self.chunks {
            if chunk.id == id {
                return Some(chunk);
            }
        }
        None
    }
}

impl<T> Chunks<T>
where
    T: Decode + Encode + PartialEq,
{
    /// Find a chunk containing a given element, searching backwards through the chunk's values.
    pub fn find_chunk_by_element(&self, elem: &T) -> Option<&Chunk<T>>
    where
        T: PartialEq,
    {
        self.chunks
            .iter()
            .find(|chunk| chunk.values.iter().rev().any(|val| val == elem))
    }
}

impl<T> Chunk<T>
where
    T: Decode + Encode,
{
    pub fn new(id: Hash256, values: Vec<T>) -> Self {
        Chunk { id, values }
    }

    /// Attempt to decode a single chunk, returning the chunk and the number of bytes read.
    pub fn decode(bytes: &[u8]) -> Result<(Self, usize), Error> {
        // NOTE: could have a sub-trait for fixed length SSZ types?
        if !<T as Decode>::is_ssz_fixed_len() {
            return Err(Error::from(ChunkError::ChunkTypeInvalid));
        }

        // Read the ID from the first 32 bytes
        let mut offset = <Hash256 as Decode>::ssz_fixed_len();
        let id = bytes
            .get(0..offset)
            .map(Hash256::from_slice)
            .ok_or(ChunkError::OutOfBounds {
                i: offset - 1,
                len: bytes.len(),
            })?;

        // Read the single length byte (we know chunk_size is a u8)
        let values_length = bytes.get(offset).copied().ok_or(ChunkError::OutOfBounds {
            i: offset,
            len: bytes.len(),
        })?;
        offset += 1;

        // Read the appropriate number of values
        let mut values = vec![];
        let value_size = <T as Decode>::ssz_fixed_len();

        for _ in 0..values_length {
            let value_bytes =
                bytes
                    .get(offset..offset + value_size)
                    .ok_or(ChunkError::OutOfBounds {
                        i: offset + value_size - 1,
                        len: bytes.len(),
                    })?;
            let value = T::from_ssz_bytes(value_bytes)?;
            values.push(value);
            offset += value_size;
        }

        Ok((Chunk { id, values }, offset))
    }

    pub fn encoded_size(&self) -> usize {
        <Hash256 as Encode>::ssz_fixed_len()
            + <u8 as Encode>::ssz_fixed_len()
            + self.values.len() * <T as Encode>::ssz_fixed_len()
    }

    /// Encode a single chunk as bytes.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        // NOTE: could have a sub-trait for fixed length SSZ types?
        if !<T as Decode>::is_ssz_fixed_len() {
            return Err(Error::from(ChunkError::ChunkTypeInvalid));
        }

        let mut result = Vec::with_capacity(self.encoded_size());

        // ID
        result.extend_from_slice(self.id.as_bytes());

        // Length byte
        let length_byte =
            u8::try_from(self.values.len()).map_err(|_| ChunkError::OversizedChunk)?;
        result.push(length_byte);

        // Values
        for value in &self.values {
            result.extend(value.as_ssz_bytes());
        }

        Ok(result)
    }
}

#[derive(Debug, PartialEq)]
pub enum ChunkError {
    OutOfBounds { i: usize, len: usize },
    OversizedChunk,
    InvalidChunkSize,
    MissingChunk,
    ChunkTypeInvalid,
    SlotIntervalTooLarge,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn stitch_basic() {
        fn v(i: u64) -> Hash256 {
            Hash256::from_low_u64_be(i)
        }

        let chunk_size = 4;

        let all_chunks = vec![
            Chunks::new(vec![
                Chunk::new(v(0), vec![v(1), v(2), v(3), v(4)]),
                Chunk::new(v(0), vec![v(1), v(2), v(5), v(6)]),
            ]),
            Chunks::new(vec![
                Chunk::new(v(6), vec![v(11), v(12), v(13), v(14)]),
                Chunk::new(v(4), vec![v(7), v(8), v(9), v(10)]),
            ]),
            Chunks::new(vec![
                Chunk::new(v(10), vec![v(15), v(16), v(17), v(18)]),
                Chunk::new(v(14), vec![v(19)]),
            ]),
        ];

        assert_eq!(
            stitch(
                all_chunks.clone(),
                v(18),
                Slot::new(0),
                Slot::new(11),
                chunk_size,
                12
            )
            .unwrap(),
            vec![
                v(1),
                v(2),
                v(3),
                v(4),
                v(7),
                v(8),
                v(9),
                v(10),
                v(15),
                v(16),
                v(17),
                v(18)
            ]
        );

        assert_eq!(
            stitch(
                all_chunks.clone(),
                v(16),
                Slot::new(2),
                Slot::new(9),
                chunk_size,
                8
            )
            .unwrap(),
            vec![v(15), v(16), v(3), v(4), v(7), v(8), v(9), v(10)]
        );
    }
}
