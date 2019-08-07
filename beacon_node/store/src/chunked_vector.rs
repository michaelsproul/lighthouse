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
    type Value: Decode + Encode;
    type Length: Unsigned;

    fn update_pattern() -> UpdatePattern;

    fn column() -> DBColumn;

    // TODO: tweak chunk size as appropriate
    fn chunk_size() -> u8 {
        8
    }

    fn get_value<E: EthSpec>(
        state: &BeaconState<E>,
        spec: &ChainSpec,
    ) -> Result<Self::Value, BeaconStateError>;
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

    fn get_value<E: EthSpec>(
        state: &BeaconState<E>,
        spec: &ChainSpec,
    ) -> Result<Self::Value, BeaconStateError> {
        state.get_active_index_root(state.current_epoch(), spec)
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

    // NOTE: using shorted 64-bit keys rather than 32 byte keys like everywhere else
    let chunk_key = &table_index.to_be_bytes()[..];

    // Look up existing chunks
    // FIXME: hardcoded Hash256, should work around with type-level mapping
    let mut chunks = Chunks::load::<F, _>(store, chunk_key)?.unwrap_or_else(Chunks::new);

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

    let vector_value = F::get_value(state, spec)?;

    match chunks.find_chunk_by_id(prev_chunk_id) {
        Some(existing_chunk) => {
            existing_chunk.id = chunk_id;
            existing_chunk.values.push(vector_value);
        }
        // At the chunk boundary, create the chunk
        None if state.slot % u64::from(chunk_size) == 0 => {
            chunks.chunks.push(Chunk::new(chunk_id, vector_value));
        }
        None => {
            return Err(Error::from(ChunkError::MissingParentChunk));
        }
    }

    // Store the updated chunks.
    chunks.store::<F, _>(store, chunk_key)?;

    Ok(())
}

#[derive(Debug)]
pub struct Chunks<T: Decode + Encode> {
    chunks: Vec<Chunk<T>>,
}

/// A chunk of a fixed-size vector from the `BeaconState`, stored in the database.
#[derive(Debug)]
pub struct Chunk<T: Decode + Encode> {
    /// Metadata that allows us to distinguish different short-lived forks, or backtrack.
    ///
    /// Usually set to the state root of the state that the last entry was sourced from.
    pub id: Hash256,
    /// A vector of up-to `chunk_size` values.
    pub values: Vec<T>,
}

impl<T> Chunks<T>
where
    T: Decode + Encode,
{
    pub fn new() -> Self {
        Chunks { chunks: vec![] }
    }

    pub fn load<F: Field, S: Store>(store: &S, key: &[u8]) -> Result<Option<Self>, Error> {
        store
            .get_bytes(F::column().into(), key)?
            .map(|bytes| Self::decode(bytes))
            .transpose()
    }

    pub fn store<F: Field, S: Store>(&self, store: &S, key: &[u8]) -> Result<(), Error> {
        store.put_bytes(F::column().into(), key, &self.encode()?)?;
        Ok(())
    }

    pub fn decode(bytes: Vec<u8>) -> Result<Self, Error> {
        let mut offset = 0;
        let mut result = Chunks::new();
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

impl<T> Chunk<T>
where
    T: Decode + Encode,
{
    pub fn new(id: Hash256, value: T) -> Self {
        Chunk {
            id,
            values: vec![value],
        }
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
    MissingParentChunk,
    ChunkTypeInvalid,
}
