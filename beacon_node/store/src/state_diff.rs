use crate::{DBColumn, Error, StoreItem};
use flate2::bufread::{ZlibDecoder, ZlibEncoder};
use ssz::{Decode, Encode};
use std::io::Read;
use types::{beacon_state::BeaconStateDiff, EthSpec};

impl<E: EthSpec> StoreItem for BeaconStateDiff<E> {
    fn db_column() -> DBColumn {
        DBColumn::BeaconStateDiff
    }

    fn as_store_bytes(&self) -> Result<Vec<u8>, Error> {
        let value = self.as_ssz_bytes();
        let mut encoder = ZlibEncoder::new(&value[..], flate2::Compression::default());
        // FIXME(sproul): try vec with capacity
        let mut compressed_value = vec![];
        encoder
            .read_to_end(&mut compressed_value)
            .map_err(Error::FlateCompression)?;
        Ok(compressed_value)
    }

    fn from_store_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let mut ssz_bytes = vec![];
        let mut decoder = ZlibDecoder::new(bytes);
        decoder
            .read_to_end(&mut ssz_bytes)
            .map_err(Error::FlateCompression)?;
        Ok(Self::from_ssz_bytes(&ssz_bytes)?)
    }
}
