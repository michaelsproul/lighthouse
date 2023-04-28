//! Hierarchical diff implementation.
use itertools::Itertools;
use qbsdiff::{Bsdiff, Bspatch};
use serde::{Deserialize, Serialize};
use ssz::Encode;
use std::io::{Read, Write};
use types::{BeaconState, Epoch, EthSpec, VList};
use zstd::{Decoder, Encoder};

#[derive(Debug)]
pub enum Error {
    InvalidHierarchy,
    XorDeletionsNotSupported,
    Bsdiff(std::io::Error),
    Compression(std::io::Error),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HierarchyConfig {
    exponents: Vec<u8>,
}

#[derive(Debug)]
pub struct HierarchyModuli {
    moduli: Vec<u64>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum StorageStrategy {
    Nothing,
    DiffFrom(Epoch),
    Snapshot,
}

/*
/// Hierarchical state diff input, a state with its balances extracted.
#[derive(Debug)]
pub struct HDiffInput<E: EthSpec> {
    state: BeaconState<E>,
    balances: VList<u64, E::ValidatorRegistryLimit>,
}
*/

/// Hierarchical diff output and working buffer.
pub struct HDiffBuffer {
    state: Vec<u8>,
    balances: Vec<u8>,
}

/// Hierarchical state diff.
#[derive(Debug)]
pub struct HDiff {
    state_diff: BytesDiff,
    balances_diff: XorDiff,
}

#[derive(Debug)]
pub struct BytesDiff {
    bytes: Vec<u8>,
}

#[derive(Debug)]
pub struct XorDiff {
    bytes: Vec<u8>,
}

impl HDiffBuffer {
    pub fn from_state<E: EthSpec>(mut beacon_state: BeaconState<E>) -> Self {
        let balances_list = std::mem::take(beacon_state.balances_mut());

        let state = beacon_state.as_ssz_bytes();
        let balances = balances_list.as_ssz_bytes();

        HDiffBuffer { state, balances }
    }
}

impl HDiff {
    pub fn compute(source: &HDiffBuffer, target: &HDiffBuffer) -> Result<Self, Error> {
        let state_diff = BytesDiff::compute(&source.state, &target.state)?;
        let mut balances_diff = XorDiff::compute(&source.balances, &target.balances)?;

        Ok(Self {
            state_diff,
            balances_diff,
        })
    }

    pub fn apply(&self, source: &mut HDiffBuffer) -> Result<(), Error> {
        // FIXME(sproul): unfortunate clone
        let source_state = source.state.clone();
        self.state_diff.apply(&source_state, &mut source.state)?;

        self.balances_diff.apply(&mut source.balances)?;
        Ok(())
    }

    pub fn state_diff_len(&self) -> usize {
        self.state_diff.bytes.len()
    }

    pub fn balances_diff_len(&self) -> usize {
        self.balances_diff.bytes.len()
    }
}

impl BytesDiff {
    pub fn compute(source: &[u8], target: &[u8]) -> Result<Self, Error> {
        // FIXME(sproul): benchmark different buffer sizes
        let mut diff = vec![];
        Bsdiff::new(source, target)
            .compression_level(1)
            .compare(&mut diff)
            .map_err(Error::Bsdiff)?;
        Ok(BytesDiff { bytes: diff })
    }

    pub fn apply(&self, source: &[u8], target: &mut Vec<u8>) -> Result<(), Error> {
        Bspatch::new(&self.bytes)
            .and_then(|patch| patch.apply(source, std::io::Cursor::new(target)))
            .map(|_: u64| ())
            .map_err(Error::Bsdiff)
    }
}

// FIXME(sproul): gotta use the u64s, keep going here
impl XorDiff {
    pub fn compute(xs: &[u8], ys: &[u8]) -> Result<Self, Error> {
        if xs.len() > ys.len() {
            return Err(Error::XorDeletionsNotSupported);
        }

        let mut uncompressed_bytes: Vec<u8> = ys
            .iter()
            .enumerate()
            .map(|(i, y)| {
                // Diff from 0 if the entry is new.
                // Zero is a neutral element for XOR: 0 ^ y = y.
                let x = xs.get(i).copied().unwrap_or(0);
                y.wrapping_sub(x)
            })
            .collect();

        // FIXME(sproul): reconsider
        let compression_level = 1;
        let mut compressed_bytes = Vec::with_capacity(2 * uncompressed_bytes.len());
        let mut encoder =
            Encoder::new(&mut compressed_bytes, compression_level).map_err(Error::Compression)?;
        encoder
            .write_all(&uncompressed_bytes)
            .map_err(Error::Compression)?;
        encoder.finish().map_err(Error::Compression)?;

        Ok(XorDiff {
            bytes: compressed_bytes,
        })
    }

    pub fn apply(&self, xs: &mut Vec<u8>) -> Result<(), Error> {
        // Decompress balances diff.
        let mut balances_diff = Vec::with_capacity(2 * self.bytes.len());
        let mut decoder = Decoder::new(&*self.bytes).map_err(Error::Compression)?;
        decoder
            .read_to_end(&mut balances_diff)
            .map_err(Error::Compression)?;

        for (i, diff) in balances_diff.iter().enumerate() {
            if let Some(x) = xs.get_mut(i) {
                *x = x.wrapping_add(*diff);
            } else {
                xs.push(*diff);
            }
        }
        Ok(())
    }
}

impl Default for HierarchyConfig {
    fn default() -> Self {
        HierarchyConfig {
            exponents: vec![0, 4, 6, 8, 11, 13, 16],
        }
    }
}

impl HierarchyConfig {
    pub fn to_moduli(&self) -> Result<HierarchyModuli, Error> {
        self.validate()?;
        let moduli = self.exponents.iter().map(|n| 1 << n).collect();
        Ok(HierarchyModuli { moduli })
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self.exponents.len() > 2
            && self
                .exponents
                .iter()
                .tuple_windows()
                .all(|(small, big)| small < big && *big < u64::BITS as u8)
        {
            Ok(())
        } else {
            Err(Error::InvalidHierarchy)
        }
    }
}

impl HierarchyModuli {
    pub fn storage_strategy(&self, epoch: Epoch) -> Result<StorageStrategy, Error> {
        let last = self.moduli.last().copied().ok_or(Error::InvalidHierarchy)?;

        if epoch % last == 0 {
            return Ok(StorageStrategy::Snapshot);
        }

        let diff_from = self.moduli.iter().rev().find_map(|&n| {
            (epoch % n == 0).then(|| {
                // Diff from the previous state.
                (epoch - 1) / n * n
            })
        });
        Ok(diff_from.map_or(StorageStrategy::Nothing, StorageStrategy::DiffFrom))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_storage_strategy() {
        let config = HierarchyConfig::default();
        config.validate().unwrap();

        let moduli = config.to_moduli().unwrap();

        // Full snapshots at multiples of 2^16.
        let snapshot_freq = Epoch::new(1 << 16);
        assert_eq!(
            moduli.storage_strategy(Epoch::new(0)).unwrap(),
            StorageStrategy::Snapshot
        );
        assert_eq!(
            moduli.storage_strategy(snapshot_freq).unwrap(),
            StorageStrategy::Snapshot
        );
        assert_eq!(
            moduli.storage_strategy(snapshot_freq * 3).unwrap(),
            StorageStrategy::Snapshot
        );

        // For the first layer of diffs
        let first_layer = Epoch::new(1 << 13);
        assert_eq!(
            moduli.storage_strategy(first_layer * 2).unwrap(),
            StorageStrategy::DiffFrom(first_layer)
        );
    }

    #[test]
    fn xor_vs_bytes_diff() {
        let x_values = vec![99u64, 55, 123, 6834857, 0, 12];
        let y_values = vec![98u64, 55, 312, 1, 1, 2, 4, 5];

        let to_bytes =
            |nums: &[u64]| -> Vec<u8> { nums.iter().flat_map(|x| x.to_be_bytes()).collect() };

        let x_bytes = to_bytes(&x_values);
        let y_bytes = to_bytes(&y_values);

        let xor_diff = XorDiff::compute(&x_bytes, &y_bytes).unwrap();

        let mut y_from_xor = x_bytes.clone();
        xor_diff.apply(&mut y_from_xor).unwrap();

        assert_eq!(y_bytes, y_from_xor);

        let bytes_diff = BytesDiff::compute(&x_bytes, &y_bytes).unwrap();

        let mut y_from_bytes = x_bytes.clone();
        bytes_diff.apply(&x_bytes, &mut y_from_bytes).unwrap();

        assert_eq!(y_bytes, y_from_bytes);

        // XOR diff wins by more than a factor of 3
        assert!(xor_diff.bytes.len() < 3 * bytes_diff.bytes.len());
    }
}
