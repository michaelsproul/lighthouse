use crate::{CachedTreeHash, Error, Hash256, TreeHashCache};
use ssz_types::{typenum::Unsigned, FixedVector, VariableList};
use std::mem::size_of;
use tree_hash::{mix_in_length, BYTES_PER_CHUNK};

/// Compute ceil(log(n))
///
/// Smallest number of bits d so that n <= 2^d
pub fn int_log(n: usize) -> usize {
    match n.checked_next_power_of_two() {
        Some(x) => x.trailing_zeros() as usize,
        None => 8 * std::mem::size_of::<usize>(),
    }
}

pub fn hash256_iter<'a>(values: &'a [Hash256]) -> impl Iterator<Item = [u8; BYTES_PER_CHUNK]> + 'a {
    values.iter().copied().map(Hash256::to_fixed_bytes)
}

pub fn u64_iter<'a>(values: &'a [u64]) -> impl Iterator<Item = [u8; BYTES_PER_CHUNK]> + 'a {
    let type_size = size_of::<u64>();
    let vals_per_chunk = BYTES_PER_CHUNK / type_size;
    values.chunks(vals_per_chunk).map(move |xs| {
        xs.iter().map(|x| x.to_le_bytes()).enumerate().fold(
            [0; BYTES_PER_CHUNK],
            |mut chunk, (i, x_bytes)| {
                chunk[i * type_size..(i + 1) * type_size].copy_from_slice(&x_bytes);
                chunk
            },
        )
    })
}

impl<N: Unsigned> CachedTreeHash for FixedVector<Hash256, N> {
    type Cache = TreeHashCache;

    fn new_tree_hash_cache(&self) -> TreeHashCache {
        TreeHashCache::new(int_log(N::to_usize()))
    }

    fn recalculate_tree_hash_root(&self, cache: &mut Self::Cache) -> Result<Hash256, Error> {
        cache.recalculate_merkle_root(hash256_iter(&self))
    }
}

impl<N: Unsigned> CachedTreeHash for FixedVector<u64, N> {
    type Cache = TreeHashCache;

    fn new_tree_hash_cache(&self) -> TreeHashCache {
        let vals_per_chunk = BYTES_PER_CHUNK / size_of::<u64>();
        TreeHashCache::new(int_log(N::to_usize() / vals_per_chunk))
    }

    fn recalculate_tree_hash_root(&self, cache: &mut Self::Cache) -> Result<Hash256, Error> {
        cache.recalculate_merkle_root(u64_iter(&self))
    }
}

impl<N: Unsigned> CachedTreeHash for VariableList<Hash256, N> {
    type Cache = TreeHashCache;

    fn new_tree_hash_cache(&self) -> TreeHashCache {
        TreeHashCache::new(int_log(N::to_usize()))
    }

    fn recalculate_tree_hash_root(&self, cache: &mut TreeHashCache) -> Result<Hash256, Error> {
        Ok(Hash256::from_slice(&mix_in_length(
            cache
                .recalculate_merkle_root(hash256_iter(&self))?
                .as_bytes(),
            self.len(),
        )))
    }
}

impl<N: Unsigned> CachedTreeHash for VariableList<u64, N> {
    type Cache = TreeHashCache;

    fn new_tree_hash_cache(&self) -> TreeHashCache {
        let vals_per_chunk = BYTES_PER_CHUNK / size_of::<u64>();
        TreeHashCache::new(int_log(N::to_usize() / vals_per_chunk))
    }

    fn recalculate_tree_hash_root(&self, cache: &mut TreeHashCache) -> Result<Hash256, Error> {
        Ok(Hash256::from_slice(&mix_in_length(
            cache.recalculate_merkle_root(u64_iter(&self))?.as_bytes(),
            self.len(),
        )))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_int_log() {
        for i in 0..63 {
            assert_eq!(int_log(2usize.pow(i)), i as usize);
        }
        assert_eq!(int_log(10), 4);
    }
}
