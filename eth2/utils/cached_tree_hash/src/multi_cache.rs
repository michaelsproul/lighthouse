use crate::{int_log, CachedTreeHash, Error, Hash256, TreeHashCache};
use ssz_types::{typenum::Unsigned, VariableList};
use tree_hash::mix_in_length;

/// Tree hash cache for the values.
// TODO: could maybe make these composable?
#[derive(Debug, PartialEq, Clone, Default)]
pub struct MultiTreeHashCache {
    list_cache: TreeHashCache,
    value_caches: Vec<TreeHashCache>,
}

impl<T, N> CachedTreeHash<MultiTreeHashCache> for VariableList<T, N>
where
    T: CachedTreeHash<TreeHashCache>,
    N: Unsigned,
{
    fn new_tree_hash_cache() -> MultiTreeHashCache {
        MultiTreeHashCache {
            list_cache: TreeHashCache::new(int_log(N::to_usize())),
            value_caches: vec![],
        }
    }

    fn recalculate_tree_hash_root(&self, cache: &mut MultiTreeHashCache) -> Result<Hash256, Error> {
        // TODO: work out how to do this in a single pass (error handling is the only issue!)
        let value_caches = &mut cache.value_caches;
        let list_cache = &mut cache.list_cache;

        if self.len() < value_caches.len() {
            return Err(Error::CannotShrink);
        }

        // Resize the value caches to the size of the list.
        value_caches.resize(self.len(), T::new_tree_hash_cache());

        // Update all individual value caches
        let value_roots = self
            .iter()
            .zip(value_caches.iter_mut())
            .map(|(value, cache)| {
                value
                    .recalculate_tree_hash_root(cache)
                    .unwrap()
                    .to_fixed_bytes()
            });

        // Pipe the value roots into the list cache, then mix in the length
        let list_root = list_cache.recalculate_merkle_root(value_roots)?;
        /*
        let list_root = cache.list_cache.recalculate_merkle_root(
            cache
                .value_caches
                .iter()
                .map(|value_cache| value_cache.root().to_fixed_bytes()),
        )?;
        */

        Ok(Hash256::from_slice(&mix_in_length(
            list_root.as_bytes(),
            self.len(),
        )))
    }
}
