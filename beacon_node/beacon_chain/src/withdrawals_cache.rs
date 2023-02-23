use lru::LruCache;
use types::{Epoch, EthSpec, Hash256, Withdrawals};

/// The cached values are quite small, but we also don't expect to see that many of them because we
/// only need 1 or 2 per viable head.
const CACHE_SIZE: usize = 16;

#[derive(Debug)]
pub struct WithdrawalsCache<E: EthSpec> {
    cache: LruCache<WithdrawalsCacheKey, Withdrawals<E>>,
}

impl<E: EthSpec> Default for WithdrawalsCache<E> {
    fn default() -> Self {
        Self {
            cache: LruCache::new(CACHE_SIZE),
        }
    }
}

/// Expected withdrawals are uniquely determined by:
///
/// - The latest block, and
/// - The epoch of the block to be proposed atop it.
#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub struct WithdrawalsCacheKey {
    pub parent_block_root: Hash256,
    pub proposal_epoch: Epoch,
}

impl<E: EthSpec> WithdrawalsCache<E> {
    pub fn get(&mut self, key: &WithdrawalsCacheKey) -> Option<Withdrawals<E>> {
        self.cache.get(key).cloned()
    }

    pub fn insert(&mut self, key: WithdrawalsCacheKey, withdrawals: &Withdrawals<E>) {
        self.cache.get_or_insert(key, || withdrawals.clone());
    }
}
