//! Defines the `BeaconForkChoiceStore` which provides the persistent storage for the `ForkChoice`
//! struct.
//!
//! Additionally, the `BalancesCache` struct is defined; a cache designed to avoid database
//! reads when fork choice requires the validator balances of the justified state.

use crate::{BeaconSnapshot, metrics};
use educe::Educe;
use fixed_bytes::FixedBytesExtended;
use fork_choice::ForkChoiceStore;
use proto_array::JustifiedBalances;
use safe_arith::ArithError;
use ssz_derive::{Decode, Encode};
use std::collections::BTreeSet;
use std::marker::PhantomData;
use std::sync::Arc;
use store::{Error as StoreError, HotColdDB, ItemStore};
use superstruct::superstruct;
use tracing::debug;
use types::{
    AbstractExecPayload, BeaconBlockRef, BeaconState, BeaconStateError, Checkpoint, Epoch, EthSpec,
    Hash256, Slot,
};

#[derive(Debug)]
pub enum Error {
    FailedToReadBlock(StoreError),
    MissingBlock(Hash256),
    FailedToReadState(StoreError),
    MissingState(Hash256),
    BeaconStateError(BeaconStateError),
    UnalignedCheckpoint { block_slot: Slot, state_slot: Slot },
    Arith(ArithError),
}

impl From<BeaconStateError> for Error {
    fn from(e: BeaconStateError) -> Self {
        Error::BeaconStateError(e)
    }
}

impl From<ArithError> for Error {
    fn from(e: ArithError) -> Self {
        Error::Arith(e)
    }
}

/// The number of validator balance sets that are cached within `BalancesCache`.
const MAX_BALANCE_CACHE_SIZE: usize = 4;

#[derive(PartialEq, Clone, Debug)]
pub(crate) struct CacheItem {
    pub(crate) block_root: Hash256,
    pub(crate) epoch: Epoch,
    pub(crate) justified_balances: JustifiedBalances,
}

#[derive(PartialEq, Clone, Default, Debug)]
pub struct BalancesCache {
    pub(crate) items: Vec<CacheItem>,
}

impl BalancesCache {
    /// Add an entry for the given epoch boundary block root, built from `state`.
    ///
    /// The caller must pass a `state` at the epoch boundary slot, so that the cached balances
    /// match the checkpoint state exactly (see `on_verified_block`).
    pub fn insert<E: EthSpec>(
        &mut self,
        epoch_boundary_root: Hash256,
        state: &BeaconState<E>,
    ) -> Result<(), Error> {
        let epoch = state.current_epoch();
        if self.position(epoch_boundary_root, epoch).is_none() {
            let item = CacheItem {
                block_root: epoch_boundary_root,
                epoch,
                justified_balances: JustifiedBalances::from_justified_state(state)?,
            };

            if self.items.len() == MAX_BALANCE_CACHE_SIZE {
                self.items.remove(0);
            }

            self.items.push(item);
        }

        Ok(())
    }

    fn position(&self, block_root: Hash256, epoch: Epoch) -> Option<usize> {
        self.items
            .iter()
            .position(|item| item.block_root == block_root && item.epoch == epoch)
    }

    /// Get the balances for the given `block_root`, if any.
    ///
    /// If some balances are found, they are cloned from the cache.
    pub fn get(&mut self, block_root: Hash256, epoch: Epoch) -> Option<JustifiedBalances> {
        let i = self.position(block_root, epoch)?;
        Some(self.items[i].justified_balances.clone())
    }
}

/// Implements `fork_choice::ForkChoiceStore` in order to provide a persistent backing to the
/// `fork_choice::ForkChoice` struct.
#[derive(Debug, Educe)]
#[educe(PartialEq(bound(E: EthSpec, Hot: ItemStore, Cold: ItemStore)))]
pub struct BeaconForkChoiceStore<E: EthSpec, Hot: ItemStore, Cold: ItemStore> {
    #[educe(PartialEq(ignore))]
    store: Arc<HotColdDB<E, Hot, Cold>>,
    balances_cache: BalancesCache,
    time: Slot,
    finalized_checkpoint: Checkpoint,
    justified_checkpoint: Checkpoint,
    /// The balances of the state for `justified_checkpoint`, which has root `justified_state_root`,
    /// as built by `JustifiedBalances::from_justified_state` (possibly via the `BalancesCache`).
    ///
    /// Invariant: `justified_checkpoint`, `justified_state_root` and `justified_balances` always
    /// describe the same checkpoint. They are only ever assigned together, by a constructor or by
    /// `set_justified_checkpoint`, which leaves all three unchanged if it fails.
    ///
    /// The invariant also relies on callers of `set_justified_checkpoint` passing the root of the
    /// checkpoint's own state, which is not checked. See the precondition on
    /// `ForkChoiceStore::set_justified_checkpoint`.
    justified_balances: JustifiedBalances,
    justified_state_root: Hash256,
    unrealized_justified_checkpoint: Checkpoint,
    unrealized_justified_state_root: Hash256,
    unrealized_finalized_checkpoint: Checkpoint,
    proposer_boost_root: Hash256,
    equivocating_indices: BTreeSet<u64>,
    _phantom: PhantomData<E>,
}

impl<E, Hot, Cold> BeaconForkChoiceStore<E, Hot, Cold>
where
    E: EthSpec,
    Hot: ItemStore,
    Cold: ItemStore,
{
    /// Initialize `Self` from some `anchor` checkpoint which may or may not be the genesis state.
    ///
    /// ## Specification
    ///
    /// Equivalent to:
    ///
    /// https://github.com/ethereum/eth2.0-specs/blob/v0.12.1/specs/phase0/fork-choice.md#get_forkchoice_store
    ///
    /// ## Notes:
    ///
    /// It is assumed that `anchor` is already persisted in `store`.
    pub fn get_forkchoice_store(
        store: Arc<HotColdDB<E, Hot, Cold>>,
        anchor: BeaconSnapshot<E>,
    ) -> Result<Self, Error> {
        let unadvanced_state_root = anchor.beacon_state_root();
        let mut anchor_state = anchor.beacon_state;
        let mut anchor_block_header = anchor_state.latest_block_header().clone();

        // The anchor state MUST be on an epoch boundary (it should be advanced by the caller).
        if !anchor_state
            .slot()
            .as_u64()
            .is_multiple_of(E::slots_per_epoch())
        {
            return Err(Error::UnalignedCheckpoint {
                block_slot: anchor_block_header.slot,
                state_slot: anchor_state.slot(),
            });
        }

        // Compute the accurate block root for the checkpoint block.
        if anchor_block_header.state_root.is_zero() {
            anchor_block_header.state_root = unadvanced_state_root;
        }
        let anchor_block_root = anchor_block_header.canonical_root();
        let anchor_epoch = anchor_state.current_epoch();
        let justified_checkpoint = Checkpoint {
            epoch: anchor_epoch,
            root: anchor_block_root,
        };
        let finalized_checkpoint = justified_checkpoint;
        let justified_balances = JustifiedBalances::from_justified_state(&anchor_state)?;
        let justified_state_root = anchor_state.canonical_root()?;

        Ok(Self {
            store,
            balances_cache: <_>::default(),
            time: anchor_state.slot(),
            justified_checkpoint,
            justified_balances,
            justified_state_root,
            finalized_checkpoint,
            unrealized_justified_checkpoint: justified_checkpoint,
            unrealized_justified_state_root: justified_state_root,
            unrealized_finalized_checkpoint: finalized_checkpoint,
            proposer_boost_root: Hash256::zero(),
            equivocating_indices: BTreeSet::new(),
            _phantom: PhantomData,
        })
    }

    /// Save the current state of `Self` to a `PersistedForkChoiceStore` which can be stored to the
    /// on-disk database.
    pub fn to_persisted(&self) -> PersistedForkChoiceStore {
        PersistedForkChoiceStore {
            time: self.time,
            finalized_checkpoint: self.finalized_checkpoint,
            justified_checkpoint: self.justified_checkpoint,
            justified_state_root: self.justified_state_root,
            unrealized_justified_checkpoint: self.unrealized_justified_checkpoint,
            unrealized_justified_state_root: self.unrealized_justified_state_root,
            unrealized_finalized_checkpoint: self.unrealized_finalized_checkpoint,
            proposer_boost_root: self.proposer_boost_root,
            equivocating_indices: self.equivocating_indices.clone(),
        }
    }

    /// Restore `Self` from a previously-generated `PersistedForkChoiceStore`.
    pub fn from_persisted(
        persisted: PersistedForkChoiceStore,
        store: Arc<HotColdDB<E, Hot, Cold>>,
    ) -> Result<Self, Error> {
        let justified_checkpoint = persisted.justified_checkpoint;
        let justified_state_root = persisted.justified_state_root;

        let update_cache = true;
        let justified_state = store
            .get_hot_state(&justified_state_root, update_cache)
            .map_err(Error::FailedToReadState)?
            .ok_or(Error::MissingState(justified_state_root))?;

        let justified_balances = JustifiedBalances::from_justified_state(&justified_state)?;
        Ok(Self {
            store,
            balances_cache: <_>::default(),
            time: persisted.time,
            finalized_checkpoint: persisted.finalized_checkpoint,
            justified_checkpoint,
            justified_balances,
            justified_state_root,
            unrealized_justified_checkpoint: persisted.unrealized_justified_checkpoint,
            unrealized_justified_state_root: persisted.unrealized_justified_state_root,
            unrealized_finalized_checkpoint: persisted.unrealized_finalized_checkpoint,
            proposer_boost_root: persisted.proposer_boost_root,
            equivocating_indices: persisted.equivocating_indices,
            _phantom: PhantomData,
        })
    }
}

impl<E, Hot, Cold> ForkChoiceStore<E> for BeaconForkChoiceStore<E, Hot, Cold>
where
    E: EthSpec,
    Hot: ItemStore,
    Cold: ItemStore,
{
    type Error = Error;

    fn get_current_slot(&self) -> Slot {
        self.time
    }

    fn set_current_slot(&mut self, slot: Slot) {
        self.time = slot
    }

    fn on_verified_block<Payload: AbstractExecPayload<E>>(
        &mut self,
        _block: BeaconBlockRef<E, Payload>,
        block_root: Hash256,
        state: &BeaconState<E>,
    ) -> Result<(), Self::Error> {
        let epoch = state.current_epoch();
        let epoch_boundary_slot = epoch.start_slot(E::slots_per_epoch());

        if state.slot() == epoch_boundary_slot {
            return self.balances_cache.insert(block_root, state);
        }

        let epoch_boundary_root = *state.get_block_root(epoch_boundary_slot)?;
        if self
            .balances_cache
            .position(epoch_boundary_root, epoch)
            .is_some()
        {
            return Ok(());
        }

        // The epoch boundary slot was skipped. We need to make sure to use the boundary state. If
        // not, a slashing included in first post boundary block would cause us to cache incorrect
        // balances.
        let epoch_boundary_state_root = *state.get_state_root(epoch_boundary_slot)?;
        debug!(
            slot = %epoch_boundary_slot,
            state_root = ?epoch_boundary_state_root,
            "Loading epoch boundary state for balances cache"
        );
        let _timer = metrics::start_timer(&metrics::BALANCES_CACHE_BOUNDARY_STATE_LOAD_TIMES);
        let update_cache = true;
        if let Some(epoch_boundary_state) = self
            .store
            .get_hot_state(&epoch_boundary_state_root, update_cache)
            .map_err(Error::FailedToReadState)?
        {
            self.balances_cache
                .insert(epoch_boundary_root, &epoch_boundary_state)?;
        }

        Ok(())
    }

    fn justified_checkpoint(&self) -> &Checkpoint {
        &self.justified_checkpoint
    }

    fn justified_state_root(&self) -> Hash256 {
        self.justified_state_root
    }

    fn justified_balances(&self) -> &JustifiedBalances {
        &self.justified_balances
    }

    fn finalized_checkpoint(&self) -> &Checkpoint {
        &self.finalized_checkpoint
    }

    fn unrealized_justified_checkpoint(&self) -> &Checkpoint {
        &self.unrealized_justified_checkpoint
    }

    fn unrealized_justified_state_root(&self) -> Hash256 {
        self.unrealized_justified_state_root
    }

    fn unrealized_finalized_checkpoint(&self) -> &Checkpoint {
        &self.unrealized_finalized_checkpoint
    }

    fn proposer_boost_root(&self) -> Hash256 {
        self.proposer_boost_root
    }

    fn set_finalized_checkpoint(&mut self, checkpoint: Checkpoint) {
        self.finalized_checkpoint = checkpoint
    }

    /// Set the justified checkpoint, its state root and its balances.
    ///
    /// The new balances are obtained first, from the `BalancesCache` or by loading the state at
    /// `justified_state_root`. The three fields are only assigned once that has succeeded, so on
    /// error the store still describes the previous justified checkpoint (see the invariant on the
    /// `justified_balances` field).
    ///
    /// `justified_state_root` must be the root of the checkpoint state for `checkpoint`. This is
    /// not checked: a `BalancesCache` hit is keyed by `checkpoint` alone and stores
    /// `justified_state_root` as given.
    fn set_justified_checkpoint(
        &mut self,
        checkpoint: Checkpoint,
        justified_state_root: Hash256,
    ) -> Result<(), Error> {
        let justified_balances = if let Some(justified_balances) =
            self.balances_cache.get(checkpoint.root, checkpoint.epoch)
        {
            metrics::inc_counter(&metrics::BALANCES_CACHE_HITS);
            justified_balances
        } else {
            metrics::inc_counter(&metrics::BALANCES_CACHE_MISSES);

            // Justified state is reasonably useful to cache, it might be finalized soon.
            let update_cache = true;
            let state = self
                .store
                .get_hot_state(&justified_state_root, update_cache)
                .map_err(Error::FailedToReadState)?
                .ok_or(Error::MissingState(justified_state_root))?;

            JustifiedBalances::from_justified_state(&state)?
        };

        self.justified_checkpoint = checkpoint;
        self.justified_state_root = justified_state_root;
        self.justified_balances = justified_balances;

        Ok(())
    }

    fn set_unrealized_justified_checkpoint(&mut self, checkpoint: Checkpoint, state_root: Hash256) {
        self.unrealized_justified_checkpoint = checkpoint;
        self.unrealized_justified_state_root = state_root;
    }

    fn set_unrealized_finalized_checkpoint(&mut self, checkpoint: Checkpoint) {
        self.unrealized_finalized_checkpoint = checkpoint;
    }

    fn set_proposer_boost_root(&mut self, proposer_boost_root: Hash256) {
        self.proposer_boost_root = proposer_boost_root;
    }

    fn equivocating_indices(&self) -> &BTreeSet<u64> {
        &self.equivocating_indices
    }

    fn extend_equivocating_indices(&mut self, indices: impl IntoIterator<Item = u64>) {
        self.equivocating_indices.extend(indices);
    }
}

pub type PersistedForkChoiceStore = PersistedForkChoiceStoreV28;

/// A container which allows persisting the `BeaconForkChoiceStore` to the on-disk database.
#[superstruct(variants(V28), variant_attributes(derive(Encode, Decode)), no_enum)]
pub struct PersistedForkChoiceStore {
    pub time: Slot,
    pub finalized_checkpoint: Checkpoint,
    pub justified_checkpoint: Checkpoint,
    pub justified_state_root: Hash256,
    pub unrealized_justified_checkpoint: Checkpoint,
    pub unrealized_justified_state_root: Hash256,
    pub unrealized_finalized_checkpoint: Checkpoint,
    pub proposer_boost_root: Hash256,
    pub equivocating_indices: BTreeSet<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{BeaconChainHarness, EphemeralHarnessType};
    use state_processing::AllCaches;
    use state_processing::state_advance::complete_state_advance;
    use store::MemoryStore;
    use store::hdiff::StorageStrategy;
    use types::{Spec, Validator};

    type E = Spec;

    #[test]
    fn balances_cache_hit_matches_justified_state() {
        let spec = E::default_spec();
        let mut state: BeaconState<E> = BeaconState::new(0, <_>::default(), &spec);
        for i in 0..4u64 {
            state
                .validators_mut()
                .push(Validator {
                    effective_balance: 32_000_000_000,
                    slashed: i == 1,
                    activation_epoch: Epoch::new(0),
                    exit_epoch: spec.far_future_epoch,
                    withdrawable_epoch: spec.far_future_epoch,
                    ..Default::default()
                })
                .unwrap();
        }

        let block_root = Hash256::repeat_byte(1);
        let mut cache = BalancesCache::default();
        cache.insert(block_root, &state).unwrap();

        let cached = cache.get(block_root, state.current_epoch()).unwrap();
        let expected = JustifiedBalances::from_justified_state(&state).unwrap();
        assert_eq!(cached, expected);
        assert!(!cached.slashed_balances().is_empty());
        assert!(cache.get(block_root, state.current_epoch() + 1).is_none());
    }

    // >= 32 validators required for Gloas genesis under the mainnet preset (32 slots/epoch).
    const VALIDATOR_COUNT: usize = 32;

    /// The justified checkpoint, state root and balances of a store.
    #[derive(PartialEq, Debug)]
    struct Justified {
        checkpoint: Checkpoint,
        state_root: Hash256,
        balances: JustifiedBalances,
    }

    /// A fork choice store whose justified checkpoint is the genesis checkpoint `A`, and a later
    /// checkpoint `B` with different balances, which the store can't obtain yet.
    struct JustifiedCheckpointTest {
        harness: BeaconChainHarness<EphemeralHarnessType<E>>,
        fc_store: BeaconForkChoiceStore<E, MemoryStore, MemoryStore>,
        justified_a: Justified,
        justified_b: Justified,
        state_b: BeaconState<E>,
    }

    impl JustifiedCheckpointTest {
        fn new() -> Self {
            let harness = BeaconChainHarness::builder(E::default())
                .default_spec()
                .deterministic_keypairs(VALIDATOR_COUNT)
                .fresh_ephemeral_store()
                .build();
            let genesis = (*harness.chain.head_snapshot()).clone();
            let genesis_state_root = genesis.beacon_state_root();
            let fc_store = BeaconForkChoiceStore::get_forkchoice_store(
                harness.chain.store.clone(),
                genesis.clone(),
            )
            .unwrap();

            let justified_a = Justified {
                checkpoint: Checkpoint {
                    epoch: Epoch::new(0),
                    root: genesis.beacon_block_root,
                },
                state_root: genesis_state_root,
                balances: JustifiedBalances::from_justified_state(&genesis.beacon_state).unwrap(),
            };

            // B's state is the genesis state at a later epoch boundary, with a validator slashed
            // so that its balances differ from A's. Use the first epoch boundary whose state the
            // database stores, rather than replaying it from an earlier state, which would lose the
            // slashing when the state is loaded from disk.
            let epoch_b = (1..64)
                .map(Epoch::new)
                .find(|epoch| {
                    let strategy = harness
                        .chain
                        .store
                        .hot_storage_strategy(epoch.start_slot(E::slots_per_epoch()))
                        .unwrap();
                    !matches!(strategy, StorageStrategy::ReplayFrom(_))
                })
                .unwrap();
            let mut state_b = genesis.beacon_state;
            complete_state_advance(
                &mut state_b,
                Some(genesis_state_root),
                epoch_b.start_slot(E::slots_per_epoch()),
                None,
                &harness.spec,
            )
            .unwrap();
            state_b.validators_mut().get_mut(1).unwrap().slashed = true;
            // Setting `slashed` directly bypasses `slash_validator`, so rebuild the caches to keep
            // the `SlashingsCache` consistent with `validators()`.
            state_b.drop_all_caches().unwrap();
            state_b.build_all_caches(&harness.spec).unwrap();
            assert!(state_b.slashings_cache().is_slashed(1));
            let justified_b = Justified {
                checkpoint: Checkpoint {
                    epoch: epoch_b,
                    root: genesis.beacon_block_root,
                },
                state_root: state_b.update_tree_hash_cache().unwrap(),
                balances: JustifiedBalances::from_justified_state(&state_b).unwrap(),
            };

            let test = Self {
                harness,
                fc_store,
                justified_a,
                justified_b,
                state_b,
            };
            assert_eq!(test.justified(), test.justified_a);
            assert_ne!(test.justified_a.checkpoint, test.justified_b.checkpoint);
            assert_ne!(test.justified_a.state_root, test.justified_b.state_root);
            assert_ne!(test.justified_a.balances, test.justified_b.balances);
            test
        }

        fn justified(&self) -> Justified {
            Justified {
                checkpoint: self.fc_store.justified_checkpoint,
                state_root: self.fc_store.justified_state_root,
                balances: self.fc_store.justified_balances.clone(),
            }
        }

        fn set_justified_checkpoint_b(&mut self) -> Result<(), Error> {
            self.fc_store
                .set_justified_checkpoint(self.justified_b.checkpoint, self.justified_b.state_root)
        }

        /// Updating to B fails, because B's balances are neither cached nor loadable, and leaves
        /// the store describing A.
        fn assert_update_to_b_fails(&mut self) {
            let result = self.set_justified_checkpoint_b();
            assert!(
                matches!(result, Err(Error::MissingState(root)) if root == self.justified_b.state_root),
                "{result:?}"
            );
            assert_eq!(self.justified(), self.justified_a);
        }

        /// Updating to B succeeds, and the store describes B.
        fn assert_update_to_b_succeeds(&mut self) {
            self.set_justified_checkpoint_b().unwrap();
            assert_eq!(self.justified(), self.justified_b);
        }
    }

    /// Regression test: a failed update must not leave B's checkpoint with A's balances. A retry
    /// succeeds once B's balances are in the `BalancesCache` (B's state is never stored).
    #[test]
    fn set_justified_checkpoint_is_atomic_with_balances_cache() {
        let mut test = JustifiedCheckpointTest::new();
        test.assert_update_to_b_fails();

        test.fc_store
            .balances_cache
            .insert(test.justified_b.checkpoint.root, &test.state_b)
            .unwrap();
        test.assert_update_to_b_succeeds();
    }

    /// As above, but the retry succeeds once B's state is stored in the hot database.
    #[test]
    fn set_justified_checkpoint_is_atomic_with_hot_state() {
        let mut test = JustifiedCheckpointTest::new();
        test.assert_update_to_b_fails();

        let state_root_b = test.justified_b.state_root;
        let store = &test.harness.chain.store;
        store.put_state(&state_root_b, &test.state_b).unwrap();
        // Load B's state from disk, rather than from the state cache.
        store.state_cache.lock().delete_state(&state_root_b);
        test.assert_update_to_b_succeeds();
    }
}
