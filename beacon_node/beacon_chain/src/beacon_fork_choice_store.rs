//! Defines the `BeaconForkChoiceStore` which provides the persistent storage for the `ForkChoice`
//! struct.
//!
//! Additionally, the `BalancesCache` struct is defined; a cache designed to avoid database
//! reads when fork choice requires the validator balances of the justified state.

use crate::{BeaconSnapshot, metrics};
use educe::Educe;
use fixed_bytes::FixedBytesExtended;
use fork_choice::ForkChoiceStore;
use proto_array::{FromStatePath, JustifiedBalances};
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
    UnalignedBalancesCacheState { state_slot: Slot },
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
pub const MAX_BALANCE_CACHE_SIZE: usize = 4;

/// Build the balances of `state` with `JustifiedBalances::from_state`, which reads the state's
/// caches if they are valid and otherwise its validator registry, and count which path was taken.
fn justified_balances_from_state<E: EthSpec>(
    state: &BeaconState<E>,
) -> Result<(JustifiedBalances, FromStatePath), Error> {
    let (justified_balances, path) = JustifiedBalances::from_state(state)?;
    metrics::inc_counter_vec(
        &metrics::FORK_CHOICE_BALANCES_FROM_STATE,
        &path.metric_labels(),
    );
    if let FromStatePath::Fallback(reason) = &path {
        debug!(
            slot = %state.slot(),
            ?reason,
            "Built fork choice balances from the validator registry"
        );
    }
    Ok((justified_balances, path))
}

/// An entry of the `BalancesCache`: the balances of the checkpoint state of
/// `Checkpoint { epoch, root: block_root }`.
#[derive(PartialEq, Clone, Debug)]
pub struct BalancesCacheItem {
    block_root: Hash256,
    epoch: Epoch,
    justified_balances: JustifiedBalances,
    path: FromStatePath,
}

impl BalancesCacheItem {
    /// The checkpoint whose state the balances describe.
    pub fn checkpoint(&self) -> Checkpoint {
        Checkpoint {
            epoch: self.epoch,
            root: self.block_root,
        }
    }

    /// The balances of the checkpoint state.
    pub fn justified_balances(&self) -> &JustifiedBalances {
        &self.justified_balances
    }

    /// Whether the balances were read from the checkpoint state's caches or from its validator
    /// registry. Both give the same balances.
    pub fn path(&self) -> &FromStatePath {
        &self.path
    }
}

/// A cache of the balances of the `MAX_BALANCE_CACHE_SIZE` most recently inserted checkpoint
/// states, keyed by checkpoint, which fork choice adopts when its justified checkpoint changes.
///
/// ## Invariant
///
/// Every entry for `(block_root, epoch)` is `JustifiedBalances::from_justified_state` of the
/// *exact* checkpoint state of `Checkpoint { epoch, root: block_root }`: the spec's
/// `store_target_checkpoint_state`, which is the post-state of block `block_root` advanced to the
/// start slot of `epoch`. When `on_verified_block` imports a block at the start slot, it builds
/// the entry from that block's post-state. Otherwise it loads the state at the start slot from the
/// database. That is the post-state of the block at the start slot if there is one (its entry was
/// evicted, it was imported before a restart, or it is the anchor block, which is never passed to
/// `on_verified_block`), and otherwise the state at the skipped start slot. So an operation
/// included in a later block of `epoch`, such as a slashing, is never reflected in the entry, even
/// though fork choice is given the post-states of those blocks.
///
/// Fork choice relies on this when it adopts an entry as its justified balances. The fast
/// confirmation rule relies on it for safety when it adopts an entry as the balances of an
/// observed justified checkpoint, because its output becomes the execution layer's
/// `safe_block_hash`.
#[derive(PartialEq, Clone, Default, Debug)]
pub struct BalancesCache {
    items: Vec<BalancesCacheItem>,
}

impl BalancesCache {
    /// Add an entry for the checkpoint `(epoch_boundary_root, state.current_epoch())`, built from
    /// `state`, unless the cache already has one. Evicts the oldest entry if the cache is full.
    ///
    /// `state` must be the checkpoint state of that checkpoint (see the invariant on
    /// `BalancesCache`): the state at the start slot of its current epoch, on a chain whose latest
    /// block at or before that slot is `epoch_boundary_root`. Returns an error without inserting
    /// if `state` is not at the start slot of its epoch. The block root is not checked.
    pub fn insert<E: EthSpec>(
        &mut self,
        epoch_boundary_root: Hash256,
        state: &BeaconState<E>,
    ) -> Result<(), Error> {
        let epoch = state.current_epoch();
        if state.slot() != epoch.start_slot(E::slots_per_epoch()) {
            return Err(Error::UnalignedBalancesCacheState {
                state_slot: state.slot(),
            });
        }

        if self.position(epoch_boundary_root, epoch).is_none() {
            let (justified_balances, path) = justified_balances_from_state(state)?;
            let item = BalancesCacheItem {
                block_root: epoch_boundary_root,
                epoch,
                justified_balances,
                path,
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

    /// Get the balances of the checkpoint state of `Checkpoint { epoch, root: block_root }`, if
    /// cached.
    ///
    /// Only an entry for exactly this block root and epoch matches: the same block root at another
    /// epoch is another checkpoint, with another state. The returned balances share their
    /// allocation with the entry.
    pub fn get(&self, block_root: Hash256, epoch: Epoch) -> Option<JustifiedBalances> {
        let i = self.position(block_root, epoch)?;
        self.items
            .get(i)
            .map(|item| item.justified_balances.clone())
    }

    /// The entries, from the oldest to the most recently inserted.
    pub fn items(&self) -> &[BalancesCacheItem] {
        &self.items
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
    /// as built by `JustifiedBalances::from_state` (possibly via the `BalancesCache`), which equals
    /// `JustifiedBalances::from_justified_state` of that state.
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
        let (justified_balances, _) = justified_balances_from_state(&anchor_state)?;
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

        let (justified_balances, _) = justified_balances_from_state(&justified_state)?;
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

    /// The cache of checkpoint state balances, from which `set_justified_checkpoint` takes the
    /// justified balances when it can. The fast confirmation rule also reads entries by exact
    /// checkpoint, relying on the invariant documented on `BalancesCache`.
    pub fn balances_cache(&self) -> &BalancesCache {
        &self.balances_cache
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

    /// Insert the balances of the checkpoint state of the block's epoch into the `BalancesCache`,
    /// if they are not already cached. Only ever inserts the checkpoint state itself, never
    /// `state` from later in the epoch (see the invariant on `BalancesCache`).
    fn on_verified_block<Payload: AbstractExecPayload<E>>(
        &mut self,
        _block: BeaconBlockRef<E, Payload>,
        block_root: Hash256,
        state: &BeaconState<E>,
    ) -> Result<(), Self::Error> {
        let epoch = state.current_epoch();
        let epoch_boundary_slot = epoch.start_slot(E::slots_per_epoch());

        // The block is at the epoch boundary slot, so its post-state is the checkpoint state of
        // `(block_root, epoch)`.
        if state.slot() == epoch_boundary_slot {
            return self.balances_cache.insert(block_root, state);
        }

        // The checkpoint of this block's chain for `epoch` is the latest block at or before the
        // epoch boundary slot.
        let epoch_boundary_root = *state.get_block_root(epoch_boundary_slot)?;
        if self
            .balances_cache
            .position(epoch_boundary_root, epoch)
            .is_some()
        {
            return Ok(());
        }

        // `state` may include operations from blocks after the epoch boundary, such as a slashing
        // in this very block, so load the state at the epoch boundary slot instead. That is the
        // post-state of the boundary block if there is one (its entry was evicted, it was imported
        // before a restart, or it is the anchor block, which is never passed to
        // `on_verified_block`), and otherwise the state at the skipped boundary slot. If it can't
        // be found, nothing is inserted.
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

            let (justified_balances, _) = justified_balances_from_state(&state)?;
            justified_balances
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
    use proto_array::StateCachesError;
    use state_processing::AllCaches;
    use state_processing::state_advance::complete_state_advance;
    use store::MemoryStore;
    use store::hdiff::StorageStrategy;
    use types::test_utils::generate_deterministic_keypair;
    use types::{ChainSpec, Spec, Validator};

    type E = Spec;

    /// A state at the start of epoch 0 with four active validators, one of them slashed, and no
    /// caches built.
    fn state_with_slashed_validator(spec: &ChainSpec) -> BeaconState<E> {
        let mut state: BeaconState<E> = BeaconState::new(0, <_>::default(), spec);
        for i in 0..4 {
            state
                .validators_mut()
                .push(Validator {
                    pubkey: generate_deterministic_keypair(i).pk.compress(),
                    effective_balance: 32_000_000_000,
                    slashed: i == 1,
                    activation_epoch: Epoch::new(0),
                    exit_epoch: spec.far_future_epoch,
                    withdrawable_epoch: spec.far_future_epoch,
                    ..Default::default()
                })
                .unwrap();
            state.balances_mut().push(32_000_000_000).unwrap();
        }
        state
    }

    /// An entry built from a state without caches reads the registry, and one built from a state
    /// with caches reads the caches. Both equal `from_justified_state`, and are only found by their
    /// exact checkpoint.
    #[test]
    fn balances_cache_hit_matches_justified_state() {
        let spec = E::default_spec();
        let mut state = state_with_slashed_validator(&spec);
        let epoch = state.current_epoch();
        let expected = JustifiedBalances::from_justified_state(&state).unwrap();
        assert!(!expected.slashed_balances().is_empty());

        let mut cache = BalancesCache::default();
        let registry_root = Hash256::repeat_byte(1);
        cache.insert(registry_root, &state).unwrap();
        state.build_all_caches(&spec).unwrap();
        let caches_root = Hash256::repeat_byte(2);
        cache.insert(caches_root, &state).unwrap();

        let [registry_item, caches_item] = cache.items() else {
            panic!("expected two entries, got {:?}", cache.items());
        };
        assert!(
            matches!(
                registry_item.path(),
                FromStatePath::Fallback(StateCachesError::EpochCacheInvalid(_))
            ),
            "{:?}",
            registry_item.path()
        );
        assert_eq!(caches_item.path(), &FromStatePath::Caches);

        for (root, item) in [(registry_root, registry_item), (caches_root, caches_item)] {
            assert_eq!(item.checkpoint(), Checkpoint { epoch, root });
            let cached = cache.get(root, epoch).unwrap();
            assert_eq!(cached, expected);
            assert!(JustifiedBalances::ptr_eq(
                &cached,
                item.justified_balances()
            ));
            assert!(cache.get(root, epoch + 1).is_none());
        }
    }

    /// A state that isn't at the start slot of its epoch is not a checkpoint state, so it is
    /// rejected.
    #[test]
    fn balances_cache_rejects_unaligned_state() {
        let spec = E::default_spec();
        let mut state = state_with_slashed_validator(&spec);
        *state.slot_mut() = Slot::new(1);

        let mut cache = BalancesCache::default();
        let result = cache.insert(Hash256::repeat_byte(1), &state);
        assert!(
            matches!(
                result,
                Err(Error::UnalignedBalancesCacheState { state_slot }) if state_slot == 1
            ),
            "{result:?}"
        );
        assert!(cache.items().is_empty());
    }

    /// The cache keeps the most recently inserted entries, and inserting a cached checkpoint
    /// again leaves its entry unchanged.
    #[test]
    fn balances_cache_evicts_oldest_entry() {
        let spec = E::default_spec();
        let mut state = state_with_slashed_validator(&spec);
        let epoch = state.current_epoch();
        let root = |i: usize| Hash256::from_low_u64_be(i as u64 + 1);

        let mut cache = BalancesCache::default();
        for i in 0..MAX_BALANCE_CACHE_SIZE {
            cache.insert(root(i), &state).unwrap();
        }
        let full_cache = cache.clone();
        state.build_all_caches(&spec).unwrap();
        cache.insert(root(0), &state).unwrap();
        assert_eq!(cache, full_cache);

        cache.insert(root(MAX_BALANCE_CACHE_SIZE), &state).unwrap();
        let roots = cache
            .items()
            .iter()
            .map(|item| item.checkpoint().root)
            .collect::<Vec<_>>();
        assert_eq!(
            roots,
            (1..=MAX_BALANCE_CACHE_SIZE).map(root).collect::<Vec<_>>()
        );
        assert!(cache.get(root(0), epoch).is_none());
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
