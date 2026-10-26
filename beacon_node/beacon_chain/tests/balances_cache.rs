#![cfg(not(debug_assertions))]

//! Differential tests of the invariant of the fork choice store's `BalancesCache`.
//!
//! Every entry for `(block_root, epoch)`, and the store's justified balances, must equal
//! `JustifiedBalances::from_justified_state` of the spec's `store_target_checkpoint_state`: the
//! post-state of block `block_root` advanced to the start slot of `epoch`. These tests compute
//! that state independently of the cache, by loading the block's own post-state and advancing
//! it, whereas the store uses the post-state of a block at the epoch boundary slot or loads the
//! state at that slot.

use beacon_chain::test_utils::{
    BaseHarnessType, BeaconChainHarness, DiskHarnessType, EphemeralHarnessType, test_spec,
};
use beacon_chain::{BalancesCacheItem, ChainConfig, MAX_BALANCE_CACHE_SIZE, WhenSlotSkipped};
use bls::SignatureBytes;
use fork_choice::ForkChoiceStore;
use proto_array::{FromStatePath, JustifiedBalances, StateCachesError};
use state_processing::state_advance::complete_state_advance;
use std::sync::Arc;
use store::database::interface::BeaconNodeBackend;
use store::{HotColdDB, ItemStore, StoreConfig};
use tempfile::{TempDir, tempdir};
use types::test_utils::generate_deterministic_keypair;
use types::{
    Address, BeaconState, ChainSpec, Checkpoint, ConsolidationRequest, DepositData, DepositRequest,
    Epoch, EpochCacheError, EthSpec, ExecutionRequests, ExecutionRequestsElectra,
    ExecutionRequestsGloas, ForkName, Hash256, Slot, Spec,
};

type E = Spec;
type Harness<Hot, Cold> = BeaconChainHarness<BaseHarnessType<E, Hot, Cold>>;

const VALIDATOR_COUNT: usize = 64;

fn chain_config() -> ChainConfig {
    // Keep finalized states, from which the expected checkpoint states are built.
    ChainConfig {
        archive: true,
        ..ChainConfig::default()
    }
}

fn harness() -> BeaconChainHarness<EphemeralHarnessType<E>> {
    BeaconChainHarness::builder(E::default())
        .spec(Arc::new(test_spec::<E>()))
        .chain_config(chain_config())
        .deterministic_keypairs(VALIDATOR_COUNT)
        .fresh_ephemeral_store()
        .mock_execution_layer()
        .build()
}

/// The spec's `store_target_checkpoint_state(checkpoint)`: the post-state of block
/// `checkpoint.root`, advanced to the start slot of `checkpoint.epoch`.
fn checkpoint_state<Hot: ItemStore, Cold: ItemStore>(
    harness: &Harness<Hot, Cold>,
    checkpoint: Checkpoint,
) -> BeaconState<E> {
    let store = &harness.chain.store;
    let block = store
        .get_blinded_block(&checkpoint.root)
        .unwrap()
        .unwrap_or_else(|| panic!("no block for {checkpoint:?}"));
    let state_root = block.state_root();
    let mut state = store
        .get_state(&state_root, Some(block.slot()), false)
        .unwrap()
        .unwrap_or_else(|| panic!("no block state for {checkpoint:?}"));
    let start_slot = checkpoint.epoch.start_slot(E::slots_per_epoch());
    assert!(state.slot() <= start_slot, "{checkpoint:?}");
    complete_state_advance(
        &mut state,
        Some(state_root),
        start_slot,
        None,
        &harness.spec,
    )
    .unwrap();
    state
}

fn expected_balances<Hot: ItemStore, Cold: ItemStore>(
    harness: &Harness<Hot, Cold>,
    checkpoint: Checkpoint,
) -> JustifiedBalances {
    JustifiedBalances::from_justified_state(&checkpoint_state(harness, checkpoint)).unwrap()
}

/// The path that `JustifiedBalances::from_state` takes for the balances cache entry for
/// `checkpoint`, which must not be the genesis checkpoint. The store doesn't build caches on the
/// states it loads, so this depends on where the checkpoint state came from:
///
/// - The post-state of a block at the epoch boundary slot has the caches that block processing
///   builds for its epoch, so it takes the cache path. So does any state that
///   `HotColdDB::get_hot_state` loads from disk, because it builds every cache of such a state.
/// - The state at a skipped epoch boundary slot is put in the state cache by block verification,
///   just after the slot processing that produced it, with its caches as epoch processing left
///   them. Slot processing builds the committee and slashings caches, and Altair's single-pass
///   epoch processing builds the next epoch's epoch cache, so the cache path is taken. Phase0's
///   epoch processing leaves the epoch cache at the epoch it processed, and the upgrades to Altair
///   through Electra reset it at their fork epoch, so then the entry falls back to the validator
///   registry. These tests never evict such a state from the state cache before the store loads
///   it.
fn expected_path<Hot: ItemStore, Cold: ItemStore>(
    harness: &Harness<Hot, Cold>,
    checkpoint: Checkpoint,
) -> FromStatePath {
    let spec = &harness.spec;
    let block_slot = harness
        .chain
        .store
        .get_blinded_block(&checkpoint.root)
        .unwrap()
        .unwrap_or_else(|| panic!("no block for {checkpoint:?}"))
        .slot();
    if block_slot == checkpoint.epoch.start_slot(E::slots_per_epoch()) {
        return FromStatePath::Caches;
    }

    let epoch_cache_invalid =
        |reason| FromStatePath::Fallback(StateCachesError::EpochCacheInvalid(reason));
    let epoch_cache_reset = [
        spec.altair_fork_epoch,
        spec.bellatrix_fork_epoch,
        spec.capella_fork_epoch,
        spec.deneb_fork_epoch,
        spec.electra_fork_epoch,
    ]
    .contains(&Some(checkpoint.epoch));
    let processed_epoch = checkpoint.epoch - 1;
    if epoch_cache_reset {
        epoch_cache_invalid(EpochCacheError::CacheNotInitialized)
    } else if spec.fork_name_at_epoch(processed_epoch) == ForkName::Base {
        epoch_cache_invalid(EpochCacheError::IncorrectEpoch {
            cache: processed_epoch,
            state: checkpoint.epoch,
        })
    } else {
        FromStatePath::Caches
    }
}

/// Asserts that every balances cache entry, and the store's justified balances, equal the
/// balances of their checkpoint state. Returns the entries, from the oldest.
///
/// Also asserts which path built each entry: `expected_path`, except for the entry for the genesis
/// checkpoint. The genesis state is put in the state cache without an epoch cache, so that entry
/// falls back to the validator registry.
fn check_balances_cache<Hot: ItemStore, Cold: ItemStore>(
    harness: &Harness<Hot, Cold>,
) -> Vec<BalancesCacheItem> {
    let (items, justified_checkpoint, justified_balances) = {
        let fork_choice = harness.chain.canonical_head.fork_choice_read_lock();
        let fc_store = fork_choice.fc_store();
        (
            fc_store.balances_cache().items().to_vec(),
            *fc_store.justified_checkpoint(),
            fc_store.justified_balances().clone(),
        )
    };
    for item in &items {
        let checkpoint = item.checkpoint();
        assert_eq!(
            item.justified_balances(),
            &expected_balances(harness, checkpoint),
            "balances cache entry for {checkpoint:?}"
        );
        if checkpoint.root == harness.chain.genesis_block_root {
            assert!(
                matches!(
                    item.path(),
                    FromStatePath::Fallback(StateCachesError::EpochCacheInvalid(_))
                ),
                "balances cache entry for {checkpoint:?}: {:?}",
                item.path()
            );
        } else {
            assert_eq!(
                item.path(),
                &expected_path(harness, checkpoint),
                "balances cache entry for {checkpoint:?}"
            );
        }
    }
    assert_eq!(
        justified_balances,
        expected_balances(harness, justified_checkpoint),
        "justified balances for {justified_checkpoint:?}"
    );
    items
}

fn checkpoints(items: &[BalancesCacheItem]) -> Vec<Checkpoint> {
    items.iter().map(BalancesCacheItem::checkpoint).collect()
}

fn entry(items: &[BalancesCacheItem], checkpoint: Checkpoint) -> &JustifiedBalances {
    items
        .iter()
        .find(|item| item.checkpoint() == checkpoint)
        .unwrap_or_else(|| panic!("no balances cache entry for {checkpoint:?}"))
        .justified_balances()
}

fn is_slashed(balances: &JustifiedBalances, validator_index: u64) -> bool {
    let slashed = balances.slashed_balances().contains_key(&validator_index);
    if slashed {
        assert_eq!(balances.unslashed_balance(validator_index as usize), 0);
    }
    slashed
}

/// The checkpoint of the canonical chain for `epoch`: the latest block at or before its start
/// slot.
fn canonical_checkpoint<Hot: ItemStore, Cold: ItemStore>(
    harness: &Harness<Hot, Cold>,
    epoch: Epoch,
) -> Checkpoint {
    let root = harness
        .chain
        .block_root_at_slot(
            epoch.start_slot(E::slots_per_epoch()),
            WhenSlotSkipped::Prev,
        )
        .unwrap()
        .unwrap();
    Checkpoint { epoch, root }
}

fn block_at_slot<Hot: ItemStore, Cold: ItemStore>(
    harness: &Harness<Hot, Cold>,
    slot: Slot,
) -> Option<Hash256> {
    harness
        .chain
        .block_root_at_slot(slot, WhenSlotSkipped::None)
        .unwrap()
}

fn is_slashed_in_head_state<Hot: ItemStore, Cold: ItemStore>(
    harness: &Harness<Hot, Cold>,
    validator_index: u64,
) -> bool {
    harness
        .get_current_state()
        .get_validator(validator_index as usize)
        .unwrap()
        .slashed
}

/// The proposer at `slot` on top of `state`, for a `slot` no later than the end of the epoch after
/// `state`'s.
fn proposer_index<Hot: ItemStore, Cold: ItemStore>(
    harness: &Harness<Hot, Cold>,
    state: &BeaconState<E>,
    slot: Slot,
) -> u64 {
    let mut state = state.clone();
    complete_state_advance(&mut state, None, slot, None, &harness.spec).unwrap();
    state.build_caches(&harness.spec).unwrap();
    state
        .get_beacon_proposer_index(slot, &harness.spec)
        .unwrap() as u64
}

fn proposer_is_slashed<Hot: ItemStore, Cold: ItemStore>(
    harness: &Harness<Hot, Cold>,
    state: &BeaconState<E>,
    slot: Slot,
) -> bool {
    let proposer_index = proposer_index(harness, state, slot);
    state
        .get_validator(proposer_index as usize)
        .unwrap()
        .slashed
}

/// The lowest validator index, other than `excluded`, of an unslashed validator that doesn't
/// propose at any of `slots` on the canonical chain, so that slashing it leaves those blocks in
/// place.
fn validator_to_slash<Hot: ItemStore, Cold: ItemStore>(
    harness: &Harness<Hot, Cold>,
    slots: &[Slot],
    excluded: &[u64],
) -> u64 {
    let state = harness.get_current_state();
    let proposers = slots
        .iter()
        .map(|&slot| proposer_index(harness, &state, slot))
        .collect::<Vec<_>>();
    (0..VALIDATOR_COUNT as u64)
        .find(|index| {
            !proposers.contains(index)
                && !excluded.contains(index)
                && !state.get_validator(*index as usize).unwrap().slashed
        })
        .unwrap()
}

/// Produces and imports a block at `slot` on `state`, attested by every validator, and checks
/// the balances cache. Returns the block root and its post-state.
async fn add_block<Hot: ItemStore, Cold: ItemStore>(
    harness: &Harness<Hot, Cold>,
    state: BeaconState<E>,
    slot: Slot,
) -> (Hash256, BeaconState<E>) {
    let (block_hash, post_state) = harness
        .add_attested_block_at_slot(slot, state, &harness.get_all_validators())
        .await
        .unwrap();
    check_balances_cache(harness);
    (block_hash.into(), post_state)
}

/// Produces and imports a block on the head at the next slot, which must have an unslashed
/// proposer. Returns the block root.
async fn add_block_at_next_slot<Hot: ItemStore, Cold: ItemStore>(
    harness: &Harness<Hot, Cold>,
) -> Hash256 {
    harness.advance_slot();
    let slot = harness.get_current_slot();
    let state = harness.get_current_state();
    assert!(
        !proposer_is_slashed(harness, &state, slot),
        "the proposer at slot {slot} is slashed"
    );
    add_block(harness, state, slot).await.0
}

/// Extends the canonical chain up to and including `last_slot`, skipping the slots of slashed
/// proposers.
async fn extend_to<Hot: ItemStore, Cold: ItemStore>(harness: &Harness<Hot, Cold>, last_slot: Slot) {
    while harness.get_current_slot() < last_slot {
        harness.advance_slot();
        let slot = harness.get_current_slot();
        let state = harness.get_current_state();
        if !proposer_is_slashed(harness, &state, slot) {
            add_block(harness, state, slot).await;
        }
    }
}

/// Extends the canonical chain until fork choice's justified checkpoint is `checkpoint`.
async fn extend_until_justified<Hot: ItemStore, Cold: ItemStore>(
    harness: &Harness<Hot, Cold>,
    checkpoint: Checkpoint,
) {
    loop {
        let justified_checkpoint = harness
            .chain
            .canonical_head
            .fork_choice_read_lock()
            .justified_checkpoint();
        if justified_checkpoint == checkpoint {
            break;
        }
        assert!(
            justified_checkpoint.epoch < checkpoint.epoch,
            "{justified_checkpoint:?} was justified rather than {checkpoint:?}"
        );
        extend_to(harness, harness.get_current_slot() + 1).await;
    }
}

/// Extends the canonical chain until fork choice's justified checkpoint is `checkpoint`, then
/// asserts that its justified balances are the balances cache entry for `checkpoint`.
async fn extend_until_justified_from_cache<Hot: ItemStore, Cold: ItemStore>(
    harness: &Harness<Hot, Cold>,
    checkpoint: Checkpoint,
) {
    extend_until_justified(harness, checkpoint).await;

    let fork_choice = harness.chain.canonical_head.fork_choice_read_lock();
    let fc_store = fork_choice.fc_store();
    let cached = fc_store
        .balances_cache()
        .get(checkpoint.root, checkpoint.epoch)
        .unwrap();
    assert!(JustifiedBalances::ptr_eq(
        fc_store.justified_balances(),
        &cached
    ));
}

/// A slashing in the epoch boundary block is reflected in the entry for that epoch.
#[tokio::test]
async fn slashing_in_boundary_block_is_cached() {
    let harness = harness();
    let epoch = Epoch::new(2);
    let boundary_slot = epoch.start_slot(E::slots_per_epoch());
    extend_to(&harness, boundary_slot - 1).await;

    let attester = validator_to_slash(&harness, &[boundary_slot], &[]);
    let proposer = validator_to_slash(&harness, &[boundary_slot], &[attester]);
    harness.add_attester_slashing(vec![attester]).unwrap();
    harness.add_proposer_slashing(proposer).unwrap();
    let root = add_block_at_next_slot(&harness).await;
    assert_eq!(block_at_slot(&harness, boundary_slot), Some(root));

    let items = check_balances_cache(&harness);
    let checkpoint = Checkpoint { epoch, root };
    let previous_checkpoint = canonical_checkpoint(&harness, epoch - 1);
    for validator_index in [attester, proposer] {
        assert!(is_slashed(entry(&items, checkpoint), validator_index));
        assert!(!is_slashed(
            entry(&items, previous_checkpoint),
            validator_index
        ));
    }

    // Fork choice adopts the entry once the checkpoint is justified.
    extend_until_justified_from_cache(&harness, checkpoint).await;
}

/// A slashing in a later block of an epoch is not reflected in the entry for that epoch, only
/// in the entry for the next one.
#[tokio::test]
async fn slashing_after_boundary_block_is_not_cached() {
    let harness = harness();
    let epoch = Epoch::new(2);
    let boundary_slot = epoch.start_slot(E::slots_per_epoch());
    let next_boundary_slot = (epoch + 1).start_slot(E::slots_per_epoch());
    extend_to(&harness, boundary_slot + 1).await;
    let checkpoint = canonical_checkpoint(&harness, epoch);
    assert_eq!(
        block_at_slot(&harness, boundary_slot),
        Some(checkpoint.root)
    );

    let attester = validator_to_slash(&harness, &[boundary_slot + 2, next_boundary_slot], &[]);
    harness.add_attester_slashing(vec![attester]).unwrap();
    add_block_at_next_slot(&harness).await;
    assert!(is_slashed_in_head_state(&harness, attester));
    let items = check_balances_cache(&harness);
    assert!(!is_slashed(entry(&items, checkpoint), attester));

    extend_to(&harness, next_boundary_slot).await;
    let next_checkpoint = canonical_checkpoint(&harness, epoch + 1);
    assert_eq!(
        block_at_slot(&harness, next_boundary_slot),
        Some(next_checkpoint.root)
    );
    let items = check_balances_cache(&harness);
    assert!(is_slashed(entry(&items, next_checkpoint), attester));
    assert!(!is_slashed(entry(&items, checkpoint), attester));

    extend_until_justified_from_cache(&harness, checkpoint).await;
}

/// When the epoch boundary slot is skipped, the entry is built from the state at that slot, which
/// the store loads. A slashing in the first block after the skipped slot is not reflected in it.
#[tokio::test]
async fn skipped_boundary_slot() {
    let harness = harness();
    let slots_per_epoch = E::slots_per_epoch();

    // Without a slashing.
    let epoch = Epoch::new(2);
    let boundary_slot = epoch.start_slot(slots_per_epoch);
    extend_to(&harness, boundary_slot - 1).await;
    let checkpoint = Checkpoint {
        epoch,
        root: harness.head_block_root(),
    };
    harness.advance_slot();
    add_block_at_next_slot(&harness).await;
    assert_eq!(block_at_slot(&harness, boundary_slot), None);
    assert_eq!(canonical_checkpoint(&harness, epoch), checkpoint);
    let items = check_balances_cache(&harness);
    entry(&items, checkpoint);

    // With a slashing in the first block after the skipped slot.
    let epoch = epoch + 1;
    let boundary_slot = epoch.start_slot(slots_per_epoch);
    let next_boundary_slot = (epoch + 1).start_slot(slots_per_epoch);
    extend_to(&harness, boundary_slot - 1).await;
    let checkpoint = Checkpoint {
        epoch,
        root: harness.head_block_root(),
    };
    harness.advance_slot();
    let attester = validator_to_slash(&harness, &[boundary_slot + 1, next_boundary_slot], &[]);
    harness.add_attester_slashing(vec![attester]).unwrap();
    add_block_at_next_slot(&harness).await;
    assert!(is_slashed_in_head_state(&harness, attester));
    let items = check_balances_cache(&harness);
    assert!(!is_slashed(entry(&items, checkpoint), attester));

    extend_to(&harness, next_boundary_slot).await;
    let items = check_balances_cache(&harness);
    assert!(is_slashed(
        entry(&items, canonical_checkpoint(&harness, epoch + 1)),
        attester
    ));

    extend_until_justified_from_cache(&harness, checkpoint).await;
}

/// Competing chains have distinct entries for the same epoch, and the oldest entries are evicted
/// once more than `MAX_BALANCE_CACHE_SIZE` have been inserted.
#[tokio::test]
async fn forks_and_eviction() {
    let harness = harness();
    let slots_per_epoch = E::slots_per_epoch();
    let epoch = Epoch::new(3);
    let boundary_slot = epoch.start_slot(slots_per_epoch);

    extend_to(&harness, boundary_slot - 2).await;
    let ancestor_root = harness.head_block_root();
    let ancestor_state = harness.get_current_state();
    let mut expected_checkpoints = (0..epoch.as_u64())
        .map(|epoch| canonical_checkpoint(&harness, Epoch::new(epoch)))
        .collect::<Vec<_>>();

    // Chain A has a block at the epoch boundary slot, which includes a slashing.
    let attester = validator_to_slash(&harness, &[boundary_slot, boundary_slot + 1], &[]);
    harness.add_attester_slashing(vec![attester]).unwrap();
    let (root_a, _) = add_block(&harness, ancestor_state.clone(), boundary_slot).await;
    let checkpoint_a = Checkpoint {
        epoch,
        root: root_a,
    };

    // Chain B skips the epoch boundary slot, so its checkpoint is the common ancestor. Its first
    // block also includes the slashing, and isn't attested to.
    let (_, _, state_b) = harness
        .add_block_at_slot(boundary_slot + 1, ancestor_state)
        .await
        .unwrap();
    assert!(state_b.get_validator(attester as usize).unwrap().slashed);
    assert_eq!(harness.head_block_root(), root_a);
    let checkpoint_b = Checkpoint {
        epoch,
        root: ancestor_root,
    };

    let items = check_balances_cache(&harness);
    assert!(is_slashed(entry(&items, checkpoint_a), attester));
    assert!(!is_slashed(entry(&items, checkpoint_b), attester));
    expected_checkpoints.extend([checkpoint_a, checkpoint_b]);
    assert!(expected_checkpoints.len() > MAX_BALANCE_CACHE_SIZE);
    let first = expected_checkpoints.len() - MAX_BALANCE_CACHE_SIZE;
    assert_eq!(checkpoints(&items), expected_checkpoints[first..]);

    // Each later epoch of chain A inserts an entry and evicts the oldest, until both entries for
    // the forked epoch are gone.
    for later_epoch in (1..=MAX_BALANCE_CACHE_SIZE as u64).map(|offset| epoch + offset) {
        extend_to(&harness, later_epoch.start_slot(slots_per_epoch)).await;
        expected_checkpoints.push(canonical_checkpoint(&harness, later_epoch));
        let items = check_balances_cache(&harness);
        let first = expected_checkpoints.len() - MAX_BALANCE_CACHE_SIZE;
        assert_eq!(checkpoints(&items), expected_checkpoints[first..]);
    }
    let items = check_balances_cache(&harness);
    assert!(!checkpoints(&items).contains(&checkpoint_a));
    assert!(!checkpoints(&items).contains(&checkpoint_b));
}

fn disk_store(
    db: &TempDir,
    spec: Arc<ChainSpec>,
) -> Arc<HotColdDB<E, BeaconNodeBackend, BeaconNodeBackend>> {
    HotColdDB::open(
        &db.path().join("chain_db"),
        &db.path().join("freezer_db"),
        &db.path().join("blobs_db"),
        |_, _, _| Ok(()),
        StoreConfig::default(),
        spec,
    )
    .unwrap()
}

type DiskHarness = BeaconChainHarness<DiskHarnessType<E>>;

/// A harness on a fresh database in `db`.
fn disk_harness(db: &TempDir) -> DiskHarness {
    let spec = Arc::new(test_spec::<E>());
    let store = disk_store(db, spec.clone());
    BeaconChainHarness::builder(E::default())
        .spec(spec)
        .chain_config(chain_config())
        .deterministic_keypairs(VALIDATOR_COUNT)
        .fresh_disk_store(store)
        .mock_execution_layer()
        .build()
}

/// Restarts `harness`: persists its fork choice and rebuilds the chain from its database, so that
/// `BeaconForkChoiceStore::from_persisted` rebuilds the fork choice store with an empty balances
/// cache. The new harness shares the database, including its in-memory state cache, so the states
/// in `evicted_state_roots` are first evicted from the state cache, to be loaded from disk as
/// after a real restart.
fn restart_harness(harness: &DiskHarness, evicted_state_roots: &[Hash256]) -> DiskHarness {
    harness.chain.persist_fork_choice().unwrap();
    let store = harness.chain.store.clone();
    {
        let mut state_cache = store.state_cache.lock();
        for state_root in evicted_state_roots {
            state_cache.delete_state(state_root);
        }
    }
    let resumed = BeaconChainHarness::builder(E::default())
        .spec(harness.spec.clone())
        .chain_config(chain_config())
        .deterministic_keypairs(VALIDATOR_COUNT)
        .resumed_disk_store(store)
        .testing_slot_clock(harness.chain.slot_clock.clone())
        .execution_layer(harness.chain.execution_layer.clone())
        .build();
    {
        let fork_choice = resumed.chain.canonical_head.fork_choice_read_lock();
        assert!(fork_choice.fc_store().balances_cache().items().is_empty());
        assert_eq!(
            fork_choice.justified_checkpoint(),
            harness
                .chain
                .canonical_head
                .fork_choice_read_lock()
                .justified_checkpoint()
        );
    }
    check_balances_cache(&resumed);
    resumed
}

/// The root of the post-state of block `block_root`.
fn block_state_root<Hot: ItemStore, Cold: ItemStore>(
    harness: &Harness<Hot, Cold>,
    block_root: Hash256,
) -> Hash256 {
    harness
        .chain
        .store
        .get_blinded_block(&block_root)
        .unwrap()
        .unwrap_or_else(|| panic!("no block {block_root:?}"))
        .state_root()
}

/// The root of the state of fork choice's justified checkpoint.
fn justified_state_root<Hot: ItemStore, Cold: ItemStore>(harness: &Harness<Hot, Cold>) -> Hash256 {
    harness
        .chain
        .canonical_head
        .fork_choice_read_lock()
        .fc_store()
        .justified_state_root()
}

/// After a restart, the justified balances, which `from_persisted` builds from the justified state
/// loaded from disk, and the entries inserted later are still correct. That includes an entry
/// built from the post-state of an epoch boundary block imported before the restart, which is
/// loaded from disk.
#[tokio::test]
async fn restart() {
    let db = tempdir().unwrap();
    let harness = disk_harness(&db);

    let slots_per_epoch = E::slots_per_epoch();
    let epoch = Epoch::new(3);
    let boundary_slot = epoch.start_slot(slots_per_epoch);
    let next_boundary_slot = (epoch + 1).start_slot(slots_per_epoch);
    extend_to(&harness, boundary_slot + 1).await;
    let checkpoint = canonical_checkpoint(&harness, epoch);
    assert_eq!(
        block_at_slot(&harness, boundary_slot),
        Some(checkpoint.root)
    );
    assert!(
        harness
            .chain
            .canonical_head
            .fork_choice_read_lock()
            .justified_checkpoint()
            .epoch
            > 0
    );

    let resumed = restart_harness(&harness, &[justified_state_root(&harness)]);

    // The balances cache is empty after the restart, so the first block after it, which is in the
    // epoch of `checkpoint` but not at its boundary slot, loads the boundary block's post-state.
    // Evict it from the state cache, so that it is loaded from disk. The block includes a slashing,
    // which the entry would reflect if the block's own post-state were used instead.
    resumed
        .chain
        .store
        .state_cache
        .lock()
        .delete_state(&block_state_root(&resumed, checkpoint.root));
    let attester = validator_to_slash(&resumed, &[boundary_slot + 2, next_boundary_slot], &[]);
    resumed.add_attester_slashing(vec![attester]).unwrap();
    add_block_at_next_slot(&resumed).await;
    assert!(is_slashed_in_head_state(&resumed, attester));
    let items = check_balances_cache(&resumed);
    assert_eq!(checkpoints(&items), vec![checkpoint]);
    assert!(!is_slashed(entry(&items, checkpoint), attester));

    extend_to(&resumed, next_boundary_slot).await;
    let items = check_balances_cache(&resumed);
    assert!(is_slashed(
        entry(&items, canonical_checkpoint(&resumed, epoch + 1)),
        attester
    ));

    extend_until_justified_from_cache(&resumed, checkpoint).await;
}

/// After a restart at the end of an epoch, fork choice justifies the epoch's checkpoint at the
/// next epoch boundary, before any block can insert an entry for it into the empty balances cache.
/// So `set_justified_checkpoint` misses the cache and builds the justified balances from the
/// checkpoint state, loaded from disk. They reflect a slashing in the epoch boundary block, which
/// the previous justified balances don't, but not one in a later block of the epoch.
#[tokio::test]
async fn justified_checkpoint_not_cached_after_restart() {
    let db = tempdir().unwrap();
    let harness = disk_harness(&db);

    let slots_per_epoch = E::slots_per_epoch();
    let epoch = Epoch::new(3);
    let boundary_slot = epoch.start_slot(slots_per_epoch);
    let last_slot = (epoch + 1).start_slot(slots_per_epoch) - 1;
    // Neither slashed validator proposes in the epoch, so that every slot of it has a block and
    // its checkpoint is justified by the end of it.
    let epoch_slots = (boundary_slot.as_u64()..=last_slot.as_u64())
        .map(Slot::new)
        .collect::<Vec<_>>();

    extend_to(&harness, boundary_slot - 1).await;
    let in_boundary_block = validator_to_slash(&harness, &epoch_slots, &[]);
    harness
        .add_attester_slashing(vec![in_boundary_block])
        .unwrap();
    let root = add_block_at_next_slot(&harness).await;
    let checkpoint = Checkpoint { epoch, root };
    assert_eq!(canonical_checkpoint(&harness, epoch), checkpoint);

    let after_boundary_block = validator_to_slash(&harness, &epoch_slots, &[in_boundary_block]);
    harness
        .add_attester_slashing(vec![after_boundary_block])
        .unwrap();
    add_block_at_next_slot(&harness).await;
    assert!(is_slashed_in_head_state(&harness, after_boundary_block));
    extend_to(&harness, last_slot).await;

    // The checkpoint is unrealized justified, and fork choice will justify it at the next epoch
    // boundary.
    let checkpoint_state_root = block_state_root(&harness, root);
    {
        let fork_choice = harness.chain.canonical_head.fork_choice_read_lock();
        let fc_store = fork_choice.fc_store();
        assert_eq!(fc_store.justified_checkpoint().epoch, epoch - 1);
        assert!(!is_slashed(
            fc_store.justified_balances(),
            in_boundary_block
        ));
        assert_eq!(*fc_store.unrealized_justified_checkpoint(), checkpoint);
        assert_eq!(
            fc_store.unrealized_justified_state_root(),
            checkpoint_state_root
        );
    }

    // Also evict the checkpoint state, so that `set_justified_checkpoint` loads it from disk.
    let resumed = restart_harness(&harness, &[justified_state_root(&harness)]);
    resumed
        .chain
        .store
        .state_cache
        .lock()
        .delete_state(&checkpoint_state_root);

    // `add_block` checks the justified balances against the checkpoint state after every block.
    extend_until_justified(&resumed, checkpoint).await;
    let fork_choice = resumed.chain.canonical_head.fork_choice_read_lock();
    let fc_store = fork_choice.fc_store();
    assert_eq!(fc_store.justified_state_root(), checkpoint_state_root);
    let justified_balances = fc_store.justified_balances();
    let items = fc_store.balances_cache().items();
    assert!(!checkpoints(items).contains(&checkpoint), "{items:?}");
    assert!(
        items
            .iter()
            .all(|item| !JustifiedBalances::ptr_eq(justified_balances, item.justified_balances()))
    );
    assert!(is_slashed(justified_balances, in_boundary_block));
    assert!(!is_slashed(justified_balances, after_boundary_block));
}

/// Execution requests change the registry at later epoch boundaries: a top-up deposit raises a
/// validator's effective balance, a deposit for a new validator appends it, and a consolidation
/// exits its source. Each entry reflects exactly the changes in its checkpoint state.
#[tokio::test]
async fn execution_requests() {
    let mut spec = test_spec::<E>();
    let fork_name = spec.fork_name_at_epoch(Epoch::new(0));
    if !fork_name.electra_enabled() {
        return;
    }
    // Allow the consolidation: its source needn't have been active for long, and there must be
    // more consolidation churn than its balance. Before Gloas, that churn is the balance churn
    // less the activation and exit churn, and from Gloas a fraction of the total active balance.
    spec.shard_committee_period = 0;
    spec.min_per_epoch_churn_limit_electra =
        spec.max_per_epoch_activation_exit_churn_limit + 2 * spec.min_activation_balance;
    spec.consolidation_churn_limit_quotient = 16;
    let spec = Arc::new(spec);

    // Every validator has the same execution withdrawal address. The target of the top-up and
    // of the consolidation has compounding credentials, so its effective balance can grow.
    const TARGET: usize = 2;
    const SOURCE: usize = 3;
    let address = Address::repeat_byte(0xaa);
    let harness = BeaconChainHarness::builder(E::default())
        .spec(spec.clone())
        .chain_config(chain_config())
        .deterministic_keypairs(VALIDATOR_COUNT)
        .with_genesis_state_builder(move |builder| {
            builder.set_withdrawal_credentials_fn(Box::new(move |index, _, spec| {
                let mut credentials = [0; 32];
                credentials[0] = if index == TARGET {
                    spec.compounding_withdrawal_prefix_byte
                } else {
                    spec.eth1_address_withdrawal_prefix_byte
                };
                credentials[12..].copy_from_slice(address.as_slice());
                Hash256::from(credentials)
            }))
        })
        .fresh_ephemeral_store()
        .mock_execution_layer()
        .build();

    let slots_per_epoch = E::slots_per_epoch();
    extend_to(&harness, Slot::new(slots_per_epoch)).await;

    let state = harness.get_current_state();
    let target = state.get_validator(TARGET).unwrap().clone();
    let source = state.get_validator(SOURCE).unwrap().clone();
    let top_up_amount = 4 * spec.effective_balance_increment;
    let top_up = DepositRequest {
        pubkey: target.pubkey,
        withdrawal_credentials: target.withdrawal_credentials,
        amount: top_up_amount,
        // A top-up's signature is not checked.
        signature: SignatureBytes::empty(),
        index: 0,
    };
    let new_validator = generate_deterministic_keypair(VALIDATOR_COUNT);
    let mut deposit_data = DepositData {
        pubkey: new_validator.pk.compress(),
        withdrawal_credentials: source.withdrawal_credentials,
        amount: spec.min_activation_balance,
        signature: SignatureBytes::empty(),
    };
    deposit_data.signature = deposit_data.create_signature(&new_validator.sk, &spec);
    let new_deposit = DepositRequest {
        pubkey: deposit_data.pubkey,
        withdrawal_credentials: deposit_data.withdrawal_credentials,
        amount: deposit_data.amount,
        signature: deposit_data.signature,
        index: 1,
    };
    let consolidation = ConsolidationRequest {
        source_address: address,
        source_pubkey: source.pubkey,
        target_pubkey: target.pubkey,
    };
    let requests = if fork_name.gloas_enabled() {
        let mut requests = ExecutionRequestsGloas::<E>::default();
        requests.deposits.push(top_up).unwrap();
        requests.deposits.push(new_deposit).unwrap();
        requests.consolidations.push(consolidation).unwrap();
        ExecutionRequests::Gloas(requests)
    } else {
        let mut requests = ExecutionRequestsElectra::<E>::default();
        requests.deposits.push(top_up).unwrap();
        requests.deposits.push(new_deposit).unwrap();
        requests.consolidations.push(consolidation).unwrap();
        ExecutionRequests::Electra(requests)
    };
    harness
        .execution_block_generator()
        .set_next_execution_requests(requests);

    // Record the entry for each epoch of the canonical chain.
    let last_epoch = Epoch::new(10);
    let mut entries = vec![];
    for epoch in (2..=last_epoch.as_u64()).map(Epoch::new) {
        extend_to(&harness, epoch.start_slot(slots_per_epoch)).await;
        let items = check_balances_cache(&harness);
        let checkpoint = canonical_checkpoint(&harness, epoch);
        entries.push((epoch, entry(&items, checkpoint).clone()));
    }

    // The requests have taken effect.
    let state = harness.get_current_state();
    let target_effective_balance = target.effective_balance + top_up_amount;
    assert_eq!(
        state.get_validator(TARGET).unwrap().effective_balance,
        target_effective_balance
    );
    let exit_epoch = state.get_validator(SOURCE).unwrap().exit_epoch;
    assert!(exit_epoch <= last_epoch);
    assert_eq!(state.validators().len(), VALIDATOR_COUNT + 1);

    // Each change is in the entries from some epoch on, and not before.
    let first_epoch_with = |change: &dyn Fn(&JustifiedBalances) -> bool| {
        let position = entries
            .iter()
            .position(|(_, balances)| change(balances))
            .unwrap();
        assert!(position > 0);
        assert!(
            entries[position..]
                .iter()
                .all(|(_, balances)| change(balances))
        );
        entries[position].0
    };
    first_epoch_with(&|balances| balances.unslashed_balance(TARGET) == target_effective_balance);
    let source_exited = first_epoch_with(&|balances| balances.active_balance(SOURCE).unwrap() == 0);
    assert_eq!(source_exited, exit_epoch);
    first_epoch_with(&|balances| balances.effective_balances().len() == VALIDATOR_COUNT + 1);
}
