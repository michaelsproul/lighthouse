#![cfg(not(debug_assertions))]

//! Tests of where the fast confirmation rule gets the balances of the observed justified
//! checkpoints it rotates in at each epoch boundary (spec: `store.checkpoint_states[checkpoint]`).
//!
//! `run_fcr` takes them from fork choice's justified balances if the checkpoint is fork choice's
//! justified checkpoint, else from fork choice's balances cache, else from the checkpoint state,
//! loaded from the database. Whichever serves them, they must equal the balances of the spec's
//! `store_target_checkpoint_state`, which these tests compute independently: the checkpoint
//! block's own post-state, advanced to the start slot of the checkpoint's epoch.
//!
//! Validators exit and are slashed during the run, so that the balances of consecutive
//! checkpoints, and of a checkpoint and the head, differ. A lookup that built them from the wrong
//! state would then fail the comparison.

use beacon_chain::ChainConfig;
use beacon_chain::canonical_head::{CheckpointBalancesLookup, CheckpointBalancesSource};
use beacon_chain::chain_config::FastConfirmationMode;
use beacon_chain::observed_operations::ObservationOutcome;
use beacon_chain::test_utils::{
    BeaconChainHarness, DiskHarnessType, fork_name_from_env, test_spec,
};
use fork_choice::ForkChoiceStore;
use proto_array::JustifiedBalances;
use state_processing::state_advance::complete_state_advance;
use std::sync::Arc;
use store::database::interface::BeaconNodeBackend;
use store::{HotColdDB, StoreConfig};
use tempfile::{TempDir, tempdir};
use types::{BeaconState, ChainSpec, Checkpoint, Epoch, EthSpec, Slot, Spec};

type E = Spec;
type Harness = BeaconChainHarness<DiskHarnessType<E>>;

const VALIDATOR_COUNT: usize = 64;

/// Validators that exit voluntarily at epoch 5, one of which is slashed in epoch 3.
const FIRST_EXITS: [u64; 4] = [56, 57, 58, 59];
/// Validators that exit voluntarily at epoch 10, one of which is slashed in epoch 8.
const SECOND_EXITS: [u64; 4] = [60, 61, 62, 63];

fn spec() -> ChainSpec {
    let mut spec = test_spec::<E>();
    // Let validators exit voluntarily from genesis, and the exits of each of the batches above
    // take effect in the same epoch.
    spec.shard_committee_period = 0;
    spec.min_per_epoch_churn_limit = FIRST_EXITS.len() as u64;
    spec.min_per_epoch_churn_limit_electra = FIRST_EXITS.len() as u64 * spec.max_effective_balance;
    spec
}

fn chain_config() -> ChainConfig {
    ChainConfig {
        // Keep finalized states, from which the expected checkpoint states are built.
        archive: true,
        fast_confirmation: FastConfirmationMode::Enabled,
        ..ChainConfig::default()
    }
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

/// A harness with FCR enabled, on a fresh database in `db`.
fn harness(db: &TempDir) -> Harness {
    let spec = Arc::new(spec());
    let store = disk_store(db, spec.clone());
    BeaconChainHarness::builder(E::default())
        .spec(spec)
        .chain_config(chain_config())
        .deterministic_keypairs(VALIDATOR_COUNT)
        .fresh_disk_store(store)
        .mock_execution_layer()
        .build()
}

/// Restarts `harness` from its database after a graceful shutdown, which empties fork choice's
/// balances cache and seeds a new FCR from the finalized checkpoint.
fn restart(harness: &Harness) -> Harness {
    harness.chain.persist_fork_choice().unwrap();
    harness.chain.persist_op_pool().unwrap();
    let resumed = BeaconChainHarness::builder(E::default())
        .spec(harness.spec.clone())
        .chain_config(chain_config())
        .deterministic_keypairs(VALIDATOR_COUNT)
        .resumed_disk_store(harness.chain.store.clone())
        .testing_slot_clock(harness.chain.slot_clock.clone())
        .execution_layer(harness.chain.execution_layer.clone())
        .build();
    assert!(
        resumed
            .chain
            .canonical_head
            .fork_choice_read_lock()
            .fc_store()
            .balances_cache()
            .items()
            .is_empty()
    );
    resumed
}

/// The balances of the spec's `store_target_checkpoint_state(checkpoint)`.
fn expected_balances(harness: &Harness, checkpoint: Checkpoint) -> JustifiedBalances {
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
    JustifiedBalances::from_justified_state(&state).unwrap()
}

/// The balances of the head state.
fn head_balances(harness: &Harness) -> JustifiedBalances {
    JustifiedBalances::from_justified_state(&harness.get_current_state()).unwrap()
}

/// The proposer at `slot` on the chain of `state`, for a `slot` in the current or next epoch of
/// `state`, whose proposers are fixed.
fn proposer_index(harness: &Harness, state: &BeaconState<E>, slot: Slot) -> u64 {
    let mut state = state.clone();
    complete_state_advance(&mut state, None, slot, None, &harness.spec).unwrap();
    state.build_caches(&harness.spec).unwrap();
    state
        .get_beacon_proposer_index(slot, &harness.spec)
        .unwrap() as u64
}

/// Submits voluntary exits of `validator_indices` to the op pool, for the next block to include.
fn submit_exits(harness: &Harness, validator_indices: &[u64]) {
    let epoch = harness.get_current_state().current_epoch();
    for &validator_index in validator_indices {
        let exit = harness.make_voluntary_exit(validator_index, epoch);
        let ObservationOutcome::New(verified_exit) = harness
            .chain
            .verify_voluntary_exit_for_gossip(exit)
            .unwrap()
        else {
            panic!("the exit of validator {validator_index} should be new");
        };
        harness.chain.import_voluntary_exit(verified_exit);
    }
}

/// Asserts that each of `validator_indices` exits at `exit_epoch`.
fn assert_exit_epochs(harness: &Harness, validator_indices: &[u64], exit_epoch: Epoch) {
    let state = harness.get_current_state();
    for &validator_index in validator_indices {
        let validator = state.get_validator(validator_index as usize).unwrap();
        assert_eq!(validator.exit_epoch, exit_epoch, "{validator_index}");
    }
}

/// Submits an attester slashing of one of `candidates` to the op pool, for the next block to
/// include, and returns the slashed validator.
///
/// A slashed validator can't propose, but every slot of the run has a block. So the candidates
/// must have exited voluntarily, with their exit taking effect by the end of the next epoch, and
/// the one slashed proposes at none of the slots before then. The proposers of those slots are
/// already fixed.
fn slash_one_of(harness: &Harness, candidates: &[u64]) -> u64 {
    let slots_per_epoch = E::slots_per_epoch();
    let state = harness.get_current_state();
    let end_slot = (state.current_epoch() + 2).start_slot(slots_per_epoch);
    let proposers = (state.slot().as_u64() + 1..end_slot.as_u64())
        .map(Slot::new)
        .map(|slot| (slot, proposer_index(harness, &state, slot)))
        .collect::<Vec<_>>();
    let validator_index = candidates
        .iter()
        .copied()
        .find(|&validator_index| {
            let validator = state.get_validator(validator_index as usize).unwrap();
            let exit_slot = validator.exit_epoch.start_slot(slots_per_epoch);
            !validator.slashed
                && exit_slot <= end_slot
                && proposers
                    .iter()
                    .all(|&(slot, proposer)| slot >= exit_slot || proposer != validator_index)
        })
        .unwrap_or_else(|| panic!("no candidate of {candidates:?} can be slashed"));
    harness
        .add_attester_slashing(vec![validator_index])
        .unwrap();
    validator_index
}

/// FCR's observed justified checkpoints and balances, its latest checkpoint balances lookup, and
/// the slot of its latest per-slot update.
struct FcrView {
    previous: (Checkpoint, JustifiedBalances),
    current: (Checkpoint, JustifiedBalances),
    lookup: Option<CheckpointBalancesLookup>,
    last_update_slot: Option<Slot>,
}

fn fcr_view(harness: &Harness) -> FcrView {
    let fcr_mutex = harness.chain.canonical_head.fast_confirmation.as_ref();
    let fcr = fcr_mutex.unwrap().lock();
    let previous = &fcr.fcr.previous_epoch_observed_justified;
    let current = &fcr.fcr.current_epoch_observed_justified;
    FcrView {
        previous: (previous.checkpoint(), previous.balances().clone()),
        current: (current.checkpoint(), current.balances().clone()),
        lookup: fcr.last_checkpoint_balances_lookup,
        last_update_slot: fcr.fcr.last_update_slot(),
    }
}

/// Asserts that the balances of `checkpoint` are those of its checkpoint state, and that `source`
/// is what holds them: fork choice's justified balances, or its balances cache entry, or neither.
fn check_balances(
    harness: &Harness,
    checkpoint: Checkpoint,
    balances: &JustifiedBalances,
    source: CheckpointBalancesSource,
) {
    assert_eq!(
        balances,
        &expected_balances(harness, checkpoint),
        "{checkpoint:?} from {source:?}"
    );
    let fork_choice = harness.chain.canonical_head.fork_choice_read_lock();
    let fc_store = fork_choice.fc_store();
    let is_justified = *fc_store.justified_checkpoint() == checkpoint;
    let cached = fc_store
        .balances_cache()
        .get(checkpoint.root, checkpoint.epoch);
    match source {
        CheckpointBalancesSource::ForkChoiceJustified => {
            assert!(is_justified);
            assert!(JustifiedBalances::ptr_eq(
                fc_store.justified_balances(),
                balances
            ));
        }
        CheckpointBalancesSource::BalancesCache => {
            assert!(!is_justified);
            assert!(JustifiedBalances::ptr_eq(&cached.unwrap(), balances));
        }
        CheckpointBalancesSource::StateLoad => {
            assert!(!is_justified);
            assert!(cached.is_none());
        }
    }
}

/// A rotation of FCR's observed justified checkpoints at the start of a slot, checked by `step`.
#[derive(Debug)]
struct Rotation {
    checkpoint: Checkpoint,
    source: CheckpointBalancesSource,
    /// Whether the balances of the rotated-in checkpoint differ from those of the checkpoint it
    /// replaced. Both were checked against their checkpoint states, so the check can then tell
    /// the two checkpoints apart.
    changed: bool,
    /// Whether the balances of the rotated-in checkpoint differ from those of the head state.
    differs_from_head: bool,
}

/// The checkpoint epoch and source of each of `rotations`.
fn summary(rotations: &[Rotation]) -> Vec<(u64, CheckpointBalancesSource)> {
    rotations
        .iter()
        .map(|rotation| (rotation.checkpoint.epoch.as_u64(), rotation.source))
        .collect()
}

/// The checkpoint epochs of those of `rotations` whose balances changed.
fn changed_epochs(rotations: &[Rotation]) -> Vec<u64> {
    rotations
        .iter()
        .filter(|rotation| rotation.changed)
        .map(|rotation| rotation.checkpoint.epoch.as_u64())
        .collect()
}

/// Runs one slot: advances the clock, then runs fork choice and FCR at the start of the slot as
/// the per-slot timer does, then imports a block at the slot attested by `attesters`.
///
/// Returns the rotation at the start of the slot, if FCR rotated in a new checkpoint, after
/// checking its balances.
async fn step(harness: &Harness, attesters: &[usize]) -> Option<Rotation> {
    harness.advance_slot();
    let slot = harness.get_current_slot();
    let before = fcr_view(harness);
    harness.chain.recompute_head_at_current_slot().await;
    let after = fcr_view(harness);
    // FCR ran without error. Otherwise the block import below would retry, unobserved.
    assert_eq!(after.last_update_slot, Some(slot));

    let rotation = if after.current.0 == before.current.0 {
        assert_eq!(after.lookup, None);
        None
    } else {
        let lookup = after.lookup.expect("a new checkpoint needs balances");
        assert_eq!(lookup.checkpoint, after.current.0);
        check_balances(harness, lookup.checkpoint, &after.current.1, lookup.source);
        // The previous balance source is the former current one.
        assert_eq!(after.previous.0, before.current.0);
        assert!(JustifiedBalances::ptr_eq(
            &after.previous.1,
            &before.current.1
        ));
        Some(Rotation {
            checkpoint: lookup.checkpoint,
            source: lookup.source,
            changed: after.current.1 != after.previous.1,
            differs_from_head: after.current.1 != head_balances(harness),
        })
    };

    harness
        .add_attested_block_at_slot(slot, harness.get_current_state(), attesters)
        .await
        .unwrap();
    rotation
}

/// Runs the slots up to and including `last_slot`, each with a block, and returns the rotations.
///
/// With every validator attesting, an epoch's checkpoint becomes unrealized justified before the
/// last slot of the epoch, when FCR records the greatest unrealized justified checkpoint, and fork
/// choice justifies the same checkpoint at the next epoch boundary. With `lagging` participation,
/// no validator attests in the first slots of each epoch, so that the checkpoint only becomes
/// unrealized justified with the block at the last slot of the epoch, after FCR has recorded the
/// previous one. Fork choice then justifies a later checkpoint than FCR rotates in.
async fn run_until(harness: &Harness, last_slot: Slot, lagging: bool) -> Vec<Rotation> {
    let slots_per_epoch = E::slots_per_epoch();
    // The block at the last slot of an epoch includes the attestations of all of the epoch's
    // attested slots but the last, which just reach 2/3 of the validators. The block before it
    // doesn't.
    let unattested_slots = slots_per_epoch - 1 - (2 * slots_per_epoch).div_ceil(3);
    let all = harness.get_all_validators();
    let mut rotations = vec![];
    while harness.get_current_slot() < last_slot {
        let next_slot = harness.get_current_slot() + 1;
        let attesters = if lagging && next_slot.as_u64() % slots_per_epoch < unattested_slots {
            &[][..]
        } else {
            &all[..]
        };
        rotations.extend(step(harness, attesters).await);
    }
    rotations
}

/// In the steady state the balances come from fork choice, without a state load: from its
/// justified balances, or from its balances cache when fork choice has justified a later
/// checkpoint. After a restart empties the balances cache, the checkpoint state is loaded, both
/// to seed the new FCR from the finalized checkpoint and at the next epoch boundary.
#[tokio::test]
async fn checkpoint_balances_come_from_fork_choice() {
    // A pre-Bellatrix block has no execution block hash, so `run_fcr` errors.
    if fork_name_from_env().is_some_and(|fork| !fork.bellatrix_enabled()) {
        return;
    }
    use CheckpointBalancesSource::{BalancesCache, ForkChoiceJustified, StateLoad};

    let db = tempdir().unwrap();
    let harness = harness(&db);
    let epoch_start = |epoch: u64| Epoch::new(epoch).start_slot(E::slots_per_epoch());

    // Seeded from the genesis checkpoint, which is fork choice's justified checkpoint.
    let lookup = fcr_view(&harness).lookup.unwrap();
    assert_eq!(lookup.checkpoint.root, harness.chain.genesis_block_root);
    assert_eq!(lookup.source, ForkChoiceJustified);

    // Every validator attests for 6 epochs. The first exiting validators exit at epoch 5, and one
    // of them is slashed in the second block of epoch 3. So the checkpoints of epochs 3, 4 and 5
    // all have different balances.
    submit_exits(&harness, &FIRST_EXITS);
    let mut rotations = run_until(&harness, epoch_start(3) + 1, false).await;
    assert_exit_epochs(&harness, &FIRST_EXITS, Epoch::new(5));
    slash_one_of(&harness, &FIRST_EXITS);
    rotations.extend(run_until(&harness, epoch_start(5), false).await);
    submit_exits(&harness, &SECOND_EXITS);
    rotations.extend(run_until(&harness, epoch_start(6), false).await);
    assert_exit_epochs(&harness, &SECOND_EXITS, Epoch::new(10));
    assert_eq!(
        summary(&rotations),
        [2, 3, 4, 5].map(|epoch| (epoch, ForkChoiceJustified)),
        "{rotations:?}"
    );
    assert_eq!(changed_epochs(&rotations), [4, 5], "{rotations:?}");

    // Lagging participation. One of the second exiting validators is slashed in the second block
    // of epoch 8, before they exit at epoch 10.
    let mut rotations = run_until(&harness, epoch_start(8) + 1, true).await;
    slash_one_of(&harness, &SECOND_EXITS);
    rotations.extend(run_until(&harness, epoch_start(10), true).await);
    assert_eq!(
        summary(&rotations),
        [
            (6, ForkChoiceJustified),
            (7, BalancesCache),
            (8, BalancesCache)
        ],
        "{rotations:?}"
    );
    assert_eq!(
        changed_epochs(&rotations),
        Vec::<u64>::new(),
        "{rotations:?}"
    );

    // Restart mid-epoch.
    let rotations = run_until(&harness, epoch_start(10) + 4, true).await;
    assert!(rotations.is_empty(), "{rotations:?}");
    let resumed = restart(&harness);

    let finalized_checkpoint = resumed
        .chain
        .canonical_head
        .fork_choice_read_lock()
        .finalized_checkpoint();
    assert_eq!(finalized_checkpoint.epoch, 8);
    let view = fcr_view(&resumed);
    let lookup = view.lookup.unwrap();
    assert_eq!(lookup.checkpoint, finalized_checkpoint);
    assert_eq!(lookup.source, StateLoad);
    // The second exiting validators are active in the finalized checkpoint state, and have exited
    // in the head state.
    assert_ne!(view.current.1, head_balances(&resumed));
    for (checkpoint, balances) in [view.previous, view.current] {
        assert_eq!(checkpoint, finalized_checkpoint);
        check_balances(&resumed, checkpoint, &balances, StateLoad);
    }

    // At the next epoch boundary, the balances cache only has an entry for the epoch of the
    // restart, and fork choice has justified a later checkpoint than FCR rotates in, so its state
    // is loaded. Its balances differ from those of the finalized checkpoint, which don't have the
    // slashing, and from those of the head, where the second exiting validators have exited.
    let rotations = run_until(&resumed, epoch_start(11), true).await;
    assert_eq!(summary(&rotations), [(9, StateLoad)], "{rotations:?}");
    assert!(rotations[0].changed, "{rotations:?}");
    assert!(rotations[0].differs_from_head, "{rotations:?}");

    // Then the balances cache fills up again.
    let rotations = run_until(&resumed, epoch_start(14) + 4, true).await;
    assert_eq!(
        summary(&rotations),
        [10, 11, 12].map(|epoch| (epoch, BalancesCache)),
        "{rotations:?}"
    );
    assert_eq!(changed_epochs(&rotations), [10], "{rotations:?}");
    let rotations = run_until(&resumed, epoch_start(17) + 4, false).await;
    assert_eq!(
        summary(&rotations),
        [
            (13, BalancesCache),
            (15, ForkChoiceJustified),
            (16, ForkChoiceJustified)
        ],
        "{rotations:?}"
    );
}
