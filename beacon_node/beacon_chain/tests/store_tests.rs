#![cfg(not(debug_assertions))]

#[macro_use]
extern crate lazy_static;

use beacon_chain::test_utils::{
    AttestationStrategy, BeaconChainHarness, BlockStrategy, CheckPoint, DiskHarnessType,
};
use beacon_chain::{AttestationProcessingOutcome, StateSkipConfig};
use rand::Rng;
use sloggers::{null::NullLoggerBuilder, Build};
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use store::{
    iter::{BlockRootsIterator, StateRootsIterator},
    DiskStore, Store, StoreConfig,
};
use tempfile::{tempdir, TempDir};
use tree_hash::TreeHash;
use types::test_utils::{SeedableRng, XorShiftRng};
use types::*;

// Should ideally be divisible by 3.
pub const VALIDATOR_COUNT: usize = 24;
pub const VALIDATOR_SUPERMAJORITY: usize = (VALIDATOR_COUNT / 3) * 2;

lazy_static! {
    /// A cached set of keys.
    static ref KEYPAIRS: Vec<Keypair> = types::test_utils::generate_deterministic_keypairs(VALIDATOR_COUNT);
}

type E = MinimalEthSpec;
type TestHarness = BeaconChainHarness<DiskHarnessType<E>>;

fn get_store(db_path: &TempDir) -> Arc<DiskStore<E>> {
    let spec = MinimalEthSpec::default_spec();
    let hot_path = db_path.path().join("hot_db");
    let cold_path = db_path.path().join("cold_db");
    let config = StoreConfig::default();
    let log = NullLoggerBuilder.build().expect("logger should build");
    Arc::new(
        DiskStore::open(&hot_path, &cold_path, config, spec, log)
            .expect("disk store should initialize"),
    )
}

fn get_harness(store: Arc<DiskStore<E>>, validator_count: usize) -> TestHarness {
    let harness = BeaconChainHarness::new_with_disk_store(
        MinimalEthSpec,
        store,
        KEYPAIRS[0..validator_count].to_vec(),
    );
    harness.advance_slot();
    harness
}

#[test]
fn full_participation_no_skips() {
    let num_blocks_produced = E::slots_per_epoch() * 5;
    let db_path = tempdir().unwrap();
    let store = get_store(&db_path);
    let harness = get_harness(store.clone(), VALIDATOR_COUNT);

    harness.extend_chain(
        num_blocks_produced as usize,
        BlockStrategy::OnCanonicalHead,
        AttestationStrategy::AllValidators,
    );

    check_finalization(&harness, num_blocks_produced);
    check_split_slot(&harness, store);
    check_chain_dump(&harness, num_blocks_produced + 1);
    check_iterators(&harness);
}

#[test]
fn randomised_skips() {
    let num_slots = E::slots_per_epoch() * 5;
    let mut num_blocks_produced = 0;
    let db_path = tempdir().unwrap();
    let store = get_store(&db_path);
    let harness = get_harness(store.clone(), VALIDATOR_COUNT);
    let rng = &mut XorShiftRng::from_seed([42; 16]);

    let mut head_slot = 0;

    for slot in 1..=num_slots {
        if rng.gen_bool(0.8) {
            harness.extend_chain(
                1,
                BlockStrategy::ForkCanonicalChainAt {
                    previous_slot: Slot::new(head_slot),
                    first_slot: Slot::new(slot),
                },
                AttestationStrategy::AllValidators,
            );
            harness.advance_slot();
            num_blocks_produced += 1;
            head_slot = slot;
        } else {
            harness.advance_slot();
        }
    }

    let state = &harness.chain.head().expect("should get head").beacon_state;

    assert_eq!(state.slot, num_slots, "head should be at the current slot");

    check_split_slot(&harness, store);
    check_chain_dump(&harness, num_blocks_produced + 1);
    check_iterators(&harness);
}

#[test]
fn long_skip() {
    let db_path = tempdir().unwrap();
    let store = get_store(&db_path);
    let harness = get_harness(store.clone(), VALIDATOR_COUNT);

    // Number of blocks to create in the first run, intentionally not falling on an epoch
    // boundary in order to check that the DB hot -> cold migration is capable of reaching
    // back across the skip distance, and correctly migrating those extra non-finalized states.
    let initial_blocks = E::slots_per_epoch() * 5 + E::slots_per_epoch() / 2;
    let skip_slots = E::slots_per_historical_root() as u64 * 8;
    let final_blocks = E::slots_per_epoch() * 4;

    harness.extend_chain(
        initial_blocks as usize,
        BlockStrategy::OnCanonicalHead,
        AttestationStrategy::AllValidators,
    );

    check_finalization(&harness, initial_blocks);

    // 2. Skip slots
    for _ in 0..skip_slots {
        harness.advance_slot();
    }

    // 3. Produce more blocks, establish a new finalized epoch
    harness.extend_chain(
        final_blocks as usize,
        BlockStrategy::ForkCanonicalChainAt {
            previous_slot: Slot::new(initial_blocks),
            first_slot: Slot::new(initial_blocks + skip_slots as u64 + 1),
        },
        AttestationStrategy::AllValidators,
    );

    check_finalization(&harness, initial_blocks + skip_slots + final_blocks);
    check_split_slot(&harness, store);
    check_chain_dump(&harness, initial_blocks + final_blocks + 1);
    check_iterators(&harness);
}

/// Go forward to the point where the genesis randao value is no longer part of the vector.
///
/// This implicitly checks that:
/// 1. The chunked vector scheme doesn't attempt to store an incorrect genesis value
/// 2. We correctly load the genesis value for all required slots
/// NOTE: this test takes about a minute to run
#[test]
fn randao_genesis_storage() {
    let validator_count = 8;
    let db_path = tempdir().unwrap();
    let store = get_store(&db_path);
    let harness = get_harness(store.clone(), validator_count);

    let num_slots = E::slots_per_epoch() * (E::epochs_per_historical_vector() - 1) as u64;

    // Check we have a non-trivial genesis value
    let genesis_value = *harness
        .chain
        .head()
        .expect("should get head")
        .beacon_state
        .get_randao_mix(Epoch::new(0))
        .expect("randao mix ok");
    assert!(!genesis_value.is_zero());

    harness.extend_chain(
        num_slots as usize - 1,
        BlockStrategy::OnCanonicalHead,
        AttestationStrategy::AllValidators,
    );

    // Check that genesis value is still present
    assert!(harness
        .chain
        .head()
        .expect("should get head")
        .beacon_state
        .randao_mixes
        .iter()
        .find(|x| **x == genesis_value)
        .is_some());

    // Then upon adding one more block, it isn't
    harness.advance_slot();
    harness.extend_chain(
        1,
        BlockStrategy::OnCanonicalHead,
        AttestationStrategy::AllValidators,
    );
    assert!(harness
        .chain
        .head()
        .expect("should get head")
        .beacon_state
        .randao_mixes
        .iter()
        .find(|x| **x == genesis_value)
        .is_none());

    check_finalization(&harness, num_slots);
    check_split_slot(&harness, store);
    check_chain_dump(&harness, num_slots + 1);
    check_iterators(&harness);
}

// Check that closing and reopening a freezer DB restores the split slot to its correct value.
#[test]
fn split_slot_restore() {
    let db_path = tempdir().unwrap();

    let split_slot = {
        let store = get_store(&db_path);
        let harness = get_harness(store.clone(), VALIDATOR_COUNT);

        let num_blocks = 4 * E::slots_per_epoch();

        harness.extend_chain(
            num_blocks as usize,
            BlockStrategy::OnCanonicalHead,
            AttestationStrategy::AllValidators,
        );

        store.get_split_slot()
    };
    assert_ne!(split_slot, Slot::new(0));

    // Re-open the store
    let store = get_store(&db_path);

    assert_eq!(store.get_split_slot(), split_slot);
}

// Check attestation processing and `load_epoch_boundary_state` in the presence of a split DB.
// This is a bit of a monster test in that it tests lots of different things, but until they're
// tested elsewhere, this is as good a place as any.
#[test]
fn epoch_boundary_state_attestation_processing() {
    let num_blocks_produced = E::slots_per_epoch() * 5;
    let db_path = tempdir().unwrap();
    let store = get_store(&db_path);
    let harness = get_harness(store.clone(), VALIDATOR_COUNT);

    let late_validators = vec![0, 1];
    let timely_validators = (2..VALIDATOR_COUNT).collect::<Vec<_>>();

    let mut late_attestations = vec![];

    for _ in 0..num_blocks_produced {
        harness.extend_chain(
            1,
            BlockStrategy::OnCanonicalHead,
            AttestationStrategy::SomeValidators(timely_validators.clone()),
        );

        let head = harness.chain.head().expect("head ok");
        late_attestations.extend(harness.get_free_attestations(
            &AttestationStrategy::SomeValidators(late_validators.clone()),
            &head.beacon_state,
            head.beacon_block_root,
            head.beacon_block.slot(),
        ));

        harness.advance_slot();
    }

    check_finalization(&harness, num_blocks_produced);
    check_split_slot(&harness, store.clone());
    check_chain_dump(&harness, num_blocks_produced + 1);
    check_iterators(&harness);

    let mut checked_pre_fin = false;

    for attestation in late_attestations {
        // load_epoch_boundary_state is idempotent!
        let block_root = attestation.data.beacon_block_root;
        let block = store.get_block(&block_root).unwrap().expect("block exists");
        let epoch_boundary_state = store
            .load_epoch_boundary_state(&block.state_root())
            .expect("no error")
            .expect("epoch boundary state exists");
        let ebs_of_ebs = store
            .load_epoch_boundary_state(&epoch_boundary_state.canonical_root())
            .expect("no error")
            .expect("ebs of ebs exists");
        assert_eq!(epoch_boundary_state, ebs_of_ebs);

        // If the attestation is pre-finalization it should be rejected.
        let finalized_epoch = harness
            .chain
            .head_info()
            .expect("head ok")
            .finalized_checkpoint
            .epoch;
        let res = harness
            .chain
            .process_attestation_internal(attestation.clone());

        let current_epoch = harness.chain.epoch().expect("should get epoch");
        let attestation_epoch = attestation.data.target.epoch;

        if attestation.data.slot <= finalized_epoch.start_slot(E::slots_per_epoch())
            || attestation_epoch + 1 < current_epoch
        {
            checked_pre_fin = true;
            assert_eq!(
                res,
                Ok(AttestationProcessingOutcome::PastEpoch {
                    attestation_epoch,
                    current_epoch,
                })
            );
        } else {
            assert_eq!(res, Ok(AttestationProcessingOutcome::Processed));
        }
    }
    assert!(checked_pre_fin);
}

#[test]
fn delete_blocks_and_states() {
    let db_path = tempdir().unwrap();
    let store = get_store(&db_path);
    let harness = get_harness(store.clone(), VALIDATOR_COUNT);

    let unforked_blocks = 4 * E::slots_per_epoch();

    // Finalize an initial portion of the chain.
    harness.extend_chain(
        unforked_blocks as usize,
        BlockStrategy::OnCanonicalHead,
        AttestationStrategy::AllValidators,
    );

    // Create a fork post-finalization.
    let two_thirds = (VALIDATOR_COUNT / 3) * 2;
    let honest_validators: Vec<usize> = (0..two_thirds).collect();
    let faulty_validators: Vec<usize> = (two_thirds..VALIDATOR_COUNT).collect();

    // NOTE: should remove this -1 and/or write a similar test once #845 is resolved
    // https://github.com/sigp/lighthouse/issues/845
    let fork_blocks = 2 * E::slots_per_epoch() - 1;

    let (honest_head, faulty_head) = harness.generate_two_forks_by_skipping_a_block(
        &honest_validators,
        &faulty_validators,
        fork_blocks as usize,
        fork_blocks as usize,
    );

    assert!(honest_head != faulty_head, "forks should be distinct");
    let head_info = harness.chain.head_info().expect("should get head");
    assert_eq!(head_info.slot, unforked_blocks + fork_blocks);

    assert_eq!(
        head_info.block_root, honest_head,
        "the honest chain should be the canonical chain",
    );

    let faulty_head_block = store
        .get_block(&faulty_head)
        .expect("no errors")
        .expect("faulty head block exists");

    let faulty_head_state = store
        .get_state(
            &faulty_head_block.state_root(),
            Some(faulty_head_block.slot()),
        )
        .expect("no db error")
        .expect("faulty head state exists");

    let states_to_delete = StateRootsIterator::new(store.clone(), &faulty_head_state)
        .take_while(|(_, slot)| *slot > unforked_blocks)
        .collect::<Vec<_>>();

    // Delete faulty fork
    // Attempting to load those states should find them unavailable
    for (state_root, slot) in &states_to_delete {
        assert_eq!(store.delete_state(state_root, *slot), Ok(()));
        assert_eq!(store.get_state(state_root, Some(*slot)), Ok(None));
    }

    // Double-deleting should also be OK (deleting non-existent things is fine)
    for (state_root, slot) in &states_to_delete {
        assert_eq!(store.delete_state(state_root, *slot), Ok(()));
    }

    // Deleting the blocks from the fork should remove them completely
    let blocks_to_delete = BlockRootsIterator::new(store.clone(), &faulty_head_state)
        // Extra +1 here accounts for the skipped slot that started this fork
        .take_while(|(_, slot)| *slot > unforked_blocks + 1)
        .collect::<Vec<_>>();

    for (block_root, _) in blocks_to_delete {
        assert_eq!(store.delete_block(&block_root), Ok(()));
        assert_eq!(store.get_block(&block_root), Ok(None));
    }

    // Deleting frozen states should do nothing
    let split_slot = store.get_split_slot();
    let finalized_states = harness
        .chain
        .rev_iter_state_roots()
        .expect("rev iter ok")
        .filter(|(_, slot)| *slot < split_slot);

    for (state_root, slot) in finalized_states {
        assert_eq!(store.delete_state(&state_root, slot), Ok(()));
    }

    // After all that, the chain dump should still be OK
    check_chain_dump(&harness, unforked_blocks + fork_blocks + 1);
}

#[test]
fn prunes_abandoned_fork_between_two_finalized_checkpoints() {
    let db_path = tempdir().unwrap();
    let store = get_store(&db_path);
    let harness = get_harness(Arc::clone(&store), VALIDATOR_COUNT);
    const HONEST_VALIDATOR_COUNT: usize = VALIDATOR_SUPERMAJORITY;
    let honest_validators: Vec<usize> = (0..HONEST_VALIDATOR_COUNT).collect();
    let faulty_validators: Vec<usize> = (HONEST_VALIDATOR_COUNT..VALIDATOR_COUNT).collect();
    let slots_per_epoch: usize = MinimalEthSpec::slots_per_epoch() as usize;

    let slot = harness.get_chain_slot();
    let state = harness.get_chain_state(slot);
    let (canonical_blocks_pre_finalization, _, slot, _, state) =
        harness.add_canonical_chain_blocks(state, slot, slots_per_epoch, &honest_validators);
    let (stray_blocks, stray_states, _, stray_head, _) = harness.add_stray_blocks(
        harness.get_chain_state(slot),
        slot,
        slots_per_epoch - 1,
        &faulty_validators,
    );

    // Precondition: Ensure all stray_blocks blocks are still known
    for &block_hash in stray_blocks.values() {
        assert!(
            harness
                .chain
                .get_block(&block_hash.into())
                .unwrap()
                .is_some(),
            "stray blocks should be still present",
        );
    }

    for (&slot, &state_hash) in &stray_states {
        assert!(
            harness
                .chain
                .get_state(&state_hash.into(), Some(slot))
                .unwrap()
                .is_some(),
            "stray states should be still present",
        );
    }

    // Precondition: Only genesis is finalized
    let chain_dump = harness.chain.chain_dump().unwrap();
    assert_eq!(
        get_finalized_epoch_boundary_blocks(&chain_dump),
        vec![Hash256::zero().into()].into_iter().collect(),
    );

    assert!(harness.chain.knows_head(&stray_head));

    // Trigger finalization
    let (canonical_blocks_post_finalization, _, _, _, _) =
        harness.add_canonical_chain_blocks(state, slot, slots_per_epoch * 5, &honest_validators);

    // Postcondition: New blocks got finalized
    let chain_dump = harness.chain.chain_dump().unwrap();
    let finalized_blocks = get_finalized_epoch_boundary_blocks(&chain_dump);
    assert_eq!(
        finalized_blocks,
        vec![
            Hash256::zero().into(),
            canonical_blocks_pre_finalization[&Slot::new(slots_per_epoch as u64)],
            canonical_blocks_post_finalization[&Slot::new((slots_per_epoch * 2) as u64)],
        ]
        .into_iter()
        .collect()
    );

    // Postcondition: Ensure all stray_blocks blocks have been pruned
    for &block_hash in stray_blocks.values() {
        assert!(
            harness
                .chain
                .get_block(&block_hash.into())
                .unwrap()
                .is_none(),
            "abandoned blocks should have been pruned",
        );
    }

    for (&slot, &state_hash) in &stray_blocks {
        assert!(
            harness
                .chain
                .get_state(&state_hash.into(), Some(slot))
                .unwrap()
                .is_none(),
            "stray states should have been deleted",
        );
    }

    assert!(!harness.chain.knows_head(&stray_head));
}

#[test]
fn pruning_does_not_touch_abandoned_block_shared_with_canonical_chain() {
    let db_path = tempdir().unwrap();
    let store = get_store(&db_path);
    let harness = get_harness(Arc::clone(&store), VALIDATOR_COUNT);
    const HONEST_VALIDATOR_COUNT: usize = VALIDATOR_SUPERMAJORITY;
    let honest_validators: Vec<usize> = (0..HONEST_VALIDATOR_COUNT).collect();
    let faulty_validators: Vec<usize> = (HONEST_VALIDATOR_COUNT..VALIDATOR_COUNT).collect();
    let all_validators: Vec<usize> = (0..VALIDATOR_COUNT).collect();
    let slots_per_epoch: usize = MinimalEthSpec::slots_per_epoch() as usize;

    // Fill up 0th epoch
    let slot = harness.get_chain_slot();
    let state = harness.get_chain_state(slot);
    let (canonical_blocks_zeroth_epoch, _, slot, _, state) =
        harness.add_canonical_chain_blocks(state, slot, slots_per_epoch, &honest_validators);

    // Fill up 1st epoch
    let (_, _, canonical_slot, shared_head, canonical_state) =
        harness.add_canonical_chain_blocks(state, slot, 1, &all_validators);
    let (stray_blocks, stray_states, _, stray_head, _) = harness.add_stray_blocks(
        canonical_state.clone(),
        canonical_slot,
        1,
        &faulty_validators,
    );

    // Preconditions
    for &block_hash in stray_blocks.values() {
        assert!(
            harness
                .chain
                .get_block(&block_hash.into())
                .unwrap()
                .is_some(),
            "stray blocks should be still present",
        );
    }

    for (&slot, &state_hash) in &stray_states {
        assert!(
            harness
                .chain
                .get_state(&state_hash.into(), Some(slot))
                .unwrap()
                .is_some(),
            "stray states should be still present",
        );
    }

    let chain_dump = harness.chain.chain_dump().unwrap();
    assert_eq!(
        get_finalized_epoch_boundary_blocks(&chain_dump),
        vec![Hash256::zero().into()].into_iter().collect(),
    );

    assert!(get_blocks(&chain_dump).contains(&shared_head));

    // Trigger finalization
    let (canonical_blocks, _, _, _, _) = harness.add_canonical_chain_blocks(
        canonical_state,
        canonical_slot,
        slots_per_epoch * 5,
        &honest_validators,
    );

    // Postconditions
    let chain_dump = harness.chain.chain_dump().unwrap();
    let finalized_blocks = get_finalized_epoch_boundary_blocks(&chain_dump);
    assert_eq!(
        finalized_blocks,
        vec![
            Hash256::zero().into(),
            canonical_blocks_zeroth_epoch[&Slot::new(slots_per_epoch as u64)],
            canonical_blocks[&Slot::new((slots_per_epoch * 2) as u64)],
        ]
        .into_iter()
        .collect()
    );

    for &block_hash in stray_blocks.values() {
        assert!(
            harness
                .chain
                .get_block(&block_hash.into())
                .unwrap()
                .is_none(),
            "stray blocks should have been pruned",
        );
    }

    for (&slot, &state_hash) in &stray_blocks {
        assert!(
            harness
                .chain
                .get_state(&state_hash.into(), Some(slot))
                .unwrap()
                .is_none(),
            "stray states should have been deleted",
        );
    }

    assert!(!harness.chain.knows_head(&stray_head));
    assert!(get_blocks(&chain_dump).contains(&shared_head));
}

#[test]
fn pruning_does_not_touch_blocks_prior_to_finalization() {
    let db_path = tempdir().unwrap();
    let store = get_store(&db_path);
    let harness = get_harness(Arc::clone(&store), VALIDATOR_COUNT);
    const HONEST_VALIDATOR_COUNT: usize = VALIDATOR_SUPERMAJORITY;
    let honest_validators: Vec<usize> = (0..HONEST_VALIDATOR_COUNT).collect();
    let faulty_validators: Vec<usize> = (HONEST_VALIDATOR_COUNT..VALIDATOR_COUNT).collect();
    let slots_per_epoch: usize = MinimalEthSpec::slots_per_epoch() as usize;

    // Fill up 0th epoch with canonical chain blocks
    let slot = harness.get_chain_slot();
    let state = harness.get_chain_state(slot);
    let (canonical_blocks_zeroth_epoch, _, slot, _, state) =
        harness.add_canonical_chain_blocks(state, slot, slots_per_epoch, &honest_validators);

    // Fill up 1st epoch.  Contains a fork.
    let (stray_blocks, stray_states, _, stray_head, _) =
        harness.add_stray_blocks(state.clone(), slot, slots_per_epoch - 1, &faulty_validators);

    // Preconditions
    for &block_hash in stray_blocks.values() {
        assert!(
            harness
                .chain
                .get_block(&block_hash.into())
                .unwrap()
                .is_some(),
            "stray blocks should be still present",
        );
    }

    for (&slot, &state_hash) in &stray_states {
        assert!(
            harness
                .chain
                .get_state(&state_hash.into(), Some(slot))
                .unwrap()
                .is_some(),
            "stray states should be still present",
        );
    }

    let chain_dump = harness.chain.chain_dump().unwrap();
    assert_eq!(
        get_finalized_epoch_boundary_blocks(&chain_dump),
        vec![Hash256::zero().into()].into_iter().collect(),
    );

    // Trigger finalization
    let (_, _, _, _, _) =
        harness.add_canonical_chain_blocks(state, slot, slots_per_epoch * 4, &honest_validators);

    // Postconditions
    let chain_dump = harness.chain.chain_dump().unwrap();
    let finalized_blocks = get_finalized_epoch_boundary_blocks(&chain_dump);
    assert_eq!(
        finalized_blocks,
        vec![
            Hash256::zero().into(),
            canonical_blocks_zeroth_epoch[&Slot::new(slots_per_epoch as u64)],
        ]
        .into_iter()
        .collect()
    );

    for &block_hash in stray_blocks.values() {
        assert!(
            harness
                .chain
                .get_block(&block_hash.into())
                .unwrap()
                .is_some(),
            "stray blocks should be still present",
        );
    }

    for (&slot, &state_hash) in &stray_blocks {
        assert!(
            harness
                .chain
                .get_state(&state_hash.into(), Some(slot))
                .unwrap()
                .is_none(),
            "stray states should have been deleted",
        );
    }

    assert!(harness.chain.knows_head(&stray_head));
}

#[test]
fn prunes_fork_running_past_finalized_checkpoint() {
    let db_path = tempdir().unwrap();
    let store = get_store(&db_path);
    let harness = get_harness(Arc::clone(&store), VALIDATOR_COUNT);
    const HONEST_VALIDATOR_COUNT: usize = VALIDATOR_SUPERMAJORITY;
    let honest_validators: Vec<usize> = (0..HONEST_VALIDATOR_COUNT).collect();
    let faulty_validators: Vec<usize> = (HONEST_VALIDATOR_COUNT..VALIDATOR_COUNT).collect();
    let slots_per_epoch: usize = MinimalEthSpec::slots_per_epoch() as usize;

    // Fill up 0th epoch with canonical chain blocks
    let slot = harness.get_chain_slot();
    let state = harness.get_chain_state(slot);
    let (canonical_blocks_zeroth_epoch, _, slot, _, state) =
        harness.add_canonical_chain_blocks(state, slot, slots_per_epoch, &honest_validators);

    // Fill up 1st epoch.  Contains a fork.
    let (stray_blocks_first_epoch, stray_states_first_epoch, stray_slot, _, stray_state) =
        harness.add_stray_blocks(state.clone(), slot, slots_per_epoch, &faulty_validators);

    let (canonical_blocks_first_epoch, _, canonical_slot, _, canonical_state) =
        harness.add_canonical_chain_blocks(state, slot, slots_per_epoch, &honest_validators);

    // Fill up 2nd epoch.  Extends both the canonical chain and the fork.
    let (stray_blocks_second_epoch, stray_states_second_epoch, _, stray_head, _) = harness
        .add_stray_blocks(
            stray_state,
            stray_slot,
            slots_per_epoch - 1,
            &faulty_validators,
        );

    // Precondition: Ensure all stray_blocks blocks are still known
    let stray_blocks: HashMap<Slot, SignedBeaconBlockHash> = stray_blocks_first_epoch
        .into_iter()
        .chain(stray_blocks_second_epoch.into_iter())
        .collect();

    let stray_states: HashMap<Slot, BeaconStateHash> = stray_states_first_epoch
        .into_iter()
        .chain(stray_states_second_epoch.into_iter())
        .collect();

    for &block_hash in stray_blocks.values() {
        assert!(
            harness
                .chain
                .get_block(&block_hash.into())
                .unwrap()
                .is_some(),
            "stray blocks should be still present",
        );
    }

    for (&slot, &state_hash) in &stray_states {
        assert!(
            harness
                .chain
                .get_state(&state_hash.into(), Some(slot))
                .unwrap()
                .is_some(),
            "stray states should be still present",
        );
    }

    // Precondition: Only genesis is finalized
    let chain_dump = harness.chain.chain_dump().unwrap();
    assert_eq!(
        get_finalized_epoch_boundary_blocks(&chain_dump),
        vec![Hash256::zero().into()].into_iter().collect(),
    );

    assert!(harness.chain.knows_head(&stray_head));

    // Trigger finalization
    let (canonical_blocks_second_epoch, _, _, _, _) = harness.add_canonical_chain_blocks(
        canonical_state,
        canonical_slot,
        slots_per_epoch * 4,
        &honest_validators,
    );

    // Postconditions
    let canonical_blocks: HashMap<Slot, SignedBeaconBlockHash> = canonical_blocks_zeroth_epoch
        .into_iter()
        .chain(canonical_blocks_first_epoch.into_iter())
        .chain(canonical_blocks_second_epoch.into_iter())
        .collect();

    // Postcondition: New blocks got finalized
    let chain_dump = harness.chain.chain_dump().unwrap();
    let finalized_blocks = get_finalized_epoch_boundary_blocks(&chain_dump);
    assert_eq!(
        finalized_blocks,
        vec![
            Hash256::zero().into(),
            canonical_blocks[&Slot::new(slots_per_epoch as u64)],
            canonical_blocks[&Slot::new((slots_per_epoch * 2) as u64)],
        ]
        .into_iter()
        .collect()
    );

    // Postcondition: Ensure all stray_blocks blocks have been pruned
    for &block_hash in stray_blocks.values() {
        assert!(
            harness
                .chain
                .get_block(&block_hash.into())
                .unwrap()
                .is_none(),
            "abandoned blocks should have been pruned",
        );
    }

    for (&slot, &state_hash) in &stray_blocks {
        assert!(
            harness
                .chain
                .get_state(&state_hash.into(), Some(slot))
                .unwrap()
                .is_none(),
            "stray states should have been deleted",
        );
    }

    assert!(!harness.chain.knows_head(&stray_head));
}

// This is to check if state outside of normal block processing are pruned correctly.
#[test]
fn prunes_skipped_slots_states() {
    let db_path = tempdir().unwrap();
    let store = get_store(&db_path);
    let harness = get_harness(Arc::clone(&store), VALIDATOR_COUNT);
    const HONEST_VALIDATOR_COUNT: usize = VALIDATOR_SUPERMAJORITY;
    let honest_validators: Vec<usize> = (0..HONEST_VALIDATOR_COUNT).collect();
    let faulty_validators: Vec<usize> = (HONEST_VALIDATOR_COUNT..VALIDATOR_COUNT).collect();
    let slots_per_epoch: usize = MinimalEthSpec::slots_per_epoch() as usize;

    // Arrange skipped slots so as to cross the epoch boundary.  That way, we excercise the code
    // responsible for storing state outside of normal block processing.

    let canonical_slot = harness.get_chain_slot();
    let canonical_state = harness.get_chain_state(canonical_slot);
    let (canonical_blocks_zeroth_epoch, _, canonical_slot, _, canonical_state) = harness
        .add_canonical_chain_blocks(
            canonical_state,
            canonical_slot,
            slots_per_epoch - 1,
            &honest_validators,
        );

    let (stray_blocks, stray_states, stray_slot, _, _) = harness.add_stray_blocks(
        canonical_state.clone(),
        canonical_slot,
        slots_per_epoch,
        &faulty_validators,
    );

    // Preconditions
    for &block_hash in stray_blocks.values() {
        assert!(
            harness
                .chain
                .get_block(&block_hash.into())
                .unwrap()
                .is_some(),
            "stray blocks should be still present",
        );
    }

    for (&slot, &state_hash) in &stray_states {
        assert!(
            harness
                .chain
                .get_state(&state_hash.into(), Some(slot))
                .unwrap()
                .is_some(),
            "stray states should be still present",
        );
    }

    let chain_dump = harness.chain.chain_dump().unwrap();
    assert_eq!(
        get_finalized_epoch_boundary_blocks(&chain_dump),
        vec![Hash256::zero().into()].into_iter().collect(),
    );

    // Make sure slots were skipped
    let stray_state = harness
        .chain
        .state_at_slot(stray_slot, StateSkipConfig::WithoutStateRoots)
        .unwrap();
    let block_root = stray_state.get_block_root(canonical_slot - 1);
    assert_eq!(stray_state.get_block_root(canonical_slot), block_root);
    assert_eq!(stray_state.get_block_root(canonical_slot + 1), block_root);

    let skipped_slots = vec![canonical_slot, canonical_slot + 1];
    for &slot in &skipped_slots {
        assert_eq!(stray_state.get_block_root(slot), block_root);
        let state_hash = stray_state.get_state_root(slot).unwrap();
        assert!(
            harness
                .chain
                .get_state(&state_hash, Some(slot))
                .unwrap()
                .is_some(),
            "skipped slots state should be still present"
        );
    }

    // Trigger finalization
    let (canonical_blocks_post_finalization, _, _, _, _) = harness.add_canonical_chain_blocks(
        canonical_state,
        canonical_slot,
        slots_per_epoch * 5,
        &honest_validators,
    );

    // Postconditions
    let chain_dump = harness.chain.chain_dump().unwrap();
    let finalized_blocks = get_finalized_epoch_boundary_blocks(&chain_dump);
    let canonical_blocks: HashMap<Slot, SignedBeaconBlockHash> = canonical_blocks_zeroth_epoch
        .into_iter()
        .chain(canonical_blocks_post_finalization.into_iter())
        .collect();
    assert_eq!(
        finalized_blocks,
        vec![
            Hash256::zero().into(),
            canonical_blocks[&Slot::new(slots_per_epoch as u64)],
        ]
        .into_iter()
        .collect()
    );

    for (&slot, &state_hash) in &stray_blocks {
        assert!(
            harness
                .chain
                .get_state(&state_hash.into(), Some(slot))
                .unwrap()
                .is_none(),
            "stray states should have been deleted",
        );
    }

    for &slot in &skipped_slots {
        assert_eq!(stray_state.get_block_root(slot), block_root);
        let state_hash = stray_state.get_state_root(slot).unwrap();
        assert!(
            harness
                .chain
                .get_state(&state_hash, Some(slot))
                .unwrap()
                .is_none(),
            "skipped slot states should have been pruned"
        );
    }
}

/// Check that the head state's slot matches `expected_slot`.
fn check_slot(harness: &TestHarness, expected_slot: u64) {
    let state = &harness.chain.head().expect("should get head").beacon_state;

    assert_eq!(
        state.slot, expected_slot,
        "head should be at the current slot"
    );
}

/// Check that the chain has finalized under best-case assumptions, and check the head slot.
fn check_finalization(harness: &TestHarness, expected_slot: u64) {
    let state = &harness.chain.head().expect("should get head").beacon_state;

    check_slot(harness, expected_slot);

    assert_eq!(
        state.current_justified_checkpoint.epoch,
        state.current_epoch() - 1,
        "the head should be justified one behind the current epoch"
    );
    assert_eq!(
        state.finalized_checkpoint.epoch,
        state.current_epoch() - 2,
        "the head should be finalized two behind the current epoch"
    );
}

/// Check that the DiskStore's split_slot is equal to the start slot of the last finalized epoch.
fn check_split_slot(harness: &TestHarness, store: Arc<DiskStore<E>>) {
    let split_slot = store.get_split_slot();
    assert_eq!(
        harness
            .chain
            .head()
            .expect("should get head")
            .beacon_state
            .finalized_checkpoint
            .epoch
            .start_slot(E::slots_per_epoch()),
        split_slot
    );
    assert_ne!(split_slot, 0);
}

/// Check that all the states in a chain dump have the correct tree hash.
fn check_chain_dump(harness: &TestHarness, expected_len: u64) {
    let chain_dump = harness.chain.chain_dump().unwrap();

    assert_eq!(chain_dump.len() as u64, expected_len);

    for checkpoint in &chain_dump {
        // Check that the tree hash of the stored state is as expected
        assert_eq!(
            checkpoint.beacon_state_root,
            checkpoint.beacon_state.tree_hash_root(),
            "tree hash of stored state is incorrect"
        );

        // Check that looking up the state root with no slot hint succeeds.
        // This tests the state root -> slot mapping.
        assert_eq!(
            harness
                .chain
                .store
                .get_state(&checkpoint.beacon_state_root, None)
                .expect("no error")
                .expect("state exists")
                .slot,
            checkpoint.beacon_state.slot
        );
    }

    // Check the forwards block roots iterator against the chain dump
    let chain_dump_block_roots = chain_dump
        .iter()
        .map(|checkpoint| (checkpoint.beacon_block_root, checkpoint.beacon_block.slot()))
        .collect::<Vec<_>>();

    let head = harness.chain.head().expect("should get head");
    let mut forward_block_roots = Store::forwards_block_roots_iterator(
        harness.chain.store.clone(),
        Slot::new(0),
        head.beacon_state,
        head.beacon_block_root,
        &harness.spec,
    )
    .collect::<Vec<_>>();

    // Drop the block roots for skipped slots.
    forward_block_roots.dedup_by_key(|(block_root, _)| *block_root);

    for i in 0..std::cmp::max(chain_dump_block_roots.len(), forward_block_roots.len()) {
        assert_eq!(
            chain_dump_block_roots[i],
            forward_block_roots[i],
            "split slot is {}",
            harness.chain.store.get_split_slot()
        );
    }
}

/// Check that state and block root iterators can reach genesis
fn check_iterators(harness: &TestHarness) {
    assert_eq!(
        harness
            .chain
            .rev_iter_state_roots()
            .expect("should get iter")
            .last()
            .map(|(_, slot)| slot),
        Some(Slot::new(0))
    );
    assert_eq!(
        harness
            .chain
            .rev_iter_block_roots()
            .expect("should get iter")
            .last()
            .map(|(_, slot)| slot),
        Some(Slot::new(0))
    );
}

fn get_finalized_epoch_boundary_blocks(
    dump: &[CheckPoint<MinimalEthSpec>],
) -> HashSet<SignedBeaconBlockHash> {
    dump.iter()
        .cloned()
        .map(|checkpoint| checkpoint.beacon_state.finalized_checkpoint.root.into())
        .collect()
}

fn get_blocks(dump: &[CheckPoint<MinimalEthSpec>]) -> HashSet<SignedBeaconBlockHash> {
    dump.iter()
        .cloned()
        .map(|checkpoint| checkpoint.beacon_block_root.into())
        .collect()
}
