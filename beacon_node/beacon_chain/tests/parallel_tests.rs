#![cfg(not(debug_assertions))]

use beacon_chain::test_utils::{test_logger, BeaconChainHarness, DiskHarnessType};
use lazy_static::lazy_static;
use slog::{error, info};
use std::sync::Arc;
use store::{HotColdDB, LevelDB, StoreConfig};
use tempfile::{tempdir, TempDir};
use types::*;

pub const VALIDATOR_COUNT: usize = 1024;

lazy_static! {
    /// A cached set of keys.
    static ref KEYPAIRS: Vec<Keypair> = types::test_utils::generate_deterministic_keypairs(VALIDATOR_COUNT);
}

type E = MinimalEthSpec;
type TestHarness = BeaconChainHarness<DiskHarnessType<E>>;

fn get_store(db_path: &TempDir) -> Arc<HotColdDB<E, LevelDB<E>, LevelDB<E>>> {
    let spec = MinimalEthSpec::default_spec();
    let hot_path = db_path.path().join("hot_db");
    let cold_path = db_path.path().join("cold_db");
    let config = StoreConfig::default();
    let log = test_logger();

    Arc::new(
        HotColdDB::open(&hot_path, &cold_path, config, spec, log)
            .expect("disk store should initialize"),
    )
}

fn get_harness(
    store: Arc<HotColdDB<E, LevelDB<E>, LevelDB<E>>>,
    validator_count: usize,
) -> TestHarness {
    let harness = BeaconChainHarness::new_with_disk_store(
        MinimalEthSpec,
        store,
        KEYPAIRS[0..validator_count].to_vec(),
    );
    harness.advance_slot();
    harness
}

#[test]
fn prune_and_grow_chain_concurrent_mutation() {
    let db_path = tempdir().unwrap();
    let store = get_store(&db_path);
    let harness = get_harness(store.clone(), VALIDATOR_COUNT);

    let slots_per_epoch = E::slots_per_epoch();

    let all_validators = (0..VALIDATOR_COUNT).collect::<Vec<_>>();

    let initial_slots = (10 * slots_per_epoch..12 * slots_per_epoch)
        .map(Slot::new)
        .collect::<Vec<_>>();
    let first_fork_slot = Slot::new(11 * slots_per_epoch);

    // Create some forks.
    let initial_state = harness.get_current_state();

    let mut chains = harness.add_blocks_on_multiple_chains(vec![
        // Canonical chain.
        (initial_state.clone(), initial_slots, all_validators.clone()),
        // Fork chain beginning at the same point but skipping lots of slots.
        (initial_state.clone(), vec![first_fork_slot], vec![]),
    ]);

    // Check that finalization hasn't advanced yet.
    let (_, _, _, canonical_state) = chains.remove(0);
    let (_, _, _, fork_state) = chains.remove(0);

    assert_eq!(canonical_state.finalized_checkpoint.epoch, 0);

    let harness = Arc::new(harness);
    let h1 = harness.clone();
    let h2 = harness.clone();

    // Trigger pruning by extending the canonical chain, at the same time that
    // blocks are added to fork chains.
    hiatus::enable();

    let t1 = std::thread::spawn(move || {
        match h1.add_attested_block_at_slot(
            Slot::new(12 * slots_per_epoch),
            fork_state.clone(),
            &[],
        ) {
            Ok((block_root, _)) => {
                info!(h1.logger(), "Created fork block {:?}", block_root);
                Some(block_root)
            }
            Err(e) => {
                error!(h1.logger(), "Block processing failed with error: {}", e);
                None
            }
        }
    });
    let canonical_validators = all_validators.clone();
    let t2 = std::thread::spawn(move || {
        h2.add_attested_block_at_slot(
            Slot::new(12 * slots_per_epoch),
            canonical_state.clone(),
            &canonical_validators,
        )
        .unwrap()
    });
    t1.join().unwrap();
    let (_, canonical_state) = t2.join().unwrap();

    hiatus::disable();

    assert_eq!(canonical_state.finalized_checkpoint.epoch, 10);

    info!(
        harness.logger(),
        "Heads after first attempt at pruning: {:?}",
        harness.chain.heads()
    );
    assert_eq!(harness.chain.heads().len(), 2);

    // Re-finalize the chain and retry pruning.
    let end_slots = (12 * slots_per_epoch + 1..=13 * slots_per_epoch)
        .map(Slot::new)
        .collect::<Vec<_>>();
    harness.add_attested_blocks_at_slots(canonical_state, &end_slots, &all_validators);

    info!(
        harness.logger(),
        "Heads after second attempt: {:?}",
        harness.chain.heads()
    );
    assert_eq!(harness.chain.heads().len(), 1);
}
