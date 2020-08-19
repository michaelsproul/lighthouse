#![cfg(not(debug_assertions))]

#[macro_use]
extern crate lazy_static;

use beacon_chain::{
    test_utils::{AttestationStrategy, BeaconChainHarness, BlockStrategy},
    BeaconChain, BeaconChainTypes,
};
use sloggers::{null::NullLoggerBuilder, Build};
use std::sync::Arc;
use store::{HotColdDB, LevelDB, StoreConfig};
use tempfile::{tempdir, TempDir};
use types::{EthSpec, Keypair, MinimalEthSpec, Slot};

type E = MinimalEthSpec;

// Should ideally be divisible by 3.
pub const VALIDATOR_COUNT: usize = 24;

lazy_static! {
    /// A cached set of keys.
    static ref KEYPAIRS: Vec<Keypair> = types::test_utils::generate_deterministic_keypairs(VALIDATOR_COUNT);
}

fn get_store(db_path: &TempDir) -> Arc<HotColdDB<E, LevelDB<E>, LevelDB<E>>> {
    let spec = E::default_spec();
    let hot_path = db_path.path().join("hot_db");
    let cold_path = db_path.path().join("cold_db");
    let config = StoreConfig::default();
    let log = NullLoggerBuilder.build().expect("logger should build");
    Arc::new(
        HotColdDB::open(&hot_path, &cold_path, config, spec, log)
            .expect("disk store should initialize"),
    )
}

#[test]
fn finalizes_after_resuming_from_db() {
    let validator_count = 16;
    let slots_per_epoch = MinimalEthSpec::slots_per_epoch();
    let num_blocks_produced = slots_per_epoch * 10;
    let first_half = 6 * slots_per_epoch - 1;

    let db_path = tempdir().unwrap();
    let store = get_store(&db_path);

    let harness = BeaconChainHarness::new_with_disk_store(
        MinimalEthSpec,
        store.clone(),
        KEYPAIRS[0..validator_count].to_vec(),
    );

    harness.advance_slot();

    harness.extend_chain(
        3 * slots_per_epoch as usize - 1,
        BlockStrategy::OnCanonicalHead,
        AttestationStrategy::AllValidators,
    );

    harness.advance_slot();

    assert_eq!(harness.chain.slot().unwrap(), 3 * slots_per_epoch);

    harness.extend_chain(
        slots_per_epoch as usize - 1,
        BlockStrategy::ForkCanonicalChainAt {
            previous_slot: Slot::new(3 * slots_per_epoch - 1),
            first_slot: Slot::new(3 * slots_per_epoch + 1),
        },
        AttestationStrategy::AllValidators,
    );

    harness.advance_slot();

    assert_eq!(harness.chain.slot().unwrap(), 4 * slots_per_epoch);

    harness.extend_chain(
        2 * slots_per_epoch as usize,
        BlockStrategy::ForkCanonicalChainAt {
            previous_slot: Slot::new(4 * slots_per_epoch - 1),
            first_slot: Slot::new(4 * slots_per_epoch + 1),
        },
        AttestationStrategy::AllValidators,
    );

    assert_eq!(harness.chain.slot().unwrap(), 6 * slots_per_epoch);

    assert_eq!(
        harness
            .chain
            .head()
            .expect("should read head")
            .beacon_state
            .finalized_checkpoint
            .epoch,
        4,
        "the chain should have already finalized"
    );

    let latest_slot = harness.chain.slot().expect("should have a slot");
    println!("we're at slot {}", latest_slot);
    println!("split slot is {}", store.get_split_slot());

    harness
        .chain
        .persist_head_and_fork_choice()
        .expect("should persist the head and fork choice");
    harness
        .chain
        .persist_op_pool()
        .expect("should persist the op pool");
    harness
        .chain
        .persist_eth1_cache()
        .expect("should persist the eth1 cache");

    let data_dir = harness.data_dir;
    let original_chain = harness.chain;

    let resumed_harness = BeaconChainHarness::resume_from_disk_store(
        MinimalEthSpec,
        store,
        KEYPAIRS[0..validator_count].to_vec(),
        data_dir,
    );

    assert_chains_pretty_much_the_same(&original_chain, &resumed_harness.chain);

    // Set the slot clock of the resumed harness to be in the slot following the previous harness.
    //
    // This allows us to produce the block at the next slot.
    resumed_harness
        .chain
        .slot_clock
        .set_slot(latest_slot.as_u64() + 1);

    resumed_harness.extend_chain(
        (num_blocks_produced - first_half - 1) as usize,
        BlockStrategy::OnCanonicalHead,
        AttestationStrategy::AllValidators,
    );

    let state = &resumed_harness
        .chain
        .head()
        .expect("should read head")
        .beacon_state;
    assert_eq!(
        state.slot, num_blocks_produced,
        "head should be at the current slot"
    );
    assert_eq!(
        state.current_epoch(),
        num_blocks_produced / MinimalEthSpec::slots_per_epoch(),
        "head should be at the expected epoch"
    );
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

/// Checks that two chains are the same, for the purpose of this tests.
///
/// Several fields that are hard/impossible to check are ignored (e.g., the store).
fn assert_chains_pretty_much_the_same<T: BeaconChainTypes>(a: &BeaconChain<T>, b: &BeaconChain<T>) {
    assert_eq!(a.spec, b.spec, "spec should be equal");
    assert_eq!(a.op_pool, b.op_pool, "op_pool should be equal");
    assert_eq!(
        a.head().unwrap(),
        b.head().unwrap(),
        "head() should be equal"
    );
    assert_eq!(a.heads(), b.heads(), "heads() should be equal");
    assert_eq!(
        a.genesis_block_root, b.genesis_block_root,
        "genesis_block_root should be equal"
    );
    assert!(
        *a.fork_choice.read() == *b.fork_choice.read(),
        "fork_choice should be equal"
    );
}
