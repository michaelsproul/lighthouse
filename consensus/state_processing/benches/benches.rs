use beacon_chain::test_utils::{BeaconChainHarness, EphemeralHarnessType};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use state_processing::{per_block_processing, per_slot_processing, BlockSignatureStrategy};
use std::time::Duration;
use types::{
    test_utils::generate_deterministic_keypairs, BeaconState, Epoch, EthSpec, MainnetEthSpec,
    SignedBeaconBlock, Slot,
};

const VALIDATOR_COUNT: usize = 200_000;

// Set to `None` for phase0
const ALTAIR_FORK_EPOCH: Option<Epoch> = Some(Epoch::new(0));
// const ALTAIR_FORK_EPOCH: Option<Epoch> = None;

const BLOCK_SLOT: Slot = Slot::new(1);

type E = MainnetEthSpec;

fn setup() -> (
    BeaconState<E>,
    SignedBeaconBlock<E>,
    BeaconChainHarness<EphemeralHarnessType<E>>,
) {
    let mut spec = E::default_spec();
    spec.altair_fork_epoch = ALTAIR_FORK_EPOCH;

    let keypairs = generate_deterministic_keypairs(VALIDATOR_COUNT);

    println!("generating keys");
    let harness = BeaconChainHarness::new(MainnetEthSpec, Some(spec), keypairs);
    println!("complete");

    let genesis_block_root = harness.chain.genesis_block_root;
    let mut genesis_state = harness.get_current_state();
    let genesis_state_root = genesis_state.update_tree_hash_cache().unwrap();

    let all_validator_indices = (0..VALIDATOR_COUNT).collect::<Vec<_>>();

    // Include some attestations (this modifies the participation flags on Altair)
    let attestations = harness
        .make_attestations(
            &all_validator_indices,
            &genesis_state,
            genesis_state_root,
            genesis_block_root.into(),
            Slot::new(0),
        )
        .into_iter()
        .filter_map(|(_, agg)| Some(agg?.message.aggregate));

    let (block, _) =
        harness.make_block_with_modifier(genesis_state.clone(), BLOCK_SLOT, move |block| {
            for attestation in attestations {
                block
                    .body_mut()
                    .attestations_mut()
                    .push(attestation)
                    .unwrap();
            }
        });

    (genesis_state, block, harness)
}

fn bench_block(c: &mut Criterion) {
    let mut group = c.benchmark_group("block benchmarks");
    group.measurement_time(Duration::from_secs(30));

    let (pre_state, block, harness) = setup();
    let spec = &harness.chain.spec;

    group.bench_function("update tree hash", move |b| {
        b.iter_batched(
            || {
                let mut state = pre_state.clone();
                while state.slot() < BLOCK_SLOT {
                    per_slot_processing(&mut state, None, spec).unwrap();
                }
                per_block_processing(
                    &mut state,
                    &block,
                    None,
                    BlockSignatureStrategy::NoVerification,
                    spec,
                )
                .unwrap();
                state
            },
            |mut state| state.update_tree_hash_cache().unwrap(),
            BatchSize::LargeInput,
        )
    });

    group.finish();
}

criterion_group!(benches, bench_block);
criterion_main!(benches);
