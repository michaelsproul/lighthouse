#[macro_use]
extern crate lazy_static;

use criterion::Criterion;
use criterion::{black_box, criterion_group, criterion_main, Benchmark};
use tree_hash::TreeHash;
use types::test_utils::{generate_deterministic_keypairs, TestingBeaconStateBuilder};
use types::{BeaconState, EthSpec, Keypair, MainnetEthSpec, MinimalEthSpec};

lazy_static! {
    static ref KEYPAIRS: Vec<Keypair> = { generate_deterministic_keypairs(300_000) };
}

fn build_state<T: EthSpec>(validator_count: usize) -> BeaconState<T> {
    let (state, _keypairs) = TestingBeaconStateBuilder::from_keypairs(
        KEYPAIRS[0..validator_count].to_vec(),
        &T::default_spec(),
    )
    .build();

    assert_eq!(state.validators.len(), validator_count);
    assert_eq!(state.balances.len(), validator_count);
    assert!(state.previous_epoch_attestations.is_empty());
    assert!(state.current_epoch_attestations.is_empty());
    assert!(state.eth1_data_votes.is_empty());
    assert!(state.historical_roots.is_empty());

    state
}

fn bench_suite<T: EthSpec>(c: &mut Criterion, spec_desc: &str, validator_count: usize) {
    let state1 = build_state::<T>(validator_count);
    let state2 = state1.clone();
    let mut state3 = state1.clone();
    let state4 = state1.clone();

    /*
    c.bench(
        &format!("{}/{}_validators", spec_desc, validator_count),
        Benchmark::new("genesis_state", move |b| {
            b.iter_batched_ref(
                || state1.clone(),
                // Note: `state.canonical_root()` uses whatever `tree_hash` that the `types` crate
                // uses, which is not necessarily this crate. If you want to ensure that types is
                // using this local version of `tree_hash`, ensure you add a workspace-level
                // [dependency
                // patch](https://doc.rust-lang.org/cargo/reference/manifest.html#the-patch-section).
                |state| black_box(state.canonical_root()),
                criterion::BatchSize::SmallInput,
            )
        })
        .sample_size(10),
    );
    */

    c.bench(
        &format!("{}/{}_validators/cold_cache", spec_desc, validator_count),
        Benchmark::new("genesis_state", move |b| {
            b.iter_batched_ref(
                || state2.clone(),
                // Note: `state.canonical_root()` uses whatever `tree_hash` that the `types` crate
                // uses, which is not necessarily this crate. If you want to ensure that types is
                // using this local version of `tree_hash`, ensure you add a workspace-level
                // [dependency
                // patch](https://doc.rust-lang.org/cargo/reference/manifest.html#the-patch-section).
                |state| {
                    assert!(!state.tree_hash_cache.initialized);
                    black_box(state.update_tree_hash_cache().unwrap())
                },
                criterion::BatchSize::SmallInput,
            )
        })
        .sample_size(10),
    );

    state3.build_tree_hash_cache().unwrap();

    c.bench(
        &format!("{}/{}_validators/hot_cache", spec_desc, validator_count),
        Benchmark::new("genesis_state", move |b| {
            b.iter_batched_ref(
                || state3.clone(),
                // Note: `state.canonical_root()` uses whatever `tree_hash` that the `types` crate
                // uses, which is not necessarily this crate. If you want to ensure that types is
                // using this local version of `tree_hash`, ensure you add a workspace-level
                // [dependency
                // patch](https://doc.rust-lang.org/cargo/reference/manifest.html#the-patch-section).
                |state| {
                    assert!(state.tree_hash_cache.initialized);
                    black_box(state.update_tree_hash_cache().unwrap())
                },
                criterion::BatchSize::SmallInput,
            )
        })
        .sample_size(10),
    );

    /*
    c.bench(
        &format!("{}/{}_validators/validators", spec_desc, validator_count),
        Benchmark::new("genesis_state", move |b| {
            b.iter_batched_ref(
                || state4.clone(),
                // Note: `state.canonical_root()` uses whatever `tree_hash` that the `types` crate
                // uses, which is not necessarily this crate. If you want to ensure that types is
                // using this local version of `tree_hash`, ensure you add a workspace-level
                // [dependency
                // patch](https://doc.rust-lang.org/cargo/reference/manifest.html#the-patch-section).
                |state| black_box(state.validators.tree_hash_root()),
                criterion::BatchSize::SmallInput,
            )
        })
        .sample_size(10),
    );
    */
}

fn all_benches(c: &mut Criterion) {
    // bench_suite::<MinimalEthSpec>(c, "minimal", 100_000);
    // bench_suite::<MinimalEthSpec>(c, "minimal", 300_000);

    // bench_suite::<MainnetEthSpec>(c, "mainnet", 100_000);
    bench_suite::<MainnetEthSpec>(c, "mainnet", 300_000);
}

criterion_group!(benches, all_benches,);
criterion_main!(benches);
