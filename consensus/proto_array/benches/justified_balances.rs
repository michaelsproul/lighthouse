//! Compares building `JustifiedBalances` from a state's caches with reading its validator
//! registry.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use proto_array::JustifiedBalances;
use state_processing::epoch_cache::initialize_epoch_cache;
use types::{BeaconState, Epoch, EthSpec, MainnetEthSpec, RelativeEpoch, Validator};

type E = MainnetEthSpec;

const VALIDATOR_SET_SIZES: [usize; 2] = [100_000, 1_000_000];

/// A phase0 state with `num_validators` validators: 85% active, 10% exited and 5% pending
/// activation, with 1% of all validators slashed, half of them active and half exited. The caches
/// read by `try_from_state_caches` are built.
fn build_state(num_validators: usize) -> BeaconState<E> {
    let spec = E::default_spec();
    let mut state = BeaconState::<E>::new(0, <_>::default(), &spec);
    for i in 0..num_validators {
        let (activation_epoch, exit_epoch) = match i % 20 {
            0 | 1 => (Epoch::new(0), Epoch::new(0)),
            2 => (spec.far_future_epoch, spec.far_future_epoch),
            _ => (Epoch::new(0), spec.far_future_epoch),
        };
        state
            .validators_mut()
            .push(Validator {
                effective_balance: spec.max_effective_balance,
                slashed: i % 200 == 1 || i % 200 == 3,
                activation_eligibility_epoch: Epoch::new(0),
                activation_epoch,
                exit_epoch,
                withdrawable_epoch: spec.far_future_epoch,
                ..Default::default()
            })
            .expect("push validator");
        state
            .balances_mut()
            .push(spec.max_effective_balance)
            .expect("push balance");
    }
    state
        .apply_pending_mutations()
        .expect("apply pending mutations");
    state
        .build_committee_cache(RelativeEpoch::Current, &spec)
        .expect("committee cache");
    state.build_slashings_cache().expect("slashings cache");
    initialize_epoch_cache(&mut state, &spec).expect("epoch cache");
    state
}

fn bench_justified_balances(c: &mut Criterion) {
    let mut group = c.benchmark_group("justified_balances");
    group.sample_size(20);

    for &num_validators in &VALIDATOR_SET_SIZES {
        let state = build_state(num_validators);
        assert_eq!(
            JustifiedBalances::try_from_state_caches(&state),
            Ok(JustifiedBalances::from_justified_state(&state).expect("from registry")),
            "the cache path must be taken and agree with the registry"
        );

        group.bench_with_input(
            BenchmarkId::new("from_justified_state", num_validators),
            &state,
            |b, state| b.iter(|| JustifiedBalances::from_justified_state(state)),
        );
        group.bench_with_input(
            BenchmarkId::new("try_from_state_caches", num_validators),
            &state,
            |b, state| b.iter(|| JustifiedBalances::try_from_state_caches(state)),
        );
    }

    group.finish();
}

criterion_group!(benches, bench_justified_balances);
criterion_main!(benches);
