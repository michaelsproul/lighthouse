use env_logger::{Builder, Env};
use std::collections::HashSet;
use test_harness::BeaconChainHarness;
use types::ChainSpec;

// 8 shards attesting per epoch
// each shard has 2 validators
fn chain_spec() -> ChainSpec {
    ChainSpec {
        // prev: 64
        shard_count: 8,
        target_committee_size: 1,
        max_attestations: 128,
        ..ChainSpec::few_validators()
    }
}

#[test]
fn lots_of_attestations() {
    Builder::from_env(Env::default().default_filter_or("info")).init();

    let spec = chain_spec();
    let validator_count = 8; // prev: 128

    let mut harness = BeaconChainHarness::new(spec.clone(), validator_count);

    for _ in 0..10 * spec.slots_per_epoch {
        harness.advance_chain_with_block();
    }

    let chain_dump = harness.chain_dump().unwrap();
    let uniq_attestations = chain_dump
        .iter()
        .flat_map(|checkpoint| {
            checkpoint
                .beacon_block
                .body
                .attestations
                .iter()
                .map(|att| (att.aggregation_bitfield.clone(), att.data.clone()))
        })
        .collect::<HashSet<_>>();
    let total_attestations = chain_dump
        .iter()
        .map(|c| c.beacon_block.body.attestations.len())
        .sum::<usize>();
    assert_eq!(uniq_attestations.len(), total_attestations);

    for (i, checkpoint) in harness.chain_dump().unwrap().iter().enumerate() {
        let block = &checkpoint.beacon_block;
        println!(
            "block #{} (slot {}), {} attestations",
            i,
            block.slot.as_u64(),
            block.body.attestations.len()
        );
    }
}
