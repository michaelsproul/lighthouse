//! Benchmark the HTTP API of a beacon node.
use clap::ArgMatches;
use clap_utils::{parse_optional, parse_required};
use environment::Environment;
use eth2::{
    types::{BlockId, StateId},
    BeaconNodeHttpClient, SensitiveUrl, Timeouts,
};
use regex::Regex;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use types::{ChainSpec, EthSpec, Hash256, Slot};

use self::Category::*;
use self::Query::*;

fn hash256(hex_str: &str) -> Hash256 {
    Hash256::from_slice(&hex::decode(hex_str).unwrap())
}

/// Query against a specific endpoint including all `{name}` parameters and query options.
#[derive(Debug)]
pub enum Query {
    SszState(StateId),
    JsonState(StateId),
    SszBlindedBlock(BlockId),
    JsonBlindedBlock(BlockId),
    StateRoot(StateId),
    BlockAttestations(BlockId),
}

/// Types of queries based on difficulty and subjectivity.
///
/// - Difficulty: easy or hard depending on whether the query likely requires access to
/// hard-to-reconstruct historic states.
/// - Subjectivity: whether the query response will vary on repeat runs (subjective) or should
/// always be the same (objective). A `head` block query is subjective, a query for a specific slot
/// is objective.
#[derive(Debug, Clone, Copy)]
pub enum Category {
    EasyObjective,
    HardObjective,
    EasySubjective,
}

/// Test case consisting of a name (descriptive), query and class.
pub struct TestCase {
    name: &'static str,
    query: Query,
    category: Category,
}

/// Suite of queries to run against the test node and to measure timings for.
pub struct TestSuite {
    version: u8,
    cases: Vec<TestCase>,
}

/// A benchmark result for a `TestCase` from `TestSuite.cases`.
pub struct BenchmarkResult {
    /// Microseconds taken for the first call to this endpoint.
    cold_call_us: u128,
    /// Microseconds taken for subsequent calls to this endpoint.
    warm_call_us: Vec<u128>,
}

/// These are blocks and states that exist as of Jan 27 2023.
///
/// Subsequent versions of this suite could add newer states.
fn mainnet_test_suite() -> TestSuite {
    let version = 1;
    // States distinct 8192 slot periods are intentionally chosen to bypass unintended caching.
    let cases = vec![
        // Objective state queries (hard).
        // Slot 5660672.
        TestCase {
            name: "state_by_slot_0_mod_8192",
            query: SszState(StateId::Slot(Slot::new(5660672))),
            category: HardObjective,
        },
        // Slot 5652481.
        TestCase {
            name: "state_by_slot_1_mod_8192",
            query: SszState(StateId::Slot(Slot::new(5652481))),
            category: HardObjective,
        },
        // Slot 5644288.
        TestCase {
            name: "state_by_root_0_mod_8192",
            query: SszState(StateId::Root(hash256(
                "338befea4046a8e4b7a1096e8ea1d099102a8dc64e58bef4ce8f743ababdcc84",
            ))),
            category: HardObjective,
        },
        // Slot 5636097.
        TestCase {
            name: "state_by_root_1_mod_8192",
            query: SszState(StateId::Root(hash256(
                "af4eb904963d07c580469987cf8e9f14b330bb65fb4f318b2581886f9c51d6bc",
            ))),
            category: HardObjective,
        },
        // Slot 5636064.
        // Lies on an epoch boundary but very far from a default restore point (8192 slots).
        TestCase {
            name: "state_by_slot_8160_mod_8192",
            query: SszState(StateId::Slot(Slot::new(5636064))),
            category: HardObjective,
        },
        // Slot 5627873.
        // One slot after an epoch boundary but far from a default restore point (8192 slots).
        TestCase {
            name: "state_by_slot_8161_mod_8192",
            query: SszState(StateId::Slot(Slot::new(5627873))),
            category: HardObjective,
        },
        // Slot 5619711.
        // 31 slots after an epoch boundary, worst-case for Lighthouse.
        TestCase {
            name: "state_by_slot_8191_mod_8192",
            query: SszState(StateId::Slot(Slot::new(5619711))),
            category: HardObjective,
        },
        // Objective block queries (easy).
        // We jump around blocks in the same 8192 slot period between slot 5570560 and 5578752.
        /* Choose a new slot in Python with:
        import random; random.randint(5570560, 5578752)
        */
        // Slot 5574202.
        TestCase {
            name: "blinded_block_by_slot_00",
            query: SszBlindedBlock(BlockId::Slot(Slot::new(5574202))),
            category: EasyObjective,
        },
        // Slot 5577761.
        TestCase {
            name: "blinded_block_by_slot_01",
            query: SszBlindedBlock(BlockId::Slot(Slot::new(5577761))),
            category: EasyObjective,
        },
        TestCase {
            name: "head_state_ssz",
            query: SszState(StateId::Head),
            category: EasySubjective,
        },
        // Subjective tests (no reference to specific blocks/states).
        TestCase {
            name: "head_state_ssz",
            query: SszState(StateId::Head),
            category: EasySubjective,
        },
        TestCase {
            name: "head_state_json",
            query: JsonState(StateId::Head),
            category: EasySubjective,
        },
        TestCase {
            name: "head_blinded_block_ssz",
            query: SszBlindedBlock(BlockId::Head),
            category: EasySubjective,
        },
        TestCase {
            name: "head_blinded_block_json",
            query: JsonBlindedBlock(BlockId::Head),
            category: EasySubjective,
        },
        TestCase {
            name: "head_state_root",
            query: StateRoot(StateId::Head),
            category: EasySubjective,
        },
        TestCase {
            name: "head_block_attestations",
            query: BlockAttestations(BlockId::Head),
            category: EasySubjective,
        },
    ];

    TestSuite { version, cases }
}

impl Category {
    fn is_hard(self) -> bool {
        match self {
            HardObjective => true,
            EasyObjective | EasySubjective => false,
        }
    }

    fn is_subjective(self) -> bool {
        match self {
            EasyObjective | HardObjective => false,
            EasySubjective => true,
        }
    }

    fn should_run(&self, allow_hard: bool, allow_subjective: bool) -> bool {
        (allow_hard || !self.is_hard()) && (allow_subjective || !self.is_subjective())
    }
}

impl Query {
    async fn run<T: EthSpec>(
        &self,
        node: &BeaconNodeHttpClient,
        spec: &ChainSpec,
    ) -> Result<(), String> {
        match self {
            Query::SszState(state_id) => node
                .get_debug_beacon_states_ssz::<T>(*state_id, spec)
                .await
                .transpose()
                .ok_or_else(|| format!("missing state at {state_id:?}"))?
                .map(drop),
            Query::JsonState(state_id) => node
                .get_debug_beacon_states::<T>(*state_id)
                .await
                .transpose()
                .ok_or_else(|| format!("missing state at {state_id:?}"))?
                .map(drop),
            Query::SszBlindedBlock(block_id) => node
                .get_beacon_blinded_blocks_ssz::<T>(*block_id, spec)
                .await
                .transpose()
                .ok_or_else(|| format!("missing block at {block_id:?}"))?
                .map(drop),
            Query::JsonBlindedBlock(block_id) => node
                .get_beacon_blinded_blocks::<T>(*block_id)
                .await
                .transpose()
                .ok_or_else(|| format!("missing block at {block_id:?}"))?
                .map(drop),
            Query::StateRoot(state_id) => node
                .get_beacon_states_root(*state_id)
                .await
                .transpose()
                .ok_or_else(|| format!("missing state at {state_id:?}"))?
                .map(drop),
            Query::BlockAttestations(block_id) => node
                .get_beacon_blocks_attestations::<T>(*block_id)
                .await
                .transpose()
                .ok_or_else(|| format!("missing block at {block_id:?}"))?
                .map(drop),
            _ => unimplemented!(),
        }
        .map_err(|e| format!("error for query {self:?}: {e:?}"))
    }
}

pub fn run<T: EthSpec>(env: Environment<T>, matches: &ArgMatches) -> Result<(), String> {
    let executor = env.core_context().executor;
    let spec = env.eth2_config.spec.clone();
    executor
        .handle()
        .unwrap()
        .block_on(async move { async_run::<T>(matches, &spec).await })
        .unwrap();
    Ok(())
}

async fn async_run<'a, T: EthSpec>(
    matches: &'a ArgMatches<'a>,
    spec: &'a ChainSpec,
) -> Result<(), String> {
    let beacon_url: SensitiveUrl = parse_required(matches, "beacon-url")?;
    let timeout_seconds: u64 = parse_required(matches, "timeout")?;
    let output_path: PathBuf = parse_required(matches, "output")?;
    let num_repeats: usize = parse_required(matches, "num-repeats")?;
    let filter_regex: Option<Regex> = parse_optional(matches, "filter")?;
    let allow_hard = matches.is_present("hard");
    let allow_subjective = matches.is_present("subjective");
    let write_header = true;

    if spec.config_name.as_deref() != Some("mainnet") {
        return Err(format!(
            "Only mainnet is supported, got: {:?}",
            spec.config_name
        ));
    }

    let test_suite = mainnet_test_suite();

    info!(
        "Running test suite version {} against {}",
        test_suite.version, beacon_url,
    );

    let beacon_node = BeaconNodeHttpClient::new(
        beacon_url,
        Timeouts::set_all(Duration::from_secs(timeout_seconds)),
    );
    let mut results = Vec::with_capacity(test_suite.cases.len());

    let filtered_test_cases = test_suite
        .cases
        .iter()
        .filter(|case| {
            case.category.should_run(allow_hard, allow_subjective)
                && filter_regex
                    .as_ref()
                    .map_or(true, |regex| regex.is_match(&case.name))
        })
        .collect::<Vec<_>>();

    info!(
        "Selected {}/{} test cases to run",
        filtered_test_cases.len(),
        test_suite.cases.len()
    );

    // One little warm-up call to open the connection.
    beacon_node
        .get_node_version()
        .await
        .map_err(|e| format!("warm-up call to /eth/v1/node/version failed: {e:?}"))?;

    for test_case in &filtered_test_cases {
        debug!("Running {}", test_case.name);
        let t = Instant::now();
        test_case.query.run::<T>(&beacon_node, spec).await?;
        let cold_call_us = t.elapsed().as_micros();

        let mut warm_call_us = Vec::with_capacity(num_repeats);
        for _ in 0..num_repeats {
            let t = Instant::now();
            test_case.query.run::<T>(&beacon_node, spec).await?;
            warm_call_us.push(t.elapsed().as_micros());
        }

        results.push(BenchmarkResult {
            cold_call_us,
            warm_call_us,
        });
    }

    let mut writer = csv::Writer::from_path(output_path).unwrap();

    if write_header {
        let mut header = vec!["name".to_string(), "cold_call_us".to_string()];
        for i in 0..num_repeats {
            header.push(format!("warm_call_us_{i:02}"));
        }
        writer.write_record(header).unwrap();
    }

    for (test_case, result) in filtered_test_cases.iter().zip(results) {
        let mut row = vec![test_case.name.to_string(), result.cold_call_us.to_string()];
        row.extend(result.warm_call_us.iter().map(u128::to_string));

        writer.write_record(row).unwrap();
    }
    writer.flush().unwrap();

    Ok(())
}
