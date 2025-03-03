//! # Skip-Slots
//!
//! Use this tool to process a `BeaconState` through empty slots. Useful for benchmarking or
//! troubleshooting consensus failures.
//!
//! It can load states from file or pull them from a beaconAPI. States pulled from a beaconAPI can
//! be saved to disk to reduce future calls to that server.
//!
//! ## Examples
//!
//! ### Example 1.
//!
//! Download a state from a HTTP endpoint and skip forward an epoch, twice (the initial state is
//! advanced 32 slots twice, rather than it being advanced 64 slots):
//!
//! ```ignore
//! lcli skip-slots \
//!     --beacon-url http://localhost:5052 \
//!     --state-id 0x3cdc33cd02713d8d6cc33a6dbe2d3a5bf9af1d357de0d175a403496486ff845e \\
//!     --slots 32 \
//!     --runs 2
//! ```
//!
//! ### Example 2.
//!
//! Download a state to a SSZ file (without modifying it):
//!
//! ```ignore
//! lcli skip-slots \
//!     --beacon-url http://localhost:5052 \
//!     --state-id 0x3cdc33cd02713d8d6cc33a6dbe2d3a5bf9af1d357de0d175a403496486ff845e \
//!     --slots 0 \
//!     --runs 0 \
//!     --output-path /tmp/state-0x3cdc.ssz
//! ```
//!
//! ### Example 3.
//!
//! Do two runs over the state that was downloaded in the previous example:
//!
//! ```ignore
//! lcli skip-slots \
//!     --pre-state-path /tmp/state-0x3cdc.ssz \
//!     --slots 32 \
//!     --runs 2
//! ```
use crate::transition_blocks::load_from_ssz_with;
use clap::ArgMatches;
use clap_utils::{parse_optional, parse_required};
use environment::Environment;
use eth2::{types::StateId, BeaconNodeHttpClient, SensitiveUrl, Timeouts};
use eth2_network_config::Eth2NetworkConfig;
use log::info;
use ssz::Encode;
use state_processing::state_advance::{complete_state_advance, partial_state_advance};
use state_processing::AllCaches;
use std::fs::File;
use std::io::prelude::*;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use types::milhouse::mem::MemoryTracker;
use types::{BeaconState, EthSpec, Hash256};

const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

pub fn run<E: EthSpec>(
    env: Environment<E>,
    network_config: Eth2NetworkConfig,
    matches: &ArgMatches,
) -> Result<(), String> {
    let spec = &network_config.chain_spec::<E>()?;
    let executor = env.core_context().executor;

    let output_path: Option<PathBuf> = parse_optional(matches, "output-path")?;
    let state_path: Option<PathBuf> = parse_optional(matches, "pre-state-path")?;
    let beacon_url: Option<SensitiveUrl> = parse_optional(matches, "beacon-url")?;
    let runs: usize = parse_required(matches, "runs")?;
    let slots: u64 = parse_required(matches, "slots")?;
    let cli_state_root: Option<Hash256> = parse_optional(matches, "state-root")?;
    let partial: bool = matches.get_flag("partial-state-advance");

    info!("Using {} spec", E::spec_name());
    info!("Advancing {} slots", slots);
    info!("Doing {} runs", runs);

    let (mut state, state_root) = match (state_path, beacon_url) {
        (Some(state_path), None) => {
            info!("State path: {:?}", state_path);
            let state = load_from_ssz_with(&state_path, spec, BeaconState::from_ssz_bytes)?;
            (state, None)
        }
        (None, Some(beacon_url)) => {
            let state_id: StateId = parse_required(matches, "state-id")?;
            let client = BeaconNodeHttpClient::new(beacon_url, Timeouts::set_all(HTTP_TIMEOUT));
            let state = executor
                .handle()
                .ok_or("shutdown in progress")?
                .block_on(async move {
                    client
                        .get_debug_beacon_states::<E>(state_id)
                        .await
                        .map_err(|e| format!("Failed to download state: {:?}", e))
                })
                .map_err(|e| format!("Failed to complete task: {:?}", e))?
                .ok_or_else(|| format!("Unable to locate state at {:?}", state_id))?
                .data;
            let state_root = match state_id {
                StateId::Root(root) => Some(root),
                _ => None,
            };
            (state, state_root)
        }
        _ => return Err("must supply either --state-path or --beacon-url".into()),
    };
    let mut post_state = None;

    let initial_slot = state.slot();
    let target_slot = initial_slot + slots;

    state
        .build_all_caches(spec)
        .map_err(|e| format!("Unable to build caches: {:?}", e))?;

    let state_root = if let Some(root) = cli_state_root.or(state_root) {
        root
    } else {
        state
            .update_tree_hash_cache()
            .map_err(|e| format!("Unable to build THC: {:?}", e))?
    };

    // Intra-rebase pre state. In most cases we will be diffing off states from that cache that
    // have already been intra-rebased.
    state
        .inactivity_scores_mut()
        .unwrap()
        .intra_rebase()
        .unwrap();

    for i in 0..runs {
        let mut post_state_mut = state.clone();

        let start = Instant::now();

        let mut memory_tracker = MemoryTracker::default();

        let pre_balances = memory_tracker.track_item(state.balances());
        let pre_validators = memory_tracker.track_item(state.validators());
        let pre_inactivity_scores = memory_tracker.track_item(state.inactivity_scores().unwrap());

        if partial {
            partial_state_advance(&mut post_state_mut, Some(state_root), target_slot, spec)
                .map_err(|e| format!("Unable to perform partial advance: {:?}", e))?;
        } else {
            complete_state_advance(&mut post_state_mut, Some(state_root), target_slot, spec)
                .map_err(|e| format!("Unable to perform complete advance: {:?}", e))?;
        }

        post_state_mut.update_tree_hash_cache().unwrap();

        // Sneaky intra-rebases.
        post_state_mut.balances_mut().intra_rebase().unwrap();

        let rebase_time = std::time::Instant::now();
        post_state_mut
            .inactivity_scores_mut()
            .unwrap()
            .intra_rebase()
            .unwrap();
        info!(
            "Inactivity score intra-rebase time: {}ms",
            rebase_time.elapsed().as_millis()
        );

        let post_balances = memory_tracker.track_item(post_state_mut.balances());
        let post_validators = memory_tracker.track_item(post_state_mut.validators());
        let post_inactivity_scores =
            memory_tracker.track_item(post_state_mut.inactivity_scores().unwrap());

        info!(
            "Post-state balances size (total/diff): {}-{} B/{} B",
            pre_balances.total_size, post_balances.total_size, post_balances.differential_size
        );
        info!(
            "Post-state validators size: {}-{} B/{} B",
            pre_validators.total_size,
            post_validators.total_size,
            post_validators.differential_size
        );
        info!(
            "Post-state inactivity_scores size: {}-{} B/{} B",
            pre_inactivity_scores.total_size,
            post_inactivity_scores.total_size,
            post_inactivity_scores.differential_size
        );

        /*
        let mut consecutive_scores = post_state_mut
            .balances()
            .to_vec()
            .chunk_by(|x, y| x == y)
            .map(|scores| (scores[0], scores.len()))
            .collect::<Vec<_>>();

        consecutive_scores.sort_by_key(|(_, count)| usize::MAX - count);

        for (score, count) in consecutive_scores {
            info!("Inactivity score {}: {} entries", score, count);
        }
        */

        let duration = Instant::now().duration_since(start);
        info!("Run {}: {:?}", i, duration);
        post_state = Some(post_state_mut);
    }

    if let (Some(post_state), Some(output_path)) = (post_state, output_path) {
        let mut output_file = File::create(output_path)
            .map_err(|e| format!("Unable to create output file: {:?}", e))?;

        output_file
            .write_all(&post_state.as_ssz_bytes())
            .map_err(|e| format!("Unable to write to output file: {:?}", e))?;
    }

    Ok(())
}
