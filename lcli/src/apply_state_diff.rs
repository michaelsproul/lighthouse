use clap::ArgMatches;
use environment::Environment;
use eth2_network_config::Eth2NetworkConfig;
use ssz::{Decode, Encode};
use std::fs::File;
use std::io::Read;
use store::{hdiff::HDiff, hdiff::HDiffBuffer, StoreConfig};
use types::{BeaconState, EthSpec};

pub fn run<E: EthSpec>(
    env: Environment<E>,
    _network_config: Eth2NetworkConfig,
    matches: &ArgMatches,
) -> Result<(), String> {
    let base_state_path = matches
        .get_one::<String>("base-state-path")
        .ok_or("base-state-path is required")?;

    let diff_path = matches
        .get_one::<String>("diff-path")
        .ok_or("diff-path is required")?;

    let output_path = matches
        .get_one::<String>("output-path")
        .ok_or("output-path is required")?;

    let runs = matches
        .get_one::<String>("runs")
        .map(|s| s.parse::<usize>())
        .transpose()
        .map_err(|e| format!("Invalid runs value: {}", e))?
        .unwrap_or(1);

    println!("Loading base state from: {}", base_state_path);
    let base_state = load_state_from_file::<E>(base_state_path, &env.eth2_config.spec)?;

    println!("Loading state diff from: {}", diff_path);
    let diff = load_diff_from_file(diff_path)?;

    let mut total_duration = std::time::Duration::ZERO;

    for run in 0..runs {
        let start = std::time::Instant::now();
        
        // Convert base state to HDiffBuffer
        let mut buffer = HDiffBuffer::from_state(base_state.clone());
        
        // Apply the diff
        let store_config = StoreConfig::default();
        diff.apply(&mut buffer, &store_config)
            .map_err(|e| format!("Failed to apply diff: {:?}", e))?;

        // Convert buffer back to state
        let result_state = buffer
            .as_state::<E>(&env.eth2_config.spec)
            .map_err(|e| format!("Failed to convert buffer to state: {:?}", e))?;

        let duration = start.elapsed();
        total_duration += duration;

        if runs == 1 {
            println!("Applied state diff in {:?}", duration);
        } else {
            println!("Run {}: Applied state diff in {:?}", run + 1, duration);
        }

        // Only write output on the last run
        if run == runs - 1 {
            println!("Writing result state to: {}", output_path);
            write_state_to_file(&result_state, output_path)?;
        }
    }

    if runs > 1 {
        println!(
            "Average time over {} runs: {:?}",
            runs,
            total_duration / runs as u32
        );
    }

    println!("State diff applied successfully!");
    Ok(())
}

fn load_state_from_file<E: EthSpec>(
    path: &str,
    spec: &types::ChainSpec,
) -> Result<BeaconState<E>, String> {
    let mut file = File::open(path).map_err(|e| format!("Failed to open state file: {}", e))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|e| format!("Failed to read state file: {}", e))?;

    BeaconState::from_ssz_bytes(&bytes, spec)
        .map_err(|e| format!("Failed to decode state: {:?}", e))
}

fn load_diff_from_file(path: &str) -> Result<HDiff, String> {
    let mut file = File::open(path).map_err(|e| format!("Failed to open diff file: {}", e))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|e| format!("Failed to read diff file: {}", e))?;

    HDiff::from_ssz_bytes(&bytes).map_err(|e| format!("Failed to decode diff: {:?}", e))
}

fn write_state_to_file<E: EthSpec>(state: &BeaconState<E>, path: &str) -> Result<(), String> {
    let bytes = state.as_ssz_bytes();
    std::fs::write(path, bytes).map_err(|e| format!("Failed to write state file: {}", e))
}