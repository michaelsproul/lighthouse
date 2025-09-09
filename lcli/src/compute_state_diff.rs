use clap::ArgMatches;
use environment::Environment;
use eth2_network_config::Eth2NetworkConfig;
use ssz::Encode;
use std::fs::File;
use std::io::Read;
use store::{StoreConfig, hdiff::HDiff, hdiff::HDiffBuffer};
use types::{BeaconState, EthSpec};

pub fn run<E: EthSpec>(
    env: Environment<E>,
    network_config: Eth2NetworkConfig,
    matches: &ArgMatches,
) -> Result<(), String> {
    let spec = &network_config.chain_spec::<E>()?;
    let source_state_path = matches
        .get_one::<String>("source-state-path")
        .ok_or("source-state-path is required")?;

    let target_state_path = matches
        .get_one::<String>("target-state-path")
        .ok_or("target-state-path is required")?;

    let output_path = matches
        .get_one::<String>("output-path")
        .ok_or("output-path is required")?;

    let runs = matches
        .get_one::<String>("runs")
        .map(|s| s.parse::<usize>())
        .transpose()
        .map_err(|e| format!("Invalid runs value: {}", e))?
        .unwrap_or(1);

    println!("Loading source state from: {}", source_state_path);
    let source_state = load_state_from_file::<E>(source_state_path, spec)?;

    println!("Loading target state from: {}", target_state_path);
    let target_state = load_state_from_file::<E>(target_state_path, spec)?;

    let mut total_duration = std::time::Duration::ZERO;
    let mut diff = None;

    for run in 0..runs {
        let start = std::time::Instant::now();

        // Convert states to HDiffBuffers
        let source_buffer = HDiffBuffer::from_state(source_state.clone());
        let target_buffer = HDiffBuffer::from_state(target_state.clone());

        // Compute the diff
        let store_config = StoreConfig::default();
        let computed_diff = HDiff::compute(&source_buffer, &target_buffer, &store_config)
            .map_err(|e| format!("Failed to compute diff: {:?}", e))?;

        let duration = start.elapsed();
        total_duration += duration;

        if runs == 1 {
            println!("Computed state diff in {:?}", duration);
        } else {
            println!("Run {}: Computed state diff in {:?}", run + 1, duration);
        }

        // Only write output on the last run
        if run == runs - 1 {
            diff = Some(computed_diff);
        }
    }

    if runs > 1 {
        println!(
            "Average time over {} runs: {:?}",
            runs,
            total_duration / runs as u32
        );
    }

    if let Some(diff) = diff {
        println!("Writing diff to: {}", output_path);
        println!("Diff size: {} bytes", diff.size());
        println!("Diff component sizes: {:?}", diff.sizes());
        write_diff_to_file(&diff, output_path)?;
    }

    println!("State diff computed successfully!");
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

fn write_diff_to_file(diff: &HDiff, path: &str) -> Result<(), String> {
    let bytes = diff.as_ssz_bytes();
    std::fs::write(path, bytes).map_err(|e| format!("Failed to write diff file: {}", e))
}
