use crate::transition_blocks::load_from_ssz_with;
use clap::ArgMatches;
use clap_utils::{parse_optional, parse_required};
use environment::Environment;
use eth2::{types::BlockId, BeaconNodeHttpClient, SensitiveUrl, Timeouts};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use store::hdiff::{HDiff, HDiffBuffer};
use types::{BeaconState, EthSpec, FullPayload, SignedBeaconBlock};

pub fn run<T: EthSpec>(env: Environment<T>, matches: &ArgMatches) -> Result<(), String> {
    let state1_path: PathBuf = parse_required(matches, "state1")?;
    let state2_path: PathBuf = parse_required(matches, "state2")?;
    let spec = &T::default_spec();

    let state1 = load_from_ssz_with(&state1_path, spec, BeaconState::<T>::from_ssz_bytes)?;
    let state2 = load_from_ssz_with(&state2_path, spec, BeaconState::<T>::from_ssz_bytes)?;

    let buffer1 = HDiffBuffer::from_state(state1);
    let buffer2 = HDiffBuffer::from_state(state2);

    let t = std::time::Instant::now();
    let diff = HDiff::compute(&buffer1, &buffer2).unwrap();
    let elapsed = t.elapsed();

    println!("Diff size");
    println!("- state: {} bytes", diff.state_diff_len());
    println!("- balances: {} bytes", diff.balances_diff_len());
    println!("Computation time: {}ms", elapsed.as_millis());

    Ok(())
}
