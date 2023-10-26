use beacon_chain::{
    test_utils::EphemeralHarnessType, validator_pubkey_cache::ValidatorPubkeyCache,
};
use clap::ArgMatches;
use clap_utils::{parse_optional, parse_required};
use environment::{null_logger, Environment};
use eth2::{
    types::{BlockId, StateId},
    BeaconNodeHttpClient, SensitiveUrl, Timeouts,
};
use eth2_network_config::Eth2NetworkConfig;
use ssz::{Decode, Encode};
use state_processing::state_advance::complete_state_advance;
use state_processing::{
    per_block_processing::process_operations::altair_deneb::process_attestations, AllCaches,
    BlockSignatureStrategy, ConsensusContext, StateProcessingStrategy, VerifyBlockRoot,
    VerifySignatures,
};
use std::borrow::Cow;
use std::fs::File;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use store::HotColdDB;
use types::{Attestation, BeaconState, ChainSpec, EthSpec, Hash256, SignedBeaconBlock};

pub fn run<T: EthSpec>(
    env: Environment<T>,
    network_config: Eth2NetworkConfig,
    matches: &ArgMatches,
) -> Result<(), String> {
    let spec = &network_config.chain_spec::<T>()?;
    let executor = env.core_context().executor;

    /*
     * Parse (most) CLI arguments.
     */

    let pre_state_path: PathBuf = parse_required(matches, "pre-state-path")?;
    let attestation_path: PathBuf = parse_required(matches, "attestation-path")?;
    let post_state_output_path: Option<PathBuf> =
        parse_optional(matches, "post-state-output-path")?;

    info!("Using {} spec", T::spec_name());
    info!("Block path: {:?}", attestation_path);
    info!("Pre-state path: {:?}", pre_state_path);
    let mut pre_state: BeaconState<T> =
        load_from_ssz_with(&pre_state_path, spec, BeaconState::from_ssz_bytes)?;
    let attestation = load_from_ssz_with(&attestation_path, spec, |bytes, _| {
        Attestation::from_ssz_bytes(bytes)
    })?;

    pre_state
        .build_all_caches(spec)
        .map_err(|e| format!("Unable to build caches: {:?}", e))?;

    let mut post_state = pre_state.clone();

    let mut ctxt = ConsensusContext::new(pre_state.slot());
    process_attestations(
        &mut post_state,
        &[attestation],
        VerifySignatures::False,
        &mut ctxt,
        spec,
    )
    .unwrap();
    /*
     * Write artifacts to disk, if required.
     */

    if let Some(path) = post_state_output_path {
        let mut output_file =
            File::create(path).map_err(|e| format!("Unable to create output file: {:?}", e))?;

        output_file
            .write_all(&post_state.as_ssz_bytes())
            .map_err(|e| format!("Unable to write to output file: {:?}", e))?;
    }

    drop(pre_state);

    Ok(())
}

pub fn load_from_ssz_with<T>(
    path: &Path,
    spec: &ChainSpec,
    decoder: impl FnOnce(&[u8], &ChainSpec) -> Result<T, ssz::DecodeError>,
) -> Result<T, String> {
    let mut file =
        File::open(path).map_err(|e| format!("Unable to open file {:?}: {:?}", path, e))?;
    let mut bytes = vec![];
    file.read_to_end(&mut bytes)
        .map_err(|e| format!("Unable to read from file {:?}: {:?}", path, e))?;
    let t = Instant::now();
    let result = decoder(&bytes, spec).map_err(|e| format!("Ssz decode failed: {:?}", e));
    debug!("SSZ decoding {}: {:?}", path.display(), t.elapsed());
    result
}
