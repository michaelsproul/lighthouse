use crate::per_epoch_processing::single_pass::{process_epoch_single_pass, SinglePassConfig};
use crate::EpochProcessingError;
use types::beacon_state::BeaconState;
use types::chain_spec::ChainSpec;
use types::eth_spec::EthSpec;

/// Slow version of `process_inactivity_updates`.
///
/// Should only be used for testing.
pub fn process_inactivity_updates_slow<T: EthSpec>(
    state: &mut BeaconState<T>,
    spec: &ChainSpec,
) -> Result<(), EpochProcessingError> {
    process_epoch_single_pass(
        state,
        spec,
        SinglePassConfig {
            inactivity_updates: true,
            ..SinglePassConfig::disable_all()
        },
    )
}
