use beacon_chain::{BeaconChain, BeaconChainTypes};
use eth2_libp2p::NetworkGlobals;
use slog::error;
use slot_clock::SlotClock;
use state_processing::common::get_indexed_attestation;
use std::sync::Arc;
use std::time::Duration;
use types::Signature;

/// Spawns a service which produces a block locally every slot.
pub fn spawn_block_dreamer<T: BeaconChainTypes>(
    executor: task_executor::TaskExecutor,
    beacon_chain: Arc<BeaconChain<T>>,
    network: Arc<NetworkGlobals<T::EthSpec>>,
    seconds_per_slot: u64,
) -> Result<(), String> {
    let slot_duration = Duration::from_secs(seconds_per_slot);
    let duration_to_next_slot = beacon_chain
        .slot_clock
        .duration_to_next_slot()
        .ok_or("slot_notifier unable to determine time to next slot")?;

    // Toggle this bool to switch between competing (propose at start of slot) and comparing
    // (propose half way through slot, stealing attestations).
    let compete = true;
    let offset = if compete {
        Duration::from_millis(0)
    } else {
        slot_duration / 2
    };
    let start_instant = tokio::time::Instant::now() + duration_to_next_slot + offset;

    // Run this each slot.
    let interval_duration = slot_duration;

    let log = executor.log().clone();
    let mut interval = tokio::time::interval_at(start_instant, interval_duration);

    executor.spawn(
        async move {
            loop {
                interval.tick().await;

                let slot = match beacon_chain.slot() {
                    Ok(slot) => slot,
                    Err(e) => {
                        error!(
                            log,
                            "Error reading slot clock";
                            "error" => ?e,
                        );
                        continue;
                    }
                };
                if network.sync_state().is_synced() {
                    // Hold lock to prevent concurrent block application (a bit naughty innit)
                    let mut observed_block_attesters =
                        beacon_chain.observed_block_attesters.write();
                    match beacon_chain.produce_block(Signature::empty(), slot, None) {
                        Ok((block, state)) => {
                            // Observe block attesters
                            for attestation in block.body().attestations() {
                                let committee = state
                                    .get_beacon_committee(
                                        attestation.data.slot,
                                        attestation.data.index,
                                    )
                                    .unwrap();
                                let indexed_attestation =
                                    get_indexed_attestation(committee.committee, attestation)
                                        .unwrap();

                                for &validator_index in &indexed_attestation.attesting_indices {
                                    observed_block_attesters
                                        .observe_validator(
                                            attestation.data.target.epoch,
                                            validator_index as usize,
                                        )
                                        .unwrap();
                                }
                            }
                        }
                        Err(e) => {
                            error!(
                                log,
                                "Error hallucinating dream block";
                                "error" => ?e,
                            );
                        }
                    }
                }
            }
        },
        "block_dreamer",
    );

    Ok(())
}
