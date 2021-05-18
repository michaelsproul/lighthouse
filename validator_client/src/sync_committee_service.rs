use crate::beacon_node_fallback::{BeaconNodeFallback, RequireSynced};
use crate::{
    duties_service::{DutiesService, DutyAndProof},
    http_metrics::metrics,
    validator_store::ValidatorStore,
};
use environment::RuntimeContext;
use eth2::types::BlockId;
use futures::future::FutureExt;
use slog::{crit, debug, error, info, trace};
use slot_clock::SlotClock;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use tokio::time::{sleep, sleep_until, Duration, Instant};
use tree_hash::TreeHash;
use types::{
    AggregateSignature, ChainSpec, CommitteeIndex, EthSpec, Hash256, Slot, SyncDuty,
    SyncSelectionProof,
};

pub struct SyncCommitteeService<T: SlotClock + 'static, E: EthSpec> {
    inner: Arc<Inner<T, E>>,
}

impl<T: SlotClock + 'static, E: EthSpec> Clone for SyncCommitteeService<T, E> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: SlotClock + 'static, E: EthSpec> Deref for SyncCommitteeService<T, E> {
    type Target = Inner<T, E>;

    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}

pub struct Inner<T: SlotClock + 'static, E: EthSpec> {
    duties_service: Arc<DutiesService<T, E>>,
    validator_store: ValidatorStore<T, E>,
    slot_clock: T,
    beacon_nodes: Arc<BeaconNodeFallback<T, E>>,
    context: RuntimeContext<E>,
}

impl<T: SlotClock + 'static, E: EthSpec> SyncCommitteeService<T, E> {
    pub fn new(
        duties_service: Arc<DutiesService<T, E>>,
        validator_store: ValidatorStore<T, E>,
        slot_clock: T,
        beacon_nodes: Arc<BeaconNodeFallback<T, E>>,
        context: RuntimeContext<E>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                duties_service,
                validator_store,
                slot_clock,
                beacon_nodes,
                context,
            }),
        }
    }

    pub fn start_update_service(self, spec: &ChainSpec) -> Result<(), String> {
        let log = self.context.log().clone();
        let slot_duration = Duration::from_secs(spec.seconds_per_slot);
        let duration_to_next_slot = self
            .slot_clock
            .duration_to_next_slot()
            .ok_or("Unable to determine duration to next slot")?;

        info!(
            log,
            "Sync committee service started";
            "next_update_millis" => duration_to_next_slot.as_millis()
        );

        let executor = self.context.executor.clone();

        let interval_fut = async move {
            loop {
                if let Some(duration_to_next_slot) = self.slot_clock.duration_to_next_slot() {
                    sleep(duration_to_next_slot + slot_duration / 3).await;
                    let log = self.context.log();

                    if let Err(e) = self.spawn_contribution_tasks(slot_duration).await {
                        crit!(
                            log,
                            "Failed to spawn attestation tasks";
                            "error" => e
                        )
                    } else {
                        trace!(
                            log,
                            "Spawned attestation tasks";
                        )
                    }
                } else {
                    error!(log, "Failed to read slot clock");
                    // If we can't read the slot clock, just wait another slot.
                    sleep(slot_duration).await;
                }
            }
        };

        executor.spawn(interval_fut, "sync_committee_service");
        Ok(())
    }

    async fn spawn_contribution_tasks(&self, slot_duration: Duration) -> Result<(), String> {
        let log = self.context.log().clone();
        let slot = self.slot_clock.now().ok_or("Failed to read slot clock")?;
        let duration_to_next_slot = self
            .slot_clock
            .duration_to_next_slot()
            .ok_or("Unable to determine duration to next slot")?;

        // If a validator needs to publish a sync aggregate, they must do so at 2/3
        // through the slot. This delay triggers at this time
        let aggregate_production_instant = Instant::now()
            + duration_to_next_slot
                .checked_sub(slot_duration / 3)
                .unwrap_or_else(|| Duration::from_secs(0));

        // Group duties by sync committee subnet (for later aggregation).
        let duties_by_subnet = self
            .duties_service
            .sync_duties
            .get_duties_for_slot::<E>(slot, &self.duties_service.spec)
            .ok_or_else(|| format!("Error fetching duties for slot {}", slot))?;

        if duties_by_subnet.subnet_duties.is_empty() {
            debug!(
                log,
                "No local validators in current sync committee";
                "slot" => slot,
            );
            return Ok(());
        }

        // Fetch block root for `SyncCommitteeContribution`.
        let block_root = self
            .beacon_nodes
            .first_success(RequireSynced::Yes, |beacon_node| async move {
                beacon_node
                    .get_beacon_blocks_root(BlockId::Slot(slot))
                    .await
            })
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("No block root found for slot {}", slot))?
            .data
            .root;

        // For each subnet for this slot:
        //
        // - Create and publish a `SyncCommitteeContribution` for all required validators.
        // - Create and publish a `SignedContributionAndProof` for all aggregating validators.
        duties_by_subnet
            .subnet_duties
            .into_iter()
            .for_each(|(subnet_id, validator_duties)| {
                // Spawn a separate task for each subnet.
                // let sub_service = self.clone();
                self.inner.context.executor.spawn(
                    self.clone()
                        .publish_signatures_and_contributions(
                            slot,
                            subnet_id,
                            block_root,
                            validator_duties,
                            aggregate_production_instant,
                        )
                        .map(|_| ()),
                    "sync_committee_publish",
                );
            });

        Ok(())
    }

    async fn publish_signatures_and_contributions(
        self,
        slot: Slot,
        subnet_id: u64,
        beacon_block_root: Hash256,
        validator_duties: Vec<(SyncDuty, Option<SyncSelectionProof>)>,
        aggregate_production_instant: Instant,
    ) -> Result<(), ()> {
        let log = self.context.log().clone();

        // Publish sync committee signatures
        // NOTE: because this is happening in a subnet-specific thread, we may end up publishing
        // multiple identical signatures for the same validator, which is a slight waste of effort
        let committee_signatures = validator_duties
            .iter()
            .filter_map(|(duty, _)| {
                self.validator_store
                    .produce_sync_committee_signature(
                        slot,
                        beacon_block_root,
                        duty.validator_index,
                        &duty.pubkey,
                    )
                    .or_else(|| {
                        crit!(
                            log,
                            "Failed to sign sync committee signature";
                            "validator_index" => duty.validator_index,
                            "slot" => slot,
                        );
                        None
                    })
            })
            .collect::<Vec<_>>();

        for (duty, proof) in &validator_duties {
            info!(
                log,
                "Publishing sync contribution";
                "validator_index" => duty.validator_index,
                "slot" => slot,
                "is_aggregator" => proof.is_some(),
            );
        }

        Ok(())
    }

    /*
    async fn produce_and_publish_contributions(
        &self,
        slot: Slot,
        subnet_id: u64,
        beacon_block_root: Hash256,
        validator_duties: &[(SyncDuty, Option<SyncSelectionProof>)],
    ) -> Result<Option<AttestationData>, String> {
        let log = self.context.log();

        if validator_duties.is_empty() {
            return Ok(None);
        }

        let current_epoch = self
            .slot_clock
            .now()
            .ok_or("Unable to determine current slot from clock")?
            .epoch(E::slots_per_epoch());

        let attestation_data = self
            .beacon_nodes
            .first_success(RequireSynced::No, |beacon_node| async move {
                beacon_node
                    .get_validator_attestation_data(slot, committee_index)
                    .await
                    .map_err(|e| format!("Failed to produce attestation data: {:?}", e))
                    .map(|result| result.data)
            })
            .await
            .map_err(|e| e.to_string())?;

        let mut attestations = Vec::with_capacity(validator_duties.len());

        for duty_and_proof in validator_duties {
            let duty = &duty_and_proof.duty;

            // Ensure that the attestation matches the duties.
            #[allow(clippy::suspicious_operation_groupings)]
            if duty.slot != attestation_data.slot || duty.committee_index != attestation_data.index
            {
                crit!(
                    log,
                    "Inconsistent validator duties during signing";
                    "validator" => ?duty.pubkey,
                    "duty_slot" => duty.slot,
                    "attestation_slot" => attestation_data.slot,
                    "duty_index" => duty.committee_index,
                    "attestation_index" => attestation_data.index,
                );
                continue;
            }

            let mut attestation = Attestation {
                aggregation_bits: BitList::with_capacity(duty.committee_length as usize).unwrap(),
                data: attestation_data.clone(),
                signature: AggregateSignature::infinity(),
            };

            if self
                .validator_store
                .sign_attestation(
                    &duty.pubkey,
                    duty.validator_committee_index as usize,
                    &mut attestation,
                    current_epoch,
                )
                .is_some()
            {
                attestations.push(attestation);
            } else {
                crit!(
                    log,
                    "Failed to sign attestation";
                    "committee_index" => committee_index,
                    "slot" => slot.as_u64(),
                );
                continue;
            }
        }

        let attestations_slice = attestations.as_slice();
        match self
            .beacon_nodes
            .first_success(RequireSynced::No, |beacon_node| async move {
                beacon_node
                    .post_beacon_pool_attestations(attestations_slice)
                    .await
            })
            .await
        {
            Ok(()) => info!(
                log,
                "Successfully published attestations";
                "count" => attestations.len(),
                "head_block" => ?attestation_data.beacon_block_root,
                "committee_index" => attestation_data.index,
                "slot" => attestation_data.slot.as_u64(),
                "type" => "unaggregated",
            ),
            Err(e) => error!(
                log,
                "Unable to publish attestations";
                "error" => %e,
                "committee_index" => attestation_data.index,
                "slot" => slot.as_u64(),
                "type" => "unaggregated",
            ),
        }

        Ok(Some(attestation_data))
    }
    */
}
