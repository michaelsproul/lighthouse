use crate::duties_service::{DutiesService, Error};
use itertools::Itertools;
use parking_lot::{MappedRwLockReadGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use slog::{crit, debug, info, warn};
use slot_clock::SlotClock;
use std::collections::HashMap;
use std::sync::Arc;
use types::{
    ChainSpec, Epoch, EthSpec, PublicKeyBytes, Slot, SyncDuty, SyncSelectionProof, SyncSubnetId,
};

/// Number of epochs in advance to compute selection proofs.
pub const AGGREGATION_PRE_COMPUTE_EPOCHS: u64 = 2;

// FIXME(sproul): need to prune this at some point
pub struct SyncDutiesMap {
    /// Map from sync committee period to duties for members of that sync committee.
    committees: RwLock<HashMap<u64, CommitteeDuties>>,
}

#[derive(Default)]
pub struct CommitteeDuties {
    /// Map from validator index to validator duties.
    ///
    /// A `None` value indicates that the validator index is known *not* to be a member of the sync
    /// committee, while a `Some` indicates a known member. An absent value indicates that the
    /// validator index was not part of the set of local validators when the duties were fetched.
    /// This allows us to track changes to the set of local validators.
    validators: RwLock<HashMap<u64, Option<ValidatorDuties>>>,
}

pub struct ValidatorDuties {
    /// The sync duty: including validator sync committee indices & pubkey.
    duty: SyncDuty,
    /// The aggregator duties: cached selection proofs for upcoming epochs.
    aggregation_duties: AggregatorDuties,
}

/// Aggregator duties for a single validator.
pub struct AggregatorDuties {
    /// The epoch up to which aggregation proofs have already been computed (inclusive).
    pre_compute_epoch: RwLock<Option<Epoch>>,
    /// Map from slot & subnet ID to proof that this validator is an aggregator.
    ///
    /// The slot is the slot at which the signed contribution and proof should be broadcast,
    /// which is 1 less than the slot for which the `duty` was computed.
    proofs: RwLock<HashMap<(Slot, SyncSubnetId), SyncSelectionProof>>,
}

/// Duties for a single slot.
pub struct SlotDuties {
    /// List of duties for all sync committee members at this slot.
    ///
    /// Note: this is intentionally NOT split by subnet so that we only sign
    /// one `SyncCommitteeMessage` per validator (recall a validator may be part of multiple
    /// subnets).
    pub duties: Vec<SyncDuty>,
    /// Map from subnet ID to validator index and selection proof of each aggregator.
    pub aggregators: HashMap<SyncSubnetId, Vec<(u64, PublicKeyBytes, SyncSelectionProof)>>,
}

impl Default for SyncDutiesMap {
    fn default() -> Self {
        Self {
            committees: RwLock::new(HashMap::new()),
        }
    }
}

impl SyncDutiesMap {
    pub fn all_duties_known(&self, committee_period: u64, validator_indices: &[u64]) -> bool {
        self.committees
            .read()
            .get(&committee_period)
            .map_or(false, |committee_duties| {
                let validator_duties = committee_duties.validators.read();
                validator_indices
                    .iter()
                    .all(|index| validator_duties.contains_key(index))
            })
    }

    /// Get the sync duties of all validators who have not yet had aggregation proofs pre-computed
    /// given the lookahead from the `current_epoch`.
    pub fn prepare_for_aggregator_pre_compute(
        &self,
        committee_period: u64,
        current_epoch: Epoch,
        spec: &ChainSpec,
    ) -> (Epoch, Vec<(Epoch, SyncDuty)>) {
        let default_start_epoch =
            std::cmp::max(current_epoch, first_epoch_of_period(committee_period, spec));
        let pre_compute_epoch = std::cmp::min(
            current_epoch + AGGREGATION_PRE_COMPUTE_EPOCHS,
            last_epoch_of_period(committee_period, spec),
        );

        // FIXME(sproul): think about deadlocks from lock ordering
        let pre_compute_duties = self.committees.read().get(&committee_period).map_or_else(
            Vec::new,
            |committee_duties| {
                let validator_duties = committee_duties.validators.read();
                validator_duties
                    .values()
                    .filter_map(|maybe_duty| {
                        let duty = maybe_duty.as_ref()?;
                        let old_pre_compute_epoch = duty
                            .aggregation_duties
                            .pre_compute_epoch
                            .write()
                            .replace(pre_compute_epoch);

                        match old_pre_compute_epoch {
                            // No proofs pre-computed previously, compute all from the start of
                            // the period or the current epoch (whichever is later).
                            None => Some((default_start_epoch, duty.duty.clone())),
                            // Proofs computed up to `prev`, start from the subsequent epoch.
                            Some(prev) if prev < pre_compute_epoch => {
                                Some((prev + 1, duty.duty.clone()))
                            }
                            // Proofs already known, no need to compute.
                            _ => None,
                        }
                    })
                    .collect()
            },
        );
        (pre_compute_epoch, pre_compute_duties)
    }

    pub fn get_or_create_committee_duties<'a, 'b>(
        &'a self,
        committee_period: u64,
        validator_indices: impl IntoIterator<Item = &'b u64>,
    ) -> MappedRwLockReadGuard<'a, CommitteeDuties> {
        let mut committees_writer = self.committees.write();

        committees_writer
            .entry(committee_period)
            .or_insert_with(CommitteeDuties::default)
            .init(validator_indices);

        // Return shared reference
        RwLockReadGuard::map(
            RwLockWriteGuard::downgrade(committees_writer),
            |committees_reader| &committees_reader[&committee_period],
        )
    }

    pub fn get_duties_for_slot<E: EthSpec>(
        &self,
        wall_clock_slot: Slot,
        spec: &ChainSpec,
    ) -> Option<SlotDuties> {
        // Sync duties lag their assigned slot by 1
        let duty_slot = wall_clock_slot + 1;

        let sync_committee_period = duty_slot
            .epoch(E::slots_per_epoch())
            .sync_committee_period(spec)
            .ok()?;

        let committees_reader = self.committees.read();
        let committee_duties = committees_reader.get(&sync_committee_period)?;

        let mut duties = vec![];
        let mut aggregators = HashMap::new();

        committee_duties
            .validators
            .read()
            .values()
            // Filter out non-members & failed subnet IDs.
            .filter_map(|opt_duties| {
                let duty = opt_duties.as_ref()?;
                let subnet_ids = duty.duty.subnet_ids::<E>().ok()?;
                Some((duty, subnet_ids))
            })
            // Add duties for members to the vec of all duties, and aggregators to the
            // aggregators map.
            .for_each(|(validator_duty, subnet_ids)| {
                duties.push(validator_duty.duty.clone());

                let proofs = validator_duty.aggregation_duties.proofs.read();

                for subnet_id in subnet_ids {
                    if let Some(proof) = proofs.get(&(wall_clock_slot, subnet_id)) {
                        aggregators.entry(subnet_id).or_insert_with(Vec::new).push((
                            validator_duty.duty.validator_index,
                            validator_duty.duty.pubkey,
                            proof.clone(),
                        ));
                    }
                }
            });

        Some(SlotDuties {
            duties,
            aggregators,
        })
    }
}

impl CommitteeDuties {
    fn init<'b>(&mut self, validator_indices: impl IntoIterator<Item = &'b u64>) {
        validator_indices.into_iter().for_each(|validator_index| {
            self.validators
                .get_mut()
                .entry(*validator_index)
                .or_insert(None);
        })
    }
}

impl ValidatorDuties {
    fn new(duty: SyncDuty) -> Self {
        Self {
            duty,
            aggregation_duties: AggregatorDuties {
                pre_compute_epoch: RwLock::new(None),
                proofs: RwLock::new(HashMap::new()),
            },
        }
    }
}

/// Number of epochs to wait from the start of the period before actually fetching duties.
fn epoch_offset(spec: &ChainSpec) -> u64 {
    spec.epochs_per_sync_committee_period.as_u64() / 2
}

fn first_epoch_of_period(sync_committee_period: u64, spec: &ChainSpec) -> Epoch {
    spec.epochs_per_sync_committee_period * sync_committee_period
}

fn last_epoch_of_period(sync_committee_period: u64, spec: &ChainSpec) -> Epoch {
    first_epoch_of_period(sync_committee_period + 1, spec) - 1
}

pub async fn poll_sync_committee_duties<T: SlotClock + 'static, E: EthSpec>(
    duties_service: &Arc<DutiesService<T, E>>,
) -> Result<(), Error> {
    let sync_duties = &duties_service.sync_duties;
    let spec = &duties_service.spec;
    let current_epoch = duties_service
        .slot_clock
        .now()
        .ok_or(Error::UnableToReadSlotClock)?
        .epoch(E::slots_per_epoch());

    // If the Altair fork is yet to be activated, do not attempt to poll for duties.
    if spec
        .altair_fork_epoch
        .map_or(true, |altair_epoch| current_epoch < altair_epoch)
    {
        return Ok(());
    }

    let current_sync_committee_period = current_epoch.sync_committee_period(spec)?;
    let next_sync_committee_period = current_sync_committee_period + 1;

    let local_pubkeys = duties_service.local_pubkeys();
    let local_indices = duties_service.local_indices(&local_pubkeys);

    // If duties aren't known for the current period, poll for them.
    if !sync_duties.all_duties_known(current_sync_committee_period, &local_indices) {
        poll_sync_committee_duties_for_period(
            duties_service,
            &local_indices,
            current_sync_committee_period,
        )
        .await?;
    }

    // Pre-compute aggregator selection proofs for the current period.
    let (current_pre_compute_epoch, new_pre_compute_duties) = sync_duties
        .prepare_for_aggregator_pre_compute(current_sync_committee_period, current_epoch, spec);

    if !new_pre_compute_duties.is_empty() {
        let sub_duties_service = duties_service.clone();
        duties_service.context.executor.spawn_blocking(
            move || {
                fill_in_aggregation_proofs(
                    sub_duties_service,
                    &new_pre_compute_duties,
                    current_sync_committee_period,
                    current_epoch,
                    current_pre_compute_epoch,
                )
            },
            "duties_service_sync_selection_proofs",
        );
    }

    // If we're past the point in the current period where we should determine duties for the next
    // period and they are not yet known, then poll.
    if current_epoch.as_u64() % spec.epochs_per_sync_committee_period.as_u64() >= epoch_offset(spec)
        && !sync_duties.all_duties_known(next_sync_committee_period, &local_indices)
    {
        poll_sync_committee_duties_for_period(
            duties_service,
            &local_indices,
            next_sync_committee_period,
        )
        .await?;
    }

    // Pre-compute aggregator selection proofs for the next period.
    if (current_epoch + AGGREGATION_PRE_COMPUTE_EPOCHS).sync_committee_period(spec)?
        == next_sync_committee_period
    {
        let (pre_compute_epoch, new_pre_compute_duties) = sync_duties
            .prepare_for_aggregator_pre_compute(next_sync_committee_period, current_epoch, spec);

        if !new_pre_compute_duties.is_empty() {
            let sub_duties_service = duties_service.clone();
            duties_service.context.executor.spawn_blocking(
                move || {
                    fill_in_aggregation_proofs(
                        sub_duties_service,
                        &new_pre_compute_duties,
                        next_sync_committee_period,
                        current_epoch,
                        pre_compute_epoch,
                    )
                },
                "duties_service_sync_selection_proofs",
            );
        }
    }

    Ok(())
}

pub async fn poll_sync_committee_duties_for_period<T: SlotClock + 'static, E: EthSpec>(
    duties_service: &Arc<DutiesService<T, E>>,
    local_indices: &[u64],
    sync_committee_period: u64,
) -> Result<(), Error> {
    let spec = &duties_service.spec;
    let log = duties_service.context.log();

    info!(
        log,
        "Fetching sync committee duties";
        "sync_committee_period" => sync_committee_period,
        "num_validators" => local_indices.len(),
    );

    let period_start_epoch = spec.epochs_per_sync_committee_period * sync_committee_period;

    let duties_response = duties_service
        .beacon_nodes
        .first_success(duties_service.require_synced, |beacon_node| async move {
            beacon_node
                .post_validator_duties_sync(period_start_epoch, local_indices)
                .await
        })
        .await;

    let duties = match duties_response {
        Ok(res) => res.data,
        Err(e) => {
            warn!(
                log,
                "Failed to download sync committee duties";
                "sync_committee_period" => sync_committee_period,
                "error" => %e,
            );
            return Ok(());
        }
    };

    info!(log, "Fetched duties from BN"; "count" => duties.len());

    // Add duties to map.
    let committee_duties = duties_service
        .sync_duties
        .get_or_create_committee_duties(sync_committee_period, local_indices);

    let mut validator_writer = committee_duties.validators.write();
    for duty in duties {
        let validator_duties = validator_writer
            .get_mut(&duty.validator_index)
            .ok_or(Error::SyncDutiesNotFound(duty.validator_index))?;

        let updated = validator_duties.as_ref().map_or(true, |existing_duties| {
            let updated_due_to_reorg = existing_duties.duty.validator_sync_committee_indices
                != duty.validator_sync_committee_indices;
            if updated_due_to_reorg {
                warn!(
                    log,
                    "Sync committee duties changed";
                    "message" => "this could be due to a really long re-org, or a bug"
                );
            }
            updated_due_to_reorg
        });

        if updated {
            info!(
                log,
                "Validator in sync committee";
                "validator_index" => duty.validator_index,
                "sync_committee_period" => sync_committee_period,
            );

            *validator_duties = Some(ValidatorDuties::new(duty));
        }
    }

    Ok(())
}

pub fn fill_in_aggregation_proofs<T: SlotClock + 'static, E: EthSpec>(
    duties_service: Arc<DutiesService<T, E>>,
    pre_compute_duties: &[(Epoch, SyncDuty)],
    sync_committee_period: u64,
    current_epoch: Epoch,
    pre_compute_epoch: Epoch,
) {
    let log = duties_service.context.log();
    info!(log, "Filling in aggregation proofs";
        "period" => sync_committee_period,
        "current_epoch" => current_epoch,
        "pre_compute_epoch" => pre_compute_epoch
    );

    // Generate selection proofs for each validator at each slot, one epoch at a time.
    for epoch in (current_epoch.as_u64()..=pre_compute_epoch.as_u64()).map(Epoch::new) {
        // Generate proofs.
        let validator_proofs: Vec<(u64, Vec<_>)> = pre_compute_duties
            .iter()
            .filter_map(|(validator_start_epoch, duty)| {
                // Proofs are already known at this epoch for this validator.
                if epoch < *validator_start_epoch {
                    return None;
                }

                let subnet_ids = duty
                    .subnet_ids::<E>()
                    .map_err(|e| {
                        crit!(
                            log,
                            "Arithmetic error computing subnet IDs";
                            "error" => ?e,
                        );
                    })
                    .ok()?;

                let proofs = epoch
                    .slot_iter(E::slots_per_epoch())
                    .cartesian_product(&subnet_ids)
                    .filter_map(|(duty_slot, &subnet_id)| {
                        // Construct proof for prior slot.
                        let slot = duty_slot - 1;

                        let proof = duties_service
                            .validator_store
                            .produce_sync_selection_proof(&duty.pubkey, slot, subnet_id)
                            .or_else(|| {
                                warn!(
                                    log,
                                    "Pubkey missing when signing selection proof";
                                    "pubkey" => ?duty.pubkey,
                                    "slot" => slot,
                                );
                                None
                            })?;

                        let is_aggregator = proof
                            .is_aggregator::<E>()
                            .map_err(|e| {
                                warn!(
                                    log,
                                    "Error determining is_aggregator";
                                    "pubkey" => ?duty.pubkey,
                                    "slot" => slot,
                                    "error" => ?e,
                                );
                            })
                            .ok()?;

                        if is_aggregator {
                            info!(
                                log,
                                "Validator is sync aggregator";
                                "validator_index" => duty.validator_index,
                                "slot" => slot,
                                "subnet_id" => %subnet_id,
                            );
                            Some(((slot, subnet_id), proof))
                        } else {
                            None
                        }
                    })
                    .collect();

                Some((duty.validator_index, proofs))
            })
            .collect();

        // Add to global storage (we add regularly in case the proofs are required).
        // FIXME(sproul): this needn't obtain a write lock
        let committee_duties = duties_service.sync_duties.get_or_create_committee_duties(
            sync_committee_period,
            validator_proofs.iter().map(|(index, _)| index),
        );
        let validators_reader = committee_duties.validators.read();

        let num_validators_updated = validator_proofs.len();

        for (validator_index, proofs) in validator_proofs {
            if let Some(Some(duty)) = validators_reader.get(&validator_index) {
                duty.aggregation_duties.proofs.write().extend(proofs);
            } else {
                debug!(
                    log,
                    "Missing sync duty to update";
                    "validator_index" => validator_index,
                );
            }
        }

        info!(
            log,
            "Finished pre-computing sync selection proofs";
            "epoch" => epoch,
            "updated_validators" => num_validators_updated,
        );
    }
}
