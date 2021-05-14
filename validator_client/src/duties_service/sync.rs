use crate::duties_service::{DutiesService, Error};
use parking_lot::{MappedRwLockReadGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use slog::{info, warn};
use slot_clock::SlotClock;
use std::collections::HashMap;
use types::{ChainSpec, EthSpec, Signature, Slot};

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
    /// The sync duty
    validator_sync_committee_indices: Vec<u64>,
    /// Map from slot to proof that this validator is an aggregator at that slot.
    aggregation_proofs: RwLock<HashMap<Slot, Signature>>,
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

    pub fn get_or_create_committee_duties<'a>(
        &'a self,
        committee_period: u64,
        validator_indices: &[u64],
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
}

impl CommitteeDuties {
    fn init(&mut self, validator_indices: &[u64]) {
        validator_indices.iter().for_each(|validator_index| {
            self.validators
                .get_mut()
                .entry(*validator_index)
                .or_insert(None);
        })
    }
}

impl ValidatorDuties {
    fn new(validator_sync_committee_indices: Vec<u64>) -> Self {
        Self {
            validator_sync_committee_indices,
            aggregation_proofs: RwLock::new(HashMap::new()),
        }
    }
}

/// Number of epochs to wait from the start of the period before actually fetching duties.
fn epoch_offset(spec: &ChainSpec) -> u64 {
    spec.epochs_per_sync_committee_period.as_u64() / 2
}

pub async fn poll_sync_committee_duties<T: SlotClock + 'static, E: EthSpec>(
    duties_service: &DutiesService<T, E>,
) -> Result<(), Error> {
    let sync_duties = &duties_service.sync_duties;
    let spec = &duties_service.spec;
    let current_epoch = duties_service
        .slot_clock
        .now()
        .ok_or(Error::UnableToReadSlotClock)?
        .epoch(E::slots_per_epoch());

    let current_sync_committee_period = current_epoch.sync_committee_period(spec)?;
    let next_sync_committee_period = current_sync_committee_period + 1;

    let local_pubkeys = duties_service.local_pubkeys();
    let local_indices = duties_service.local_indices(&local_pubkeys);

    // If duties aren't known for the current period, poll for them
    if !sync_duties.all_duties_known(current_sync_committee_period, &local_indices) {
        poll_sync_committee_duties_for_period(
            duties_service,
            &local_indices,
            current_sync_committee_period,
        )
        .await?;
    }

    // If we're past the point in the current period where we should determine duties for the next epoch
    // and they are not yet known, then poll.
    if current_sync_committee_period % spec.epochs_per_sync_committee_period.as_u64()
        >= epoch_offset(spec)
        && !sync_duties.all_duties_known(next_sync_committee_period, &local_indices)
    {
        poll_sync_committee_duties_for_period(
            duties_service,
            &local_indices,
            next_sync_committee_period,
        )
        .await?;
    }

    Ok(())
}

pub async fn poll_sync_committee_duties_for_period<T: SlotClock + 'static, E: EthSpec>(
    duties_service: &DutiesService<T, E>,
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

    // Track updated validator indices
    let mut updated_validator_indices = vec![];

    {
        let mut validator_writer = committee_duties.validators.write();
        for duty in duties {
            let validator_duties = validator_writer
                .get_mut(&duty.validator_index)
                .ok_or(Error::SyncDutiesNotFound(duty.validator_index))?;

            let updated = validator_duties.map_or(true, |existing_duties| {
                existing_duties.validator_sync_committee_indices
                    != duty.validator_sync_committee_indices
            });

            if updated {
                info!(
                    log,
                    "Validator in sync committee";
                    "validator_index" => duty.validator_index,
                    "sync_committee_period" => sync_committee_period,
                );

                updated_validator_indices.push(duty.validator_index);
                *validator_duties =
                    Some(ValidatorDuties::new(duty.validator_sync_committee_indices));
            }
        }
    }

    // TODO: spawn background thread to fill in aggregator proofs

    Ok(())
}

pub fn fill_in_aggregation_proofs(
    duties_service: &DutiesService<T, E>,
    updated_indices: &[u64],
    sync_committee_period: u64,
) {
    // Generate selection proofs for each validator at each slot
}
