use crate::beacon_node_fallback::{BeaconNodeFallback, RequireSynced};
use crate::{
    block_service::BlockServiceNotification,
    duties_service::{DutiesService, Error},
    http_metrics::metrics,
    validator_store::ValidatorStore,
};
use environment::RuntimeContext;
use eth2::types::{AttesterData, BeaconCommitteeSubscription, ProposerData, StateId, ValidatorId};
use parking_lot::RwLock;
use safe_arith::ArithError;
use slog::{debug, error, info, warn, Logger};
use slot_clock::SlotClock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::{sync::mpsc::Sender, time::sleep};
use types::{ChainSpec, Epoch, EthSpec, Hash256, PublicKeyBytes, SelectionProof, Signature, Slot};

pub struct SyncDutiesMap {
    /// Map from sync committee period to duties for members of that sync committee.
    committees: RwLock<HashMap<u64, CommitteeDuties>>,
}

pub struct CommitteeDuties {
    /// Map from validator index to validator duties.
    validators: RwLock<HashMap<u64, ValidatorDuties>>,
}

pub struct ValidatorDuties {
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

/// Number of epochs to wait from the start of the period before actually fetching duties.
fn epoch_offset(spec: &ChainSpec) -> u64 {
    spec.epochs_per_sync_committee_period.as_u64() / 2
}

pub async fn poll_sync_committee_duties<T: SlotClock + 'static, E: EthSpec>(
    duties_service: &DutiesService<T, E>,
) -> Result<(), Error> {
    let current_epoch = duties_service
        .slot_clock
        .now()
        .ok_or(Error::UnableToReadSlotClock)?
        .epoch(E::slots_per_epoch());

    let sync_duties = &duties_service.sync_duties;

    Ok(())
}
