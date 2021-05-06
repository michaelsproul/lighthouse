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
use types::{ChainSpec, Epoch, EthSpec, Hash256, PublicKeyBytes, SelectionProof, Slot};

pub async fn poll_sync_committee_duties<T: SlotClock + 'static, E: EthSpec>(
    duties_service: &DutiesService<T, E>,
) -> Result<(), Error> {
    Ok(())
}
