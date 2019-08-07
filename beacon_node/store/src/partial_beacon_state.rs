use serde_derive::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use types::*;

/// Lightweight variant of the `BeaconState` that is stored in the database.
///
/// Utilises lazy-loading from separate storage for its vector fields.
///
/// Spec v0.8.1
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Encode, Decode)]
#[serde(bound = "T: EthSpec")]
pub struct PartialBeaconState<T>
where
    T: EthSpec,
{
    // Versioning
    pub genesis_time: u64,
    pub slot: Slot,
    pub fork: Fork,

    // History
    pub latest_block_header: BeaconBlockHeader,

    #[serde(default)]
    #[ssz(skip_serializing)]
    #[ssz(skip_deserializing)]
    pub block_roots: Option<FixedVector<Hash256, T::SlotsPerHistoricalRoot>>,
    #[serde(default)]
    #[ssz(skip_serializing)]
    #[ssz(skip_deserializing)]
    pub state_roots: Option<FixedVector<Hash256, T::SlotsPerHistoricalRoot>>,

    pub historical_roots: VariableList<Hash256, T::HistoricalRootsLimit>,

    // Ethereum 1.0 chain data
    pub eth1_data: Eth1Data,
    pub eth1_data_votes: VariableList<Eth1Data, T::SlotsPerEth1VotingPeriod>,
    pub eth1_deposit_index: u64,

    // Registry
    pub validators: VariableList<Validator, T::ValidatorRegistryLimit>,
    pub balances: VariableList<u64, T::ValidatorRegistryLimit>,

    // Shuffling
    pub start_shard: u64,

    #[serde(default)]
    #[ssz(skip_serializing)]
    #[ssz(skip_deserializing)]
    pub randao_mixes: Option<FixedVector<Hash256, T::EpochsPerHistoricalVector>>,
    #[serde(default)]
    #[ssz(skip_serializing)]
    #[ssz(skip_deserializing)]
    pub active_index_roots: Option<FixedVector<Hash256, T::EpochsPerHistoricalVector>>,
    #[serde(default)]
    #[ssz(skip_serializing)]
    #[ssz(skip_deserializing)]
    pub compact_committees_roots: Option<FixedVector<Hash256, T::EpochsPerHistoricalVector>>,

    // Slashings
    slashings: FixedVector<u64, T::EpochsPerSlashingsVector>,

    // Attestations
    pub previous_epoch_attestations: VariableList<PendingAttestation<T>, T::MaxPendingAttestations>,
    pub current_epoch_attestations: VariableList<PendingAttestation<T>, T::MaxPendingAttestations>,

    // Crosslinks
    pub previous_crosslinks: FixedVector<Crosslink, T::ShardCount>,
    pub current_crosslinks: FixedVector<Crosslink, T::ShardCount>,

    // Finality
    pub justification_bits: BitVector<T::JustificationBitsLength>,
    pub previous_justified_checkpoint: Checkpoint,
    pub current_justified_checkpoint: Checkpoint,
    pub finalized_checkpoint: Checkpoint,

    // Caching (not in the spec)
    #[serde(default)]
    #[ssz(skip_serializing)]
    #[ssz(skip_deserializing)]
    pub committee_caches: [CommitteeCache; CACHED_EPOCHS],
}

impl<T: EthSpec> PartialBeaconState<T> {
    /// Convert a `BeaconState` to a `PartialBeaconState`, while dropping the optional fields.
    pub fn from_state_forgetful(s: &BeaconState<T>) -> Self {
        // TODO: could use references/Cow for fields to avoid cloning
        PartialBeaconState {
            genesis_time: s.genesis_time,
            slot: s.slot,
            fork: s.fork.clone(),

            // History
            latest_block_header: s.latest_block_header.clone(),
            block_roots: None,
            state_roots: None,
            historical_roots: s.historical_roots.clone(),

            // Eth1
            eth1_data: s.eth1_data.clone(),
            eth1_data_votes: s.eth1_data_votes.clone(),
            eth1_deposit_index: s.eth1_deposit_index,

            // Validator registry
            validators: s.validators.clone(),
            balances: s.balances.clone(),

            // Shuffling
            start_shard: s.start_shard,
            randao_mixes: None,
            active_index_roots: None,
            compact_committees_roots: None,

            // Slashings
            slashings: s.get_all_slashings().to_vec().into(),

            // Attestations
            previous_epoch_attestations: s.previous_epoch_attestations.clone(),
            current_epoch_attestations: s.current_epoch_attestations.clone(),

            // Crosslinks
            previous_crosslinks: s.previous_crosslinks.clone(),
            current_crosslinks: s.current_crosslinks.clone(),

            // Finality
            justification_bits: s.justification_bits.clone(),
            previous_justified_checkpoint: s.previous_justified_checkpoint.clone(),
            current_justified_checkpoint: s.current_justified_checkpoint.clone(),
            finalized_checkpoint: s.finalized_checkpoint.clone(),

            // Caching
            committee_caches: s.committee_caches.clone(),
        }
    }
}
