use crate::{beacon_fork_choice_store::PersistedForkChoiceStoreV28, metrics};
use ssz::{Decode, Encode};
use ssz_derive::{Decode, Encode};
use store::{DBColumn, Error, KeyValueStoreOp, StoreConfig};
use superstruct::superstruct;
use types::Hash256;

// If adding a new version you should update this type alias and fix the breakages.
pub type PersistedForkChoice = PersistedForkChoiceV29;

/// V29 uses `sszen`, like the fork choice it holds.
#[superstruct(
    variants(V28, V29),
    specific_variant_attributes(
        V28(derive(Encode, Decode)),
        V29(derive(sszen::SszSize, sszen::Encode, sszen::Decode)),
    ),
    no_enum
)]
pub struct PersistedForkChoice {
    #[superstruct(only(V28))]
    pub fork_choice_v28: fork_choice::PersistedForkChoiceV28,
    #[superstruct(only(V29))]
    pub fork_choice: fork_choice::PersistedForkChoiceV29,
    #[superstruct(only(V28, V29))]
    pub fork_choice_store: PersistedForkChoiceStoreV28,
}

impl PersistedForkChoiceV28 {
    pub fn from_bytes(bytes: &[u8], store_config: &StoreConfig) -> Result<Self, Error> {
        let decompressed_bytes = store_config
            .decompress_bytes(bytes)
            .map_err(Error::Compression)?;
        Self::from_ssz_bytes(&decompressed_bytes).map_err(Into::into)
    }

    pub fn as_bytes(&self, store_config: &StoreConfig) -> Result<Vec<u8>, Error> {
        let encode_timer = metrics::start_timer(&metrics::FORK_CHOICE_ENCODE_TIMES);
        let ssz_bytes = self.as_ssz_bytes();
        drop(encode_timer);

        let _compress_timer = metrics::start_timer(&metrics::FORK_CHOICE_COMPRESS_TIMES);
        store_config
            .compress_bytes(&ssz_bytes)
            .map_err(Error::Compression)
    }

    pub fn as_kv_store_op(
        &self,
        key: Hash256,
        store_config: &StoreConfig,
    ) -> Result<KeyValueStoreOp, Error> {
        Ok(KeyValueStoreOp::PutKeyValue(
            DBColumn::ForkChoice,
            key.as_slice().to_vec(),
            self.as_bytes(store_config)?,
        ))
    }
}

impl PersistedForkChoiceV29 {
    pub fn from_bytes(bytes: &[u8], store_config: &StoreConfig) -> Result<Self, Error> {
        let decompressed_bytes = store_config
            .decompress_bytes(bytes)
            .map_err(Error::Compression)?;
        <Self as sszen::Decode>::from_ssz_bytes(&decompressed_bytes)
            .map_err(|e| ssz::DecodeError::BytesInvalid(e.to_string()).into())
    }

    pub fn as_bytes(&self, store_config: &StoreConfig) -> Result<Vec<u8>, Error> {
        let encode_timer = metrics::start_timer(&metrics::FORK_CHOICE_ENCODE_TIMES);
        let ssz_bytes = sszen::Encode::as_ssz_bytes(self);
        drop(encode_timer);

        let _compress_timer = metrics::start_timer(&metrics::FORK_CHOICE_COMPRESS_TIMES);
        store_config
            .compress_bytes(&ssz_bytes)
            .map_err(Error::Compression)
    }

    pub fn as_kv_store_op(
        &self,
        key: Hash256,
        store_config: &StoreConfig,
    ) -> Result<KeyValueStoreOp, Error> {
        Ok(KeyValueStoreOp::PutKeyValue(
            DBColumn::ForkChoice,
            key.as_slice().to_vec(),
            self.as_bytes(store_config)?,
        ))
    }
}

impl From<PersistedForkChoiceV28> for PersistedForkChoiceV29 {
    fn from(v28: PersistedForkChoiceV28) -> Self {
        Self {
            fork_choice: v28.fork_choice_v28.into(),
            fork_choice_store: v28.fork_choice_store,
        }
    }
}

impl From<PersistedForkChoiceV29> for PersistedForkChoiceV28 {
    fn from(v29: PersistedForkChoiceV29) -> Self {
        Self {
            fork_choice_v28: v29.fork_choice.into(),
            fork_choice_store: v29.fork_choice_store,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto_array::core::{ProtoNode, VoteTracker};
    use proto_array::{ExecutionStatus, ProtoArrayForkChoice};
    use std::collections::BTreeSet;
    use types::{AttestationShufflingId, Checkpoint, Epoch, EthSpec, MainnetEthSpec, Slot};

    /// `PersistedForkChoiceV29` and the containers it holds, with the `ethereum_ssz` codec they had
    /// before they moved to `sszen`.
    #[derive(Encode)]
    struct EthereumSszPersistedForkChoiceV29 {
        fork_choice: EthereumSszForkChoiceV29,
        fork_choice_store: PersistedForkChoiceStoreV28,
    }

    #[derive(Encode)]
    struct EthereumSszForkChoiceV29 {
        proto_array: EthereumSszContainerV29,
    }

    #[derive(Encode)]
    struct EthereumSszContainerV29 {
        votes: Vec<VoteTracker>,
        prune_threshold: usize,
        nodes: Vec<ProtoNode>,
        indices: Vec<(Hash256, usize)>,
    }

    #[test]
    fn encoding_matches_ethereum_ssz() {
        let spec = MainnetEthSpec::default_spec();
        let checkpoint = Checkpoint {
            epoch: Epoch::new(2),
            root: Hash256::repeat_byte(1),
        };
        let shuffling_id = AttestationShufflingId::from_components(Epoch::new(2), Hash256::ZERO);
        let proto_array = ProtoArrayForkChoice::new::<MainnetEthSpec>(
            Slot::new(64),
            Slot::new(64),
            Hash256::repeat_byte(2),
            checkpoint,
            checkpoint,
            shuffling_id.clone(),
            shuffling_id,
            ExecutionStatus::irrelevant(),
            None,
            None,
            3,
            &spec,
        )
        .unwrap();
        let mut container = proto_array.as_ssz_container();
        container.votes = vec![VoteTracker::default(); 3];
        let fork_choice_store = || PersistedForkChoiceStoreV28 {
            time: Slot::new(70),
            finalized_checkpoint: checkpoint,
            justified_checkpoint: checkpoint,
            justified_state_root: Hash256::repeat_byte(3),
            unrealized_justified_checkpoint: checkpoint,
            unrealized_justified_state_root: Hash256::repeat_byte(4),
            unrealized_finalized_checkpoint: checkpoint,
            proposer_boost_root: Hash256::repeat_byte(5),
            equivocating_indices: BTreeSet::from([7, 9]),
        };

        let ethereum_ssz_bytes = EthereumSszPersistedForkChoiceV29 {
            fork_choice: EthereumSszForkChoiceV29 {
                proto_array: EthereumSszContainerV29 {
                    votes: container.votes.clone(),
                    prune_threshold: container.prune_threshold,
                    nodes: container.nodes.clone(),
                    indices: container.indices.clone(),
                },
            },
            fork_choice_store: fork_choice_store(),
        }
        .as_ssz_bytes();
        let persisted = PersistedForkChoiceV29 {
            fork_choice: fork_choice::PersistedForkChoiceV29 {
                proto_array: container,
            },
            fork_choice_store: fork_choice_store(),
        };
        assert_eq!(sszen::Encode::as_ssz_bytes(&persisted), ethereum_ssz_bytes);

        // Fork choice persisted by earlier versions must still decode.
        let decoded =
            <PersistedForkChoiceV29 as sszen::Decode>::from_ssz_bytes(&ethereum_ssz_bytes).unwrap();
        assert_eq!(sszen::Encode::as_ssz_bytes(&decoded), ethereum_ssz_bytes);
    }
}
