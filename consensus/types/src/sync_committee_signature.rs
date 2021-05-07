use crate::test_utils::TestRandom;
use crate::{AggregateSignature, Hash256, Slot};

use crate::attestation::SlotData;
use serde_derive::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use test_random_derive::TestRandom;
use tree_hash_derive::TreeHash;

/// The data upon which a `SyncCommitteeContribution` is based.
///
/// Spec v1.1.0
#[cfg_attr(feature = "arbitrary-fuzz", derive(arbitrary::Arbitrary))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Encode, Decode, TreeHash, TestRandom)]
pub struct SyncCommitteeSignature {
    pub slot: Slot,
    pub beacon_block_root: Hash256,
    #[serde(with = "serde_utils::quoted_u64")]
    pub validator_index: u64,
    // Signature by the validator over the block root of `slot`
    pub signature: AggregateSignature,
}

impl SlotData for SyncCommitteeSignature {
    fn get_slot(&self) -> Slot {
        self.slot
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    ssz_and_tree_hash_tests!(SyncCommitteeSignature);
}
