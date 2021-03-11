use crate::test_utils::TestRandom;
use crate::*;

use serde_derive::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use ssz_types::VariableList;
use superstruct::superstruct;
use test_random_derive::TestRandom;
use tree_hash_derive::TreeHash;

/// The body of a `BeaconChain` block, containing operations.
///
/// This *superstruct* abstracts over the hard-fork.
#[superstruct(
    variants(Base, Altair),
    derive_all(
        Debug,
        PartialEq,
        Clone,
        Serialize,
        Deserialize,
        Encode,
        Decode,
        TreeHash
    )
)]
// #[serde(bound = "T: EthSpec")]
pub struct BeaconBlockBody<T: EthSpec> {
    pub randao_reveal: Signature,
    pub eth1_data: Eth1Data,
    pub graffiti: Graffiti,
    pub proposer_slashings: VariableList<ProposerSlashing, T::MaxProposerSlashings>,
    pub attester_slashings: VariableList<AttesterSlashing<T>, T::MaxAttesterSlashings>,
    pub attestations: VariableList<Attestation<T>, T::MaxAttestations>,
    pub deposits: VariableList<Deposit, T::MaxDeposits>,
    pub voluntary_exits: VariableList<SignedVoluntaryExit, T::MaxVoluntaryExits>,
    #[superstruct(only(Altair))]
    pub sync_committee_bits: BitVector<T::SyncCommitteeSize>,
    #[superstruct(only(Altair))]
    pub sync_committee_signature: Signature,
}

#[cfg(test)]
mod tests {
    use super::*;

    ssz_and_tree_hash_tests!(BeaconBlockBody<MainnetEthSpec>);
    ssz_and_tree_hash_tests!(BeaconBlockBodyBase<MainnetEthSpec>);
    ssz_and_tree_hash_tests!(BeaconBlockBodyAltair<MainnetEthSpec>);
}
