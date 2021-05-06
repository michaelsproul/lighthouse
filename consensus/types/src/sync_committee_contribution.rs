use super::{AggregateSignature, ChainSpec, Domain, EthSpec, Fork, SecretKey, SignedRoot};
use crate::attestation::SlotData;
use crate::{test_utils::TestRandom, BitVector, Hash256, Slot};
use safe_arith::ArithError;
use serde_derive::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use test_random_derive::TestRandom;
use tree_hash_derive::TreeHash;

#[derive(Debug, PartialEq)]
pub enum Error {
    SszTypesError(ssz_types::Error),
    AlreadySigned(usize),
    SubnetCountIsZero(ArithError),
}

/// Details an attestation that can be slashable.
///
/// Spec v1.1.0
#[cfg_attr(feature = "arbitrary-fuzz", derive(arbitrary::Arbitrary))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Encode, Decode, TreeHash, TestRandom)]
#[serde(bound = "T: EthSpec")]
pub struct SyncCommitteeContribution<T: EthSpec> {
    pub slot: Slot,
    pub beacon_block_root: Hash256,
    pub subcommittee_index: u64,
    pub aggregation_bits: BitVector<T::SyncCommitteeSubnetSize>,
    pub signature: AggregateSignature,
}

//TODO: verify all this
impl<T: EthSpec> SyncCommitteeContribution<T> {
    /// Are the aggregation bitfields of these attestations disjoint?
    pub fn signers_disjoint_from(&self, other: &Self) -> bool {
        self.aggregation_bits
            .intersection(&other.aggregation_bits)
            .is_zero()
    }

    /// Aggregate another `SyncCommitteeContribution` into this one.
    ///
    /// The aggregation bitfields must be disjoint, and the data must be the same.
    pub fn aggregate(&mut self, other: &Self) {
        debug_assert_eq!(self.slot, other.slot);
        debug_assert_eq!(self.beacon_block_root, other.beacon_block_root);
        debug_assert_eq!(self.subcommittee_index, other.subcommittee_index);
        debug_assert!(self.signers_disjoint_from(other));

        self.aggregation_bits = self.aggregation_bits.union(&other.aggregation_bits);
        self.signature.add_assign_aggregate(&other.signature);
    }

    /// Signs `self`, setting the `committee_position`'th bit of `aggregation_bits` to `true`.
    ///
    /// Returns an `AlreadySigned` error if the `committee_position`'th bit is already `true`.
    pub fn sign(
        &mut self,
        secret_key: &SecretKey,
        committee_position: usize,
        fork: &Fork,
        genesis_validators_root: Hash256,
        spec: &ChainSpec,
    ) -> Result<(), Error> {
        if self
            .aggregation_bits
            .get(committee_position)
            .map_err(Error::SszTypesError)?
        {
            Err(Error::AlreadySigned(committee_position))
        } else {
            self.aggregation_bits
                .set(committee_position, true)
                .map_err(Error::SszTypesError)?;

            let domain = spec.get_domain(
                self.slot.epoch(T::slots_per_epoch()),
                Domain::BeaconAttester,
                fork,
                genesis_validators_root,
            );
            let message = self.beacon_block_root.signing_root(domain);

            self.signature.add_assign(&secret_key.sign(message));

            Ok(())
        }
    }
}

//TODO: verify
impl SignedRoot for Hash256 {}

/// This is not in the spec, but useful for determining uniqueness of sync committee contributions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Encode, Decode, TreeHash, TestRandom)]
pub struct SyncContributionData {
    slot: Slot,
    beacon_block_root: Hash256,
    subcommittee_index: u64,
}

impl SyncContributionData {
    pub fn from_contribution<T: EthSpec>(signing_data: &SyncCommitteeContribution<T>) -> Self {
        Self {
            slot: signing_data.slot,
            beacon_block_root: signing_data.beacon_block_root,
            subcommittee_index: signing_data.subcommittee_index,
        }
    }
}

impl<T: EthSpec> SlotData for SyncCommitteeContribution<T> {
    fn get_slot(&self) -> Slot {
        self.slot
    }
}

impl SlotData for SyncContributionData {
    fn get_slot(&self) -> Slot {
        self.slot
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;

    ssz_and_tree_hash_tests!(SyncCommitteeContribution<MainnetEthSpec>);
}
