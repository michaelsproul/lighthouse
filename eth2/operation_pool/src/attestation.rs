use crate::max_cover::MaxCover;
use boolean_bitfield::BooleanBitfield;
use types::{Attestation, BeaconState, ChainSpec};

pub struct AttMaxCover<'a> {
    /// Underlying attestation.
    att: &'a Attestation,
    /// Bitfield of validators that are covered by this attestation.
    fresh_validators: BooleanBitfield,
}

impl<'a> AttMaxCover<'a> {
    pub fn new(att: &'a Attestation, fresh_validators: BooleanBitfield) -> Self {
        Self {
            att,
            fresh_validators,
        }
    }
}

impl<'a> MaxCover for AttMaxCover<'a> {
    type Object = Attestation;
    type Set = BooleanBitfield;

    fn object(&self) -> Attestation {
        self.att.clone()
    }

    fn covering_set(&self) -> &BooleanBitfield {
        &self.fresh_validators
    }

    /// Sneaky: we keep all the attestations together in one bucket, even though
    /// their aggregation bitfields refer to different committees. In order to avoid
    /// confusing committees when updating covering sets, we update only those attestations
    /// whose shard and slot match the attestation being included in the solution, by the logic
    /// that a shard and slot uniquely identify a committee.
    fn update_covering_set(
        &mut self,
        best_att: &Attestation,
        covered_validators: &BooleanBitfield,
    ) {
        if self.att.data.shard == best_att.data.shard && self.att.data.slot == best_att.data.slot {
            self.fresh_validators.difference_inplace(covered_validators);
        }
    }

    fn score(&self) -> usize {
        self.fresh_validators.num_set_bits()
    }
}

/// Compute a fitness score for an attestation.
///
/// The score is calculated by determining the number of *new* attestations that
/// the aggregate attestation introduces, and is proportional to the size of the reward we will
/// receive for including it in a block.
// TODO: this could be optimised with a map from validator index to whether that validator has
// attested in each of the current and previous epochs. Currently quadractic in number of validators.
pub fn attestation_score(
    attestation: &Attestation,
    state: &BeaconState,
    spec: &ChainSpec,
) -> BooleanBitfield {
    // Bitfield of validators whose attestations are new/fresh.
    let mut new_validators = attestation.aggregation_bitfield.clone();

    let attestation_epoch = attestation.data.slot.epoch(spec.slots_per_epoch);

    let state_attestations = if attestation_epoch == state.current_epoch(spec) {
        &state.current_epoch_attestations
    } else if attestation_epoch == state.previous_epoch(spec) {
        &state.previous_epoch_attestations
    } else {
        return BooleanBitfield::from_elem(attestation.aggregation_bitfield.len(), false);
    };

    state_attestations
        .iter()
        // In a single epoch, an attester should only be attesting for one shard.
        // TODO: we avoid including slashable attestations in the state here,
        // but maybe we should do something else with them (like construct slashings).
        .filter(|current_attestation| current_attestation.data.shard == attestation.data.shard)
        .for_each(|current_attestation| {
            // Remove the validators who have signed the existing attestation (they are not new)
            new_validators.difference_inplace(&current_attestation.aggregation_bitfield);
        });

    new_validators
}
