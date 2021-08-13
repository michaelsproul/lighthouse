use crate::{BeaconChain, BeaconChainError, BeaconChainTypes};
use operation_pool::AttMaxCover;
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use types::{BeaconBlockRef, BeaconState, RelativeEpoch};

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct BlockReward {
    total: u64,
    /// Map from validator index to reward (in GWei) for including that validator's attestation.
    attestation_rewards: HashMap<u64, u64>,
    // FIXME(sproul): other components
}

impl<T: BeaconChainTypes> BeaconChain<T> {
    pub fn compute_block_reward(
        &self,
        block: BeaconBlockRef<'_, T::EthSpec>,
        state: &BeaconState<T::EthSpec>,
    ) -> Result<BlockReward, BeaconChainError> {
        // FIXME(sproul): add slot sanity check

        let active_indices = state.get_cached_active_validator_indices(RelativeEpoch::Current)?;
        let total_active_balance = state.get_total_balance(active_indices, &self.spec)?;
        let per_attestation_rewards = block.body().attestations().iter().filter_map(|att| {
            // FIXME(sproul): handle error
            AttMaxCover::new(att, state, total_active_balance, &self.spec)
                .map(|cover| cover.fresh_validators_rewards)
        });

        // Sum the per attestation rewards, keeping the first reward value for a validator.
        let mut attestation_rewards = HashMap::new();

        for rewards in per_attestation_rewards {
            for (validator_index, reward) in rewards {
                attestation_rewards.entry(validator_index).or_insert(reward);
            }
        }

        let total = attestation_rewards.values().sum();

        Ok(BlockReward {
            total,
            attestation_rewards,
        })
    }
}
