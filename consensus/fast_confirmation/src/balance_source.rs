//! Validator-balance snapshots used by the Fast Confirmation Rule.
//!
//! Every balance source is a `JustifiedBalances`, the projection of a state's validator registry
//! at its current epoch that fork choice also uses. It holds exactly what FCR reads from a balance
//! source: the effective balances of the active validators, which of them are slashed, and the
//! totals with and without the slashed ones.

use crate::{Error, metrics};
use proto_array::{FromStatePath, JustifiedBalances};
use safe_arith::SafeArith;
use tracing::{debug, debug_span};
use types::{BeaconState, Checkpoint, Epoch, EthSpec, Hash256};

/// Cache key identifying the validator-set view the head `BalanceSourceData` was built from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BalanceSourceKey {
    /// No slashings have been processed in the `epoch` (`state.slashings[epoch % E] == 0`): the
    /// epoch's boundary root (block at the last slot of the previous epoch) pins the effective
    /// balances, activation set and slashed flags for the whole epoch.
    NoSlashings {
        epoch_boundary_root: Hash256,
        epoch: Epoch,
    },
    /// At least one slashing has been processed in the epoch. Slashings flip `Validator.slashed`
    /// mid-epoch without moving the epoch boundary root, so the snapshot is keyed on its own block
    /// root instead and every head change rebuilds it for the rest of the epoch.
    SlashingsPresent { head_block_root: Hash256 },
}

impl BalanceSourceKey {
    /// Create a key for a specific `block_root` and its state.
    pub(crate) fn compute<E: EthSpec>(
        state: &BeaconState<E>,
        block_root: Hash256,
    ) -> Result<Self, Error> {
        let epoch = state.current_epoch();
        let epoch_slashings = state
            .get_slashings(epoch)
            .map_err(|e| Error::SlashingsOutOfBounds(format!("slashings lookup: {e:?}")))?;
        if epoch_slashings > 0 {
            Ok(Self::SlashingsPresent {
                head_block_root: block_root,
            })
        } else {
            Ok(Self::NoSlashings {
                epoch_boundary_root: get_epoch_boundary_root::<E>(state)?,
                epoch,
            })
        }
    }
}

/// The head balance source (spec: `get_pulled_up_head_state`), with the key that decides when it
/// must be rebuilt.
///
/// The [`BalanceSourceKey`] fixes the balances: two chains sharing it have the same view. The
/// observed-justified balance sources need no key, because their checkpoint fixes them (see
/// `CheckpointAndBalance`).
#[derive(Clone, Debug)]
pub struct BalanceSourceData {
    pub key: BalanceSourceKey,
    pub balances: JustifiedBalances,
    /// Whether `balances` were read from the state's caches or from its validator registry. Both
    /// give the same balances.
    pub path: FromStatePath,
}

impl BalanceSourceData {
    /// Create the head balance source for `state` at its current epoch, reading the state's caches
    /// if they are valid and otherwise its validator registry.
    ///
    /// The state must be pulled up to the desired epoch prior to calling this function.
    pub(crate) fn new<E: EthSpec>(
        state: &BeaconState<E>,
        block_root: Hash256,
    ) -> Result<Self, Error> {
        let key = BalanceSourceKey::compute(state, block_root)?;
        let (balances, path) = balances_from_state(state, "head")?;
        Ok(Self {
            key,
            balances,
            path,
        })
    }
}

/// Build the balance source of `checkpoint` (spec: `store.checkpoint_states[checkpoint]`) from
/// its checkpoint state: the checkpoint block's post-state advanced to the first slot of
/// `checkpoint.epoch`.
///
/// For callers that have no ready-made balances for the checkpoint. Fork choice's justified
/// balances and the entries of its balances cache are such balances.
pub fn checkpoint_balances_from_state<E: EthSpec>(
    checkpoint_state: &BeaconState<E>,
    checkpoint: Checkpoint,
) -> Result<JustifiedBalances, Error> {
    let _span = debug_span!("fcr_build_checkpoint_balance").entered();
    // Sanity: the supplied state must be the checkpoint's state, advanced to the checkpoint's
    // epoch.
    if checkpoint_state.current_epoch() != checkpoint.epoch {
        return Err(Error::MissingCheckpointState(checkpoint));
    }
    let (balances, _) = balances_from_state(checkpoint_state, "checkpoint")?;
    Ok(balances)
}

/// Build the balances of `state` with `JustifiedBalances::from_state`, and count which path was
/// taken for the `balance_source`.
fn balances_from_state<E: EthSpec>(
    state: &BeaconState<E>,
    balance_source: &'static str,
) -> Result<(JustifiedBalances, FromStatePath), Error> {
    let (balances, path) = JustifiedBalances::from_state(state)?;
    let [path_label, reason_label] = path.metric_labels();
    metrics::inc_counter_vec(
        &metrics::FAST_CONFIRMATION_BALANCES_FROM_STATE,
        &[balance_source, path_label, reason_label],
    );
    if let FromStatePath::Fallback(reason) = &path {
        debug!(
            slot = %state.slot(),
            balance_source,
            ?reason,
            "Built FCR balances from the validator registry"
        );
    }
    Ok((balances, path))
}

/// Block root at the last slot of the previous epoch.
///
/// Used to identify the balance source fields of the `validator` registry in the absence of
/// slashings.
fn get_epoch_boundary_root<E: EthSpec>(state: &BeaconState<E>) -> Result<Hash256, Error> {
    if state.current_epoch() == 0 {
        return Ok(Hash256::ZERO);
    }
    let slot = state
        .current_epoch()
        .start_slot(E::slots_per_epoch())
        .safe_sub(1)?;
    Ok(*state
        .get_block_root(slot)
        .map_err(|e| Error::BlockRootsOutOfBounds(format!("{e:?}")))?)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::FastConfirmationRule;
    use state_processing::AllCaches;
    use std::cmp;
    use std::collections::BTreeSet;
    use types::test_utils::generate_deterministic_keypair;
    use types::{ChainSpec, MinimalEthSpec, Validator};

    const ETH: u64 = 1_000_000_000;
    const PROPOSER_SCORE_BOOST: u64 = 40;

    /// The registry scan that built every balance source (`BalanceSourceData::new`) before balance
    /// sources were backed by `JustifiedBalances`, kept as a test oracle.
    pub(crate) struct RegistryScan {
        /// The raw total effective balance of the active validators, slashed or not.
        pub(crate) total_active_balance: u64,
        /// Effective balance per validator index. 0 for inactive.
        effective_balances: Vec<u64>,
        slashed: Vec<bool>,
    }

    impl RegistryScan {
        pub(crate) fn new<E: EthSpec>(state: &BeaconState<E>) -> Self {
            let current_epoch = state.current_epoch();
            let validators = state.validators();
            let mut effective_balances = Vec::with_capacity(validators.len());
            let mut slashed = Vec::with_capacity(validators.len());
            let mut total_active_balance = 0u64;

            for validator in validators.iter() {
                slashed.push(validator.slashed);
                if validator.is_active_at(current_epoch) {
                    effective_balances.push(validator.effective_balance);
                    total_active_balance =
                        total_active_balance.saturating_add(validator.effective_balance);
                } else {
                    effective_balances.push(0);
                }
            }

            Self {
                total_active_balance,
                effective_balances,
                slashed,
            }
        }

        /// The validator's effective balance if it is active, slashed or not.
        fn balance(&self, val_idx: usize) -> u64 {
            self.effective_balances.get(val_idx).copied().unwrap_or(0)
        }

        fn unslashed_and_active_indices(&self) -> impl Iterator<Item = (usize, u64)> + '_ {
            self.effective_balances
                .iter()
                .copied()
                .enumerate()
                .filter_map(|(i, balance)| {
                    (balance > 0 && !self.slashed.get(i).copied().unwrap_or(false))
                        .then_some((i, balance))
                })
        }

        fn unslashed_balance(&self, val_idx: usize) -> u64 {
            if self.slashed.get(val_idx).copied().unwrap_or(false) {
                0
            } else {
                self.balance(val_idx)
            }
        }

        /// `compute_proposer_score` as computed before it matched the spec: from the unfloored
        /// total including slashed validators.
        pub(crate) fn proposer_score_before_fix<E: EthSpec>(&self) -> u64 {
            self.total_active_balance / E::slots_per_epoch() * PROPOSER_SCORE_BOOST / 100
        }
    }

    /// Spec: `calculate_committee_fraction(state, committee_percent)` (`phase0/fork-choice.md`),
    /// read from the registry.
    pub(crate) fn calculate_committee_fraction<E: EthSpec>(
        state: &BeaconState<E>,
        committee_percent: u64,
        spec: &ChainSpec,
    ) -> u64 {
        let epoch = state.current_epoch();
        let unslashed_and_active_balance = state
            .validators()
            .iter()
            .filter(|validator| validator.is_active_at(epoch) && !validator.slashed)
            .map(|validator| validator.effective_balance)
            .sum::<u64>();
        // `get_total_balance` floors the total.
        let total_balance = cmp::max(
            spec.effective_balance_increment,
            unslashed_and_active_balance,
        );
        let committee_weight = total_balance / E::slots_per_epoch();
        committee_weight * committee_percent / 100
    }

    /// Asserts that every per-validator read FCR makes of `balances` agrees with the registry
    /// scan of `state`, as do the raw totals.
    #[track_caller]
    pub(crate) fn assert_agrees_with_registry_scan<E: EthSpec>(
        balances: &JustifiedBalances,
        state: &BeaconState<E>,
    ) {
        let registry = RegistryScan::new(state);
        // Two past the end, which FCR reads as inactive.
        let indices = 0..state.validators().len() + 2;

        // `get_node_support_between_slots`, `aggregate_vote_balances`.
        for i in indices.clone() {
            assert_eq!(
                balances.unslashed_balance(i),
                registry.unslashed_balance(i),
                "unslashed balance of validator {i}"
            );
        }
        // `get_node_support_between_slots`, `get_current_target_score`.
        assert_eq!(
            balances.nonzero_unslashed_balances().collect::<Vec<_>>(),
            registry.unslashed_and_active_indices().collect::<Vec<_>>()
        );
        // `get_equivocation_score`.
        for i in indices {
            assert_eq!(
                balances.active_balance(i),
                Ok(registry.balance(i)),
                "active balance of validator {i}"
            );
        }
        // Floored by `get_total_active_balance`.
        assert_eq!(
            balances.raw_total_active_balance(),
            registry.total_active_balance
        );
    }

    /// A validator with the given effective balance, active from `activation_epoch` until
    /// `exit_epoch`.
    pub(crate) fn validator(
        effective_balance: u64,
        slashed: bool,
        activation_epoch: Epoch,
        exit_epoch: Epoch,
    ) -> Validator {
        Validator {
            effective_balance,
            slashed,
            activation_epoch,
            exit_epoch,
            withdrawable_epoch: exit_epoch,
            ..Default::default()
        }
    }

    pub(crate) fn active(effective_balance: u64, spec: &ChainSpec) -> Validator {
        validator(
            effective_balance,
            false,
            Epoch::new(0),
            spec.far_future_epoch,
        )
    }

    pub(crate) fn active_slashed(effective_balance: u64, spec: &ChainSpec) -> Validator {
        validator(
            effective_balance,
            true,
            Epoch::new(0),
            spec.far_future_epoch,
        )
    }

    /// A genesis state with `validators`, each with a balance equal to its effective balance and
    /// a distinct pubkey, and all caches built.
    pub(crate) fn state_with_validators<E: EthSpec>(
        validators: &[Validator],
        spec: &ChainSpec,
    ) -> BeaconState<E> {
        let mut state = BeaconState::new(0, Default::default(), spec);
        for (i, validator) in validators.iter().enumerate() {
            let validator = Validator {
                pubkey: generate_deterministic_keypair(i).pk.compress(),
                ..validator.clone()
            };
            state
                .balances_mut()
                .push(validator.effective_balance)
                .unwrap();
            state.validators_mut().push(validator).unwrap();
        }
        state.build_all_caches(spec).unwrap();
        state
    }

    /// Every operation in which FCR reads a balance source agrees with the registry scan it
    /// replaced, on balances read from the state's caches and from its registry, except where
    /// the scan deviated from the spec: the proposer score counts unslashed validators only, and
    /// every total is floored at `EFFECTIVE_BALANCE_INCREMENT`.
    fn check_agreement<E: EthSpec>(validators: &[Validator], spec: &ChainSpec) {
        let state = state_with_validators::<E>(validators, spec);
        let mut state_without_caches = state.clone();
        state_without_caches.drop_all_caches().unwrap();

        let block_root = Hash256::repeat_byte(1);
        let checkpoint = Checkpoint {
            epoch: state.current_epoch(),
            root: block_root,
        };
        let fcr = FastConfirmationRule::new::<E>(
            block_root,
            &state,
            checkpoint,
            checkpoint_balances_from_state(&state, checkpoint).unwrap(),
            None,
            25,
            PROPOSER_SCORE_BOOST,
            spec,
        )
        .unwrap();

        let registry = RegistryScan::new(&state);
        // Where the registry scan's proposer score deviated from the spec.
        let epoch = state.current_epoch();
        let unslashed_total = state
            .validators()
            .iter()
            .filter(|validator| validator.is_active_at(epoch) && !validator.slashed)
            .map(|validator| validator.effective_balance)
            .sum::<u64>();
        let proposer_score_was_wrong = unslashed_total != registry.total_active_balance
            || unslashed_total < spec.effective_balance_increment;
        let every_index = (0..validators.len() as u64 + 2).collect::<BTreeSet<_>>();
        let epoch_start = state.current_epoch().start_slot(E::slots_per_epoch());
        let epoch_end = state.current_epoch().end_slot(E::slots_per_epoch());

        for (state, expected_caches) in [(&state, true), (&state_without_caches, false)] {
            let head_source = BalanceSourceData::new(state, block_root).unwrap();
            assert_eq!(
                head_source.path == FromStatePath::Caches,
                expected_caches,
                "{:?}",
                head_source.path
            );
            let balances = head_source.balances;
            assert_eq!(
                checkpoint_balances_from_state(state, checkpoint).unwrap(),
                balances
            );
            assert_agrees_with_registry_scan(&balances, state);

            // Every validator active in the epoch attests once in it, so every validator
            // equivocating counts each active validator's balance, slashed or not, once.
            assert_eq!(
                fcr.get_equivocation_score(&balances, epoch_start, epoch_end, &every_index)
                    .unwrap(),
                registry.total_active_balance
            );

            // The intended differences.
            assert_eq!(
                fcr.get_total_active_balance(&balances),
                cmp::max(
                    spec.effective_balance_increment,
                    registry.total_active_balance
                )
            );
            let proposer_score = fcr.compute_proposer_score::<E>(&balances).unwrap();
            assert_eq!(
                proposer_score,
                calculate_committee_fraction(state, PROPOSER_SCORE_BOOST, spec)
            );
            if proposer_score_was_wrong {
                assert_ne!(proposer_score, registry.proposer_score_before_fix::<E>());
            } else {
                assert_eq!(proposer_score, registry.proposer_score_before_fix::<E>());
            }
        }
    }

    #[test]
    fn agrees_with_registry_scan_on_mixed_registry() {
        type E = MinimalEthSpec;
        let spec = E::default_spec();
        let far_future = spec.far_future_epoch;
        let mut validators = vec![active(32 * ETH, &spec); E::slots_per_epoch() as usize];
        validators.extend([
            active(31 * ETH, &spec),
            active(0, &spec),
            active_slashed(32 * ETH, &spec),
            active_slashed(0, &spec),
            // Exited.
            validator(32 * ETH, false, Epoch::new(0), Epoch::new(0)),
            validator(32 * ETH, true, Epoch::new(0), Epoch::new(0)),
            // Pending and activating.
            validator(32 * ETH, false, far_future, far_future),
            validator(32 * ETH, true, far_future, far_future),
            validator(32 * ETH, false, Epoch::new(1), far_future),
        ]);
        check_agreement::<E>(&validators, &spec);
    }

    /// Slashed validators that are not active don't count, so the registry scan's proposer score
    /// was already right.
    #[test]
    fn agrees_with_registry_scan_without_active_slashed_validators() {
        type E = MinimalEthSpec;
        let spec = E::default_spec();
        let far_future = spec.far_future_epoch;
        let mut validators = vec![active(32 * ETH, &spec); E::slots_per_epoch() as usize];
        validators.extend([
            validator(32 * ETH, true, Epoch::new(0), Epoch::new(0)),
            validator(32 * ETH, true, far_future, far_future),
        ]);
        check_agreement::<E>(&validators, &spec);
    }

    /// The unslashed total is floored, so the proposer score is not 0.
    #[test]
    fn agrees_with_registry_scan_when_every_active_validator_is_slashed() {
        type E = MinimalEthSpec;
        let spec = E::default_spec();
        let far_future = spec.far_future_epoch;
        let mut validators = vec![active_slashed(32 * ETH, &spec); E::slots_per_epoch() as usize];
        validators.extend([
            validator(32 * ETH, false, Epoch::new(0), Epoch::new(0)),
            validator(32 * ETH, false, far_future, far_future),
        ]);
        check_agreement::<E>(&validators, &spec);
    }

    /// Both totals are floored.
    #[test]
    fn agrees_with_registry_scan_when_every_active_validator_has_zero_balance() {
        type E = MinimalEthSpec;
        let spec = E::default_spec();
        let far_future = spec.far_future_epoch;
        let mut validators = vec![active(0, &spec); E::slots_per_epoch() as usize];
        validators.extend([
            active_slashed(0, &spec),
            validator(32 * ETH, false, far_future, far_future),
        ]);
        check_agreement::<E>(&validators, &spec);
    }
}
