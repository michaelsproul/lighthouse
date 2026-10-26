use std::cmp;
use std::collections::BTreeMap;
use std::sync::Arc;

use safe_arith::{ArithError, SafeArith};
use types::{BeaconState, BeaconStateError, EpochCacheError, EthSpec, Hash256, RelativeEpoch};

/// The balances of a state's validator registry at the state's current epoch, as fork choice
/// weighs votes with them.
///
/// Terminology used by the fields and methods:
///
/// - *active*: `is_active_validator(validator, get_current_epoch(state))`, slashed or not;
/// - *unslashed*: active and not slashed;
/// - *raw*: an unfloored sum. The spec's `get_total_balance` floors every total at
///   `EFFECTIVE_BALANCE_INCREMENT`; the totals without a `raw_` prefix apply that floor.
///
/// The exception is `effective_balances`, which keeps its historical name: it is the unslashed
/// view, 0 for inactive and slashed validators. Unlike `EpochCache::effective_balances`, it is not
/// every validator's effective balance.
///
/// A snapshot is immutable and shared: cloning it only increments a reference count.
///
/// `from_justified_state` defines the semantics, and every other constructor that reads a state
/// must produce a structurally equal value:
///
/// - `effective_balances[i]` is validator `i`'s effective balance if it is unslashed, and 0
///   otherwise. Its length is `state.validators().len()`.
/// - `num_unslashed_validators` (formerly `num_active_validators`) counts unslashed validators,
///   *including* those with an effective balance of 0. It is therefore not the number of nonzero
///   entries of `effective_balances`.
/// - `slashed_balances` holds *every* active slashed validator, including those with an effective
///   balance of 0.
///
/// Active and slashed validators are those of the state's current epoch. Inactive validators'
/// `slashed` flags are not recorded.
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct JustifiedBalances {
    inner: Arc<Inner>,
}

#[derive(Debug, PartialEq, Eq, Clone, Default)]
struct Inner {
    /// The effective balance of each unslashed validator, and 0 for every other validator.
    effective_balances: Vec<u64>,
    /// The raw sum of `effective_balances`, i.e. the total effective balance of unslashed
    /// validators.
    raw_total_unslashed_balance: u64,
    /// The raw total effective balance of all active validators, slashed or not. Equal to
    /// `raw_total_unslashed_balance` plus the sum of `slashed_balances`.
    raw_total_active_balance: u64,
    /// The number of unslashed validators, including those with an effective balance of 0.
    num_unslashed_validators: u64,
    /// A mapping from the index of each active slashed validator to its effective balance.
    slashed_balances: BTreeMap<u64, u64>,
}

/// The reason `JustifiedBalances::try_from_state_caches` could not use a state's caches.
#[derive(Debug, PartialEq, Clone)]
pub enum StateCachesError {
    /// The decision root that keys the epoch cache could not be computed.
    EpochCacheDecisionRoot(BeaconStateError),
    /// The epoch cache is not initialized for the state's current epoch and decision block.
    EpochCacheInvalid(EpochCacheError),
    /// The committee cache for the state's current epoch is not initialized.
    CommitteeCacheUninitialized(BeaconStateError),
    /// The slashings cache is not initialized at the state's latest block slot.
    SlashingsCacheUninitialized(BeaconStateError),
    /// A validator that is active in the current epoch has no entry in the epoch cache.
    MissingEffectiveBalance {
        validator_index: usize,
    },
    Arith(ArithError),
}

impl From<ArithError> for StateCachesError {
    fn from(e: ArithError) -> Self {
        Self::Arith(e)
    }
}

impl StateCachesError {
    /// A name for the precondition that failed, for use as a metric label.
    pub fn metric_label(&self) -> &'static str {
        match self {
            Self::EpochCacheDecisionRoot(_) => "epoch_cache_decision_root",
            Self::EpochCacheInvalid(_) => "epoch_cache_invalid",
            Self::CommitteeCacheUninitialized(_) => "committee_cache_uninitialized",
            Self::SlashingsCacheUninitialized(_) => "slashings_cache_uninitialized",
            Self::MissingEffectiveBalance { .. } => "missing_effective_balance",
            Self::Arith(_) => "arith",
        }
    }
}

/// Which construction `JustifiedBalances::from_state` used.
#[derive(Debug, PartialEq, Clone)]
pub enum FromStatePath {
    /// The balances were read from the state's caches.
    Caches,
    /// The state's caches could not be used for the given reason, so the balances were read from
    /// the validator registry.
    Fallback(StateCachesError),
}

impl FromStatePath {
    /// The `path` and `reason` metric labels for this path: `caches` and `none`, or `fallback`
    /// and the precondition that failed.
    pub fn metric_labels(&self) -> [&'static str; 2] {
        match self {
            Self::Caches => ["caches", "none"],
            Self::Fallback(reason) => ["fallback", reason.metric_label()],
        }
    }
}

impl JustifiedBalances {
    /// Build the balances by reading every validator in `state.validators()`.
    pub fn from_justified_state<E: EthSpec>(state: &BeaconState<E>) -> Result<Self, ArithError> {
        let current_epoch = state.current_epoch();
        let mut raw_total_unslashed_balance = 0u64;
        let mut raw_total_active_balance = 0u64;
        let mut num_unslashed_validators = 0u64;
        let mut slashed_balances = BTreeMap::new();

        let effective_balances = state
            .validators()
            .iter()
            .enumerate()
            .map(|(i, validator)| {
                if !validator.is_active_at(current_epoch) {
                    return Ok(0);
                }
                raw_total_active_balance.safe_add_assign(validator.effective_balance)?;

                if validator.slashed {
                    slashed_balances.insert(i as u64, validator.effective_balance);
                    Ok(0)
                } else {
                    raw_total_unslashed_balance.safe_add_assign(validator.effective_balance)?;
                    num_unslashed_validators.safe_add_assign(1)?;
                    Ok(validator.effective_balance)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self::from_inner(Inner {
            effective_balances,
            raw_total_unslashed_balance,
            raw_total_active_balance,
            num_unslashed_validators,
            slashed_balances,
        }))
    }

    /// Build synthetic balances in which every nonzero entry of `effective_balances` is an
    /// unslashed validator, every zero entry is inactive, and no validator is slashed.
    ///
    /// Intended for tests and benchmarks. Unlike `from_justified_state`,
    /// `num_unslashed_validators` counts the nonzero entries, so it excludes unslashed validators
    /// with an effective balance of 0.
    pub fn from_effective_balances(effective_balances: Vec<u64>) -> Result<Self, ArithError> {
        let mut raw_total_unslashed_balance = 0u64;
        let mut num_unslashed_validators = 0u64;

        for &balance in &effective_balances {
            if balance != 0 {
                raw_total_unslashed_balance.safe_add_assign(balance)?;
                num_unslashed_validators.safe_add_assign(1)?;
            }
        }

        Ok(Self::from_inner(Inner {
            effective_balances,
            raw_total_unslashed_balance,
            raw_total_active_balance: raw_total_unslashed_balance,
            num_unslashed_validators,
            slashed_balances: BTreeMap::new(),
        }))
    }

    /// Build the balances from the state's caches rather than from `state.validators()`.
    ///
    /// The result is structurally equal to `from_justified_state(state)` whenever the state's
    /// caches are consistent with its registry, which the state transition maintains:
    ///
    /// - The epoch cache holds every validator's effective balance at the start of the current
    ///   epoch. Effective balances only change during epoch processing, and fork upgrades only
    ///   zero those of validators that are not yet active, so these are the effective balances of
    ///   all validators active in the epoch.
    /// - The current epoch's committee cache holds exactly the active validators. Validators
    ///   added to the registry after it was built are not active in this epoch.
    /// - The slashings cache holds every slashed validator, because `slash_validator` records
    ///   each slashing in it.
    ///
    /// The cache checks below detect stale caches but not arbitrary mutation of the registry, for
    /// example setting `validator.slashed` without updating the slashings cache.
    ///
    /// Returns an error naming the first precondition that fails, in which case the caller should
    /// fall back to `from_justified_state` (see `from_state`).
    pub fn try_from_state_caches<E: EthSpec>(
        state: &BeaconState<E>,
    ) -> Result<Self, StateCachesError> {
        let current_epoch = state.current_epoch();

        // The same validity check as `state_processing::epoch_cache::is_epoch_cache_initialized`.
        let decision_block_root = state
            .epoch_cache_decision_root(Hash256::ZERO)
            .map_err(StateCachesError::EpochCacheDecisionRoot)?;
        let epoch_cache = state.epoch_cache();
        epoch_cache
            .check_validity(current_epoch, decision_block_root)
            .map_err(StateCachesError::EpochCacheInvalid)?;
        let cached_effective_balances = epoch_cache
            .effective_balances()
            .map_err(StateCachesError::EpochCacheInvalid)?;

        // Errors unless the cache is initialized at `current_epoch`.
        let committee_cache = state
            .committee_cache(RelativeEpoch::Current)
            .map_err(StateCachesError::CommitteeCacheUninitialized)?;

        let slashings_cache = state.slashings_cache();
        slashings_cache
            .check_initialized(state.latest_block_header().slot)
            .map_err(StateCachesError::SlashingsCacheUninitialized)?;

        // First pass: the effective balance of every active validator, slashed or not.
        let num_validators = state.validators().len();
        let mut effective_balances = Vec::with_capacity(num_validators);
        let mut raw_total_active_balance = 0u64;
        let mut num_active_validators = 0u64;

        for validator_index in 0..num_validators {
            // Check activity first: an inactive validator's balance is 0 whether or not the epoch
            // cache has an entry for it.
            let effective_balance = if committee_cache.shuffled_position(validator_index).is_some()
            {
                // A missing entry must not be read as 0. It means the caches are inconsistent.
                let effective_balance = cached_effective_balances
                    .get(validator_index)
                    .copied()
                    .ok_or(StateCachesError::MissingEffectiveBalance { validator_index })?;
                raw_total_active_balance.safe_add_assign(effective_balance)?;
                num_active_validators.safe_add_assign(1)?;
                effective_balance
            } else {
                0
            };
            effective_balances.push(effective_balance);
        }

        // Second pass: move each active slashed validator's balance into `slashed_balances`.
        let mut raw_total_unslashed_balance = raw_total_active_balance;
        let mut num_unslashed_validators = num_active_validators;
        let mut slashed_balances = BTreeMap::new();

        for validator_index in slashings_cache.slashed_validator_indices() {
            if committee_cache.shuffled_position(validator_index).is_none() {
                continue;
            }
            let Some(balance) = effective_balances.get_mut(validator_index) else {
                continue;
            };
            slashed_balances.insert(validator_index as u64, *balance);
            raw_total_unslashed_balance.safe_sub_assign(*balance)?;
            num_unslashed_validators.safe_sub_assign(1)?;
            *balance = 0;
        }

        Ok(Self::from_inner(Inner {
            effective_balances,
            raw_total_unslashed_balance,
            raw_total_active_balance,
            num_unslashed_validators,
            slashed_balances,
        }))
    }

    /// Build the balances from the state's caches if possible, and otherwise from
    /// `state.validators()`. Also returns which path was taken.
    ///
    /// In debug builds, a result read from the caches is checked against `from_justified_state`.
    pub fn from_state<E: EthSpec>(
        state: &BeaconState<E>,
    ) -> Result<(Self, FromStatePath), ArithError> {
        match Self::try_from_state_caches(state) {
            Ok(balances) => {
                if cfg!(debug_assertions) {
                    let matches_registry = Self::from_justified_state(state)
                        .is_ok_and(|from_registry| from_registry == balances);
                    debug_assert!(
                        matches_registry,
                        "balances from the state caches differ from the registry at slot {}",
                        state.slot()
                    );
                }
                Ok((balances, FromStatePath::Caches))
            }
            Err(reason) => Ok((
                Self::from_justified_state(state)?,
                FromStatePath::Fallback(reason),
            )),
        }
    }

    fn from_inner(inner: Inner) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Returns `true` if `a` and `b` share the same allocation, which implies that they are equal.
    pub fn ptr_eq(a: &Self, b: &Self) -> bool {
        Arc::ptr_eq(&a.inner, &b.inner)
    }

    /// The effective balance of each unslashed validator, and 0 for every other validator,
    /// indexed by validator index.
    pub fn effective_balances(&self) -> &[u64] {
        &self.inner.effective_balances
    }

    /// The effective balance of the validator if it is unslashed, and otherwise 0.
    pub fn unslashed_balance(&self, validator_index: usize) -> u64 {
        self.inner
            .effective_balances
            .get(validator_index)
            .copied()
            .unwrap_or(0)
    }

    /// The `(validator_index, effective_balance)` of each unslashed validator with a nonzero
    /// effective balance, in index order.
    pub fn nonzero_unslashed_balances(&self) -> impl Iterator<Item = (usize, u64)> + '_ {
        self.inner
            .effective_balances
            .iter()
            .copied()
            .enumerate()
            .filter(|&(_, balance)| balance != 0)
    }

    /// The effective balance of the validator if it is active, slashed or not, and otherwise 0.
    pub fn active_balance(&self, validator_index: usize) -> Result<u64, ArithError> {
        // At most one of the two is nonzero. An active slashed validator's entry in
        // `effective_balances` is 0 rather than missing, so add them rather than using one as a
        // fallback for the other.
        let slashed_balance = self
            .inner
            .slashed_balances
            .get(&(validator_index as u64))
            .copied()
            .unwrap_or(0);
        self.unslashed_balance(validator_index)
            .safe_add(slashed_balance)
    }

    /// A mapping from the index of each active slashed validator to its effective balance.
    pub fn slashed_balances(&self) -> &BTreeMap<u64, u64> {
        &self.inner.slashed_balances
    }

    /// The number of unslashed validators, including those with an effective balance of 0.
    pub fn num_unslashed_validators(&self) -> u64 {
        self.inner.num_unslashed_validators
    }

    /// The raw (unfloored) total effective balance of unslashed validators.
    pub fn raw_total_unslashed_balance(&self) -> u64 {
        self.inner.raw_total_unslashed_balance
    }

    /// The raw (unfloored) total effective balance of all active validators, slashed or not.
    pub fn raw_total_active_balance(&self) -> u64 {
        self.inner.raw_total_active_balance
    }

    /// The total effective balance of unslashed validators, floored at
    /// `effective_balance_increment`, the spec's `EFFECTIVE_BALANCE_INCREMENT`.
    ///
    /// Spec: `get_total_balance` of the unslashed active validators, as used by
    /// `calculate_committee_fraction`.
    pub fn total_unslashed_balance(&self, effective_balance_increment: u64) -> u64 {
        cmp::max(
            effective_balance_increment,
            self.inner.raw_total_unslashed_balance,
        )
    }

    /// The total effective balance of all active validators, slashed or not, floored at
    /// `effective_balance_increment`, the spec's `EFFECTIVE_BALANCE_INCREMENT`.
    ///
    /// Spec: `get_total_active_balance`.
    pub fn total_active_balance(&self, effective_balance_increment: u64) -> u64 {
        cmp::max(
            effective_balance_increment,
            self.inner.raw_total_active_balance,
        )
    }

    /// Overwrite the unslashed total and validator count without changing the balances, giving a
    /// snapshot whose metadata is inconsistent with its balances.
    #[cfg(test)]
    pub(crate) fn set_metadata_for_testing(
        &mut self,
        raw_total_unslashed_balance: u64,
        num_unslashed_validators: u64,
    ) {
        let inner = Arc::make_mut(&mut self.inner);
        inner.raw_total_unslashed_balance = raw_total_unslashed_balance;
        inner.num_unslashed_validators = num_unslashed_validators;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bls::{PublicKeyBytes, SignatureBytes};
    use proptest::prelude::*;
    use state_processing::common::slash_validator;
    use state_processing::per_block_processing::{VerifyBlockRoot, process_block_header};
    use state_processing::{
        AllCaches, ConsensusContext, GloasVerificationContext, per_slot_processing,
    };
    use std::sync::LazyLock;
    use types::test_utils::generate_deterministic_keypair;
    use types::{
        ActivationQueue, BeaconBlockHeader, ChainSpec, DepositData, Epoch, EpochCache,
        EpochCacheKey, ForkName, MainnetEthSpec, MinimalEthSpec, PendingConsolidation,
        PendingDeposit, SlashingsCache, Slot, Validator,
    };

    type E = MinimalEthSpec;

    const ETH: u64 = 1_000_000_000;

    fn push_validator(
        state: &mut BeaconState<E>,
        spec: &ChainSpec,
        effective_balance: u64,
        slashed: bool,
        activation_epoch: Epoch,
    ) {
        state
            .validators_mut()
            .push(Validator {
                effective_balance,
                slashed,
                activation_epoch,
                exit_epoch: spec.far_future_epoch,
                withdrawable_epoch: spec.far_future_epoch,
                ..Default::default()
            })
            .unwrap();
    }

    /// Validators 0 and 1 are active, validators 2 and 3 are not. Validators 1 and 3 are slashed.
    fn mixed_registry_state(spec: &ChainSpec) -> BeaconState<E> {
        let mut state: BeaconState<E> = BeaconState::new(0, <_>::default(), spec);
        let epoch = state.current_epoch();

        push_validator(&mut state, spec, 32 * ETH, false, epoch);
        push_validator(&mut state, spec, 31 * ETH, true, epoch);
        push_validator(&mut state, spec, 30 * ETH, false, spec.far_future_epoch);
        push_validator(&mut state, spec, 29 * ETH, true, spec.far_future_epoch);
        state
    }

    #[test]
    fn from_justified_state_handles_slashed_and_inactive_validators() {
        let spec = E::default_spec();
        let state = mixed_registry_state(&spec);

        let justified_balances = JustifiedBalances::from_justified_state(&state).unwrap();

        assert_eq!(
            justified_balances.effective_balances(),
            &[32 * ETH, 0, 0, 0]
        );
        assert_eq!(justified_balances.raw_total_unslashed_balance(), 32 * ETH);
        assert_eq!(justified_balances.raw_total_active_balance(), 63 * ETH);
        assert_eq!(justified_balances.num_unslashed_validators(), 1);
        assert_eq!(
            justified_balances.slashed_balances(),
            &BTreeMap::from([(1, 31 * ETH)])
        );
    }

    #[test]
    fn from_effective_balances_has_empty_slashed_balances() {
        let justified_balances =
            JustifiedBalances::from_effective_balances(vec![32 * ETH, 0, 31 * ETH]).unwrap();

        assert_eq!(justified_balances.raw_total_unslashed_balance(), 63 * ETH);
        assert_eq!(justified_balances.raw_total_active_balance(), 63 * ETH);
        assert_eq!(justified_balances.num_unslashed_validators(), 2);
        assert!(justified_balances.slashed_balances().is_empty());
    }

    #[test]
    fn per_validator_accessors() {
        let spec = E::default_spec();
        let justified_balances =
            JustifiedBalances::from_justified_state(&mixed_registry_state(&spec)).unwrap();

        // Unslashed, active slashed, inactive, inactive slashed, and out of range.
        let unslashed = (0..5)
            .map(|i| justified_balances.unslashed_balance(i))
            .collect::<Vec<_>>();
        assert_eq!(unslashed, vec![32 * ETH, 0, 0, 0, 0]);
        let active = (0..5)
            .map(|i| justified_balances.active_balance(i).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(active, vec![32 * ETH, 31 * ETH, 0, 0, 0]);

        assert_eq!(
            justified_balances
                .nonzero_unslashed_balances()
                .collect::<Vec<_>>(),
            vec![(0, 32 * ETH)]
        );
        assert_eq!(
            justified_balances.total_unslashed_balance(spec.effective_balance_increment),
            32 * ETH
        );
        assert_eq!(
            justified_balances.total_active_balance(spec.effective_balance_increment),
            63 * ETH
        );
    }

    #[test]
    fn zero_balance_active_validators_are_recorded() {
        let spec = E::default_spec();
        let mut state: BeaconState<E> = BeaconState::new(0, <_>::default(), &spec);
        let epoch = state.current_epoch();
        push_validator(&mut state, &spec, 0, false, epoch);
        push_validator(&mut state, &spec, 0, true, epoch);
        push_validator(&mut state, &spec, 32 * ETH, false, epoch);

        let justified_balances = JustifiedBalances::from_justified_state(&state).unwrap();

        assert_eq!(justified_balances.effective_balances(), &[0, 0, 32 * ETH]);
        // The unslashed validator with zero balance is counted, and the slashed one is recorded.
        assert_eq!(justified_balances.num_unslashed_validators(), 2);
        assert_eq!(
            justified_balances.slashed_balances(),
            &BTreeMap::from([(1, 0)])
        );
        assert_eq!(justified_balances.active_balance(1), Ok(0));
        assert_eq!(
            justified_balances
                .nonzero_unslashed_balances()
                .collect::<Vec<_>>(),
            vec![(2, 32 * ETH)]
        );

        // The synthetic constructor can't tell a zero-balance validator from an inactive one.
        let synthetic = JustifiedBalances::from_effective_balances(vec![0, 0, 32 * ETH]).unwrap();
        assert_eq!(synthetic.num_unslashed_validators(), 1);
    }

    #[test]
    fn floors_apply_when_every_active_validator_is_slashed() {
        let spec = E::default_spec();
        let mut state: BeaconState<E> = BeaconState::new(0, <_>::default(), &spec);
        let epoch = state.current_epoch();
        push_validator(&mut state, &spec, 32 * ETH, true, epoch);
        push_validator(&mut state, &spec, 31 * ETH, true, epoch);
        push_validator(&mut state, &spec, 30 * ETH, false, spec.far_future_epoch);

        let justified_balances = JustifiedBalances::from_justified_state(&state).unwrap();

        assert_eq!(justified_balances.raw_total_unslashed_balance(), 0);
        assert_eq!(
            justified_balances.total_unslashed_balance(spec.effective_balance_increment),
            spec.effective_balance_increment
        );
        // The slashed-inclusive total comes from the raw unslashed total, not the floored one.
        assert_eq!(justified_balances.raw_total_active_balance(), 63 * ETH);
        assert_eq!(
            justified_balances.total_active_balance(spec.effective_balance_increment),
            63 * ETH
        );
        assert_eq!(justified_balances.num_unslashed_validators(), 0);
    }

    #[test]
    fn floors_apply_to_an_empty_registry() {
        let spec = E::default_spec();
        let state: BeaconState<E> = BeaconState::new(0, <_>::default(), &spec);

        for justified_balances in [
            JustifiedBalances::from_justified_state(&state).unwrap(),
            JustifiedBalances::from_effective_balances(vec![]).unwrap(),
            JustifiedBalances::default(),
        ] {
            assert!(justified_balances.effective_balances().is_empty());
            assert_eq!(justified_balances.num_unslashed_validators(), 0);
            assert_eq!(justified_balances.raw_total_unslashed_balance(), 0);
            assert_eq!(justified_balances.raw_total_active_balance(), 0);
            assert_eq!(
                justified_balances.total_unslashed_balance(spec.effective_balance_increment),
                spec.effective_balance_increment
            );
            assert_eq!(
                justified_balances.total_active_balance(spec.effective_balance_increment),
                spec.effective_balance_increment
            );
            assert_eq!(justified_balances.unslashed_balance(0), 0);
            assert_eq!(justified_balances.active_balance(0), Ok(0));
        }
    }

    #[test]
    fn clones_share_an_allocation() {
        let justified_balances = JustifiedBalances::from_effective_balances(vec![32]).unwrap();
        let clone = justified_balances.clone();
        assert!(JustifiedBalances::ptr_eq(&justified_balances, &clone));

        let equal = JustifiedBalances::from_effective_balances(vec![32]).unwrap();
        assert_eq!(justified_balances, equal);
        assert!(!JustifiedBalances::ptr_eq(&justified_balances, &equal));
    }

    // The tests below build states with `state_processing` and check `try_from_state_caches`
    // against `from_justified_state`.

    /// Always-active, never-slashed validators at the start of every generated registry, so that
    /// committees, proposers and sync committees can always be computed.
    fn num_anchor_validators<E: EthSpec>() -> usize {
        E::slots_per_epoch() as usize
    }

    /// More than any test registry holds.
    const MAX_TEST_VALIDATORS: usize = 128;

    /// A distinct, valid pubkey for each validator: the pubkey cache must tell validators apart,
    /// and sync committees aggregate their pubkeys.
    fn pubkey(validator_index: usize) -> PublicKeyBytes {
        static PUBKEYS: LazyLock<Vec<PublicKeyBytes>> = LazyLock::new(|| {
            (0..MAX_TEST_VALIDATORS)
                .map(|i| generate_deterministic_keypair(i).pk.compress())
                .collect()
        });
        PUBKEYS[validator_index]
    }

    /// A genesis validator, whose balance equals its effective balance.
    #[derive(Debug, Clone)]
    struct ValidatorSetup {
        effective_balance: u64,
        /// `None` for a validator that is not yet scheduled for activation.
        activation_epoch: Option<u64>,
        exit_epoch: Option<u64>,
        withdrawable_epoch: Option<u64>,
        slashed: bool,
        compounding: bool,
    }

    impl ValidatorSetup {
        fn active(effective_balance: u64) -> Self {
            Self {
                effective_balance,
                activation_epoch: Some(0),
                exit_epoch: None,
                withdrawable_epoch: None,
                slashed: false,
                compounding: false,
            }
        }

        fn pending(effective_balance: u64) -> Self {
            Self {
                activation_epoch: None,
                ..Self::active(effective_balance)
            }
        }

        fn activating_at(mut self, epoch: u64) -> Self {
            self.activation_epoch = Some(epoch);
            self
        }

        fn exiting_at(mut self, epoch: u64) -> Self {
            self.exit_epoch = Some(epoch);
            self
        }

        fn withdrawable_at(mut self, epoch: u64) -> Self {
            self.withdrawable_epoch = Some(epoch);
            self
        }

        fn slashed(mut self) -> Self {
            self.slashed = true;
            self
        }

        fn compounding(mut self) -> Self {
            self.compounding = true;
            self
        }

        fn validator(&self, validator_index: usize, spec: &ChainSpec) -> Validator {
            let epoch = |epoch: Option<u64>| epoch.map_or(spec.far_future_epoch, Epoch::new);
            // A validator that is not yet scheduled for activation is not yet eligible either,
            // so that it stays pending (and is queued as a deposit by the Electra upgrade).
            let activation_eligibility_epoch = if self.activation_epoch.is_some() {
                Epoch::new(0)
            } else {
                spec.far_future_epoch
            };
            let exit_epoch = epoch(self.exit_epoch);
            let withdrawable_epoch = self.withdrawable_epoch.map_or_else(
                || exit_epoch.saturating_add(spec.min_validator_withdrawability_delay),
                Epoch::new,
            );
            let mut withdrawal_credentials = Hash256::ZERO;
            if self.compounding {
                withdrawal_credentials.0[0] = spec.compounding_withdrawal_prefix_byte;
            }
            Validator {
                pubkey: pubkey(validator_index),
                withdrawal_credentials,
                effective_balance: self.effective_balance,
                slashed: self.slashed,
                activation_eligibility_epoch,
                activation_epoch: epoch(self.activation_epoch),
                exit_epoch,
                withdrawable_epoch,
            }
        }
    }

    /// `E`'s spec with every fork up to and including `last_fork` scheduled at epoch 1, and no
    /// later fork.
    fn spec_with_forks_at_epoch_1<E: EthSpec>(last_fork: ForkName) -> ChainSpec {
        let mut spec = ForkName::Base.make_genesis_spec(E::default_spec());
        let fork_epoch = |fork: ForkName| (fork <= last_fork).then_some(Epoch::new(1));
        spec.altair_fork_epoch = fork_epoch(ForkName::Altair);
        spec.bellatrix_fork_epoch = fork_epoch(ForkName::Bellatrix);
        spec.capella_fork_epoch = fork_epoch(ForkName::Capella);
        spec.deneb_fork_epoch = fork_epoch(ForkName::Deneb);
        spec.electra_fork_epoch = fork_epoch(ForkName::Electra);
        spec.fulu_fork_epoch = fork_epoch(ForkName::Fulu);
        spec.gloas_fork_epoch = fork_epoch(ForkName::Gloas);
        spec
    }

    /// A phase0 genesis state with the anchor validators followed by `validators`, and all caches
    /// built.
    fn genesis_state<E: EthSpec>(
        validators: &[ValidatorSetup],
        spec: &ChainSpec,
    ) -> BeaconState<E> {
        let mut state = BeaconState::new(0, <_>::default(), spec);
        let anchors =
            vec![ValidatorSetup::active(spec.max_effective_balance); num_anchor_validators::<E>()];
        for (validator_index, setup) in anchors.iter().chain(validators).enumerate() {
            state
                .validators_mut()
                .push(setup.validator(validator_index, spec))
                .unwrap();
            state.balances_mut().push(setup.effective_balance).unwrap();
        }
        state.build_all_caches(spec).unwrap();
        state
    }

    /// Advance `state` to `slot` with `per_slot_processing`, skipping every slot including any
    /// epoch boundary.
    fn advance_to<E: EthSpec>(state: &mut BeaconState<E>, slot: Slot, spec: &ChainSpec) {
        while state.slot() < slot {
            per_slot_processing(
                state,
                None,
                GloasVerificationContext::FullVerification,
                spec,
            )
            .unwrap();
        }
    }

    /// Process the header of an otherwise empty block at the state's slot, as block processing
    /// does. Returns `false` without changing the state if the slot's proposer is slashed.
    fn apply_block_header<E: EthSpec>(state: &mut BeaconState<E>, spec: &ChainSpec) -> bool {
        let slot = state.slot();
        assert!(slot > state.latest_block_header().slot);
        let mut ctxt = ConsensusContext::new(slot);
        let proposer_index = ctxt.get_proposer_index(state, spec).unwrap();
        if state
            .get_validator(proposer_index as usize)
            .unwrap()
            .slashed
        {
            return false;
        }
        let block_header = BeaconBlockHeader {
            slot,
            proposer_index,
            parent_root: state.latest_block_header().canonical_root(),
            state_root: Hash256::ZERO,
            body_root: Hash256::repeat_byte(0xbb),
        };
        process_block_header(state, block_header, VerifyBlockRoot::False, &mut ctxt, spec).unwrap();
        true
    }

    /// Slash the validator with `slash_validator`, as a slashing included in a block does, if it
    /// is slashable.
    fn slash<E: EthSpec>(state: &mut BeaconState<E>, validator_index: usize, spec: &ChainSpec) {
        let epoch = state.current_epoch();
        let slashable = state
            .get_validator(validator_index)
            .is_ok_and(|validator| validator.is_slashable_at(epoch));
        if slashable {
            let mut ctxt = ConsensusContext::new(state.slot());
            slash_validator(state, validator_index, None, &mut ctxt, spec).unwrap();
        }
    }

    /// Append a new validator to the registry, as deposit processing does.
    fn append_validator<E: EthSpec>(state: &mut BeaconState<E>, spec: &ChainSpec) {
        let validator_index = state.validators().len();
        state
            .add_validator_to_registry(
                pubkey(validator_index),
                Hash256::ZERO,
                spec.max_effective_balance,
                spec,
            )
            .unwrap();
    }

    /// Assert that the totals agree with the per-validator balances.
    fn assert_totals_consistent(justified_balances: &JustifiedBalances) {
        let unslashed = justified_balances.effective_balances().iter().sum::<u64>();
        let slashed = justified_balances.slashed_balances().values().sum::<u64>();
        assert_eq!(justified_balances.raw_total_unslashed_balance(), unslashed);
        assert_eq!(
            justified_balances.raw_total_active_balance(),
            unslashed + slashed
        );
    }

    /// Assert that the state's caches can be used and agree with its registry, and return the
    /// balances.
    #[track_caller]
    fn assert_cache_path_matches_registry<E: EthSpec>(state: &BeaconState<E>) -> JustifiedBalances {
        let from_registry = JustifiedBalances::from_justified_state(state).unwrap();
        assert_eq!(
            JustifiedBalances::try_from_state_caches(state),
            Ok(from_registry.clone())
        );
        assert_eq!(
            JustifiedBalances::from_state(state).unwrap(),
            (from_registry.clone(), FromStatePath::Caches)
        );
        assert_eq!(
            from_registry.effective_balances().len(),
            state.validators().len()
        );
        assert_totals_consistent(&from_registry);
        from_registry
    }

    /// Assert that `try_from_state_caches` fails with `reason`, and that `from_state` falls back.
    #[track_caller]
    fn assert_fallback<E: EthSpec>(state: &BeaconState<E>, reason: StateCachesError) {
        assert_eq!(
            JustifiedBalances::try_from_state_caches(state),
            Err(reason.clone())
        );
        assert_eq!(
            JustifiedBalances::from_state(state).unwrap(),
            (
                JustifiedBalances::from_justified_state(state).unwrap(),
                FromStatePath::Fallback(reason)
            )
        );
    }

    /// Assert that `from_state` takes the `expected` path and agrees with the registry.
    #[track_caller]
    fn assert_path<E: EthSpec>(state: &BeaconState<E>, expected: FromStatePath) {
        match expected {
            FromStatePath::Caches => {
                assert_cache_path_matches_registry(state);
            }
            FromStatePath::Fallback(reason) => assert_fallback(state, reason),
        }
    }

    /// The path `from_state` takes on a state that `per_slot_processing` has advanced from
    /// `slot_before_advance`, where its caches were valid, without rebuilding them.
    ///
    /// Slot processing builds the committee and slashings caches at every epoch boundary, so
    /// only the epoch cache can be stale:
    ///
    /// - phase0 epoch processing initializes the epoch cache only for the epoch it processes;
    /// - Altair's single-pass epoch processing builds the next epoch's;
    /// - the upgrades to Altair through Electra reset it, and later upgrades keep it;
    /// - the genesis epoch's decision block is the block at its own first slot, so a cache built
    ///   at the genesis slot is keyed by the zero root and is stale at every later slot.
    fn expected_path_after_advance<E: EthSpec>(
        slot_before_advance: Slot,
        state: &BeaconState<E>,
        spec: &ChainSpec,
    ) -> FromStatePath {
        let epoch_before_advance = slot_before_advance.epoch(E::slots_per_epoch());
        let current_epoch = state.current_epoch();
        let epoch_cache_invalid =
            |reason| FromStatePath::Fallback(StateCachesError::EpochCacheInvalid(reason));

        if current_epoch == epoch_before_advance {
            if slot_before_advance == spec.genesis_slot && state.slot() > spec.genesis_slot {
                return epoch_cache_invalid(EpochCacheError::IncorrectDecisionBlock {
                    cache: Hash256::ZERO,
                    state: *state.get_block_root(spec.genesis_slot).unwrap(),
                });
            }
            return FromStatePath::Caches;
        }

        let epoch_cache_reset = [
            spec.altair_fork_epoch,
            spec.bellatrix_fork_epoch,
            spec.capella_fork_epoch,
            spec.deneb_fork_epoch,
            spec.electra_fork_epoch,
        ]
        .contains(&Some(current_epoch));
        let processed_epoch = current_epoch - 1;
        if epoch_cache_reset {
            epoch_cache_invalid(EpochCacheError::CacheNotInitialized)
        } else if spec.fork_name_at_epoch(processed_epoch) == ForkName::Base {
            epoch_cache_invalid(EpochCacheError::IncorrectEpoch {
                cache: processed_epoch,
                state: current_epoch,
            })
        } else {
            FromStatePath::Caches
        }
    }

    /// Every kind of validator the balances distinguish, including active validators with an
    /// effective balance of 0, across skipped epoch boundaries and a mid-epoch block. Not run
    /// under Gloas, for the reason given on `scenario_strategy`.
    fn check_validator_categories<E: EthSpec>(last_fork: ForkName) {
        let spec = spec_with_forks_at_epoch_1::<E>(last_fork);
        let first = num_anchor_validators::<E>();
        let zero_balance = first;
        let zero_balance_slashed = first + 1;
        let slashed = first + 2;
        let exited = first + 3;
        let exited_slashed = first + 4;
        let pending = first + 5;
        let activating = first + 6;
        let slashed_mid_epoch = first + 7;
        let exited_slashed_mid_epoch = first + 8;
        let mut state = genesis_state::<E>(
            &[
                ValidatorSetup::active(0),
                ValidatorSetup::active(0).slashed(),
                ValidatorSetup::active(31 * ETH).slashed(),
                ValidatorSetup::active(32 * ETH).exiting_at(1),
                ValidatorSetup::active(32 * ETH).exiting_at(1).slashed(),
                ValidatorSetup::pending(32 * ETH),
                ValidatorSetup::active(32 * ETH).activating_at(2),
                ValidatorSetup::active(32 * ETH),
                ValidatorSetup::active(32 * ETH).exiting_at(2),
            ],
            &spec,
        );
        assert_cache_path_matches_registry(&state);

        // Skip the epoch 1 and 2 boundary slots. Altair's epoch processing builds the next
        // epoch's epoch cache, so the cache path is taken without rebuilding the caches. Phase0's
        // leaves it at the epoch it processed, so the caches must be rebuilt.
        advance_to(&mut state, Slot::new(2 * E::slots_per_epoch() + 2), &spec);
        if last_fork == ForkName::Base {
            assert_fallback(
                &state,
                StateCachesError::EpochCacheInvalid(EpochCacheError::IncorrectEpoch {
                    cache: Epoch::new(1),
                    state: Epoch::new(2),
                }),
            );
            state.build_all_caches(&spec).unwrap();
        }
        assert_cache_path_matches_registry(&state);

        // Mid-epoch, slash an active and an exited validator in a block, then append validators.
        // Slashing updates the slashings cache but not effective balances.
        assert!(apply_block_header(&mut state, &spec));
        slash(&mut state, slashed_mid_epoch, &spec);
        slash(&mut state, exited_slashed_mid_epoch, &spec);
        assert!(state.slashings_cache().is_slashed(slashed_mid_epoch));
        assert!(state.slashings_cache().is_slashed(exited_slashed_mid_epoch));
        append_validator(&mut state, &spec);
        append_validator(&mut state, &spec);
        let epoch_cache_balances = state.epoch_cache().effective_balances().unwrap();
        assert!(epoch_cache_balances.len() < state.validators().len());
        // Inactive validators have nonzero entries in the epoch cache.
        assert_ne!(epoch_cache_balances[exited], 0);

        let justified_balances = assert_cache_path_matches_registry(&state);

        let state_effective_balance =
            |i: usize| state.validators().get(i).unwrap().effective_balance;
        assert_eq!(
            justified_balances.slashed_balances(),
            &BTreeMap::from([
                (zero_balance_slashed as u64, 0),
                (slashed as u64, state_effective_balance(slashed)),
                (slashed_mid_epoch as u64, 32 * ETH),
            ])
        );
        // The anchors, the zero-balance validator and the newly activated one.
        assert_eq!(
            justified_balances.num_unslashed_validators(),
            first as u64 + 2
        );
        for inactive in [exited, exited_slashed, pending, exited_slashed_mid_epoch] {
            assert_eq!(justified_balances.active_balance(inactive), Ok(0));
        }
        assert_eq!(justified_balances.unslashed_balance(zero_balance), 0);
        assert_eq!(
            justified_balances.unslashed_balance(activating),
            state_effective_balance(activating)
        );
        assert_eq!(
            justified_balances.active_balance(slashed_mid_epoch),
            Ok(32 * ETH)
        );
    }

    #[test]
    fn cache_path_handles_every_validator_category_phase0_minimal() {
        check_validator_categories::<MinimalEthSpec>(ForkName::Base);
    }

    #[test]
    fn cache_path_handles_every_validator_category_fulu_minimal() {
        check_validator_categories::<MinimalEthSpec>(ForkName::Fulu);
    }

    #[test]
    fn cache_path_handles_every_validator_category_phase0_mainnet() {
        check_validator_categories::<MainnetEthSpec>(ForkName::Base);
    }

    #[test]
    fn cache_path_handles_every_validator_category_electra_mainnet() {
        check_validator_categories::<MainnetEthSpec>(ForkName::Electra);
    }

    /// Electra pending deposits and consolidations, processed at an epoch transition.
    fn check_electra_pending_deposits_and_consolidations<E: EthSpec>(last_fork: ForkName) {
        let spec = spec_with_forks_at_epoch_1::<E>(last_fork);
        let first = num_anchor_validators::<E>();
        let topped_up = first;
        let consolidation_source = first + 1;
        let consolidation_target = first + 2;
        let pending = first + 3;
        let slashed_mid_epoch = first + 4;
        let mut state = genesis_state::<E>(
            &[
                ValidatorSetup::active(20 * ETH),
                ValidatorSetup::active(32 * ETH)
                    .exiting_at(1)
                    .withdrawable_at(2),
                ValidatorSetup::active(32 * ETH).compounding(),
                ValidatorSetup::pending(32 * ETH),
                ValidatorSetup::active(32 * ETH),
            ],
            &spec,
        );

        // The upgrade to Electra at epoch 1 resets the epoch cache, and moves the pending
        // validator's balance to a pending deposit, zeroing its effective balance.
        advance_to(&mut state, Slot::new(E::slots_per_epoch()), &spec);
        assert!(state.fork_name_unchecked().electra_enabled());
        assert_fallback(
            &state,
            StateCachesError::EpochCacheInvalid(EpochCacheError::CacheNotInitialized),
        );
        state.build_all_caches(&spec).unwrap();
        assert_cache_path_matches_registry(&state);
        assert_eq!(
            state.validators().get(pending).unwrap().effective_balance,
            0
        );
        assert_eq!(state.pending_deposits().unwrap().len(), 1);

        // Queue a top-up, a deposit for a new validator, and a consolidation.
        let keypair = generate_deterministic_keypair(MAX_TEST_VALIDATORS);
        let mut new_deposit = DepositData {
            pubkey: keypair.pk.compress(),
            withdrawal_credentials: Hash256::ZERO,
            amount: 16 * ETH,
            signature: SignatureBytes::empty(),
        };
        new_deposit.signature = new_deposit.create_signature(&keypair.sk, &spec);
        let mut pending_deposits = state.pending_deposits_mut().unwrap();
        pending_deposits
            .push(PendingDeposit {
                pubkey: pubkey(topped_up),
                withdrawal_credentials: Hash256::ZERO,
                amount: 8 * ETH,
                signature: SignatureBytes::empty(),
                slot: spec.genesis_slot,
            })
            .unwrap();
        pending_deposits
            .push(PendingDeposit {
                pubkey: new_deposit.pubkey,
                withdrawal_credentials: new_deposit.withdrawal_credentials,
                amount: new_deposit.amount,
                signature: new_deposit.signature,
                slot: spec.genesis_slot,
            })
            .unwrap();
        state
            .pending_consolidations_mut()
            .unwrap()
            .push(PendingConsolidation {
                source_index: consolidation_source as u64,
                target_index: consolidation_target as u64,
            })
            .unwrap();
        assert_cache_path_matches_registry(&state);

        // A mid-epoch slashing in a block.
        advance_to(&mut state, Slot::new(E::slots_per_epoch() + 3), &spec);
        assert!(apply_block_header(&mut state, &spec));
        slash(&mut state, slashed_mid_epoch, &spec);
        assert_cache_path_matches_registry(&state);

        // Process the queues at the (skipped) epoch 2 boundary. Epoch processing builds the next
        // epoch's epoch cache, so the cache path is taken without rebuilding the caches.
        let num_validators = state.validators().len();
        advance_to(&mut state, Slot::new(2 * E::slots_per_epoch() + 1), &spec);
        let justified_balances = assert_cache_path_matches_registry(&state);

        // The epoch transition applied the queues (less a little for missed attestations) and
        // updated the effective balances. The cache path agrees on all of them.
        let state_effective_balance =
            |i: usize| state.validators().get(i).unwrap().effective_balance;
        assert_eq!(state.validators().len(), num_validators + 1);
        assert!(state_effective_balance(topped_up) > 26 * ETH);
        assert_eq!(
            justified_balances.unslashed_balance(topped_up),
            state_effective_balance(topped_up)
        );
        assert!(state_effective_balance(consolidation_target) > 62 * ETH);
        assert_eq!(
            justified_balances.unslashed_balance(consolidation_target),
            state_effective_balance(consolidation_target)
        );
        // The pending validator and the new one are not active.
        assert_eq!(state_effective_balance(pending), 32 * ETH);
        assert_eq!(justified_balances.active_balance(pending), Ok(0));
        assert_eq!(state_effective_balance(num_validators), 16 * ETH);
        assert_eq!(justified_balances.active_balance(num_validators), Ok(0));
        assert_eq!(
            justified_balances.slashed_balances(),
            &BTreeMap::from([(slashed_mid_epoch as u64, 32 * ETH)])
        );
    }

    #[test]
    fn cache_path_handles_electra_pending_deposits_and_consolidations_minimal() {
        check_electra_pending_deposits_and_consolidations::<MinimalEthSpec>(ForkName::Electra);
    }

    #[test]
    fn cache_path_handles_gloas_pending_deposits_and_consolidations_minimal() {
        check_electra_pending_deposits_and_consolidations::<MinimalEthSpec>(ForkName::Gloas);
    }

    #[test]
    fn cache_path_handles_electra_pending_deposits_and_consolidations_mainnet() {
        check_electra_pending_deposits_and_consolidations::<MainnetEthSpec>(ForkName::Fulu);
    }

    #[test]
    fn each_failed_precondition_is_named() {
        let spec = E::default_spec();
        let mut state = genesis_state::<E>(&[ValidatorSetup::active(32 * ETH)], &spec);
        advance_to(&mut state, Slot::new(E::slots_per_epoch() + 1), &spec);
        state.build_all_caches(&spec).unwrap();
        let previous_epoch_cache = state.epoch_cache().clone();
        advance_to(&mut state, Slot::new(2 * E::slots_per_epoch() + 1), &spec);
        state.build_all_caches(&spec).unwrap();
        assert_cache_path_matches_registry(&state);

        let current_epoch = state.current_epoch();
        let decision_block_root = state.epoch_cache_decision_root(Hash256::ZERO).unwrap();
        let cached_effective_balances = state.epoch_cache().effective_balances().unwrap().to_vec();
        let epoch_cache = |decision_block_root, effective_balances| {
            EpochCache::new(
                EpochCacheKey {
                    epoch: current_epoch,
                    decision_block_root,
                },
                effective_balances,
                vec![],
                ActivationQueue::default(),
                &spec,
            )
        };

        // Epoch cache: uninitialized, stale, or for another decision block.
        let mut modified = state.clone();
        *modified.epoch_cache_mut() = EpochCache::default();
        assert_fallback(
            &modified,
            StateCachesError::EpochCacheInvalid(EpochCacheError::CacheNotInitialized),
        );
        *modified.epoch_cache_mut() = previous_epoch_cache;
        assert_fallback(
            &modified,
            StateCachesError::EpochCacheInvalid(EpochCacheError::IncorrectEpoch {
                cache: current_epoch - 1,
                state: current_epoch,
            }),
        );
        let other_root = Hash256::repeat_byte(0xdd);
        *modified.epoch_cache_mut() = epoch_cache(other_root, cached_effective_balances.clone());
        assert_fallback(
            &modified,
            StateCachesError::EpochCacheInvalid(EpochCacheError::IncorrectDecisionBlock {
                cache: other_root,
                state: decision_block_root,
            }),
        );

        // Current epoch committee cache: uninitialized or stale.
        let mut modified = state.clone();
        modified
            .drop_committee_cache(RelativeEpoch::Current)
            .unwrap();
        let uninitialized = StateCachesError::CommitteeCacheUninitialized(
            BeaconStateError::CommitteeCacheUninitialized(Some(RelativeEpoch::Current)),
        );
        assert_fallback(&modified, uninitialized.clone());
        let previous_committee_cache = state
            .committee_cache(RelativeEpoch::Previous)
            .unwrap()
            .clone();
        modified
            .set_committee_cache(RelativeEpoch::Current, previous_committee_cache)
            .unwrap();
        assert_fallback(&modified, uninitialized);

        // Slashings cache: uninitialized, or stale after a block.
        let latest_block_slot = state.latest_block_header().slot;
        let mut modified = state.clone();
        *modified.slashings_cache_mut() = SlashingsCache::default();
        assert_fallback(
            &modified,
            StateCachesError::SlashingsCacheUninitialized(
                BeaconStateError::SlashingsCacheUninitialized {
                    initialized_slot: None,
                    latest_block_slot,
                },
            ),
        );
        let mut modified = state.clone();
        modified.latest_block_header_mut().slot = state.slot();
        assert_fallback(
            &modified,
            StateCachesError::SlashingsCacheUninitialized(
                BeaconStateError::SlashingsCacheUninitialized {
                    initialized_slot: Some(latest_block_slot),
                    latest_block_slot: state.slot(),
                },
            ),
        );

        // An active validator missing from the epoch cache.
        let last = cached_effective_balances.len() - 1;
        assert!(
            state
                .validators()
                .get(last)
                .unwrap()
                .is_active_at(current_epoch)
        );
        let mut modified = state.clone();
        *modified.epoch_cache_mut() = epoch_cache(
            decision_block_root,
            cached_effective_balances[..last].to_vec(),
        );
        assert_fallback(
            &modified,
            StateCachesError::MissingEffectiveBalance {
                validator_index: last,
            },
        );

        // Inactive validators missing from the epoch cache are fine.
        let mut modified = state.clone();
        append_validator(&mut modified, &spec);
        append_validator(&mut modified, &spec);
        *modified.epoch_cache_mut() =
            epoch_cache(decision_block_root, cached_effective_balances.clone());
        assert_cache_path_matches_registry(&modified);
    }

    /// The debug-build cross-check catches a state whose caches miss a change to its registry.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "balances from the state caches differ from the registry")]
    fn from_state_cross_checks_the_cache_path() {
        let spec = E::default_spec();
        let mut state = genesis_state::<E>(&[ValidatorSetup::active(32 * ETH)], &spec);
        // Bypass `slash_validator`, so that the slashings cache misses the slashing.
        let validator_index = num_anchor_validators::<E>();
        state.get_validator_mut(validator_index).unwrap().slashed = true;
        let _ = JustifiedBalances::from_state(&state);
    }

    /// One round of changes to a generated state.
    #[derive(Debug, Clone)]
    struct Step {
        /// The number of slots to advance, skipping every slot.
        slots: u64,
        /// Whether to process a block header at the slot reached.
        block: bool,
        /// Validators to slash at the slot reached, as indices into the generated validators.
        slashings: Vec<prop::sample::Index>,
        /// The number of validators to append to the registry after rebuilding the caches.
        appended_validators: usize,
    }

    #[derive(Debug, Clone)]
    struct Scenario {
        last_fork: ForkName,
        validators: Vec<ValidatorSetup>,
        steps: Vec<Step>,
    }

    fn validator_setup_strategy() -> impl Strategy<Value = ValidatorSetup> {
        (
            prop::sample::select(vec![0, 1, 16, 17, 31, 32]),
            prop::option::weighted(0.8, 0..4u64),
            prop::option::weighted(0.4, 1..6u64),
            prop::bool::weighted(0.2),
            prop::bool::weighted(0.1),
        )
            .prop_map(
                |(effective_balance_eth, activation_epoch, exit_epoch, slashed, compounding)| {
                    ValidatorSetup {
                        effective_balance: effective_balance_eth * ETH,
                        activation_epoch,
                        exit_epoch,
                        withdrawable_epoch: None,
                        slashed,
                        compounding,
                    }
                },
            )
    }

    fn step_strategy(max_slots: u64) -> impl Strategy<Value = Step> {
        (
            0..=max_slots,
            any::<bool>(),
            prop::collection::vec(any::<prop::sample::Index>(), 0..4),
            0..3usize,
        )
            .prop_map(|(slots, block, slashings, appended_validators)| Step {
                slots,
                block,
                slashings,
                appended_validators,
            })
    }

    /// Gloas is left out: its payload timeliness committee is sampled by effective balance from
    /// each slot's committees, which takes very long (or forever) when they hold mostly
    /// low-balance validators.
    fn scenario_strategy<E: EthSpec>() -> impl Strategy<Value = Scenario> {
        (
            prop::sample::select(vec![
                ForkName::Base,
                ForkName::Altair,
                ForkName::Electra,
                ForkName::Fulu,
            ]),
            prop::collection::vec(validator_setup_strategy(), 0..24),
            prop::collection::vec(step_strategy(2 * E::slots_per_epoch()), 1..4),
        )
            .prop_map(|(last_fork, validators, steps)| Scenario {
                last_fork,
                validators,
                steps,
            })
    }

    fn run_scenario<E: EthSpec>(scenario: &Scenario) {
        let spec = spec_with_forks_at_epoch_1::<E>(scenario.last_fork);
        let mut state = genesis_state::<E>(&scenario.validators, &spec);
        assert_cache_path_matches_registry(&state);

        for step in &scenario.steps {
            let slot_before_advance = state.slot();
            advance_to(&mut state, slot_before_advance + step.slots, &spec);
            assert_path(
                &state,
                expected_path_after_advance(slot_before_advance, &state, &spec),
            );
            state.build_all_caches(&spec).unwrap();
            assert_cache_path_matches_registry(&state);

            if step.block && state.slot() > state.latest_block_header().slot {
                apply_block_header(&mut state, &spec);
            }
            if !scenario.validators.is_empty() {
                for index in &step.slashings {
                    let validator_index =
                        num_anchor_validators::<E>() + index.index(scenario.validators.len());
                    slash(&mut state, validator_index, &spec);
                }
            }
            for _ in 0..step.appended_validators {
                append_validator(&mut state, &spec);
            }
            assert_cache_path_matches_registry(&state);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn cache_path_matches_registry_minimal(scenario in scenario_strategy::<MinimalEthSpec>()) {
            run_scenario::<MinimalEthSpec>(&scenario);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(8))]

        #[test]
        fn cache_path_matches_registry_mainnet(scenario in scenario_strategy::<MainnetEthSpec>()) {
            run_scenario::<MainnetEthSpec>(&scenario);
        }
    }
}
