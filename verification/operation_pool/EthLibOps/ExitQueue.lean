module

public import EthLib.Consensus.Proofs.Transition.Totality.RegistryElectra
public import EthLib.Consensus.Proofs.Mutators

/-!
# The exit queue has room for one more exit

`initiate_validator_exit` gives a validator its place in the exit queue. From Electra the queue
counts balance: `compute_exit_epoch_and_update_churn` spends the churn of the earliest open epoch
and moves on by whole epochs when the churn runs out.

EthLib checks every sum and every product of that computation, so an exit can fail. This file
shows that it succeeds when the queue has room, and says how far it moves the queue:

* `churnQueue_ok`: the queue arithmetic succeeds, and the exit lands at most
  `amount / churn + 1` epochs after the later of the two epochs it starts from.
* `initiateValidatorExit_ok_of_room`: an exit of a validator in the registry succeeds, and has
  the `ExitEffect` on the state.

`exitStart` is the epoch the next exit starts from. `exitStep` bounds the epochs one exit of a
given effective balance moves it.
-/

@[expose] public section

namespace EthLibOps

open EthLib EthLib.Base EthLib.Base.Uint64 EthLib.Consensus

variable {preset : Preset} {fork : Base.Fork} {config : Config}

/-- The constants the exit churn divides and compares: the increment is positive, and both the
least churn and the largest churn of activations and exits are at least one increment. -/
structure ChurnOk (preset : Preset) (config : Config) : Prop where
  /-- `EFFECTIVE_BALANCE_INCREMENT` is positive. -/
  increment : 0 < preset.EFFECTIVE_BALANCE_INCREMENT
  /-- `MIN_PER_EPOCH_CHURN_LIMIT_ELECTRA` is at least one increment. -/
  minimum : preset.EFFECTIVE_BALANCE_INCREMENT ≤ config.MIN_PER_EPOCH_CHURN_LIMIT_ELECTRA
  /-- `MAX_PER_EPOCH_ACTIVATION_EXIT_CHURN_LIMIT` is at least one increment. -/
  maximum :
    preset.EFFECTIVE_BALANCE_INCREMENT ≤ config.MAX_PER_EPOCH_ACTIVATION_EXIT_CHURN_LIMIT

/-- The fork is Fulu: from the Fulu upgrade, before Gloas. The forks that change the blob
parameters alone lie in this range. -/
structure IsFulu (fork : Base.Fork) : Prop where
  /-- The Fulu upgrade has happened. -/
  fulu : .fusaka ≤ fork
  /-- The Gloas upgrade has not. -/
  before : fork < .glamsterdam

/-- Fulu is after Electra. -/
theorem IsFulu.electra (h : IsFulu fork) : .pectra ≤ fork :=
  Nat.le_trans (by decide) h.fulu

/-- The epoch the next exit starts from: `compute_exit_epoch_and_update_churn` takes the later of
`earliest_exit_epoch` and `E + 1 + MAX_SEED_LOOKAHEAD`, for the current epoch `E`. -/
def exitStart (electra : .pectra ≤ fork) (s : BeaconState preset fork) : Nat :=
  max (s.earliestExitEpoch electra).toNat
    ((getCurrentEpoch s).toNat + 1 + preset.MAX_SEED_LOOKAHEAD.toNat)

/-- The epochs one exit of effective balance `balance` can move the exit queue. The churn of an
epoch is at least one increment. -/
def exitStep (preset : Preset) (balance : Nat) : Nat :=
  balance / preset.EFFECTIVE_BALANCE_INCREMENT.toNat + 1

/-- A larger exit can move the queue at least as far. -/
theorem exitStep_mono {a b : Nat} (h : a ≤ b) : exitStep preset a ≤ exitStep preset b := by
  unfold exitStep
  have := Nat.div_le_div_right (c := preset.EFFECTIVE_BALANCE_INCREMENT.toNat) h
  omega

/-- The queue arithmetic succeeds when the churn is positive, the amount plus one churn fits in
64 bits, and the start plus the amount in churns, plus one, fits too. The exit lands at most
`amount / churn + 1` epochs after the start. -/
theorem churnQueue_ok {earliest first churn left amount : UInt64}
    (positive : 0 < churn.toNat)
    (epochRoom : max earliest.toNat first.toNat + amount.toNat / churn.toNat + 1 < 2 ^ 64)
    (churnRoom : amount.toNat + churn.toNat ≤ 2 ^ 64) :
    ∃ epoch rest, churnQueue earliest first churn left amount = .ok (epoch, rest) ∧
      epoch.toNat ≤ max earliest.toNat first.toNat + amount.toNat / churn.toNat + 1 := by
  unfold churnQueue
  dsimp only
  rw [← toNat_max] at epochRoom ⊢
  generalize max earliest first = start at epochRoom ⊢
  generalize (if earliest < start then churn else left) = available
  split
  · exact ⟨_, _, rfl, Nat.le_trans (Nat.le_add_right _ _) (Nat.le_add_right _ _)⟩
  · rename_i over
    have over : available.toNat < amount.toNat := by
      rw [UInt64.le_iff_toNat_le] at over
      omega
    have nonzero : churn ≠ 0 := by
      intro zero
      subst zero
      simp at positive
    -- `amount - available - 1` does not wrap.
    have difference : (amount - available).toNat = amount.toNat - available.toNat :=
      UInt64.toNat_sub_of_le _ _ (UInt64.le_iff_toNat_le.mpr (Nat.le_of_lt over))
    have one : (1 : UInt64) ≤ amount - available := by
      rw [UInt64.le_iff_toNat_le, difference]
      show 1 ≤ _
      omega
    have rest : (amount - available - 1).toNat = amount.toNat - available.toNat - 1 := by
      rw [UInt64.toNat_sub_of_le _ _ one, difference]
      rfl
    have quotientValue : ((amount - available - 1) / churn).toNat =
        (amount.toNat - available.toNat - 1) / churn.toNat := by
      rw [UInt64.toNat_div, rest]
    generalize named : (amount - available - 1) / churn = quotient at quotientValue
    have divided : divChecked (amount - available - 1) churn = .ok quotient := by
      simp only [divChecked, nonzero, ↓reduceIte, named]
    have quotientLe : quotient.toNat ≤ amount.toNat / churn.toNat :=
      quotientValue ▸ Nat.div_le_div_right (by omega)
    have quotientMul : quotient.toNat * churn.toNat ≤ amount.toNat - available.toNat - 1 :=
      quotientValue ▸ Nat.div_mul_le_self _ _
    have additional : (quotient + 1).toNat = quotient.toNat + 1 :=
      toNat_add_of_lt (by simp; omega)
    have epochFits : start.toNat + (quotient + 1).toNat < 2 ^ 64 := by omega
    have productFits : (quotient + 1).toNat * churn.toNat < 2 ^ 64 := by
      rw [additional, Nat.add_mul, Nat.one_mul]
      omega
    have productValue : ((quotient + 1) * churn).toNat = (quotient + 1).toNat * churn.toNat := by
      rw [UInt64.toNat_mul, Nat.mod_eq_of_lt productFits]
    have sumFits : available.toNat + ((quotient + 1) * churn).toNat < 2 ^ 64 := by
      rw [productValue, additional, Nat.add_mul, Nat.one_mul]
      omega
    simp only [divided, addChecked_of_lt epochFits, mulChecked_of_lt productFits,
      addChecked_of_lt sumFits, bind, Except.bind, pure, Except.pure]
    refine ⟨_, _, rfl, ?_⟩
    rw [toNat_add_of_lt epochFits]
    omega

/-- Before Gloas the registry is within its limit: the list type carries the bound. -/
theorem registryWithinLimit (before : fork < .glamsterdam) (s : BeaconState preset fork) :
    s.RegistryWithinLimit :=
  s.validators.bound before

/-- Before Gloas the churn of exits succeeds when the effective balances sum below `2 ^ 64`. It
is at least one increment and at most `MAX_PER_EPOCH_ACTIVATION_EXIT_CHURN_LIMIT`. -/
theorem exitChurnLimit_ok (ok : ChurnOk preset config) (before : fork < .glamsterdam)
    {s : BeaconState preset fork} (summed : effectiveBalanceSum s < 2 ^ 64) :
    ∃ churn, exitChurnLimit config s = .ok churn ∧
      preset.EFFECTIVE_BALANCE_INCREMENT.toNat ≤ churn.toNat ∧
      churn.toNat ≤ config.MAX_PER_EPOCH_ACTIVATION_EXIT_CHURN_LIMIT.toNat := by
  obtain ⟨total, totalOk, -, -⟩ :=
    getTotalActiveBalance_exists_ok (registryWithinLimit before s) summed
  have gloas : ¬ .glamsterdam ≤ fork := Nat.not_le.mpr before
  obtain ⟨lower, -⟩ := roundToIncrement_share_bounds (preset := preset) (config := config) total
    config.CHURN_LIMIT_QUOTIENT ok.increment ok.minimum
  have maximum := UInt64.le_iff_toNat_le.mp ok.maximum
  refine ⟨_, by
    simp only [exitChurnLimit, gloas, ↓reduceIte, getActivationExitChurnLimit,
      getBalanceChurnLimit, totalOk, bind, Except.bind, pure, Except.pure]; rfl, ?_, ?_⟩
  · rw [toNat_min]
    omega
  · rw [toNat_min]
    omega

/-- `ExitEffect electra i v E s s'`: what `initiate_validator_exit` of validator `i`, whose
record on `s` is `v`, does to the state. Only the record of `i` and the two churn fields change.
The record of `i` changes in its exit epoch and its withdrawable epoch alone. The epoch the next
exit starts from moves by `E` epochs at most. -/
structure ExitEffect (electra : .pectra ≤ fork) (i : ValidatorIndex) (v : Validator) (E : Nat)
    (s s' : BeaconState preset fork) : Prop where
  slot : s'.slot = s.slot
  forkVersions : s'.forkVersions = s.forkVersions
  genesisValidatorsRoot : s'.genesisValidatorsRoot = s.genesisValidatorsRoot
  proposerLookahead : s'.proposerLookahead = s.proposerLookahead
  pending : s'.pendingPartialWithdrawals = s.pendingPartialWithdrawals
  balances : s'.balances = s.balances
  slashings : s'.slashings = s.slashings
  size : s'.validators.data.size = s.validators.data.size
  others : ∀ j, j ≠ i.toNat → s'.validators.data[j]? = s.validators.data[j]?
  self : ∃ v', s'.validators.data[i.toNat]? = some v' ∧
    v' = { v with exitEpoch := v'.exitEpoch, withdrawableEpoch := v'.withdrawableEpoch }
  start : exitStart electra s' ≤ exitStart electra s + E

/-- An exit of a validator in the registry succeeds when the queue has room for it: the epoch the
exit starts from, plus `exitStep` of the validator's effective balance, plus
`MIN_VALIDATOR_WITHDRAWABILITY_DELAY`, is below `2 ^ 64`. -/
theorem initiateValidatorExit_ok_of_room (ok : ChurnOk preset config) (electra : .pectra ≤ fork)
    (before : fork < .glamsterdam) {s : BeaconState preset fork} {i : ValidatorIndex}
    {v : Validator} (found : getValidator s i = .ok v)
    (summed : effectiveBalanceSum s < 2 ^ 64)
    (churnRoom : v.effectiveBalance.toNat +
      config.MAX_PER_EPOCH_ACTIVATION_EXIT_CHURN_LIMIT.toNat ≤ 2 ^ 64)
    (room : exitStart electra s + exitStep preset v.effectiveBalance.toNat +
      config.MIN_VALIDATOR_WITHDRAWABILITY_DELAY.toNat < 2 ^ 64) :
    ∃ s', initiateValidatorExit config s i = .ok s' ∧
      ExitEffect electra i v (exitStep preset v.effectiveBalance.toNat) s s' := by
  have entry : s.validators.data[i.toNat]? = some v := getValidator_eq_ok_iff.mp found
  have inside : i.toNat < s.validators.data.size := (Array.getElem?_eq_some_iff.mp entry).1
  by_cases far : v.exitEpoch = FAR_FUTURE_EPOCH
  · -- `step` stands for the `exitStep` of the validator, so that the bounds below are linear.
    have stepDef : exitStep preset v.effectiveBalance.toNat =
        v.effectiveBalance.toNat / preset.EFFECTIVE_BALANCE_INCREMENT.toNat + 1 := rfl
    generalize exitStep preset v.effectiveBalance.toNat = step at room stepDef ⊢
    -- The first epoch an exit may take.
    unfold exitStart at room
    have firstFits : (getCurrentEpoch s).toNat + (1 : UInt64).toNat < 2 ^ 64 := by
      simp only [UInt64.toNat_one]
      omega
    have firstValue : (getCurrentEpoch s + 1).toNat = (getCurrentEpoch s).toNat + 1 := by
      rw [toNat_add_of_lt firstFits]
      rfl
    have lookaheadFits :
        (getCurrentEpoch s + 1).toNat + preset.MAX_SEED_LOOKAHEAD.toNat < 2 ^ 64 := by omega
    have firstOk : computeActivationExitEpoch preset (getCurrentEpoch s) =
        .ok (getCurrentEpoch s + 1 + preset.MAX_SEED_LOOKAHEAD) := by
      simp only [computeActivationExitEpoch, addChecked_of_lt firstFits,
        addChecked_of_lt lookaheadFits, bind, Except.bind]
    have firstNat : (getCurrentEpoch s + 1 + preset.MAX_SEED_LOOKAHEAD).toNat =
        (getCurrentEpoch s).toNat + 1 + preset.MAX_SEED_LOOKAHEAD.toNat := by
      rw [toNat_add_of_lt lookaheadFits, firstValue]
    -- The churn of exits, and the place in the queue.
    obtain ⟨churn, churnOk, churnLower, churnUpper⟩ := exitChurnLimit_ok ok before summed
    have incrementPos : 0 < preset.EFFECTIVE_BALANCE_INCREMENT.toNat :=
      UInt64.lt_iff_toNat_lt.mp ok.increment
    have stepLe : v.effectiveBalance.toNat / churn.toNat ≤
        v.effectiveBalance.toNat / preset.EFFECTIVE_BALANCE_INCREMENT.toNat :=
      Nat.div_le_div_left churnLower incrementPos
    obtain ⟨epoch, left, queueOk, epochLe⟩ := churnQueue_ok
      (earliest := s.earliestExitEpoch electra)
      (first := getCurrentEpoch s + 1 + preset.MAX_SEED_LOOKAHEAD) (churn := churn)
      (left := s.exitBalanceToConsume electra) (amount := v.effectiveBalance) (by omega)
      (by rw [firstNat]; omega) (by omega)
    rw [firstNat] at epochLe
    have delayFits :
        epoch.toNat + config.MIN_VALIDATOR_WITHDRAWABILITY_DELAY.toNat < 2 ^ 64 := by omega
    let changed : Validator := { v with
      exitEpoch := epoch, withdrawableEpoch := epoch + config.MIN_VALIDATOR_WITHDRAWABILITY_DELAY }
    let queued : BeaconState preset fork := { s with
      exitBalanceToConsume := fun _ => left, earliestExitEpoch := fun _ => epoch }
    refine ⟨queued.putValidator i.toNat inside changed, ?_, rfl, rfl, rfl, rfl, rfl, rfl, rfl,
      ?_, ?_, ?_, ?_⟩
    · simp only [initiateValidatorExit, found, far, bne_self_eq_false, exitQueue, electra,
        ↓reduceDIte, computeExitEpochAndUpdateChurn, firstOk, churnOk, queueOk, liftArith,
        addChecked_of_lt delayFits, Except.mapError, bind, Except.bind, Bool.false_eq_true,
        ↓reduceIte, pure, Except.pure]
      simp [BeaconState.setValidator, inside, queued, changed]
    · simp [BeaconState.putValidator, queued]
    · intro j different
      simp only [BeaconState.putValidator, Serialization.Ssz.RootedForkList.data_set, queued]
      exact Array.getElem?_set_ne inside (Ne.symm different)
    · exact ⟨changed, by simp [BeaconState.putValidator, queued, inside], rfl⟩
    · show max epoch.toNat _ ≤ _
      have : getCurrentEpoch (queued.putValidator i.toNat inside changed) = getCurrentEpoch s :=
        rfl
      rw [this]
      unfold exitStart
      omega
  · refine ⟨s, ?_, rfl, rfl, rfl, rfl, rfl, rfl, rfl, rfl, fun _ _ => rfl, ⟨v, entry, rfl⟩,
      Nat.le_add_right _ _⟩
    simp [initiateValidatorExit, found, far, bind, Except.bind, pure, Except.pure]

end EthLibOps
