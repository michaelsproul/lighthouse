module

public import EthLibOps.SlashFrame
public import EthLib.Consensus.Proofs.Transition.Totality.SlashValidator

/-!
# `slash_validator` succeeds and has the slash frame

`slash_validator` starts the exit of a validator, marks it slashed, adds its effective balance to
the slashed balance of the epoch, takes a first penalty from its balance and pays the
whistleblower reward to the proposer of the slot.

`slashValidator_ok_frame` shows that the call succeeds on a validator that is not yet slashed,
when the state has room for it, and that the state after it has the `SlashFrame` of the state
before it. The frame is for the slashed validator alone. The exit queue moves by `exitStep` of
its effective balance, and no balance grows by more than `whistleblowerReward`.

EthLib's `slashValidator_total_of_room` gives the success. The frame is read from the steps of the
call, which `slashValidator_ok` lists.
-/

@[expose] public section

namespace EthLibOps

open EthLib EthLib.Base EthLib.Base.Uint64 EthLib.Consensus

variable {preset : Preset} {fork : Base.Fork} {config : Config}

/-- The whistleblower reward for a slashed validator of effective balance `balance`. With no
whistleblower named, `slash_validator` pays all of it to the proposer, in two parts. -/
def whistleblowerReward (preset : Preset) (fork : Base.Fork) (balance : Gwei) : Gwei :=
  balance / (parameters preset fork).WHISTLEBLOWER_REWARD_QUOTIENT

/-- A larger effective balance earns at least as large a whistleblower reward. -/
theorem whistleblowerReward_le {a : Gwei} {B : Nat} (h : a.toNat ≤ B) :
    (whistleblowerReward preset fork a).toNat ≤
      B / (parameters preset fork).WHISTLEBLOWER_REWARD_QUOTIENT.toNat := by
  unfold whistleblowerReward
  rw [UInt64.toNat_div]
  exact Nat.div_le_div_right h

/-- The epoch the next exit starts from reads the slot and the earliest exit epoch alone. -/
theorem exitStart_congr (electra : .pectra ≤ fork) {s t : BeaconState preset fork}
    (slot : t.slot = s.slot) (earliest : t.earliestExitEpoch = s.earliestExitEpoch) :
    exitStart electra t = exitStart electra s := by
  simp [exitStart, getCurrentEpoch, slot, earliest]

/-- Marking a validator slashed takes its effective balance out of the balances not yet
slashed. -/
theorem unslashedSum_of_marked {s t : BeaconState preset fork} {i : Nat}
    {validator marked : Validator} (entry : s.validators.data[i]? = some validator)
    (flag : validator.slashed = false) (isMarked : marked.slashed = true)
    (list : t.validators.data.toList = s.validators.data.toList.set i marked) :
    unslashedSum t + validator.effectiveBalance.toNat = unslashedSum s := by
  have inside : i < s.validators.data.toList.length := by
    simpa using (Array.getElem?_eq_some_iff.mp entry).1
  have value : s.validators.data.toList[i] = validator := by
    simpa using (Array.getElem?_eq_some_iff.mp entry).2
  have sum := sum_map_set unslashedBalance _ _ inside marked
  rw [← list, value] at sum
  simp only [unslashedBalance, flag, isMarked, Bool.false_eq_true, ↓reduceIte] at sum
  unfold unslashedSum
  omega

/-- `slash_validator` succeeds on a validator `i` that is not yet slashed, when the state has
room for the slashing. The state after it has the slash frame of the state before it.

* `sizes`: the balances have the registry's length.
* `proposer`, `proposerIn`: the proposer of the slot is in the registry.
* `summed`, `churnRoom`, `exits`: the room `initiateValidatorExit_ok_of_room` asks for.
* `vector`, `window`: the slashings vector has a nonzero length, and the current epoch plus that
  length fits in 64 bits.
* `slashedRoom`: the slashed balance of the epoch plus the effective balances not yet slashed
  fits in 64 bits.
* `weight`: the reward times the proposer weight fits in 64 bits.
* `rewardRoom`: the proposer's balance has room for the effective balance of the validator. -/
theorem slashValidator_ok_frame (ok : ChurnOk preset config) (electra : .pectra ≤ fork)
    (before : fork < .glamsterdam) {s : BeaconState preset fork} {i p : ValidatorIndex}
    {x : Validator} (found : getValidator s i = .ok x) (unslashed : x.slashed = false)
    (sizes : BalancesMatch s)
    (proposer : getBeaconProposerIndex s = .ok p) (proposerIn : p.toNat < s.validators.data.size)
    (summed : effectiveBalanceSum s < 2 ^ 64)
    (churnRoom : x.effectiveBalance.toNat +
      config.MAX_PER_EPOCH_ACTIVATION_EXIT_CHURN_LIMIT.toNat ≤ 2 ^ 64)
    (exits : exitStart electra s + exitStep preset x.effectiveBalance.toNat +
      config.MIN_VALIDATOR_WITHDRAWABILITY_DELAY.toNat < 2 ^ 64)
    (vector : preset.EPOCHS_PER_SLASHINGS_VECTOR ≠ 0)
    (window : (getCurrentEpoch s).toNat + preset.EPOCHS_PER_SLASHINGS_VECTOR.toNat < 2 ^ 64)
    (slashedRoom : slashedNow s + unslashedSum s < 2 ^ 64)
    (weight : (whistleblowerReward preset fork x.effectiveBalance).toNat * PROPOSER_WEIGHT.toNat <
      2 ^ 64)
    (rewardRoom : ∀ b, getBalance s p = .ok b → b.toNat + x.effectiveBalance.toNat < 2 ^ 64) :
    ∃ s', slashValidator config s i = .ok s' ∧
      SlashFrame electra (fun j => j = i) (exitStep preset x.effectiveBalance.toNat)
        (whistleblowerReward preset fork x.effectiveBalance).toNat s s' := by
  have entry := getValidator_eq_ok_iff.mp found
  have inside : i.toNat < s.validators.data.size := (Array.getElem?_eq_some_iff.mp entry).1
  obtain ⟨exited, exitedOk, effect⟩ :=
    initiateValidatorExit_ok_of_room ok electra before found summed churnRoom exits
  -- The call succeeds: EthLib's room holds.
  have balanceAt : ∀ j : ValidatorIndex, j.toNat < s.validators.data.size →
      ∃ b, getBalance s j = .ok b := by
    intro j hj
    have hb : j.toNat < s.balances.data.size := by
      rw [show s.balances.data.size = s.validators.data.size from sizes]
      exact hj
    exact ⟨_, getBalance_eq_ok_iff.mpr (Array.getElem?_eq_getElem hb)⟩
  obtain ⟨bp, bpOk⟩ := balanceAt p proposerIn
  have room : SlashRoom config s x i :=
    { exit := ⟨exited, exitedOk⟩
      window := window
      vector := vector
      slashed := fun amount read => by
        have now : slashedNow s = amount.toNat := by simp [slashedNow, read]
        have := effectiveBalance_le_unslashedSum found unslashed
        omega
      reward := fun _ => weight
      proposer := ⟨p, bp, proposer, bpOk, rewardRoom bp bpOk⟩
      balance := balanceAt i inside }
  obtain ⟨after, computed⟩ := slashValidator_total_of_room found room
  refine ⟨after, computed, ?_⟩
  -- The balances after the call.
  obtain ⟨payee, share', -, shareOk', paid⟩ := balance_slashValidator found computed
  -- The steps of the call.
  obtain ⟨exited', validator, withdrawable, marked, amount, total, slashings, lowered, proposer',
    share, raised, last, exitedOk', readOk, -, markedOk, amountOk, totalOk, slashingsOk, loweredOk,
    -, -, raisedOk, lastOk, done⟩ := slashValidator_ok computed
  cases exitedOk.symm.trans exitedOk'
  have gloas : ¬ .glamsterdam ≤ fork := Nat.not_le.mpr before
  rw [dite_eq_right gloas] at done
  subst done
  -- The record the exit leaves is the one the call reads back.
  obtain ⟨y, hy, shape⟩ := effect.self
  have same : validator = y :=
    Option.some.inj ((getValidator_eq_ok_iff.mp readOk).symm.trans hy)
  subst same
  have flag : validator.slashed = false := by
    rw [shape]
    exact unslashed
  have balanceEq : validator.effectiveBalance = x.effectiveBalance := by
    rw [shape]
  obtain ⟨markInside, hmarked⟩ := setValidator_ok markedOk
  obtain ⟨lowInside, hlowered⟩ := decreaseBalance_eq_ok_iff.mp loweredOk
  obtain ⟨raiseInside, -, hraised⟩ := increaseBalance_eq_ok_iff.mp raisedOk
  obtain ⟨lastInside, -, hlast⟩ := increaseBalance_eq_ok_iff.mp lastOk
  -- What the steps after the exit keep, and what they write.
  have keeps : after.slot = exited.slot ∧ after.forkVersions = exited.forkVersions ∧
      after.genesisValidatorsRoot = exited.genesisValidatorsRoot ∧
      after.proposerLookahead = exited.proposerLookahead ∧
      after.pendingPartialWithdrawals = exited.pendingPartialWithdrawals ∧
      after.earliestExitEpoch = exited.earliestExitEpoch ∧ after.slashings = slashings ∧
      after.validators.data = exited.validators.data.set i.toNat
        { validator with
          slashed := true
          withdrawableEpoch := max validator.withdrawableEpoch withdrawable } markInside ∧
      after.balances.data.size = exited.balances.data.size := by
    subst hlast hraised hlowered hmarked
    refine ⟨rfl, rfl, rfl, rfl, rfl, rfl, rfl, ?_, ?_⟩
    · simp [BeaconState.putBalance, BeaconState.putValidator]
    · simp [BeaconState.putBalance, BeaconState.putValidator]
  obtain ⟨slotEq, forkEq, rootEq, lookEq, pendEq, earliestEq, slashEq, valEq, balSizeEq⟩ := keeps
  -- The slashed balance of the epoch after the call.
  have amountS : readCircular s.slashings.elements (getCurrentEpoch s) = .ok amount := by
    rw [← effect.slashings]
    subst hmarked
    exact amountOk
  have written : writeCircular exited.slashings.elements (getCurrentEpoch s) total =
      .ok slashings.elements := by
    have := writeCircularRooted_elements exited.slashings (getCurrentEpoch s) total
    rw [← this]
    have slashingsOk' : writeCircularRooted exited.slashings (getCurrentEpoch s) total =
        .ok slashings := by
      subst hmarked
      exact slashingsOk
    rw [slashingsOk']
    rfl
  have readBack := readCircular_writeCircular written
  have totalValue := (addChecked_ok_iff.mp totalOk).2
  refine ⟨slotEq.trans effect.slot, forkEq.trans effect.forkVersions,
    rootEq.trans effect.genesisValidatorsRoot, lookEq.trans effect.proposerLookahead,
    pendEq.trans effect.pending, ?_, ?_, fun j hj => ?_, fun j a b ha hb => ?_, ?_,
    fun j b b' hb hb' => ?_, ?_⟩
  · rw [valEq, Array.size_set]
    exact effect.size
  · rw [balSizeEq, effect.balances]
  · have differ : j.toNat ≠ i.toNat := fun eq => hj (UInt64.toNat_inj.mp eq)
    rw [valEq, Array.getElem?_set_ne _ (Ne.symm differ)]
    exact effect.others _ differ
  · rw [valEq] at hb
    by_cases same : j = i.toNat
    · subst same
      cases entry.symm.trans ha
      rw [Array.getElem?_set_self] at hb
      cases hb
      exact ⟨by rw [shape], by rw [shape]⟩
    · rw [Array.getElem?_set_ne _ (Ne.symm same)] at hb
      cases ((effect.others j same).symm.trans hb).symm.trans ha
      exact ⟨rfl, rfl⟩
  · rw [exitStart_congr electra slotEq earliestEq]
    exact effect.start
  · obtain ⟨b'', hb'', value⟩ := paid j b hb
    cases hb''.symm.trans hb'
    have shareLe := proposerShare_le shareOk'
    simp only [Option.getD_none] at value
    change _ ≤ _ + (x.effectiveBalance / (parameters preset fork).WHISTLEBLOWER_REWARD_QUOTIENT).toNat
    by_cases h1 : i.toNat = j <;> by_cases h2 : payee.toNat = j <;>
      simp only [h1, h2, ↓reduceIte] at value <;> omega
  · -- The slashed balance grows by the effective balance that stops being unslashed.
    have nowS : slashedNow s = amount.toNat := by simp [slashedNow, amountS]
    have nowExited : slashedNow exited = slashedNow s := by
      simp [slashedNow, getCurrentEpoch, effect.slot, effect.slashings]
    have exitedSum := (effect.slashFrame found).slashed
    have sumAfter := unslashedSum_of_marked (t := after)
      (marked := { validator with
        slashed := true
        withdrawableEpoch := max validator.withdrawableEpoch withdrawable })
      hy flag rfl (by rw [valEq]; simp)
    have nowAfter : slashedNow after = total.toNat := by
      have epochEq : getCurrentEpoch after = getCurrentEpoch s := by
        simp [getCurrentEpoch, slotEq, effect.slot]
      simp [slashedNow, epochEq, slashEq, readBack]
    omega

end EthLibOps
