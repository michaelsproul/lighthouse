import StateProcessing.VerifyExit

/-!
# The `verify_proposer_slashing` core

`verify_proposer_slashing` in
`consensus/state_processing/src/per_block_processing/verify_proposer_slashing.rs` delegates the
check that reads the proposer's record to `verify_proposer_slashing_validator_checks`, which is
free of `EthSpec`. This file states what that function computes.

It returns `Ok` exactly when the proposer exists and its record passes `SlashableChecks`. It
cannot panic: the theorem gives the returned value.
-/

open Aeneas Aeneas.Std Result

namespace state_processing

open types.core.slot_epoch (Epoch)
open types.validator.validator (Validator)
open per_block_processing.verify_proposer_slashing
open per_block_processing.errors

/-- `Validator::is_slashable_at`: not slashed, and `activation_epoch <= epoch <
withdrawable_epoch`. -/
theorem is_slashable_at_eq (v : Validator) (epoch : Epoch) :
    types.validator.validator.Validator.is_slashable_at v epoch =
      ok (decide (v.slashed = false ∧ v.activation_epoch.val ≤ epoch.val ∧
        epoch.val < v.withdrawable_epoch.val)) := by
  simp only [types.validator.validator.Validator.is_slashable_at, epoch_le, epoch_lt]
  cases hs : v.slashed <;> by_cases h : v.activation_epoch.val ≤ epoch.val <;> simp [h]

/-- The conditions on a validator's record under which it can be slashed at `currentEpoch`. -/
structure SlashableChecks (v : Validator) (currentEpoch : Epoch) : Prop where
  /-- The validator has not been slashed. -/
  not_slashed : v.slashed = false
  /-- The validator has been activated. -/
  active_from : v.activation_epoch.val ≤ currentEpoch.val
  /-- The validator's withdrawable epoch has not been reached. -/
  not_withdrawable : currentEpoch.val < v.withdrawable_epoch.val

/-- `verify_proposer_slashing_validator_checks` never panics, and returns `Ok` exactly when the
proposer exists and its record passes `SlashableChecks`. -/
theorem verify_proposer_slashing_validator_checks_ok_iff (proposer : Option Validator)
    (proposerIndex : U64) (currentEpoch : Epoch) :
    ∃ r, verify_proposer_slashing_validator_checks proposer proposerIndex currentEpoch = ok r ∧
      (r = core.result.Result.Ok () ↔ ∃ v, proposer = some v ∧ SlashableChecks v currentEpoch) := by
  cases proposer with
  | none =>
    refine ⟨.Err (.Invalid (.ProposerUnknown proposerIndex)), ?_, by simp⟩
    simp [verify_proposer_slashing_validator_checks, error, BlockOperationError.invalid]
  | some v =>
    simp only [verify_proposer_slashing_validator_checks, is_slashable_at_eq, Option.some.injEq,
      exists_eq_left']
    by_cases h : v.slashed = false ∧ v.activation_epoch.val ≤ currentEpoch.val ∧
        currentEpoch.val < v.withdrawable_epoch.val
    · refine ⟨.Ok (), by simp [h], ?_⟩
      simp only [true_iff]
      exact ⟨h.1, h.2.1, h.2.2⟩
    · refine ⟨.Err (.Invalid (.ProposerNotSlashable proposerIndex)), ?_, ?_⟩
      · simp [h, BlockOperationError.invalid]
      · simp only [reduceCtorEq, false_iff]
        exact fun hc => h ⟨hc.not_slashed, hc.active_from, hc.not_withdrawable⟩

end state_processing
