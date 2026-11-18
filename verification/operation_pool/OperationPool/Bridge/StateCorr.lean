import StateProcessing.VerifyExit
import StateProcessing.VerifyProposerSlashing
import OperationPool.Bridge.Scalars
import EthCLSpecs.Proofs.Fulu.VoluntaryExits
import EthCLSpecs.Proofs.Fulu.ProposerSlashings

/-!
# From Lighthouse's checks to the spec's gates

Lighthouse's `verify_exit` reads four things from its `BeaconState`: the exiting validator's
record, the current epoch, the validator's pending balance to withdraw, and two constants of the
chain spec. `verify_proposer_slashing` reads the proposer's record and the current epoch.
`StateView` names those reads. `StateCorr view s` says that they agree with the spec state `s`.
That agreement is TRUSTED: `BeaconState<E>` and its accessors are not extracted.

Three theorems are the bridge. `exitGates_of_coresOk`: when both extracted cores of
`verify_exit` accept an exit on a view that corresponds to `s`, the spec's `ExitGates` hold for
that exit on `s`. `proposerSlashingGates_of_coreOk`: when the extracted core of
`verify_proposer_slashing` accepts a proposer, the spec's `ProposerSlashingGates` hold for a
slashing of that proposer. `isSlashableValidator_of_slashableOn`: a validator that the extracted
`Validator::is_slashable_at` accepts is slashable by the spec's `is_slashable_validator`.

Names: this file opens the spec's namespace. Extracted Rust items are written with their full
names, because both sides define `Validator`, `Epoch` and `VoluntaryExit`.
-/

namespace OperationPool.Bridge

open EthCLLib.Spec (HasherTag)
open EthCLSpecs.Fulu
open EthCLSpecs.Proofs.Fulu

/-- The Rust `Validator`. -/
abbrev RValidator : Type := state_processing.types.validator.validator.Validator

/-- The Rust `VoluntaryExit`. -/
abbrev RVoluntaryExit : Type := state_processing.types.exit.voluntary_exit.VoluntaryExit

/-- What `verify_exit` reads from Lighthouse's `BeaconState` and `ChainSpec`. -/
structure StateView where
  /-- `state.validators().get(index as usize)`. -/
  validatorAt : Aeneas.Std.U64 → Option RValidator
  /-- `state.current_epoch()`. -/
  currentEpoch : Aeneas.Std.U64
  /-- `state.get_pending_balance_to_withdraw(index as usize).ok()`. -/
  pendingAt : Aeneas.Std.U64 → Option Aeneas.Std.U64
  /-- `spec.far_future_epoch`. -/
  farFutureEpoch : Aeneas.Std.U64
  /-- `spec.shard_committee_period`. -/
  shardCommitteePeriod : Aeneas.Std.U64

/-- The core of `verify_proposer_slashing` accepts the proposer on the view. -/
def ProposerCoreOk (view : StateView) (proposerIndex : Aeneas.Std.U64) : Prop :=
  state_processing.per_block_processing.verify_proposer_slashing.verify_proposer_slashing_validator_checks
      (view.validatorAt proposerIndex) proposerIndex view.currentEpoch =
    Aeneas.Std.Result.ok (Aeneas.Std.core.result.Result.Ok ())

/-- The view has a validator at `index`, and `Validator::is_slashable_at` accepts it at the
current epoch. This is the check that `AttesterSlashingMaxCover::new` makes on each validator
that both attestations of a slashing name. -/
def SlashableOn (view : StateView) (index : Aeneas.Std.U64) : Prop :=
  ∃ v, view.validatorAt index = some v ∧
    state_processing.types.validator.validator.Validator.is_slashable_at v view.currentEpoch =
      Aeneas.Std.Result.ok true

/-- Both cores of `verify_exit` accept the exit on the view. -/
def CoresOk (view : StateView) (exit : RVoluntaryExit) : Prop :=
  state_processing.per_block_processing.verify_exit.verify_exit_validator_checks
      (view.validatorAt exit.validator_index) exit view.currentEpoch view.farFutureEpoch
      view.shardCommitteePeriod =
    Aeneas.Std.Result.ok (Aeneas.Std.core.result.Result.Ok ()) ∧
  state_processing.per_block_processing.verify_exit.verify_exit_pending_balance
      exit.validator_index (view.pendingAt exit.validator_index) =
    Aeneas.Std.Result.ok (Aeneas.Std.core.result.Result.Ok ())

section

variable [Preset] [Config]

/-- The reads of `view` agree with the spec state `s`. -/
structure StateCorr (view : StateView) (s : BeaconState) : Prop where
  /-- The current epoch is the epoch of the state's slot. -/
  epoch : u64ToSpec view.currentEpoch = computeEpochAtSlot s.slot
  /-- `far_future_epoch` is the spec's `FAR_FUTURE_EPOCH`. -/
  farFuture : u64ToSpec view.farFutureEpoch = Const.farFutureEpoch
  /-- `shard_committee_period` is the spec's `SHARD_COMMITTEE_PERIOD`. -/
  period : u64ToSpec view.shardCommitteePeriod = Const.shardCommitteePeriod
  /-- A validator the view returns is in the registry, with the same three epochs and the same
  slashed flag. -/
  validator : ∀ (i : Aeneas.Std.U64) (v : RValidator), view.validatorAt i = some v →
    i.val < s.validators.val.size ∧
    (s.validators[i.val]!).activationEpoch = u64ToSpec v.activation_epoch ∧
    (s.validators[i.val]!).exitEpoch = u64ToSpec v.exit_epoch ∧
    (s.validators[i.val]!).withdrawableEpoch = u64ToSpec v.withdrawable_epoch ∧
    (s.validators[i.val]!).slashed = v.slashed
  /-- The pending balance was computed: the state has a pending partial withdrawals queue, and
  the sum over it did not overflow. -/
  pendingSome : ∀ i : Aeneas.Std.U64, ∃ p, view.pendingAt i = some p
  /-- The pending balance is at least each pending amount of the validator, as a sum that did
  not overflow is. -/
  pendingBound : ∀ (i p : Aeneas.Std.U64), view.pendingAt i = some p →
    ∀ q ∈ s.pendingPartialWithdrawals.val, q.validatorIndex = u64ToSpec i →
      q.amount.toNat ≤ p.val

/-- An exit that both cores of `verify_exit` accept passes the spec's gates on the corresponding
state. `sve` is the spec's form of the exit: same validator index and same epoch. -/
theorem exitGates_of_coresOk {view : StateView} {s : BeaconState} (hcorr : StateCorr view s)
    {exit : RVoluntaryExit} {sve : SignedVoluntaryExit}
    (hidx : sve.message.validatorIndex = u64ToSpec exit.validator_index)
    (hepoch : sve.message.epoch = u64ToSpec exit.epoch)
    (hok : CoresOk view exit) : ExitGates s sve := by
  obtain ⟨hchecks, hpending⟩ := hok
  -- The record checks give a validator that passes `ExitChecks`.
  obtain ⟨r, hr, hiff⟩ := state_processing.verify_exit_validator_checks_ok_iff
    (view.validatorAt exit.validator_index) exit view.currentEpoch view.farFutureEpoch
    view.shardCommitteePeriod
  have hrOk : r = Aeneas.Std.core.result.Result.Ok () := by
    have := hchecks.symm.trans hr
    simpa using this.symm
  obtain ⟨v, hv, hc⟩ := hiff.mp hrOk
  obtain ⟨hlt, hact, hexit, _⟩ := hcorr.validator _ v hv
  -- The pending-balance check gives a zero pending balance.
  obtain ⟨r', hr', hiff'⟩ := state_processing.verify_exit_pending_balance_ok_iff
    exit.validator_index (view.pendingAt exit.validator_index)
  have hr'Ok : r' = Aeneas.Std.core.result.Result.Ok () := by
    have := hpending.symm.trans hr'
    simpa using this.symm
  have hidxNat : sve.message.validatorIndex.toNat = exit.validator_index.val := by
    rw [hidx]
    rfl
  have hvEq : exitValidator s sve = s.validators[exit.validator_index.val]! := by
    show s.validators[sve.message.validatorIndex.toNat]! = _
    rw [hidxNat]
  refine ⟨by rw [hidxNat]; exact hlt, ?_, ?_, ?_, ?_, ?_⟩
  · rw [hvEq]
    unfold isActiveValidator
    rw [hact, hexit, ← hcorr.epoch]
    simp only [Bool.and_eq_true, decide_eq_true_eq, UInt64.le_iff_toNat_le,
      UInt64.lt_iff_toNat_lt, u64ToSpec_toNat]
    exact ⟨hc.active_from, hc.active_until⟩
  · rw [hvEq, hexit, hc.not_exited]
    exact hcorr.farFuture
  · rw [hepoch, ← hcorr.epoch, UInt64.le_iff_toNat_le, u64ToSpec_toNat, u64ToSpec_toNat]
    exact hc.not_future
  · rw [hvEq, hact, ← hcorr.epoch, ← hcorr.period, u64ToSpec_toNat, u64ToSpec_toNat,
      u64ToSpec_toNat]
    exact hc.old_enough
  · intro q hq hqidx
    obtain ⟨p, hp⟩ := hcorr.pendingSome exit.validator_index
    have hp0 : p.val = 0 := hiff'.mp hr'Ok p hp
    have hle := hcorr.pendingBound _ p hp q hq (hqidx.trans hidx)
    apply UInt64.toNat_inj.mp
    simp only [UInt64.toNat_zero]
    omega

/-- A proposer that the core of `verify_proposer_slashing` accepts passes the spec's gates on
the corresponding state. `ps` is the spec's form of a slashing of that proposer. -/
theorem proposerSlashingGates_of_coreOk {view : StateView} {s : BeaconState}
    (hcorr : StateCorr view s) {proposerIndex : Aeneas.Std.U64} {ps : ProposerSlashing}
    (hidx : ps.signedHeader1.message.proposerIndex = u64ToSpec proposerIndex)
    (hok : ProposerCoreOk view proposerIndex) : ProposerSlashingGates s ps := by
  obtain ⟨r, hr, hiff⟩ := state_processing.verify_proposer_slashing_validator_checks_ok_iff
    (view.validatorAt proposerIndex) proposerIndex view.currentEpoch
  have hrOk : r = Aeneas.Std.core.result.Result.Ok () := by
    have := hok.symm.trans hr
    simpa using this.symm
  obtain ⟨v, hv, hc⟩ := hiff.mp hrOk
  obtain ⟨hlt, hact, _, hwd, hsl⟩ := hcorr.validator _ v hv
  have hidxNat : ps.signedHeader1.message.proposerIndex.toNat = proposerIndex.val := by
    rw [hidx]
    rfl
  refine ⟨by rw [hidxNat]; exact hlt, ?_⟩
  show isSlashableValidator (s.validators[ps.signedHeader1.message.proposerIndex.toNat]!)
    (computeEpochAtSlot s.slot) = true
  rw [hidxNat]
  unfold isSlashableValidator
  rw [hsl, hact, hwd, ← hcorr.epoch, hc.not_slashed]
  simp only [Bool.not_false, Bool.true_and, Bool.and_eq_true, decide_eq_true_eq,
    UInt64.le_iff_toNat_le, UInt64.lt_iff_toNat_lt, u64ToSpec_toNat]
  exact ⟨hc.active_from, hc.not_withdrawable⟩

/-- A validator that `Validator::is_slashable_at` accepts on the view is slashable on the
corresponding state, by the spec's `is_slashable_validator`. -/
theorem isSlashableValidator_of_slashableOn {view : StateView} {s : BeaconState}
    (hcorr : StateCorr view s) {index : Aeneas.Std.U64} (h : SlashableOn view index) :
    isSlashableValidator (s.validators[(u64ToSpec index).toNat]!) (computeEpochAtSlot s.slot) =
      true := by
  obtain ⟨v, hv, hslash⟩ := h
  rw [state_processing.is_slashable_at_eq] at hslash
  have hc : v.slashed = false ∧ v.activation_epoch.val ≤ view.currentEpoch.val ∧
      view.currentEpoch.val < v.withdrawable_epoch.val := by
    simpa using hslash
  obtain ⟨_, hact, _, hwd, hsl⟩ := hcorr.validator _ v hv
  rw [u64ToSpec_toNat]
  unfold isSlashableValidator
  rw [hsl, hact, hwd, ← hcorr.epoch, hc.1]
  simp only [Bool.not_false, Bool.true_and, Bool.and_eq_true, decide_eq_true_eq,
    UInt64.le_iff_toNat_le, UInt64.lt_iff_toNat_lt, u64ToSpec_toNat]
  exact ⟨hc.2.1, hc.2.2⟩

end

end OperationPool.Bridge
