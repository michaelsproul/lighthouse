module

public import EthLibOps.Room
public import EthLibOps.ProposerSlashings

/-!
# The voluntary exits of a block succeed

`process_voluntary_exit` checks that a validator is active, has not started to exit, has been
active for `SHARD_COMMITTEE_PERIOD` epochs, has no pending partial withdrawal, and signed the
exit. It then starts the validator's exit.

Two predicates name the checks, for an exit `sve` on a state `s`:

* `ExitGates config electra s sve`: the checks that read the state.
* `ExitSigned config checks s sve`: the signature check.

`processVoluntaryExit_ok_of_frame` is one exit, on a state that has the `SlashFrame` of `s`. The
state `s` is the one the exit is checked on, and the frame covers the slashings that run before
the exits of a block. `voluntaryExits_foldlM_ok` is the fold `process_operations` runs over the
voluntary exits of a block body.
-/

@[expose] public section

namespace EthLibOps

open EthLib EthLib.Base EthLib.Base.Uint64 EthLib.Consensus

variable {preset : Preset} {fork : Base.Fork} {config : Config} {checks : SignatureChecks}

/-- The checks of `process_voluntary_exit` that read the state, for the exit `sve` on `s`. The
signature check is `ExitSigned`. -/
structure ExitGates (config : Config) (electra : .pectra ≤ fork) (s : BeaconState preset fork)
    (sve : SignedVoluntaryExit) (v : Validator) : Prop where
  /-- The exit names the validator `v` of the registry. -/
  found : getValidator s sve.message.validatorIndex = .ok v
  /-- The validator is active at the current epoch. -/
  active : isActiveValidator v (getCurrentEpoch s) = true
  /-- The validator has no exit epoch yet. -/
  notExited : v.exitEpoch = FAR_FUTURE_EPOCH
  /-- The epoch the exit names has come. -/
  epochReached : sve.message.epoch ≤ getCurrentEpoch s
  /-- The validator has been active for `SHARD_COMMITTEE_PERIOD` epochs. The bound is in `Nat`,
  so the checked sum on the left does not overflow. -/
  shardPeriod :
    v.activationEpoch.toNat + config.SHARD_COMMITTEE_PERIOD.toNat ≤ (getCurrentEpoch s).toNat
  /-- The validator has no pending partial withdrawal with a nonzero amount. -/
  noPending : ∀ q ∈ (s.pendingPartialWithdrawals electra).data,
    q.validatorIndex = sve.message.validatorIndex → q.amount = 0

/-- The signature check of `process_voluntary_exit`: the exit's signature verifies against the
validator's public key, under the voluntary-exit domain. -/
def ExitSigned (config : Config) (checks : SignatureChecks) (s : BeaconState preset fork)
    (sve : SignedVoluntaryExit) : Prop :=
  ∃ v domain root, getValidator s sve.message.validatorIndex = .ok v ∧
    voluntaryExitDomain config s sve.message.epoch = .ok domain ∧
    computeSigningRoot sve.message domain = .ok root ∧
    checks.verify v.pubkey (packBytes root) sve.signature = true

/-- The pending balance to withdraw is zero when every pending partial withdrawal of the
validator has a zero amount. -/
theorem getPendingBalanceToWithdraw_eq_zero {s : BeaconState preset fork}
    (electra : .pectra ≤ fork) {i : ValidatorIndex}
    (h : ∀ q ∈ (s.pendingPartialWithdrawals electra).data, q.validatorIndex = i → q.amount = 0) :
    getPendingBalanceToWithdraw s electra i = .ok 0 := by
  have zero : ((s.pendingPartialWithdrawals electra).data.toList.map fun withdrawal =>
      if withdrawal.validatorIndex = i then withdrawal.amount.toNat else 0).sum = 0 := by
    apply List.sum_eq_zero
    intro x hx
    obtain ⟨q, hq, rfl⟩ := List.mem_map.mp hx
    split
    · rename_i same
      rw [h q (Array.mem_toList_iff.mp hq) same]
      rfl
    · rfl
  simp [getPendingBalanceToWithdraw, zero]

/-- The domain of a voluntary exit reads the fork versions and the genesis validators root of the
state, and nothing else. -/
theorem voluntaryExitDomain_congr {s w : BeaconState preset fork}
    (forkVersions : w.forkVersions = s.forkVersions)
    (genesisValidatorsRoot : w.genesisValidatorsRoot = s.genesisValidatorsRoot) (epoch : Epoch) :
    voluntaryExitDomain config w epoch = voluntaryExitDomain config s epoch := by
  simp only [voluntaryExitDomain, getDomain, forkVersions, genesisValidatorsRoot,
    Option.getD_some]

variable {electra : .pectra ≤ fork} {S : ValidatorIndex → Prop} {E R B : Nat}
  {s w : BeaconState preset fork}

/-- `process_voluntary_exit` succeeds on a state `w` that has the frame of `s`, for an exit that
passes the gates and the signature check on `s` and names a validator outside the set of the
frame. The exit queue of `s` has room for the bound of the frame and one more exit.

The state after it has the frame of `s`, with the validator added to the set. -/
theorem processVoluntaryExit_ok_of_frame (ok : ChurnOk preset config)
    (before : fork < .glamsterdam) (room : QueueRoom config B s)
    (frame : SlashFrame electra S E R s w) {sve : SignedVoluntaryExit} {v : Validator}
    (gates : ExitGates config electra s sve v) (signed : ExitSigned config checks s sve)
    (fresh : ¬ S sve.message.validatorIndex)
    (exits : exitStart electra s + E + exitStep preset B +
      config.MIN_VALIDATOR_WITHDRAWABILITY_DELAY.toNat < 2 ^ 64) :
    ∃ w', processVoluntaryExit config checks w sve = .ok w' ∧
      SlashFrame electra (fun j => S j ∨ j = sve.message.validatorIndex)
        (E + exitStep preset B) R s w' := by
  obtain ⟨v', domain, root, found', domainOk, rootOk, verified⟩ := signed
  cases gates.found.symm.trans found'
  -- The checks read the same things on `w`.
  have foundW : getValidator w sve.message.validatorIndex = .ok v := by
    rw [frame.getValidator_eq fresh]
    exact gates.found
  have epochW := frame.currentEpoch
  have activeW : isActiveValidator v (getCurrentEpoch w) = true := by
    rw [epochW]
    exact gates.active
  have reachedW : sve.message.epoch ≤ getCurrentEpoch w := by
    rw [epochW]
    exact gates.epochReached
  -- The sum `activation_epoch + SHARD_COMMITTEE_PERIOD` does not overflow.
  have period := gates.shardPeriod
  have epochLt := (getCurrentEpoch s).toNat_lt
  have periodFits :
      v.activationEpoch.toNat + config.SHARD_COMMITTEE_PERIOD.toNat < 2 ^ 64 := by omega
  have eligibleW : v.activationEpoch + config.SHARD_COMMITTEE_PERIOD ≤ getCurrentEpoch w := by
    rw [epochW, UInt64.le_iff_toNat_le, toNat_add_of_lt periodFits]
    exact period
  have pendingW : checkNoPendingWithdrawal w sve.message.validatorIndex = .ok () := by
    have zero : getPendingBalanceToWithdraw w electra sve.message.validatorIndex = .ok 0 :=
      getPendingBalanceToWithdraw_eq_zero electra (by
        rw [frame.pending]
        exact gates.noPending)
    simp only [checkNoPendingWithdrawal, electra, ↓reduceDIte, zero, liftAccessor,
      Except.mapError, ensure_of trivial, bind, Except.bind]
  have domainW : voluntaryExitDomain config w sve.message.epoch = .ok domain := by
    rw [voluntaryExitDomain_congr frame.forkVersions frame.genesisValidatorsRoot]
    exact domainOk
  obtain ⟨w', computed, frame'⟩ :=
    initiateValidatorExit_ok_of_frame ok before room frame foundW exits
  refine ⟨w', ?_, frame'⟩
  simp only [processVoluntaryExit, foundW, liftAccessor, Except.mapError, ensure_of activeW,
    ensure_of gates.notExited, ensure_of reachedW, liftArith, addChecked_of_lt periodFits,
    ensure_of eligibleW, pendingW, domainW, liftSsz, rootOk, ensure_of verified, computed, bind,
    Except.bind]

/-- The voluntary exits of a list succeed, one after the other, from a state `w` that has the
frame of `s`. The checks are made on `s`. The exits name pairwise distinct validators outside the
set of the frame. The exit queue of `s` has room for the bound of the frame and for one exit step
for each exit.

The end state has the frame of `s`, for the set of the frame and every validator that exits. -/
theorem voluntaryExits_foldlM_ok (ok : ChurnOk preset config) (before : fork < .glamsterdam)
    (room : QueueRoom config B s) :
    ∀ (exits : List SignedVoluntaryExit) (S : ValidatorIndex → Prop) (E : Nat)
      (w : BeaconState preset fork),
      SlashFrame electra S E R s w →
      exits.Pairwise (fun a b => a.message.validatorIndex ≠ b.message.validatorIndex) →
      (∀ sve ∈ exits, ¬ S sve.message.validatorIndex) →
      (∀ sve ∈ exits, (∃ v, ExitGates config electra s sve v) ∧
        ExitSigned config checks s sve) →
      exitStart electra s + E + exits.length * exitStep preset B +
        config.MIN_VALIDATOR_WITHDRAWABILITY_DELAY.toNat < 2 ^ 64 →
      ∃ w', exits.foldlM (processVoluntaryExit config checks) w = .ok w' ∧
        SlashFrame electra (fun j => S j ∨ ∃ sve ∈ exits, sve.message.validatorIndex = j)
          (E + exits.length * exitStep preset B) R s w'
  | [], S, E, w, frame, _, _, _, _ =>
    ⟨w, rfl, frame.mono (fun _ h => Or.inl h) (by simp) (Nat.le_refl _)⟩
  | sve :: rest, S, E, w, frame, distinct, fresh, passes, room' => by
    generalize hstep : exitStep preset B = step at room' ⊢
    simp only [List.length_cons, Nat.succ_mul] at room' ⊢
    obtain ⟨⟨v, gates⟩, signed⟩ := passes sve (List.mem_cons_self ..)
    obtain ⟨w₁, computed, frame₁⟩ := processVoluntaryExit_ok_of_frame ok before room frame gates
      signed (fresh sve (List.mem_cons_self ..)) (by rw [hstep]; omega)
    rw [hstep] at frame₁
    obtain ⟨w₂, restOk, frame₂⟩ := voluntaryExits_foldlM_ok ok before room rest
      (fun j => S j ∨ j = sve.message.validatorIndex) (E + step) w₁ frame₁
      (List.pairwise_cons.mp distinct).2
      (fun q hq hS => by
        rcases hS with hS | same
        · exact fresh q (List.mem_cons_of_mem _ hq) hS
        · exact (List.pairwise_cons.mp distinct).1 q hq same.symm)
      (fun q hq => passes q (List.mem_cons_of_mem _ hq)) (by rw [hstep]; omega)
    rw [hstep] at frame₂
    refine ⟨w₂, ?_, frame₂.mono (fun j hj => ?_) (by omega) (Nat.le_refl _)⟩
    · simp only [List.foldlM_cons, computed, bind, Except.bind]
      exact restOk
    · rcases hj with (hj | rfl) | ⟨q, hq, rfl⟩
      · exact Or.inl hj
      · exact Or.inr ⟨sve, List.mem_cons_self .., rfl⟩
      · exact Or.inr ⟨q, List.mem_cons_of_mem _ hq, rfl⟩

end EthLibOps
