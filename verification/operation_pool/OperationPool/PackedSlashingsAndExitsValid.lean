import OperationPool.Concrete
import OperationPool.PackedProposerSlashingsValid
import OperationPool.PackedAttesterSlashingsValid
import OperationPool.PackedExitsValid
import EthCLSpecs.Proofs.Fulu.StateBounds
import EthCLSpecs.Proofs.Fulu.ReachableInvariants

/-!
# The packed slashings and voluntary exits of a block are valid together

`OperationPool::get_slashings_and_exits` packs the proposer slashings, the attester slashings
and the voluntary exits of one block. `process_operations` applies them in that order, and each
operation changes the state the next one reads.

`packed_slashings_and_exits_valid` is the main theorem of this project. It follows the body of
`get_slashings_and_exits` call by call, and states that the spec processes what each call
returns: the proposer slashings from the packing state `s`, then the attester slashings, then
the voluntary exits. No step fails.

## The form of the statement

The three packing theorems are generic over the iterator, the closures and the `MaxCover`
implementation, and take a hypothesis for each. This theorem states the extracted functions on
the concrete instances of `OperationPool/Concrete.lean`, for which those hypotheses hold by
construction:

* a pool is a slice, and a closure is a pure function;
* a cover is an `AttesterCover`;
* the exit filter is the closure the Rust builds: it rejects the validators of the exclusion
  set, and then applies the checks of `get_voluntary_exits`.

The packing state `s` is a reachable state of the spec, within `TimeBound` and
`SupplyCap (2 ^ 58)`. Those two bounds keep the arithmetic away from `2 ^ 64`. A valid config
leaves room within them for a block with as many operations as the preset allows
(`ConfigValid.operationsRoom`), and the three limits are at most the preset's.

The hypotheses that are not proved from extracted code are the trusted base. `README.md` lists
them.
-/

namespace OperationPool

open EthCLLib.Spec (HasherTag CryptoBackend)
open EthCLLib.Proofs (runPure)
open EthCLSpecs.Fulu
open EthCLSpecs.Proofs.Fulu
open OperationPool.Bridge
open operation_pool (sliceIntoIter pureFn cloneId AttesterCover)

/-- The Rust `ProposerSlashing`, as the extraction of the `operation_pool` crate names it. -/
abbrev RProposerSlashing : Type :=
  operation_pool.types.slashing.proposer_slashing.ProposerSlashing

section

variable [Preset] [Config] [HasherTag] [CryptoBackend] [PresetValid] [ConfigValid]

variable {OpP Im Op X : Type}

/-- The packed proposer slashings, attester slashings and voluntary exits of a block are valid:
the spec processes them in that order from the packing state `s`, and no step fails.

The data:

* `poolP`, `keepP`, `slashingOf`, `limitP`: the proposer-slashing pool, what the filter closure
  and the mapping closure compute on an entry, and `MAX_PROPOSER_SLASHINGS`.
* `covers`, `limitA`: the covers that `AttesterSlashingMaxCover::new` builds, and
  `MAX_ATTESTER_SLASHINGS_ELECTRA`.
* `pool`, `keepE`, `exitOf`, `limit`: the exit pool, the checks of `get_voluntary_exits` on an
  entry, the mapping closure, and `MAX_VOLUNTARY_EXITS`.
* `initial`: the set `to_be_slashed` when the two `insert_*` functions extend it.
* `toSpecP`, `toSpecA`, `toSpec`: the spec's form of each operation. `indicesA` and `message`
  read the Rust attesting indices and the Rust exit.

The hypotheses:

* `hindexP`, `hindicesA`, `hmessage`: the spec's forms carry the Rust indices and epochs.
* `hkeysP`, `hkeys`: a pool holds one operation per validator.
* `hcorr`: the state Lighthouse reads corresponds to the spec state `s`.
* `hlimits`: the three limits are at most the preset's block limits.
* `hanchors`, `hreach`: `s` is reachable from anchor states that have the simple invariants.
* `hproposer`, `htime`, `hsupply`: the proposer of `s` is in the registry, and `s` is within the
  time bound and a supply cap of `2 ^ 58` Gwei.
* `hkeepP`, `hkeepE`: a filter accepts an entry only when the cores of the `verify_*` function
  accept it on `view`, and the operation is well formed and signed.
* `hcoverA`: a cover holds the validators that both attestations name, that
  `Validator::is_slashable_at` accepts, and that no packed proposer slashing names.
* `hchecksA`: the slashing of each cover passes the checks of `process_attester_slashing` that
  run before the loop.

The conclusion follows `get_slashings_and_exits`. No call panics. `outP`, `outA` and `out` are
what the three packing calls return, within their limits, and `excl` is the exclusion set built
from `outP` and `outA`. The spec processes `outP` from `s` and reaches `s₁`, the slashings of
`outA` from `s₁` and reaches `s₂`, and `out` from `s₂`. -/
theorem packed_slashings_and_exits_valid
    (poolP : Aeneas.Std.Slice OpP) (keepP : OpP → Bool) (slashingOf : OpP → RProposerSlashing)
    (limitP : Aeneas.Std.Usize)
    (covers : Aeneas.Std.Slice (AttesterCover Im)) (limitA : Aeneas.Std.Usize)
    (label : Aeneas.Std.Str)
    (pool : Aeneas.Std.Slice Op) (keepE : Op → Bool) (exitOf : Op → X)
    (limit : Aeneas.Std.Usize)
    (initial : operation_pool.IndexSet)
    (toSpecP : RProposerSlashing → ProposerSlashing) (toSpecA : Im → AttesterSlashing)
    (indicesA : Im → Aeneas.Std.Slice Aeneas.Std.U64 × Aeneas.Std.Slice Aeneas.Std.U64)
    (message : X → RVoluntaryExit) (toSpec : X → SignedVoluntaryExit)
    (hindexP : ∀ r, slashedIndex (toSpecP r) =
      u64ToSpec r.signed_header_1.message.proposer_index)
    (hindicesA : ∀ i,
      (toSpecA i).attestation1.attestingIndices.toArray.toList =
        (indicesA i).1.val.map u64ToSpec ∧
      (toSpecA i).attestation2.attestingIndices.toArray.toList =
        (indicesA i).2.val.map u64ToSpec)
    (hmessage : ∀ x,
      (toSpec x).message.validatorIndex = u64ToSpec (message x).validator_index ∧
      (toSpec x).message.epoch = u64ToSpec (message x).epoch)
    (hkeysP : poolP.val.Pairwise fun a b =>
      (slashingOf a).signed_header_1.message.proposer_index ≠
        (slashingOf b).signed_header_1.message.proposer_index)
    (hkeys : pool.val.Pairwise fun a b =>
      (message (exitOf a)).validator_index ≠ (message (exitOf b)).validator_index)
    (hlimits : limitP.val ≤ Const.maxProposerSlashings ∧
      limitA.val ≤ Const.maxAttesterSlashingsElectra ∧ limit.val ≤ Const.maxVoluntaryExits)
    {view : StateView} {s : BeaconState} (hcorr : StateCorr view s)
    {anchors : Anchors} {validated : Bool} {nextForkEpoch : Epoch}
    (hanchors : ∀ u, anchors.fulu u → SimpleStateInvariants u)
    (hreach : ReachableState anchors validated nextForkEpoch s)
    (hproposer : (blockProposer s).toNat < s.validators.val.size)
    (htime : TimeBound s) (hsupply : SupplyCap (2 ^ 58) s)
    (hkeepP : ∀ op ∈ poolP.val, keepP op = true →
      ProposerCoreOk view (slashingOf op).signed_header_1.message.proposer_index ∧
      ProposerSlashingWellFormed (toSpecP (slashingOf op)) ∧
      ProposerSlashingSigned s (toSpecP (slashingOf op)))
    (hcoverA : ∀ a ∈ covers.val, ∀ u, a.covers u ↔
      u64ToSpec u ∈ attesterSlashingIndices (toSpecA a.slashing) ∧ SlashableOn view u ∧
        ¬ ∃ op ∈ (poolP.val.filter keepP).take limitP.val,
          (slashingOf op).signed_header_1.message.proposer_index = u)
    (hchecksA : ∀ a ∈ covers.val, AttesterSlashingChecks s (toSpecA a.slashing))
    (hkeepE : ∀ op ∈ pool.val, keepE op = true →
      CoresOk view (message (exitOf op)) ∧ ExitSigned s (toSpec (exitOf op))) :
    ∃ outP s₁ outA s₂ excl out s₃,
      operation_pool.filter_limit_operations (sliceIntoIter OpP)
        (Aeneas.Std.BuiltinFn OpP Bool) (Aeneas.Std.BuiltinFn OpP RProposerSlashing)
        (cloneId OpP) poolP (pureFn keepP) (pureFn slashingOf) limitP =
          Aeneas.Std.Result.ok outP ∧
      runPure (processProposerSlashings (outP.val.map toSpecP).toArray) s = .ok ((), s₁) ∧
      operation_pool.max_cover.maximum_cover (sliceIntoIter (AttesterCover Im))
        (AttesterCover.maxCover Im) covers limitA label = Aeneas.Std.Result.ok outA ∧
      runPure (processAttesterSlashings
        (outA.val.map fun c => toSpecA c.slashing).toArray) s₁ = .ok ((), s₂) ∧
      operation_pool.exclusionSet initial (Aeneas.Std.alloc.vec.Vec.deref outP)
        (outA.val.map fun c => indicesA c.slashing) = Aeneas.Std.Result.ok excl ∧
      operation_pool.filter_limit_operations (sliceIntoIter Op)
        (Aeneas.Std.BuiltinFn Op Bool) (Aeneas.Std.BuiltinFn Op X) (cloneId Op) pool
        (pureFn fun op =>
          !decide ((message (exitOf op)).validator_index ∈ excl.toList) && keepE op)
        (pureFn exitOf) limit = Aeneas.Std.Result.ok out ∧
      runPure (processVoluntaryExits (out.val.map toSpec).toArray) s₂ = .ok ((), s₃) ∧
      outP.val.length ≤ limitP.val ∧ outA.val.length ≤ limitA.val ∧
        out.val.length ≤ limit.val := by
  -- The state facts the three packing theorems take.
  have hsize : s.balances.val.size = s.validators.val.size :=
    (hreach.simpleStateInvariants hanchors).registrySizes.1
  have hroom : OperationsRoom (2 ^ 58) limitP.val limitA.val limit.val :=
    (ConfigValid.operationsRoom (self := ‹_›)).mono hlimits.1 hlimits.2.1 hlimits.2.2
  have hexits := hroom.exits_of_timeBound htime
  have hrewards := hroom.rewards_of_supplyCap hsupply (blockProposer s).toNat
  have hcap := hsupply.effectiveBalance_le
  have hmul : (limitP.val +
      limitA.val * (Const.maxValidatorsPerCommittee * Const.maxCommitteesPerSlot) + limit.val) *
        exitStep (2 ^ 64 - 1) =
      limitP.val * exitStep (2 ^ 64 - 1) +
        limitA.val * (Const.maxValidatorsPerCommittee * Const.maxCommitteesPerSlot *
          exitStep (2 ^ 64 - 1)) +
        limit.val * exitStep (2 ^ 64 - 1) := by
    rw [Nat.add_mul, Nat.add_mul, Nat.mul_assoc]
  -- The proposer slashings.
  obtain ⟨outP, s₁, houtP, hlenP, hrunP, hframeP⟩ := packed_proposer_slashings_valid_uniform
    (sliceIntoIter OpP) (Aeneas.Std.BuiltinFn OpP Bool)
    (Aeneas.Std.BuiltinFn OpP RProposerSlashing) (cloneId OpP) poolP (pureFn keepP)
    (pureFn slashingOf) limitP rfl (operation_pool.yields_slice poolP) keepP (fun _ _ => rfl)
    slashingOf (fun _ _ => rfl) (fun r => r.signed_header_1.message.proposer_index) toSpecP
    (fun op _ => hindexP (slashingOf op)) hkeysP hcorr hkeepP hsize hproposer (by omega)
    (by omega)
  -- The packed slashings are the mapped pool entries.
  obtain ⟨outP', houtP', hvalP⟩ := operation_pool.filter_limit_operations_eq (sliceIntoIter OpP)
    (Aeneas.Std.BuiltinFn OpP Bool) (Aeneas.Std.BuiltinFn OpP RProposerSlashing)
    (pureFn keepP) (pureFn slashingOf) limitP (cloneId OpP) poolP rfl
    (operation_pool.yields_slice poolP) keepP (fun _ _ => rfl) slashingOf (fun _ _ => rfl)
  have houtEq : outP' = outP := by
    have := houtP'.symm.trans houtP
    simpa using this
  subst houtEq
  -- The frame of the proposer slashings, with the bounds for `limitP` slashings.
  have hlenP' : (outP'.val.map toSpecP).length ≤ limitP.val := by simpa using hlenP
  have hframeP' := hframeP.mono (fun _ h => h)
    (Nat.le_trans (slashExitSteps_le s (outP'.val.map toSpecP))
      (Nat.mul_le_mul_right (exitStep (2 ^ 64 - 1)) hlenP'))
    (Nat.le_trans (slashRewards_le s (outP'.val.map toSpecP))
      (Nat.mul_le_mul_right _ hlenP'))
  -- The attester slashings. `to_be_slashed` holds the proposers of the packed slashings.
  obtain ⟨outA, s₂, houtA, hlenA, _, hrunA, hframeA⟩ := packed_attester_slashings_valid_uniform
    (sliceIntoIter (AttesterCover Im)) (AttesterCover.maxCover Im) covers limitA label rfl
    (operation_pool.yields_slice covers) (by scalar_tac) (AttesterCover.model Im) toSpecA hcorr
    (fun u => ∃ op ∈ (poolP.val.filter keepP).take limitP.val,
      (slashingOf op).signed_header_1.message.proposer_index = u)
    hcoverA hchecksA hframeP'
    (fun u hS => by
      obtain ⟨ps, hps, hidx⟩ := hS
      obtain ⟨r, hr, rfl⟩ := List.mem_map.mp hps
      rw [hvalP] at hr
      obtain ⟨op, hop, rfl⟩ := List.mem_map.mp hr
      exact ⟨op, hop, u64ToSpec_inj ((hindexP (slashingOf op)).symm.trans hidx)⟩)
    hsize hproposer hcap (by omega) (by omega)
  -- The exclusion set, and the exits.
  obtain ⟨excl, hexcl⟩ := operation_pool.exclusionSet_ok initial
    (Aeneas.Std.alloc.vec.Vec.deref outP') (outA.val.map fun c => indicesA c.slashing)
  have hstart := hframeA.start
  obtain ⟨out, s₃, hout, hlen, hrun⟩ := packed_exits_valid_after_slashings_uniform
    (sliceIntoIter Op) (Aeneas.Std.BuiltinFn Op Bool) (Aeneas.Std.BuiltinFn Op X) (cloneId Op)
    pool
    (pureFn fun op =>
      !decide ((message (exitOf op)).validator_index ∈ excl.toList) && keepE op)
    (pureFn exitOf) limit rfl (operation_pool.yields_slice pool)
    (fun op => !decide ((message (exitOf op)).validator_index ∈ excl.toList) && keepE op)
    (fun _ _ => rfl) exitOf (fun _ _ => rfl) message toSpec (fun op _ => hmessage (exitOf op))
    hkeys hcorr hexcl (outP'.val.map toSpecP).toArray
    (outA.val.map fun c => toSpecA c.slashing).toArray
    (fun ps hps => by
      -- Each spec slashing is the spec form of a packed Rust slashing, with the same proposer.
      obtain ⟨r, hr, rfl⟩ := List.mem_map.mp (List.mem_toArray.mp hps)
      exact ⟨r, by simpa [Aeneas.Std.alloc.vec.Vec.deref] using hr, (hindexP r).symm⟩)
    (fun asl hasl => by
      obtain ⟨c, hc, rfl⟩ := List.mem_map.mp (List.mem_toArray.mp hasl)
      exact ⟨indicesA c.slashing, List.mem_map.mpr ⟨c, hc, rfl⟩, hindicesA c.slashing⟩)
    hrunP hrunA
    (fun op hop hk => by
      simp only [Bool.and_eq_true, Bool.not_eq_true', decide_eq_false_iff_not] at hk
      exact ⟨hk.1, hkeepE op hop hk.2⟩)
    (by omega)
  exact ⟨outP', s₁, outA, s₂, excl, out, s₃, houtP, hrunP, houtA, hrunA, hexcl, hout, hrun,
    hlenP, hlenA, hlen⟩

end

end OperationPool
