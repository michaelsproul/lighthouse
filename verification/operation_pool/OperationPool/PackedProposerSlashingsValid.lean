import OperationPool.FilterLimit
import OperationPool.Bridge.StateCorr
import EthCLSpecs.Proofs.Fulu.ProposerSlashings

/-!
# The packed proposer slashings are valid

`OperationPool::get_slashings_and_exits` packs proposer slashings with `filter_limit_operations`.
`packed_proposer_slashings_valid` states that the spec processes the packed slashings, in order,
without an error, from the state they were packed on.

The theorem joins three proofs:

* `filter_limit_operations_eq` (extracted Rust): the packed slashings are the first `limit` pool
  entries that the filter closure accepts.
* `proposerSlashingGates_of_coreOk` (extracted Rust to spec): a proposer that the core of
  `verify_proposer_slashing` accepts passes the spec's `ProposerSlashingGates`.
* `proposerSlashings_succeed` (spec, in Etheorem): slashings that name distinct proposers and
  pass the checks are processed in order without an error.

`packed_proposer_slashings_valid_uniform` states the result with one bound on the exit queue and
one bound on the block proposer's balance.

The hypotheses that are not proved from extracted code are the trusted base. `README.md` lists
them.
-/

namespace OperationPool

open EthCLLib.Spec (HasherTag CryptoBackend)
open EthCLLib.Proofs (runPure)
open EthCLSpecs.Fulu
open EthCLSpecs.Proofs.Fulu
open OperationPool.Bridge

section

variable [Preset] [Config] [HasherTag] [CryptoBackend] [PresetValid] [ConfigValid]

variable {Op X I F G It : Type}
  (iterInst : Aeneas.Std.core.iter.traits.collect.IntoIterator I Op It)
  (filterInst : Aeneas.Std.core.ops.function.Fn F Op Bool)
  (mappingInst : Aeneas.Std.core.ops.function.Fn G Op X)
  (cloneInst : Aeneas.Std.core.clone.Clone Op)
  (operations : I) (filter : F) (mapping : G) (limit : Aeneas.Std.Usize)

/-- The proposer slashings that `filter_limit_operations` packs are processed by the spec without
an error, in order, from the packing state `s`.

The Rust side:

* `Op` is a pool entry and `X` a packed slashing. `pool` is what the pool's iterator yields.
* `keep` and `slashingOf` are what the filter and mapping closures compute.
* `index` reads the Rust proposer index of a packed slashing, and `toSpec` gives the spec's form
  of the slashing, with the same proposer index.
* `hkeys`: the pool entries name pairwise distinct proposers.
* `hkeep`: the filter accepts an entry only when the core of `verify_proposer_slashing` accepts
  its proposer on `view`, the slashing is well formed, and its two signatures are valid.

The spec side:

* `hcorr`: the state Lighthouse reads corresponds to the spec state `s`.
* `hsize`: the balances have the registry's length. Every reachable state has this.
* `hproposer`: the proposer of the slot is in the registry.

The conclusion: `filter_limit_operations` does not panic, it packs at most `limit` slashings, and
when the state has room for them the spec's fold over the packed slashings succeeds. The end
state has the slash frame of `s`. -/
theorem packed_proposer_slashings_valid {it : It} {pool : List Op}
    (hinto : iterInst.into_iter operations = Aeneas.Std.Result.ok it)
    (hyields : operation_pool.Yields iterInst.iteratorInst it pool)
    (keep : Op → Bool)
    (hfilter : ∀ op ∈ pool, filterInst.call filter op = Aeneas.Std.Result.ok (keep op))
    (slashingOf : Op → X)
    (hmapping : ∀ op ∈ pool, mappingInst.call mapping op = Aeneas.Std.Result.ok (slashingOf op))
    (index : X → Aeneas.Std.U64) (toSpec : X → ProposerSlashing)
    (hindex : ∀ op ∈ pool,
      slashedIndex (toSpec (slashingOf op)) = u64ToSpec (index (slashingOf op)))
    (hkeys : pool.Pairwise fun a b => index (slashingOf a) ≠ index (slashingOf b))
    {view : StateView} {s : BeaconState} (hcorr : StateCorr view s)
    (hkeep : ∀ op ∈ pool, keep op = true →
      ProposerCoreOk view (index (slashingOf op)) ∧
      ProposerSlashingWellFormed (toSpec (slashingOf op)) ∧
      ProposerSlashingSigned s (toSpec (slashingOf op)))
    (hsize : s.balances.val.size = s.validators.val.size)
    (hproposer : (blockProposer s).toNat < s.validators.val.size) :
    ∃ out, operation_pool.filter_limit_operations iterInst filterInst mappingInst cloneInst
        operations filter mapping limit = Aeneas.Std.Result.ok out ∧
      out.val.length ≤ limit.val ∧
      (SlashRoomAt (exitStart s) (s.balances[(blockProposer s).toNat]!).toNat s
          (out.val.map toSpec) →
        ∃ s₁, runPure (processProposerSlashings (out.val.map toSpec).toArray) s = .ok ((), s₁) ∧
          SlashFrame (fun j => ∃ ps ∈ out.val.map toSpec, slashedIndex ps = j)
            (slashExitSteps s (out.val.map toSpec)) (slashRewards s (out.val.map toSpec)) s s₁) := by
  obtain ⟨out, hout, hval⟩ := operation_pool.filter_limit_operations_eq iterInst filterInst
    mappingInst filter mapping limit cloneInst operations hinto hyields keep hfilter slashingOf
    hmapping
  refine ⟨out, hout, ?_, ?_⟩
  · rw [hval, List.length_map]
    exact List.length_take_le _ _
  · intro hroom
    -- The packed pool entries: a sublist of the pool, all accepted by the filter.
    have hsub : ((pool.filter keep).take limit.val).Sublist pool :=
      (List.take_sublist _ _).trans List.filter_sublist
    have hmem : ∀ op ∈ (pool.filter keep).take limit.val, op ∈ pool ∧ keep op = true :=
      fun op hop => List.mem_filter.mp (List.mem_of_mem_take hop)
    have hslashings : out.val.map toSpec =
        ((pool.filter keep).take limit.val).map (fun op => toSpec (slashingOf op)) := by
      rw [hval, List.map_map]
      rfl
    rw [hslashings] at hroom ⊢
    -- Distinct Rust indices give distinct spec indices.
    have hdistinct : (((pool.filter keep).take limit.val).map
        (fun op => toSpec (slashingOf op))).toArray.toList.Pairwise
        (fun a b => slashedIndex a ≠ slashedIndex b) := by
      simp only [List.pairwise_map]
      refine (hkeys.sublist hsub).imp_of_mem ?_
      intro a b ha hb hne heq
      refine hne (u64ToSpec_inj ?_)
      rw [← hindex a (hmem a ha).1, ← hindex b (hmem b hb).1]
      exact heq
    -- Each packed slashing comes from a pool entry that the filter accepted.
    have hfrom : ∀ ps ∈ (((pool.filter keep).take limit.val).map
        (fun op => toSpec (slashingOf op))).toArray,
        ∃ op ∈ pool, keep op = true ∧ ps = toSpec (slashingOf op) := by
      intro ps hps
      obtain ⟨op, hop, rfl⟩ := List.mem_map.mp (List.mem_toArray.mp hps)
      exact ⟨op, (hmem op hop).1, (hmem op hop).2, rfl⟩
    obtain ⟨s₁, hrun, hframe⟩ := proposerSlashings_succeed _ hdistinct
      (fun ps hps => by
        obtain ⟨op, hpool, hk, rfl⟩ := hfrom ps hps
        exact (hkeep op hpool hk).2.1)
      (fun ps hps => by
        obtain ⟨op, hpool, hk, rfl⟩ := hfrom ps hps
        exact proposerSlashingGates_of_coreOk hcorr (hindex op hpool) (hkeep op hpool hk).1)
      (fun ps hps => by
        obtain ⟨op, hpool, hk, rfl⟩ := hfrom ps hps
        exact (hkeep op hpool hk).2.2)
      hsize hproposer (by simpa using hroom)
    refine ⟨s₁, hrun, hframe.mono (fun j hj => ?_) (by simp) (by simp)⟩
    obtain ⟨ps, hps, hj⟩ := hj
    exact ⟨ps, List.mem_toArray.mp hps, hj⟩

/-- `packed_proposer_slashings_valid` with two bounds read on the packing state: the exit queue
has room for `limit` exits of any balance, and the block proposer's balance has room for `limit`
whistleblower rewards of any size.

On mainnet `limit` is 16. `exitStep (2 ^ 64 - 1)` is about `1.4e8` epochs, and a whistleblower
reward is below `2 ^ 52` Gwei. -/
theorem packed_proposer_slashings_valid_uniform {it : It} {pool : List Op}
    (hinto : iterInst.into_iter operations = Aeneas.Std.Result.ok it)
    (hyields : operation_pool.Yields iterInst.iteratorInst it pool)
    (keep : Op → Bool)
    (hfilter : ∀ op ∈ pool, filterInst.call filter op = Aeneas.Std.Result.ok (keep op))
    (slashingOf : Op → X)
    (hmapping : ∀ op ∈ pool, mappingInst.call mapping op = Aeneas.Std.Result.ok (slashingOf op))
    (index : X → Aeneas.Std.U64) (toSpec : X → ProposerSlashing)
    (hindex : ∀ op ∈ pool,
      slashedIndex (toSpec (slashingOf op)) = u64ToSpec (index (slashingOf op)))
    (hkeys : pool.Pairwise fun a b => index (slashingOf a) ≠ index (slashingOf b))
    {view : StateView} {s : BeaconState} (hcorr : StateCorr view s)
    (hkeep : ∀ op ∈ pool, keep op = true →
      ProposerCoreOk view (index (slashingOf op)) ∧
      ProposerSlashingWellFormed (toSpec (slashingOf op)) ∧
      ProposerSlashingSigned s (toSpec (slashingOf op)))
    (hsize : s.balances.val.size = s.validators.val.size)
    (hproposer : (blockProposer s).toNat < s.validators.val.size)
    (hexits : exitStart s + limit.val * exitStep (2 ^ 64 - 1) +
      Const.minValidatorWithdrawabilityDelay.toNat < 2 ^ 64)
    (hrewards : (s.balances[(blockProposer s).toNat]!).toNat + limit.val * ((2 ^ 64 - 1) /
      (UInt64.ofNat Const.whistleblowerRewardQuotientElectra).toNat) < 2 ^ 64) :
    ∃ out s₁, operation_pool.filter_limit_operations iterInst filterInst mappingInst cloneInst
        operations filter mapping limit = Aeneas.Std.Result.ok out ∧
      out.val.length ≤ limit.val ∧
      runPure (processProposerSlashings (out.val.map toSpec).toArray) s = .ok ((), s₁) ∧
      SlashFrame (fun j => ∃ ps ∈ out.val.map toSpec, slashedIndex ps = j)
        (slashExitSteps s (out.val.map toSpec)) (slashRewards s (out.val.map toSpec)) s s₁ := by
  obtain ⟨out, hout, hlen, hrun⟩ := packed_proposer_slashings_valid iterInst filterInst
    mappingInst cloneInst operations filter mapping limit hinto hyields keep hfilter slashingOf
    hmapping index toSpec hindex hkeys hcorr hkeep hsize hproposer
  obtain ⟨s₁, hs₁, hframe⟩ :=
    hrun (SlashRoomAt.of_uniform s _ (by simpa using hlen) hexits hrewards)
  exact ⟨out, s₁, hout, hlen, hs₁, hframe⟩

end

end OperationPool
