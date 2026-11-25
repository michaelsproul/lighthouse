import OperationPool.FilterLimit
import OperationPool.Bridge.StateCorr

/-!
# The packed proposer slashings are valid

`OperationPool::get_slashings_and_exits` packs proposer slashings with `filter_limit_operations`.
`packed_proposer_slashings_valid` states that the spec processes the packed slashings, in order,
without an error, from the state they were packed on.

The spec is EthLib's: `processProposerSlashing`, folded over the packed slashings as
`processOperations` folds it over the proposer slashings of a block body.

The theorem joins three proofs:

* `filter_limit_operations_eq` (extracted Rust): the packed slashings are the first `limit` pool
  entries that the filter closure accepts.
* `proposerSlashingGates_of_coreOk` (extracted Rust to spec): a proposer that the core of
  `verify_proposer_slashing` accepts passes the spec's `ProposerSlashingGates`.
* `proposerSlashings_foldlM_ok` (spec, in `EthLibOps`): slashings that name distinct proposers
  and pass the checks are processed in order without an error.

The hypotheses that are not proved from extracted code are the trusted base. `README.md` lists
them.
-/

namespace OperationPool

open EthLib EthLib.Consensus
open EthLibOps
open OperationPool.Bridge

section

variable {preset : Preset} {fork : Base.Fork} {config : Config} {checks : SignatureChecks}

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

* `hfork`, `hchurn`: the fork is Fulu, and the constants of the exit churn are positive.
* `hcorr`: the state Lighthouse reads corresponds to the spec state `s`.
* `hroom`: what a slashing reads on `s`. `B` bounds every effective balance, and `p` is the
  proposer of the slot.
* `hexits`: the exit queue has room for `limit` exits.
* `hrewards`: the proposer's balance has room for `limit` whistleblower rewards, and for one
  more effective balance.

The conclusion: `filter_limit_operations` does not panic, it packs at most `limit` slashings, and
the spec's fold over the packed slashings succeeds. The end state has the slash frame of `s`, for
the slashed proposers.

On mainnet `limit` is 16. With `B` at 2048 ETH, `exitStep preset B` is 2049 epochs and a
whistleblower reward is `5e8` Gwei. -/
theorem packed_proposer_slashings_valid (hfork : IsFulu fork) (hchurn : ChurnOk preset config)
    {it : It} {pool : List Op}
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
    {view : StateView} {s : BeaconState preset fork}
    (hcorr : StateCorr config hfork.electra view s)
    (hkeep : ∀ op ∈ pool, keep op = true →
      ProposerCoreOk view (index (slashingOf op)) ∧
      ProposerSlashingWellFormed (toSpec (slashingOf op)) ∧
      ProposerSlashingSigned config checks s (toSpec (slashingOf op)))
    {B : Nat} {p : ValidatorIndex} (hroom : StateRoom config B p s)
    (hexits : exitStart hfork.electra s + limit.val * exitStep preset B +
      config.MIN_VALIDATOR_WITHDRAWABILITY_DELAY.toNat < 2 ^ 64)
    (hrewards : ∀ b, getBalance s p = .ok b →
      b.toNat + limit.val * rewardMax preset fork B + B < 2 ^ 64) :
    ∃ out s₁, operation_pool.filter_limit_operations iterInst filterInst mappingInst cloneInst
        operations filter mapping limit = Aeneas.Std.Result.ok out ∧
      out.val.length ≤ limit.val ∧
      (out.val.map toSpec).toArray.foldlM (processProposerSlashing config checks) s = .ok s₁ ∧
      SlashFrame hfork.electra (fun j => ∃ ps ∈ out.val.map toSpec, slashedIndex ps = j)
        (limit.val * exitStep preset B) (limit.val * rewardMax preset fork B) s s₁ := by
  obtain ⟨out, hout, hval⟩ := operation_pool.filter_limit_operations_eq iterInst filterInst
    mappingInst filter mapping limit cloneInst operations hinto hyields keep hfilter slashingOf
    hmapping
  have hlen : out.val.length ≤ limit.val := by
    rw [hval, List.length_map]
    exact List.length_take_le _ _
  -- The packed pool entries: a sublist of the pool, all accepted by the filter.
  have hsub : ((pool.filter keep).take limit.val).Sublist pool :=
    (List.take_sublist _ _).trans List.filter_sublist
  have hmem : ∀ op ∈ (pool.filter keep).take limit.val, op ∈ pool ∧ keep op = true :=
    fun op hop => List.mem_filter.mp (List.mem_of_mem_take hop)
  have hslashings : out.val.map toSpec =
      ((pool.filter keep).take limit.val).map (fun op => toSpec (slashingOf op)) := by
    rw [hval, List.map_map]
    rfl
  have hlen' : (out.val.map toSpec).length ≤ limit.val := by simpa using hlen
  -- Distinct Rust indices give distinct spec indices.
  have hdistinct : (out.val.map toSpec).Pairwise
      (fun a b => slashedIndex a ≠ slashedIndex b) := by
    rw [hslashings]
    simp only [List.pairwise_map]
    refine (hkeys.sublist hsub).imp_of_mem ?_
    intro a b ha hb hne heq
    refine hne (u64ToSpec_inj ?_)
    rw [← hindex a (hmem a ha).1, ← hindex b (hmem b hb).1]
    exact heq
  -- Each packed slashing comes from a pool entry that the filter accepted.
  have hfrom : ∀ ps ∈ out.val.map toSpec,
      ∃ op ∈ pool, keep op = true ∧ ps = toSpec (slashingOf op) := by
    intro ps hps
    rw [hslashings] at hps
    obtain ⟨op, hop, rfl⟩ := List.mem_map.mp hps
    exact ⟨op, (hmem op hop).1, (hmem op hop).2, rfl⟩
  -- The room `limit` slashings need covers the packed ones.
  have hstep := Nat.mul_le_mul_right (exitStep preset B) hlen'
  have hreward := Nat.mul_le_mul_right (rewardMax preset fork B) hlen'
  obtain ⟨s₁, hrun, hframe⟩ := proposerSlashings_foldlM_ok (electra := hfork.electra)
    (checks := checks) hchurn hfork.fulu hfork.before hroom (out.val.map toSpec)
    (fun _ => False) 0 0 s (SlashFrame.refl _ _ _ _ _) hdistinct (fun _ _ h => h)
    (fun ps hps => by
      obtain ⟨op, hpool, hk, rfl⟩ := hfrom ps hps
      exact ⟨(hkeep op hpool hk).2.1,
        proposerSlashingGates_of_coreOk hcorr (hindex op hpool) (hkeep op hpool hk).1,
        (hkeep op hpool hk).2.2⟩)
    (by omega) (fun b hb => by have := hrewards b hb; omega)
  refine ⟨out, s₁, hout, hlen, ?_, hframe.mono (fun j hj => ?_) (by omega) (by omega)⟩
  · rw [List.foldlM_toArray]
    exact hrun
  · rcases hj with hj | hj
    · exact hj.elim
    · exact hj

end

end OperationPool
