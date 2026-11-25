import OperationPool.FilterLimit
import OperationPool.ExclusionSet
import OperationPool.Bridge.StateCorr

/-!
# The packed voluntary exits are valid

`OperationPool::get_voluntary_exits` packs voluntary exits
with `filter_limit_operations`. `packed_exits_valid` states that the spec processes the packed
exits, in order, without an error.

The spec is EthLib's: `processVoluntaryExit`, folded over the packed exits as `processOperations`
folds it over the voluntary exits of a block body.

The theorem joins three proofs:

* `filter_limit_operations_eq` (extracted Rust): the packed exits are the first `limit` pool
  entries that the filter closure accepts.
* `exitGates_of_coresOk` (extracted Rust to spec): an exit that the cores of `verify_exit`
  accept passes the spec's `ExitGates`.
* `voluntaryExits_foldlM_ok` (spec, in `EthLibOps`): exits that name distinct validators and pass
  the gates are processed in order without an error.

`packed_exits_valid_after_slashings` adds the slashings of the block. `get_slashings_and_exits`
excludes from the exits every validator in a set, and `exclusionSet_covers` (extracted Rust)
proves that the set holds every validator the packed slashings can slash. The slash frame of the
state after the slashings then says that the checks of each packed exit still hold.

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

/-- The voluntary exits that `filter_limit_operations` packs are processed by the spec without
an error, in order, from any state `w` that has the slash frame of the packing state `s`.

The Rust side:

* `Op` is a pool entry and `X` a packed exit. `pool` is what the pool's iterator yields.
* `keep` and `exitOf` are what the filter and mapping closures compute.
* `message` reads the Rust `VoluntaryExit` of a packed exit, and `toSpec` gives the spec's form
  of the exit, with the same validator index and epoch.
* `hkeys`: the pool entries name pairwise distinct validators.
* `hkeep`: the filter accepts an entry only when its validator is outside `S`, both cores of
  `verify_exit` accept it on `view`, and its signature is valid.

The spec side:

* `hfork`, `hchurn`: the fork is Fulu, and the constants of the exit churn are positive.
* `hcorr`: the state Lighthouse reads corresponds to the spec state `s`.
* `hframe`: `w` is the state the exits run from. `S` holds the validators that the steps between
  `s` and `w` may have changed, and those steps moved the exit queue by `E` epochs at most.
* `hroom`: `B` bounds every effective balance of `s`, and the exit churn can be computed.
* `hexits`: the exit queue has room for `limit` exits, on top of the `E` epochs.

The conclusion: `filter_limit_operations` does not panic, it packs at most `limit` exits, and the
spec's fold over the packed exits succeeds.

On mainnet `limit` is 16. With `B` at 2048 ETH, `exitStep preset B` is 2049 epochs. -/
theorem packed_exits_valid (hfork : IsFulu fork) (hchurn : ChurnOk preset config)
    {it : It} {pool : List Op}
    (hinto : iterInst.into_iter operations = Aeneas.Std.Result.ok it)
    (hyields : operation_pool.Yields iterInst.iteratorInst it pool)
    (keep : Op → Bool)
    (hfilter : ∀ op ∈ pool, filterInst.call filter op = Aeneas.Std.Result.ok (keep op))
    (exitOf : Op → X)
    (hmapping : ∀ op ∈ pool, mappingInst.call mapping op = Aeneas.Std.Result.ok (exitOf op))
    (message : X → RVoluntaryExit) (toSpec : X → SignedVoluntaryExit)
    (hmessage : ∀ op ∈ pool,
      (toSpec (exitOf op)).message.validatorIndex = u64ToSpec (message (exitOf op)).validator_index ∧
      (toSpec (exitOf op)).message.epoch = u64ToSpec (message (exitOf op)).epoch)
    (hkeys : pool.Pairwise fun a b =>
      (message (exitOf a)).validator_index ≠ (message (exitOf b)).validator_index)
    {view : StateView} {s w : BeaconState preset fork}
    (hcorr : StateCorr config hfork.electra view s)
    {S : ValidatorIndex → Prop} {E R : Nat} (hframe : SlashFrame hfork.electra S E R s w)
    (hkeep : ∀ op ∈ pool, keep op = true →
      ¬ S (toSpec (exitOf op)).message.validatorIndex ∧
      CoresOk view (message (exitOf op)) ∧
      ExitSigned config checks s (toSpec (exitOf op)))
    {B : Nat} (hroom : QueueRoom config B s)
    (hexits : exitStart hfork.electra s + E + limit.val * exitStep preset B +
      config.MIN_VALIDATOR_WITHDRAWABILITY_DELAY.toNat < 2 ^ 64) :
    ∃ out w', operation_pool.filter_limit_operations iterInst filterInst mappingInst cloneInst
        operations filter mapping limit = Aeneas.Std.Result.ok out ∧
      out.val.length ≤ limit.val ∧
      (out.val.map toSpec).toArray.foldlM (processVoluntaryExit config checks) w = .ok w' := by
  obtain ⟨out, hout, hval⟩ := operation_pool.filter_limit_operations_eq iterInst filterInst
    mappingInst filter mapping limit cloneInst operations hinto hyields keep hfilter exitOf
    hmapping
  have hlen : out.val.length ≤ limit.val := by
    rw [hval, List.length_map]
    exact List.length_take_le _ _
  -- The packed pool entries: a sublist of the pool, all accepted by the filter.
  have hsub : ((pool.filter keep).take limit.val).Sublist pool :=
    (List.take_sublist _ _).trans List.filter_sublist
  have hmem : ∀ op ∈ (pool.filter keep).take limit.val, op ∈ pool ∧ keep op = true :=
    fun op hop => List.mem_filter.mp (List.mem_of_mem_take hop)
  have hexitsEq : out.val.map toSpec =
      ((pool.filter keep).take limit.val).map (fun op => toSpec (exitOf op)) := by
    rw [hval, List.map_map]
    rfl
  have hlen' : (out.val.map toSpec).length ≤ limit.val := by simpa using hlen
  have hfrom : ∀ sve ∈ out.val.map toSpec,
      ∃ op ∈ pool, keep op = true ∧ sve = toSpec (exitOf op) := by
    intro sve hsve
    rw [hexitsEq] at hsve
    obtain ⟨op, hop, rfl⟩ := List.mem_map.mp hsve
    exact ⟨op, (hmem op hop).1, (hmem op hop).2, rfl⟩
  -- Distinct Rust indices give distinct spec indices.
  have hdistinct : (out.val.map toSpec).Pairwise
      (fun a b => a.message.validatorIndex ≠ b.message.validatorIndex) := by
    rw [hexitsEq]
    simp only [List.pairwise_map]
    refine (hkeys.sublist hsub).imp_of_mem ?_
    intro a b ha hb hne heq
    refine hne (u64ToSpec_inj ?_)
    rw [← (hmessage a (hmem a ha).1).1, ← (hmessage b (hmem b hb).1).1]
    exact heq
  have hstep := Nat.mul_le_mul_right (exitStep preset B) hlen'
  obtain ⟨w', hrun, -⟩ := voluntaryExits_foldlM_ok (electra := hfork.electra) (checks := checks)
    hchurn hfork.before hroom (out.val.map toSpec) S E w hframe hdistinct
    (fun sve hsve => by
      obtain ⟨op, hpool, hk, rfl⟩ := hfrom sve hsve
      exact (hkeep op hpool hk).1)
    (fun sve hsve => by
      obtain ⟨op, hpool, hk, rfl⟩ := hfrom sve hsve
      exact ⟨exitGates_of_coresOk hcorr (hmessage op hpool).1 (hmessage op hpool).2
        (hkeep op hpool hk).2.1, (hkeep op hpool hk).2.2⟩)
    (by omega)
  refine ⟨out, w', hout, hlen, ?_⟩
  rw [List.foldlM_toArray]
  exact hrun

/-- `packed_exits_valid` for exits that run from the packing state itself: no step between
packing and the exits changes a validator. -/
theorem packed_exits_valid_direct (hfork : IsFulu fork) (hchurn : ChurnOk preset config)
    {it : It} {pool : List Op}
    (hinto : iterInst.into_iter operations = Aeneas.Std.Result.ok it)
    (hyields : operation_pool.Yields iterInst.iteratorInst it pool)
    (keep : Op → Bool)
    (hfilter : ∀ op ∈ pool, filterInst.call filter op = Aeneas.Std.Result.ok (keep op))
    (exitOf : Op → X)
    (hmapping : ∀ op ∈ pool, mappingInst.call mapping op = Aeneas.Std.Result.ok (exitOf op))
    (message : X → RVoluntaryExit) (toSpec : X → SignedVoluntaryExit)
    (hmessage : ∀ op ∈ pool,
      (toSpec (exitOf op)).message.validatorIndex = u64ToSpec (message (exitOf op)).validator_index ∧
      (toSpec (exitOf op)).message.epoch = u64ToSpec (message (exitOf op)).epoch)
    (hkeys : pool.Pairwise fun a b =>
      (message (exitOf a)).validator_index ≠ (message (exitOf b)).validator_index)
    {view : StateView} {s : BeaconState preset fork}
    (hcorr : StateCorr config hfork.electra view s)
    (hkeep : ∀ op ∈ pool, keep op = true →
      CoresOk view (message (exitOf op)) ∧ ExitSigned config checks s (toSpec (exitOf op)))
    {B : Nat} (hroom : QueueRoom config B s)
    (hexits : exitStart hfork.electra s + limit.val * exitStep preset B +
      config.MIN_VALIDATOR_WITHDRAWABILITY_DELAY.toNat < 2 ^ 64) :
    ∃ out s', operation_pool.filter_limit_operations iterInst filterInst mappingInst cloneInst
        operations filter mapping limit = Aeneas.Std.Result.ok out ∧
      out.val.length ≤ limit.val ∧
      (out.val.map toSpec).toArray.foldlM (processVoluntaryExit config checks) s = .ok s' :=
  packed_exits_valid iterInst filterInst mappingInst cloneInst operations filter mapping limit
    hfork hchurn hinto hyields keep hfilter exitOf hmapping message toSpec hmessage hkeys hcorr
    (SlashFrame.refl hfork.electra (fun _ => False) 0 0 s)
    (fun op hop hk => ⟨fun h => h, hkeep op hop hk⟩) hroom (by omega)

/-- The packed voluntary exits are processed by the spec without an error after the packed
proposer and attester slashings of the same block.

On top of the hypotheses of `packed_exits_valid`:

* `hexcl`: `excl` is the exclusion set that `get_slashings_and_exits` builds from the packed
  slashings. `rustProposerSlashings` and `rustAttesterIndices` are the Rust data it is built
  from.
* `hproposers`, `hattesters`: the spec's slashings carry the same proposer indices and
  attesting indices as the Rust data.
* `hframe`: the slashings moved the packing state `s` to `s₂`, and changed only validators that
  they name. `packed_proposer_slashings_valid` and `packed_attester_slashings_valid` conclude
  this frame.
* `hkeep`: the filter accepts a pool entry only when its validator is outside `excl`, both
  cores of `verify_exit` accept it, and its signature is valid.

The exits run from `s₂`. -/
theorem packed_exits_valid_after_slashings (hfork : IsFulu fork)
    (hchurn : ChurnOk preset config) {it : It} {pool : List Op}
    (hinto : iterInst.into_iter operations = Aeneas.Std.Result.ok it)
    (hyields : operation_pool.Yields iterInst.iteratorInst it pool)
    (keep : Op → Bool)
    (hfilter : ∀ op ∈ pool, filterInst.call filter op = Aeneas.Std.Result.ok (keep op))
    (exitOf : Op → X)
    (hmapping : ∀ op ∈ pool, mappingInst.call mapping op = Aeneas.Std.Result.ok (exitOf op))
    (message : X → RVoluntaryExit) (toSpec : X → SignedVoluntaryExit)
    (hmessage : ∀ op ∈ pool,
      (toSpec (exitOf op)).message.validatorIndex = u64ToSpec (message (exitOf op)).validator_index ∧
      (toSpec (exitOf op)).message.epoch = u64ToSpec (message (exitOf op)).epoch)
    (hkeys : pool.Pairwise fun a b =>
      (message (exitOf a)).validator_index ≠ (message (exitOf b)).validator_index)
    {view : StateView} {s s₂ : BeaconState preset fork}
    (hcorr : StateCorr config hfork.electra view s)
    {initial excl : operation_pool.IndexSet}
    {rustProposerSlashings :
      Aeneas.Std.Slice operation_pool.types.slashing.proposer_slashing.ProposerSlashing}
    {rustAttesterIndices : List (Aeneas.Std.Slice Aeneas.Std.U64 × Aeneas.Std.Slice Aeneas.Std.U64)}
    (hexcl : operation_pool.exclusionSet initial rustProposerSlashings rustAttesterIndices =
      Aeneas.Std.Result.ok excl)
    (proposerSlashings : List ProposerSlashing)
    (attesterSlashings : List (AttesterSlashing preset fork))
    (hproposers : ∀ ps ∈ proposerSlashings, ∃ r ∈ rustProposerSlashings.val,
      u64ToSpec r.signed_header_1.message.proposer_index = slashedIndex ps)
    (hattesters : ∀ asl ∈ attesterSlashings, ∃ p ∈ rustAttesterIndices,
      asl.attestation1.attestingIndices.data.toList = p.1.val.map u64ToSpec ∧
      asl.attestation2.attestingIndices.data.toList = p.2.val.map u64ToSpec)
    {E R : Nat}
    (hframe : SlashFrame hfork.electra
      (fun j => (∃ ps ∈ proposerSlashings, slashedIndex ps = j) ∨
        ∃ asl ∈ attesterSlashings, j ∈ slashingIndices asl) E R s s₂)
    (hkeep : ∀ op ∈ pool, keep op = true →
      (message (exitOf op)).validator_index ∉ excl.toList ∧
      CoresOk view (message (exitOf op)) ∧
      ExitSigned config checks s (toSpec (exitOf op)))
    {B : Nat} (hroom : QueueRoom config B s)
    (hexits : exitStart hfork.electra s + E + limit.val * exitStep preset B +
      config.MIN_VALIDATOR_WITHDRAWABILITY_DELAY.toNat < 2 ^ 64) :
    ∃ out s₃, operation_pool.filter_limit_operations iterInst filterInst mappingInst cloneInst
        operations filter mapping limit = Aeneas.Std.Result.ok out ∧
      out.val.length ≤ limit.val ∧
      (out.val.map toSpec).toArray.foldlM (processVoluntaryExit config checks) s₂ = .ok s₃ := by
  obtain ⟨_, hcovP, hcovA⟩ := operation_pool.exclusionSet_covers hexcl
  -- The Rust exclusion set holds every validator the slashings name.
  have hframe' : SlashFrame hfork.electra (fun i => ∃ x ∈ excl.toList, u64ToSpec x = i) E R
      s s₂ := by
    refine hframe.mono (fun i hi => ?_) (Nat.le_refl _) (Nat.le_refl _)
    rcases hi with ⟨ps, hps, rfl⟩ | ⟨asl, hasl, hi⟩
    · obtain ⟨r, hr, heq⟩ := hproposers ps hps
      exact ⟨_, hcovP r hr, heq⟩
    · obtain ⟨h1, h2⟩ := mem_slashingIndices.mp hi
      obtain ⟨p, hp, hl1, hl2⟩ := hattesters asl hasl
      rw [hl1] at h1
      rw [hl2] at h2
      obtain ⟨x, hx, rfl⟩ := List.mem_map.mp h1
      obtain ⟨y, hy, hxy⟩ := List.mem_map.mp h2
      have hyx : y = x := u64ToSpec_inj hxy
      subst hyx
      exact ⟨y, hcovA p hp y hx hy, rfl⟩
  refine packed_exits_valid iterInst filterInst mappingInst cloneInst operations filter mapping
    limit hfork hchurn hinto hyields keep hfilter exitOf hmapping message toSpec hmessage hkeys
    hcorr hframe' (fun op hop hk => ?_) hroom hexits
  obtain ⟨hnot, hcores, hsig⟩ := hkeep op hop hk
  refine ⟨?_, hcores, hsig⟩
  rintro ⟨x, hx, hxeq⟩
  have hxidx : x = (message (exitOf op)).validator_index :=
    u64ToSpec_inj (hxeq.trans (hmessage op hop).1)
  exact hnot (hxidx ▸ hx)

end

end OperationPool
