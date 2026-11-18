import OperationPool.MaxCover
import OperationPool.Bridge.StateCorr
import EthCLSpecs.Proofs.Fulu.AttesterSlashings

/-!
# The packed attester slashings are valid

`OperationPool::get_attester_slashings` builds one `AttesterSlashingMaxCover` for each attester
slashing of the pool, and packs the slashings that `maximum_cover` picks.
`packed_attester_slashings_valid` states that the spec processes the packed slashings, in order,
without an error, after the proposer slashings of the same block.

The spec rejects an attester slashing that slashes no validator. A slashing packed earlier in the
block can slash every validator that a later one names, so the order of the picks matters. The
theorem joins two proofs:

* `maximum_cover_spec` (extracted Rust): each picked item covers an element that no item picked
  before it covered.
* `attesterSlashings_succeed` (spec, in Etheorem): slashings succeed in order when each one walks
  a validator that is slashable on the packing state and that no earlier slashing walks.

The link between the two is `hcover`. It states what an `AttesterSlashingMaxCover` covers: the
validators that both attestations name, that `Validator::is_slashable_at` accepts, and that the
set of proposers to be slashed does not hold.

`packed_attester_slashings_valid_uniform` states the result with one bound on the exit queue and
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

variable {T Obj Im Set I It : Type}
  (iterInst : Aeneas.Std.core.iter.traits.collect.IntoIterator I T It)
  (inst : operation_pool.max_cover.MaxCover T Obj Im Set)
  (items : I) (limit : Aeneas.Std.Usize) (label : Aeneas.Std.Str)

omit [Preset] [Config] [HasherTag] [CryptoBackend] [PresetValid] [ConfigValid] in
/-- An element of a list at the position where a second list of the same length has `a`. -/
theorem exists_getElem?_of_length_eq {α β : Type} {xs : List α} {ys : List β}
    (hlen : xs.length = ys.length) {i : Nat} {y : β} (hy : ys[i]? = some y) :
    ∃ x, xs[i]? = some x := by
  have hi : i < ys.length := (List.getElem?_eq_some_iff.mp hy).1
  exact ⟨xs[i]'(by omega), List.getElem?_eq_getElem _⟩

/-- The attester slashings that `maximum_cover` picks are processed by the spec without an
error, in order, from a state `s₁` that has the slash frame of the packing state `s`. In a block,
`s₁` is the state after the packed proposer slashings.

The Rust side:

* `T` is `AttesterSlashingMaxCover`, and `covers` lists the covers the iterator yields: one for
  each pool entry that `AttesterSlashingMaxCover::new` accepts.
* `m` is the model of the `MaxCover` implementation. An element is a validator index.
* `toSpec` gives the spec's form of the slashing a cover holds.
* `excluded` is the set `to_be_slashed` that `new` reads.
* `hcover`: a cover covers the validators that both attestations of its slashing name, that
  `Validator::is_slashable_at` accepts on `view`, and that `excluded` does not hold.
* `hchecks`: the slashing of each cover passes the checks of `process_attester_slashing` that
  run before the loop, on `s`.

The spec side:

* `hcorr`: the state Lighthouse reads corresponds to the spec state `s`.
* `hframe`, `hexcluded`: the operations before the attester slashings moved `s` to `s₁`, and
  `excluded` holds every validator they could slash.
* `hsize`: the balances have the registry's length. Every reachable state has this.
* `hproposer`: the proposer of the slot is in the registry.

The conclusion: `maximum_cover` does not panic, it picks at most `limit` covers, each picked
cover holds the slashing of a yielded cover, and when the state has room the spec's fold over the
packed slashings succeeds. The end state has the slash frame of `s`. -/
theorem packed_attester_slashings_valid {it : It} {covers : List T}
    (hinto : iterInst.into_iter items = Aeneas.Std.Result.ok it)
    (hyields : operation_pool.Yields iterInst.iteratorInst it covers)
    (hlen : covers.length ≤ Aeneas.Std.Usize.max)
    (m : operation_pool.MaxCoverModel inst Aeneas.Std.U64)
    (toSpec : Im → AttesterSlashing)
    {view : StateView} {s s₁ : BeaconState} (hcorr : StateCorr view s)
    (excluded : Aeneas.Std.U64 → Prop)
    (hcover : ∀ a ∈ covers, ∀ u, m.cov a u ↔
      u64ToSpec u ∈ attesterSlashingIndices (toSpec (m.im a)) ∧ SlashableOn view u ∧
        ¬ excluded u)
    (hchecks : ∀ a ∈ covers, AttesterSlashingChecks s (toSpec (m.im a)))
    {S : ValidatorIndex → Prop} {E R : Nat} (hframe : SlashFrame S E R s s₁)
    (hexcluded : ∀ u, S (u64ToSpec u) → excluded u)
    (hsize : s.balances.val.size = s.validators.val.size)
    (hproposer : (blockProposer s).toNat < s.validators.val.size) :
    ∃ out, operation_pool.max_cover.maximum_cover iterInst inst items limit label =
        Aeneas.Std.Result.ok out ∧
      out.val.length ≤ limit.val ∧
      (∀ t ∈ out.val, ∃ a ∈ covers, m.im t = m.im a) ∧
      (exitStart s + E +
          attesterSlashingsExitSteps s (out.val.map fun t => toSpec (m.im t)) +
          Const.minValidatorWithdrawabilityDelay.toNat < 2 ^ 64 →
        (s.balances[(blockProposer s).toNat]!).toNat + R +
          attesterSlashingsRewards s (out.val.map fun t => toSpec (m.im t)) < 2 ^ 64 →
        ∃ s₂, runPure (processAttesterSlashings
            (out.val.map fun t => toSpec (m.im t)).toArray) s₁ = .ok ((), s₂) ∧
          SlashFrame
            (fun j => S j ∨ ∃ asl ∈ out.val.map fun t => toSpec (m.im t),
              j ∈ attesterSlashingIndices asl)
            (E + attesterSlashingsExitSteps s (out.val.map fun t => toSpec (m.im t)))
            (R + attesterSlashingsRewards s (out.val.map fun t => toSpec (m.im t))) s s₂) := by
  obtain ⟨out, picked, hout, hlenout, hplen, hpmem, hfresh⟩ :=
    operation_pool.maximum_cover_spec m iterInst items hinto hyields hlen limit label
  -- A picked cover and the yielded cover at the same position hold the same slashing.
  have hsame : ∀ (i : Nat) (t : T), out.val[i]? = some t →
      ∃ a, picked[i]? = some a ∧ a ∈ covers ∧ m.im t = m.im a := by
    intro i t ht
    obtain ⟨a, ha⟩ := exists_getElem?_of_length_eq hplen ht
    exact ⟨a, ha, hpmem a (List.mem_of_getElem? ha), (hfresh i a ha).1 t ht⟩
  have hfrom : ∀ t ∈ out.val, ∃ a ∈ covers, m.im t = m.im a := by
    intro t ht
    obtain ⟨i, hi⟩ := List.getElem?_of_mem ht
    obtain ⟨a, _, hamem, heq⟩ := hsame i t hi
    exact ⟨a, hamem, heq⟩
  refine ⟨out, hout, hlenout, hfrom, ?_⟩
  intro hexits hrewards
  obtain ⟨s₂, hrun, hframe₂⟩ := attesterSlashings_succeed
    (out.val.map fun t => toSpec (m.im t)).toArray hframe
    (fun asl hasl => by
      obtain ⟨t, ht, rfl⟩ := List.mem_map.mp (List.mem_toArray.mp hasl)
      obtain ⟨a, ha, heq⟩ := hfrom t ht
      rw [heq]
      exact hchecks a ha)
    (fun pre asl post hsplit => by
      -- The slashing at position `pre.length`, its picked cover and its yielded cover.
      have hsplit' : out.val.map (fun t => toSpec (m.im t)) = pre ++ asl :: post := hsplit
      have hasl : (out.val.map fun t => toSpec (m.im t))[pre.length]? = some asl := by
        rw [hsplit']
        simp
      rw [List.getElem?_map, Option.map_eq_some_iff] at hasl
      obtain ⟨t, ht, rfl⟩ := hasl
      obtain ⟨a, ha, hamem, heq⟩ := hsame pre.length t ht
      -- The validator that the yielded cover has and no earlier pick has.
      obtain ⟨_, u, hu, hnew⟩ := hfresh pre.length a ha
      obtain ⟨hidx, hslash, hnotex⟩ := (hcover a hamem u).mp hu
      refine ⟨u64ToSpec u, by rw [heq]; exact hidx, fun hS => hnotex (hexcluded u hS),
        isSlashableValidator_of_slashableOn hcorr hslash, ?_⟩
      intro b hb hmem
      -- An earlier slashing is the slashing of an earlier pick.
      obtain ⟨j, hj⟩ := List.getElem?_of_mem hb
      have hjlt : j < pre.length := (List.getElem?_eq_some_iff.mp hj).1
      have hb' : (out.val.map fun t => toSpec (m.im t))[j]? = some b := by
        rw [hsplit', List.getElem?_append_left hjlt]
        exact hj
      rw [List.getElem?_map, Option.map_eq_some_iff] at hb'
      obtain ⟨t', ht', rfl⟩ := hb'
      obtain ⟨a', ha', ha'mem, heq'⟩ := hsame j t' ht'
      rw [heq'] at hmem
      exact hnew j a' hjlt ha' ((hcover a' ha'mem u).mpr ⟨hmem, hslash, hnotex⟩))
    hsize hproposer hexits hrewards
  exact ⟨s₂, hrun, hframe₂.mono (fun j hj => by
    rcases hj with hj | ⟨asl, hasl, hj⟩
    · exact Or.inl hj
    · exact Or.inr ⟨asl, List.mem_toArray.mp hasl, hj⟩) (Nat.le_refl _) (Nat.le_refl _)⟩

/-- `packed_attester_slashings_valid` with bounds read on the packing state. `B` bounds the
effective balance of every validator. The exit queue has room for the exits of `limit` slashings
that each name as many validators as `attesting_indices` can hold, on top of the `E` epochs the
earlier operations took. The block proposer's balance has room for their whistleblower rewards,
on top of the `R` Gwei the earlier operations paid.

On mainnet `limit` is 1 and `attesting_indices` holds `131072` validators. `exitStep (2 ^ 64 - 1)`
is about `1.4e8` epochs. With `B` at 2048 ETH, one whistleblower reward is `5e8` Gwei. -/
theorem packed_attester_slashings_valid_uniform {it : It} {covers : List T}
    (hinto : iterInst.into_iter items = Aeneas.Std.Result.ok it)
    (hyields : operation_pool.Yields iterInst.iteratorInst it covers)
    (hlen : covers.length ≤ Aeneas.Std.Usize.max)
    (m : operation_pool.MaxCoverModel inst Aeneas.Std.U64)
    (toSpec : Im → AttesterSlashing)
    {view : StateView} {s s₁ : BeaconState} (hcorr : StateCorr view s)
    (excluded : Aeneas.Std.U64 → Prop)
    (hcover : ∀ a ∈ covers, ∀ u, m.cov a u ↔
      u64ToSpec u ∈ attesterSlashingIndices (toSpec (m.im a)) ∧ SlashableOn view u ∧
        ¬ excluded u)
    (hchecks : ∀ a ∈ covers, AttesterSlashingChecks s (toSpec (m.im a)))
    {S : ValidatorIndex → Prop} {E R : Nat} (hframe : SlashFrame S E R s s₁)
    (hexcluded : ∀ u, S (u64ToSpec u) → excluded u)
    (hsize : s.balances.val.size = s.validators.val.size)
    (hproposer : (blockProposer s).toNat < s.validators.val.size)
    {B : Nat} (hcap : ∀ x ∈ s.validators.val, x.effectiveBalance.toNat ≤ B)
    (hexits : exitStart s + E + limit.val *
      (Const.maxValidatorsPerCommittee * Const.maxCommitteesPerSlot * exitStep (2 ^ 64 - 1)) +
      Const.minValidatorWithdrawabilityDelay.toNat < 2 ^ 64)
    (hrewards : (s.balances[(blockProposer s).toNat]!).toNat + R + limit.val *
      (Const.maxValidatorsPerCommittee * Const.maxCommitteesPerSlot *
        (B / (UInt64.ofNat Const.whistleblowerRewardQuotientElectra).toNat)) < 2 ^ 64) :
    ∃ out s₂, operation_pool.max_cover.maximum_cover iterInst inst items limit label =
        Aeneas.Std.Result.ok out ∧
      out.val.length ≤ limit.val ∧
      (∀ t ∈ out.val, ∃ a ∈ covers, m.im t = m.im a) ∧
      runPure (processAttesterSlashings
        (out.val.map fun t => toSpec (m.im t)).toArray) s₁ = .ok ((), s₂) ∧
      SlashFrame
        (fun j => S j ∨ ∃ asl ∈ out.val.map fun t => toSpec (m.im t),
          j ∈ attesterSlashingIndices asl)
        (E + limit.val *
          (Const.maxValidatorsPerCommittee * Const.maxCommitteesPerSlot * exitStep (2 ^ 64 - 1)))
        (R + limit.val * (Const.maxValidatorsPerCommittee * Const.maxCommitteesPerSlot *
          (B / (UInt64.ofNat Const.whistleblowerRewardQuotientElectra).toNat))) s s₂ := by
  obtain ⟨out, hout, hlenout, hfrom, hrun⟩ := packed_attester_slashings_valid iterInst inst items
    limit label hinto hyields hlen m toSpec hcorr excluded hcover hchecks hframe hexcluded hsize
    hproposer
  -- The room the packed slashings need is at most the room `limit` slashings need.
  have hlen' : (out.val.map fun t => toSpec (m.im t)).length ≤ limit.val := by simpa using hlenout
  have h1 := attesterSlashingsExitSteps_le s (out.val.map fun t => toSpec (m.im t))
  have h2 := attesterSlashingsRewards_le_of_bound s (out.val.map fun t => toSpec (m.im t)) hcap
  have h3 := Nat.mul_le_mul_right
    (Const.maxValidatorsPerCommittee * Const.maxCommitteesPerSlot * exitStep (2 ^ 64 - 1)) hlen'
  have h4 := Nat.mul_le_mul_right
    (Const.maxValidatorsPerCommittee * Const.maxCommitteesPerSlot *
      (B / (UInt64.ofNat Const.whistleblowerRewardQuotientElectra).toNat)) hlen'
  obtain ⟨s₂, hs₂, hframe₂⟩ := hrun (by omega) (by omega)
  exact ⟨out, s₂, hout, hlenout, hfrom, hs₂, hframe₂.mono (fun _ h => h) (by omega) (by omega)⟩

end

end OperationPool
