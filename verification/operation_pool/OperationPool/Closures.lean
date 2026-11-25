import OperationPool.Concrete
import OperationPool.Bridge.StateCorr

/-!
# The closures of `get_slashings_and_exits`

`OperationPool::get_slashings_and_exits` gives three closures to the packing functions. Each
closure reads Lighthouse's `BeaconState<E>`, so none of them is extracted. This file models each
one as a function that runs the extracted cores on a `StateView`:

* `proposerSlashingFilter`: the filter closure for proposer slashings. It calls the extracted
  core of `verify_proposer_slashing`.
* `voluntaryExitFilter`: the filter closure of `get_voluntary_exits`. It calls the two extracted
  cores of `verify_exit`, in the order of `verify_exit` (`verifyExit`).
* `attesterCovers`: the covers that the iterator of `get_attester_slashings` yields. Each cover
  is the result of `AttesterSlashingMaxCover::new` (`attesterCoverNew`), which calls the
  extracted `Validator::is_slashable_at`.

Each model has a theorem that states what it computes. No model panics.

A closure also makes checks that no extracted core holds. The models take those as functions:

* `stillValid` is `SigVerifiedOp::signature_is_still_valid` on the fork of the state;
* `headersOk` is the three checks of `verify_proposer_slashing` on the two headers.

Everything here is a model. That the Rust closures compute what these functions compute is
TRUSTED. Each definition quotes the Rust it models.
-/

open Aeneas Aeneas.Std Result

namespace OperationPool

open OperationPool.Bridge
open operation_pool (IndexSet AttesterCover)

/-- The Rust `ProposerSlashing`, as the extraction of the `operation_pool` crate names it. -/
abbrev RProposerSlashing : Type :=
  operation_pool.types.slashing.proposer_slashing.ProposerSlashing

/-! ## The filter for proposer slashings -/

section Proposer

variable {OpP : Type}

/-- The filter closure for proposer slashings:

```rust
|slashing| {
    slashing.signature_is_still_valid(&state.fork())
        && verify_proposer_slashing(slashing.as_inner(), state, VerifySignatures::False, spec)
            .is_ok()
}
```

`slashingOf` is `as_inner`. Without signatures, `verify_proposer_slashing` makes the three checks
on the headers and then calls its core on the proposer's record. -/
def proposerSlashingFilter (view : StateView) (stillValid : OpP → Bool)
    (headersOk : RProposerSlashing → Bool) (slashingOf : OpP → RProposerSlashing) (op : OpP) :
    Result Bool :=
  if stillValid op && headersOk (slashingOf op) then do
    let checks ←
      state_processing.per_block_processing.verify_proposer_slashing.verify_proposer_slashing_validator_checks
        (view.validatorAt (slashingOf op).signed_header_1.message.proposer_index)
        (slashingOf op).signed_header_1.message.proposer_index view.currentEpoch
    core.result.Result.is_ok checks
  else ok false

/-- The filter for proposer slashings does not panic. It accepts an entry exactly when the
signature is still valid, the headers pass their checks, and the core of
`verify_proposer_slashing` accepts the proposer on the view. -/
theorem proposerSlashingFilter_spec (view : StateView) (stillValid : OpP → Bool)
    (headersOk : RProposerSlashing → Bool) (slashingOf : OpP → RProposerSlashing) (op : OpP) :
    ∃ b, proposerSlashingFilter view stillValid headersOk slashingOf op = ok b ∧
      (b = true ↔ stillValid op = true ∧ headersOk (slashingOf op) = true ∧
        ProposerCoreOk view (slashingOf op).signed_header_1.message.proposer_index) := by
  obtain ⟨r, hr, -⟩ := state_processing.verify_proposer_slashing_validator_checks_ok_iff
    (view.validatorAt (slashingOf op).signed_header_1.message.proposer_index)
    (slashingOf op).signed_header_1.message.proposer_index view.currentEpoch
  unfold proposerSlashingFilter ProposerCoreOk
  by_cases h : (stillValid op && headersOk (slashingOf op)) = true
  · rw [ite_eq_left h, hr]
    simp only [Bool.and_eq_true] at h
    cases r with
    | Ok u => exact ⟨true, by simp [core.result.Result.is_ok], by simp [h]⟩
    | Err e => exact ⟨false, by simp [core.result.Result.is_ok], by simp⟩
  · rw [ite_eq_right h]
    refine ⟨false, rfl, ?_⟩
    simp only [Bool.and_eq_true] at h
    simp only [Bool.false_eq_true, false_iff]
    exact fun hall => h ⟨hall.1, hall.2.1⟩

end Proposer

/-! ## The filter for voluntary exits -/

section Exit

variable {Op : Type}

/-- `verify_exit(state, None, exit, VerifySignatures::False, spec)`: the core for the validator's
record, and then the core for the pending balance.

```rust
verify_exit_validator_checks(
    state.validators().get(exit.validator_index as usize),
    exit,
    current_epoch,
    spec.far_future_epoch,
    spec.shard_committee_period,
)?;
verify_exit_pending_balance(
    exit.validator_index,
    state.get_pending_balance_to_withdraw(exit.validator_index as usize).ok(),
)
``` -/
def verifyExit (view : StateView) (exit : RVoluntaryExit) :
    Result (core.result.Result Unit
      (state_processing.per_block_processing.errors.BlockOperationError
        state_processing.per_block_processing.errors.ExitInvalid)) := do
  let checks ←
    state_processing.per_block_processing.verify_exit.verify_exit_validator_checks
      (view.validatorAt exit.validator_index) exit view.currentEpoch view.farFutureEpoch
      view.shardCommitteePeriod
  match checks with
  | .Ok _ =>
    state_processing.per_block_processing.verify_exit.verify_exit_pending_balance
      exit.validator_index (view.pendingAt exit.validator_index)
  | .Err e => ok (.Err e)

/-- `verifyExit` does not panic, and returns `Ok` exactly when both cores of `verify_exit` accept
the exit on the view. -/
theorem verifyExit_spec (view : StateView) (exit : RVoluntaryExit) :
    ∃ r, verifyExit view exit = ok r ∧ (r = .Ok () ↔ CoresOk view exit) := by
  obtain ⟨r1, hr1, -⟩ := state_processing.verify_exit_validator_checks_ok_iff
    (view.validatorAt exit.validator_index) exit view.currentEpoch view.farFutureEpoch
    view.shardCommitteePeriod
  obtain ⟨r2, hr2, -⟩ := state_processing.verify_exit_pending_balance_ok_iff
    exit.validator_index (view.pendingAt exit.validator_index)
  unfold verifyExit CoresOk
  rw [hr1, hr2]
  cases r1 with
  | Ok u => exact ⟨r2, by simp, by simp⟩
  | Err e => exact ⟨.Err e, by simp, by simp⟩

/-- The filter closure of `get_voluntary_exits`:

```rust
|exit| {
    filter(exit.as_inner())
        && exit.signature_is_still_valid(&state.fork())
        && verify_exit(state, None, exit.as_inner(), VerifySignatures::False, spec)
            .is_ok()
}
```

`filter` is the closure that `get_slashings_and_exits` gives it:

```rust
|exit| !to_be_slashed.contains(&exit.message.validator_index)
```

`exitOf` reads the `VoluntaryExit` of a pool entry: `as_inner().message`. -/
def voluntaryExitFilter (view : StateView) (toBeSlashed : IndexSet) (stillValid : Op → Bool)
    (exitOf : Op → RVoluntaryExit) (op : Op) : Result Bool :=
  if !decide ((exitOf op).validator_index ∈ toBeSlashed.toList) && stillValid op then do
    let verified ← verifyExit view (exitOf op)
    core.result.Result.is_ok verified
  else ok false

/-- The filter for voluntary exits does not panic. It accepts an entry exactly when the validator
is outside `toBeSlashed`, the signature is still valid, and both cores of `verify_exit` accept
the exit on the view. -/
theorem voluntaryExitFilter_spec (view : StateView) (toBeSlashed : IndexSet)
    (stillValid : Op → Bool) (exitOf : Op → RVoluntaryExit) (op : Op) :
    ∃ b, voluntaryExitFilter view toBeSlashed stillValid exitOf op = ok b ∧
      (b = true ↔ (exitOf op).validator_index ∉ toBeSlashed.toList ∧ stillValid op = true ∧
        CoresOk view (exitOf op)) := by
  obtain ⟨r, hr, hiff⟩ := verifyExit_spec view (exitOf op)
  unfold voluntaryExitFilter
  by_cases h : (!decide ((exitOf op).validator_index ∈ toBeSlashed.toList) && stillValid op) = true
  · rw [ite_eq_left h, hr]
    simp only [Bool.and_eq_true, Bool.not_eq_true', decide_eq_false_iff_not] at h
    cases r with
    | Ok u => exact ⟨true, by simp [core.result.Result.is_ok], by simp [h, ← hiff]⟩
    | Err e =>
      refine ⟨false, by simp [core.result.Result.is_ok], ?_⟩
      simp only [Bool.false_eq_true, false_iff]
      exact fun hall => nomatch hiff.mpr hall.2.2
  · rw [ite_eq_right h]
    refine ⟨false, rfl, ?_⟩
    simp only [Bool.and_eq_true, Bool.not_eq_true', decide_eq_false_iff_not] at h
    simp only [Bool.false_eq_true, false_iff]
    exact fun hall => h ⟨hall.1, hall.2.1⟩

end Exit

/-! ## The covers of the attester slashings -/

section Attester

variable {OpA Im : Type}

/-- `proposer_slashings.iter().map(|s| s.proposer_index()).collect()`: the set `to_be_slashed`
that `get_attester_slashings` reads. -/
def proposerIndexSet (slashings : List RProposerSlashing) : IndexSet :=
  std.collections.hash.set.HashSet.ofList
    (slashings.map fun slashing => slashing.signed_header_1.message.proposer_index).dedup

/-- The set of proposer indices holds the proposer of each slashing, and no other index. -/
theorem mem_proposerIndexSet {slashings : List RProposerSlashing} {u : U64} :
    u ∈ (proposerIndexSet slashings).toList ↔
      ∃ slashing ∈ slashings, slashing.signed_header_1.message.proposer_index = u := by
  show u ∈ (slashings.map fun slashing => slashing.signed_header_1.message.proposer_index).dedup ↔ _
  rw [List.mem_dedup, List.mem_map]

/-- The indices that both lists hold, in ascending order and without repeats. In
`get_slashable_indices_modular` this is `&attesting_indices_1 & &attesting_indices_2`, where each
operand is a `BTreeSet` collected from the attesting indices of one attestation. -/
def commonIndices (first second : List U64) : List U64 :=
  ((first.filter fun index => decide (index ∈ second)).dedup).mergeSort
    fun a b => decide (a.val ≤ b.val)

/-- `commonIndices` holds the indices that are in both lists. -/
theorem mem_commonIndices {first second : List U64} {u : U64} :
    u ∈ commonIndices first second ↔ u ∈ first ∧ u ∈ second := by
  simp [commonIndices]

/-- The loop of `get_slashable_indices_modular`, with the closure that
`AttesterSlashingMaxCover::new` gives it:

```rust
for index in &attesting_indices_1 & &attesting_indices_2 {
    let validator = state
        .validators()
        .get(index as usize)
        .ok_or_else(|| error(Invalid::UnknownValidator(index)))?;

    if validator.is_slashable_at(epoch) && !proposer_slashing_indices.contains(&index) {
        slashable_indices.push(index);
    }
}
```

The second list is `slashable_indices`. The result is `none` for the error. -/
def slashableIndices (view : StateView) (toBeSlashed : IndexSet) :
    List U64 → List U64 → Result (Option (List U64))
  | [], slashable => ok (some slashable)
  | index :: rest, slashable =>
    match view.validatorAt index with
    | none => ok none
    | some validator => do
      let isSlashable ←
        state_processing.types.validator.validator.Validator.is_slashable_at validator
          view.currentEpoch
      if isSlashable && !decide (index ∈ toBeSlashed.toList) then
        slashableIndices view toBeSlashed rest (slashable ++ [index])
      else slashableIndices view toBeSlashed rest slashable

/-- The loop does not panic. When it returns no error, it has added exactly the indices that have
a validator `Validator::is_slashable_at` accepts, and that `toBeSlashed` does not hold. -/
theorem slashableIndices_spec (view : StateView) (toBeSlashed : IndexSet) :
    ∀ (indices slashable : List U64),
      ∃ r, slashableIndices view toBeSlashed indices slashable = ok r ∧
        ∀ l, r = some l → ∀ u, u ∈ l ↔
          u ∈ slashable ∨ (u ∈ indices ∧ SlashableOn view u ∧ u ∉ toBeSlashed.toList)
  | [], slashable => ⟨some slashable, rfl, by
      rintro l ⟨rfl⟩ u
      simp⟩
  | index :: rest, slashable => by
    unfold slashableIndices
    cases hv : view.validatorAt index with
    | none => exact ⟨none, rfl, fun l hl => nomatch hl⟩
    | some validator =>
      simp only [state_processing.is_slashable_at_eq, bind_ok]
      -- `SlashableOn` at `index` is the test of the loop.
      have hon : SlashableOn view index ↔
          (validator.slashed = false ∧ validator.activation_epoch.val ≤ view.currentEpoch.val ∧
            view.currentEpoch.val < validator.withdrawable_epoch.val) := by
        unfold SlashableOn
        rw [hv]
        simp [state_processing.is_slashable_at_eq]
      by_cases hc : (decide (validator.slashed = false ∧
          validator.activation_epoch.val ≤ view.currentEpoch.val ∧
          view.currentEpoch.val < validator.withdrawable_epoch.val) &&
          !decide (index ∈ toBeSlashed.toList)) = true
      · rw [ite_eq_left hc]
        obtain ⟨r, hr, hmem⟩ := slashableIndices_spec view toBeSlashed rest (slashable ++ [index])
        refine ⟨r, hr, fun l hl u => ?_⟩
        simp only [Bool.and_eq_true, decide_eq_true_eq, Bool.not_eq_true',
          decide_eq_false_iff_not] at hc
        rw [hmem l hl u, List.mem_append, List.mem_singleton, List.mem_cons]
        constructor
        · rintro ((h | rfl) | ⟨h1, h2⟩)
          · exact Or.inl h
          · exact Or.inr ⟨Or.inl rfl, hon.mpr hc.1, hc.2⟩
          · exact Or.inr ⟨Or.inr h1, h2⟩
        · rintro (h | ⟨rfl | h1, h2⟩)
          · exact Or.inl (Or.inl h)
          · exact Or.inl (Or.inr rfl)
          · exact Or.inr ⟨h1, h2⟩
      · rw [ite_eq_right hc]
        obtain ⟨r, hr, hmem⟩ := slashableIndices_spec view toBeSlashed rest slashable
        refine ⟨r, hr, fun l hl u => ?_⟩
        simp only [Bool.and_eq_true, decide_eq_true_eq, Bool.not_eq_true',
          decide_eq_false_iff_not] at hc
        rw [hmem l hl u, List.mem_cons]
        constructor
        · rintro (h | ⟨h1, h2⟩)
          · exact Or.inl h
          · exact Or.inr ⟨Or.inr h1, h2⟩
        · rintro (h | ⟨rfl | h1, h2⟩)
          · exact Or.inl h
          · exact absurd ⟨hon.mp h2.1, h2.2⟩ hc
          · exact Or.inr ⟨h1, h2⟩

/-- The second loop of `AttesterSlashingMaxCover::new`: the map from each slashable validator to
its effective balance.

```rust
for vd in slashable_validators {
    let eff_balance = state.get_effective_balance(vd as usize).ok()?;
    effective_balances.insert(vd, eff_balance);
}
```

`get_effective_balance` reads the field of the validator's record. The indices have no repeats,
so no `insert` replaces an entry. -/
def effectiveBalances (view : StateView) : List U64 → Option (List (U64 × U64))
  | [] => some []
  | index :: rest =>
    match view.validatorAt index, effectiveBalances view rest with
    | some validator, some balances => some ((index, validator.effective_balance) :: balances)
    | _, _ => none

/-- The keys of the map are the slashable validators. -/
theorem effectiveBalances_keys (view : StateView) :
    ∀ {indices : List U64} {balances : List (U64 × U64)},
      effectiveBalances view indices = some balances → balances.map Prod.fst = indices
  | [], balances, h => by
    obtain rfl : [] = balances := by simpa [effectiveBalances] using h
    rfl
  | index :: rest, balances, h => by
    unfold effectiveBalances at h
    cases hv : view.validatorAt index with
    | none => simp [hv] at h
    | some validator =>
      cases hrest : effectiveBalances view rest with
      | none => simp [hv, hrest] at h
      | some tail =>
        obtain rfl : (index, validator.effective_balance) :: tail = balances := by
          simpa [hv, hrest] using h
        rw [List.map_cons, effectiveBalances_keys view hrest]

/-- `AttesterSlashingMaxCover::new`:

```rust
let slashable_validators =
    get_slashable_indices_modular(state, slashing, |index, validator| {
        validator.is_slashable_at(epoch) && !proposer_slashing_indices.contains(&index)
    })
    .ok()?;

for vd in slashable_validators { .. }

Some(Self { slashing, effective_balances })
```

`indices` holds the attesting indices of the two attestations of `slashing`.
`get_slashable_indices_modular` returns an error when its loop finds no slashable validator. -/
def attesterCoverNew (view : StateView) (toBeSlashed : IndexSet) (slashing : Im)
    (indices : Slice U64 × Slice U64) : Result (Option (AttesterCover Im)) := do
  let slashable ←
    slashableIndices view toBeSlashed (commonIndices indices.1.val indices.2.val) []
  match slashable with
  | none => ok none
  | some validators =>
    if validators.isEmpty then ok none
    else
      match effectiveBalances view validators with
      | none => ok none
      | some balances => ok (some { slashing, balances })

/-- `AttesterSlashingMaxCover::new` does not panic. A cover it returns holds the slashing, and
covers exactly the validators that both attestations name, that `Validator::is_slashable_at`
accepts on the view, and that `toBeSlashed` does not hold. -/
theorem attesterCoverNew_spec (view : StateView) (toBeSlashed : IndexSet) (slashing : Im)
    (indices : Slice U64 × Slice U64) :
    ∃ r, attesterCoverNew view toBeSlashed slashing indices = ok r ∧
      ∀ c, r = some c → c.slashing = slashing ∧
        ∀ u, c.covers u ↔
          (u ∈ indices.1.val ∧ u ∈ indices.2.val) ∧ SlashableOn view u ∧
            u ∉ toBeSlashed.toList := by
  obtain ⟨r, hr, hmem⟩ := slashableIndices_spec view toBeSlashed
    (commonIndices indices.1.val indices.2.val) []
  unfold attesterCoverNew
  rw [hr]
  cases r with
  | none => exact ⟨none, by simp, fun c hc => nomatch hc⟩
  | some validators =>
    simp only [bind_ok]
    by_cases hempty : validators.isEmpty = true
    · rw [ite_eq_left hempty]
      exact ⟨none, rfl, fun c hc => nomatch hc⟩
    · rw [ite_eq_right hempty]
      cases hbal : effectiveBalances view validators with
      | none => exact ⟨none, rfl, fun c hc => nomatch hc⟩
      | some balances =>
        refine ⟨some { slashing, balances }, rfl, ?_⟩
        rintro c ⟨rfl⟩
        refine ⟨rfl, fun u => ?_⟩
        have hkeys := effectiveBalances_keys view hbal
        have hcov : AttesterCover.covers { slashing := slashing, balances := balances } u ↔
            u ∈ validators := by
          rw [← hkeys]
          unfold AttesterCover.covers
          constructor
          · rintro ⟨b, hb⟩
            exact List.mem_map.mpr ⟨(u, b), hb, rfl⟩
          · intro hu
            obtain ⟨kv, hkv, rfl⟩ := List.mem_map.mp hu
            exact ⟨kv.2, hkv⟩
        rw [hcov, hmem validators rfl u, mem_commonIndices]
        simp

/-- The covers that the iterator of `get_attester_slashings` yields: one for each pool entry
with a signature that is still valid and that `AttesterSlashingMaxCover::new` accepts.

```rust
reader.iter().flat_map(|slashing| {
    if slashing.signature_is_still_valid(&state.fork()) {
        AttesterSlashingMaxCover::new(slashing.as_inner().to_ref(), to_be_slashed, state)
    } else {
        None
    }
})
```

`slashingOf` is `as_inner`, and `indices` reads the attesting indices of the two attestations
of a slashing. -/
def attesterCovers (view : StateView) (stillValid : OpA → Bool) (slashingOf : OpA → Im)
    (indices : Im → Slice U64 × Slice U64) (toBeSlashed : IndexSet) :
    List OpA → Result (List (AttesterCover Im))
  | [] => ok []
  | op :: rest => do
    let cover ←
      if stillValid op then
        attesterCoverNew view toBeSlashed (slashingOf op) (indices (slashingOf op))
      else ok none
    let covers ← attesterCovers view stillValid slashingOf indices toBeSlashed rest
    ok (cover.toList ++ covers)

/-- The covers are built without a panic, and there is at most one for each pool entry. Each
cover holds the slashing of a pool entry with a signature that is still valid. It covers exactly
the validators that both attestations of that slashing name, that `Validator::is_slashable_at`
accepts on the view, and that `toBeSlashed` does not hold. -/
theorem attesterCovers_spec (view : StateView) (stillValid : OpA → Bool) (slashingOf : OpA → Im)
    (indices : Im → Slice U64 × Slice U64) (toBeSlashed : IndexSet) :
    ∀ pool : List OpA,
      ∃ covers, attesterCovers view stillValid slashingOf indices toBeSlashed pool = ok covers ∧
        covers.length ≤ pool.length ∧
        ∀ c ∈ covers, ∃ op ∈ pool, stillValid op = true ∧ c.slashing = slashingOf op ∧
          ∀ u, c.covers u ↔
            (u ∈ (indices (slashingOf op)).1.val ∧ u ∈ (indices (slashingOf op)).2.val) ∧
              SlashableOn view u ∧ u ∉ toBeSlashed.toList
  | [] => ⟨[], rfl, Nat.le_refl _, fun c hc => nomatch hc⟩
  | op :: rest => by
    obtain ⟨covers, hcovers, hlen, hfrom⟩ :=
      attesterCovers_spec view stillValid slashingOf indices toBeSlashed rest
    -- The cover of the first entry, if there is one.
    have hfirst : ∃ cover,
        (if stillValid op then
          attesterCoverNew view toBeSlashed (slashingOf op) (indices (slashingOf op))
        else ok none) = ok cover ∧
        ∀ c, cover = some c → stillValid op = true ∧ c.slashing = slashingOf op ∧
          ∀ u, c.covers u ↔
            (u ∈ (indices (slashingOf op)).1.val ∧ u ∈ (indices (slashingOf op)).2.val) ∧
              SlashableOn view u ∧ u ∉ toBeSlashed.toList := by
      by_cases hvalid : stillValid op = true
      · obtain ⟨r, hr, hspec⟩ :=
          attesterCoverNew_spec view toBeSlashed (slashingOf op) (indices (slashingOf op))
        exact ⟨r, by rw [ite_eq_left hvalid, hr], fun c hc => ⟨hvalid, hspec c hc⟩⟩
      · exact ⟨none, by rw [ite_eq_right hvalid], fun c hc => nomatch hc⟩
    obtain ⟨cover, hcover, hspec⟩ := hfirst
    refine ⟨cover.toList ++ covers, ?_, ?_, ?_⟩
    · unfold attesterCovers
      rw [hcover, hcovers]
      simp
    · rw [List.length_append, List.length_cons]
      have : cover.toList.length ≤ 1 := by cases cover <;> simp
      omega
    · intro c hc
      rcases List.mem_append.mp hc with hc | hc
      · have hsome : cover = some c := by
          cases cover with
          | none => simp at hc
          | some c' =>
            have hcc : c = c' := by simpa using hc
            rw [hcc]
        exact ⟨op, List.mem_cons_self, hspec c hsome⟩
      · obtain ⟨op', hop', h⟩ := hfrom c hc
        exact ⟨op', List.mem_cons_of_mem _ hop', h⟩

end Attester

end OperationPool
