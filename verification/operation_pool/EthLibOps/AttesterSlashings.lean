module

public import EthLibOps.ProposerSlashings
public import EthLib.Consensus.Proofs.Transition.AttestationAccepted
public import EthLib.Consensus.Proofs.Transition.ValidatorKeys

/-!
# The attester slashings of a block succeed

`process_attester_slashing` checks that two indexed attestations are slashable against each other
and that both are valid. It then walks the validators both attestations name, in order, and
slashes each one that can still be slashed. At least one must be slashed.

`AttesterSlashingChecks config checks s a` names the checks that run before the walk.
`slashingIndices a` is EthLib's name for the validators the walk visits.

* `slashCandidates_ok`: the walk succeeds on a state that has the `SlashFrame` of `s`. It slashes
  at least one validator when some walked validator is outside the set of the frame and can be
  slashed on `s`.
* `processAttesterSlashing_ok_of_frame`: one slashing.
* `attesterSlashings_foldlM_ok`: the fold `process_operations` runs over the attester slashings
  of a block body. Each slashing needs a validator that no earlier slashing walks.
-/

@[expose] public section

namespace EthLibOps

open EthLib EthLib.Base EthLib.Base.Uint64 EthLib.Consensus

variable {preset : Preset} {fork : Base.Fork} {config : Config} {checks : SignatureChecks}

/-- The checks of `process_attester_slashing` that run before the walk, on the state `s`. -/
structure AttesterSlashingChecks (config : Config) (checks : SignatureChecks)
    (s : BeaconState preset fork) (a : AttesterSlashing preset fork) : Prop where
  /-- The two attestations are a double vote or a surround vote. -/
  slashableData : isSlashableAttestationData a.attestation1.data a.attestation2.data = true
  /-- The first attestation is a valid indexed attestation on `s`. -/
  valid1 : isValidIndexedAttestation config checks s a.attestation1 = .ok true
  /-- The second attestation is a valid indexed attestation on `s`. -/
  valid2 : isValidIndexedAttestation config checks s a.attestation2 = .ok true

/-- An attester slashing walks the validators that both of its attestations name. -/
theorem mem_slashingIndices {a : AttesterSlashing preset fork} {i : ValidatorIndex} :
    i ∈ slashingIndices a ↔
      i ∈ a.attestation1.attestingIndices.data.toList ∧
        i ∈ a.attestation2.attestingIndices.data.toList := by
  simp [slashingIndices, mem_sortedSet]

/-- Before Gloas an attester slashing walks at most as many validators as `attesting_indices`
can hold. -/
theorem length_slashingIndices_le (before : fork < .glamsterdam)
    (a : AttesterSlashing preset fork) :
    (slashingIndices a).length ≤ maxAttesters preset fork := by
  have bound := a.attestation1.attestingIndices.bound before
  have filtered := List.length_filter_le
    (fun i => decide (i ∈ a.attestation2.attestingIndices.data.toList))
    a.attestation1.attestingIndices.data.toList
  have sorted := length_sortedSet_le
    (a.attestation1.attestingIndices.data.toList.filter
      (· ∈ a.attestation2.attestingIndices.data.toList))
  simp only [Array.length_toList] at filtered
  show (sortedSet (a.attestation1.attestingIndices.data.toList.filter
    (· ∈ a.attestation2.attestingIndices.data.toList))).length ≤ _
  omega

variable {electra : .pectra ≤ fork} {S : ValidatorIndex → Prop} {E R B : Nat}
  {s w : BeaconState preset fork} {p : ValidatorIndex}

/-- The frame keeps the public keys of the registry, in order. -/
theorem SlashFrame.validatorKeys_eq (frame : SlashFrame electra S E R s w) :
    validatorKeys w = validatorKeys s := by
  unfold validatorKeys
  apply List.ext_getElem
  · simp [frame.size]
  · intro j hw hs
    simp only [List.length_map, Array.length_toList] at hw hs
    simp only [List.getElem_map, Array.getElem_toList]
    exact (frame.fixed j _ _ (Array.getElem?_eq_getElem hs) (Array.getElem?_eq_getElem hw)).1

/-- An indexed attestation that is valid on `s` is valid on a state that has the frame of `s`:
the check reads the public keys, the fork versions and the genesis validators root. -/
theorem SlashFrame.isValidIndexedAttestation (frame : SlashFrame electra S E R s w)
    {indexed : IndexedAttestation preset fork}
    (valid : isValidIndexedAttestation config checks s indexed = .ok true) :
    isValidIndexedAttestation config checks w indexed = .ok true :=
  isValidIndexedAttestation_of_keys (common := s) valid (List.prefix_refl _)
    (frame.validatorKeys_eq ▸ List.prefix_refl _) frame.forkVersions
    frame.genesisValidatorsRoot
    (fun index member => frame.size ▸ isValidIndexedAttestation_lt_size valid index member)

/-- The walk of `process_attester_slashing` over a list of validators of the registry succeeds on
a state `w` that has the frame of `s`, when `s` has room for one slashing of each walked
validator.

The end state has the frame of `s`, with the walked validators added to the set. The flag the
walk returns is set when it was set before, or when some walked validator is outside the set of
the frame and can be slashed on `s`. -/
theorem slashCandidates_ok (ok : ChurnOk preset config) (fulu : .fusaka ≤ fork)
    (before : fork < .glamsterdam) (room : StateRoom config B p s) :
    ∀ (candidates : List ValidatorIndex) (S : ValidatorIndex → Prop) (E R : Nat)
      (w : BeaconState preset fork) (flag : Bool),
      SlashFrame electra S E R s w →
      (∀ i ∈ candidates, i.toNat < s.validators.data.size) →
      exitStart electra s + E + candidates.length * exitStep preset B +
        config.MIN_VALIDATOR_WITHDRAWABILITY_DELAY.toNat < 2 ^ 64 →
      (∀ b, getBalance s p = .ok b →
        b.toNat + R + candidates.length * rewardMax preset fork B + B < 2 ^ 64) →
      ∃ w' flag', candidates.foldlM (slashIfSlashable config) (w, flag) = .ok (w', flag') ∧
        SlashFrame electra (fun j => S j ∨ j ∈ candidates)
          (E + candidates.length * exitStep preset B)
          (R + candidates.length * rewardMax preset fork B) s w' ∧
        ((flag = true ∨ ∃ i ∈ candidates, ¬ S i ∧ ∃ v, getValidator s i = .ok v ∧
          isSlashableValidator v (getCurrentEpoch s) = true) → flag' = true)
  | [], S, E, R, w, flag, frame, _, _, _ =>
    ⟨w, flag, rfl, frame.mono (fun _ h => Or.inl h) (by simp) (by simp), fun h => by
      rcases h with h | ⟨i, hi, _⟩
      · exact h
      · cases hi⟩
  | c :: rest, S, E, R, w, flag, frame, inRange, exits, rewards => by
    generalize hstep : exitStep preset B = step at exits ⊢
    generalize hreward : rewardMax preset fork B = reward at rewards ⊢
    simp only [List.length_cons, Nat.succ_mul] at exits rewards ⊢
    -- The validator the walk reads at `c`.
    have inside : c.toNat < w.validators.data.size :=
      frame.size ▸ inRange c (List.mem_cons_self ..)
    have foundW : getValidator w c = .ok w.validators.data[c.toNat] :=
      getValidator_eq_ok_iff.mpr (Array.getElem?_eq_getElem inside)
    by_cases slash : isSlashableValidator w.validators.data[c.toNat] (getCurrentEpoch w) = true
    · obtain ⟨w₁, computed, frame₁⟩ := slashValidator_ok_of_frame ok fulu before room frame
        foundW (not_slashed_of_isSlashableValidator slash) (by rw [hstep]; omega)
        (fun b hb => by have := rewards b hb; omega)
      rw [hstep, hreward] at frame₁
      obtain ⟨w₂, flag₂, restOk, frame₂, flagged⟩ := slashCandidates_ok ok fulu before room rest
        (fun j => S j ∨ j = c) (E + step) (R + reward) w₁ true frame₁
        (fun i hi => inRange i (List.mem_cons_of_mem _ hi)) (by rw [hstep]; omega)
        (fun b hb => by rw [hreward]; have := rewards b hb; omega)
      rw [hstep, hreward] at frame₂
      refine ⟨w₂, flag₂, ?_, frame₂.mono (fun j hj => ?_) (by omega) (by omega),
        fun _ => flagged (Or.inl rfl)⟩
      · have stepOk : slashIfSlashable config (w, flag) c = .ok (w₁, true) := by
          simp only [slashIfSlashable, foundW, liftAccessor, Except.mapError, slash, computed,
            bind, Except.bind, ↓reduceIte, pure, Except.pure]
        simp only [List.foldlM_cons, stepOk, bind, Except.bind]
        exact restOk
      · rcases hj with (hj | rfl) | hj
        · exact Or.inl hj
        · exact Or.inr (List.mem_cons_self ..)
        · exact Or.inr (List.mem_cons_of_mem _ hj)
    · obtain ⟨w₂, flag₂, restOk, frame₂, flagged⟩ := slashCandidates_ok ok fulu before room rest
        (fun j => S j ∨ j = c) E R w flag (frame.mono (fun _ h => Or.inl h) (Nat.le_refl _)
          (Nat.le_refl _))
        (fun i hi => inRange i (List.mem_cons_of_mem _ hi)) (by rw [hstep]; omega)
        (fun b hb => by rw [hreward]; have := rewards b hb; omega)
      rw [hstep, hreward] at frame₂
      -- A validator outside the set of the frame that can be slashed on `s` is not `c`.
      have notC : ∀ i, ¬ S i → (∃ v, getValidator s i = .ok v ∧
          isSlashableValidator v (getCurrentEpoch s) = true) → i ≠ c := by
        rintro i hS ⟨v, found, slashable⟩ rfl
        apply slash
        have same : getValidator w i = .ok v := by
          rw [frame.getValidator_eq hS]
          exact found
        cases foundW.symm.trans same
        rw [frame.currentEpoch]
        exact slashable
      refine ⟨w₂, flag₂, ?_, frame₂.mono (fun j hj => ?_) (by omega) (by omega), fun h => ?_⟩
      · have stepOk : slashIfSlashable config (w, flag) c = .ok (w, flag) := by
          simp only [slashIfSlashable, foundW, liftAccessor, Except.mapError, slash, bind,
            Except.bind, Bool.false_eq_true, ↓reduceIte, pure, Except.pure]
        simp only [List.foldlM_cons, stepOk, bind, Except.bind]
        exact restOk
      · rcases hj with (hj | rfl) | hj
        · exact Or.inl hj
        · exact Or.inr (List.mem_cons_self ..)
        · exact Or.inr (List.mem_cons_of_mem _ hj)
      · rcases h with h | ⟨i, hi, hS, can⟩
        · exact flagged (Or.inl h)
        · have differ := notC i hS can
          rcases List.mem_cons.mp hi with rfl | hi
          · exact absurd rfl differ
          · exact flagged (Or.inr ⟨i, hi, fun h => h.elim hS differ, can⟩)

/-- `process_attester_slashing` succeeds on a state `w` that has the frame of `s`, for a slashing
that passes the checks on `s`. Some validator it walks is outside the set of the frame and can be
slashed on `s`. `s` has room for one slashing of each walked validator.

The state after it has the frame of `s`, with the walked validators added to the set. -/
theorem processAttesterSlashing_ok_of_frame (ok : ChurnOk preset config) (fulu : .fusaka ≤ fork)
    (before : fork < .glamsterdam) (room : StateRoom config B p s)
    (frame : SlashFrame electra S E R s w) {a : AttesterSlashing preset fork}
    (passes : AttesterSlashingChecks config checks s a)
    (witness : ∃ i ∈ slashingIndices a, ¬ S i ∧ ∃ v, getValidator s i = .ok v ∧
      isSlashableValidator v (getCurrentEpoch s) = true)
    (exits : exitStart electra s + E + (slashingIndices a).length * exitStep preset B +
      config.MIN_VALIDATOR_WITHDRAWABILITY_DELAY.toNat < 2 ^ 64)
    (rewards : ∀ b, getBalance s p = .ok b →
      b.toNat + R + (slashingIndices a).length * rewardMax preset fork B + B < 2 ^ 64) :
    ∃ w', processAttesterSlashing config checks w a = .ok w' ∧
      SlashFrame electra (fun j => S j ∨ j ∈ slashingIndices a)
        (E + (slashingIndices a).length * exitStep preset B)
        (R + (slashingIndices a).length * rewardMax preset fork B) s w' := by
  have valid1 := frame.isValidIndexedAttestation passes.valid1
  have valid2 := frame.isValidIndexedAttestation passes.valid2
  obtain ⟨w', flag', walked, frame', flagged⟩ := slashCandidates_ok ok fulu before room
    (slashingIndices a) S E R w false frame
    (fun i hi => isValidIndexedAttestation_lt_size passes.valid1 i (mem_slashingIndices.mp hi).1)
    exits rewards
  cases flagged (Or.inr witness)
  refine ⟨w', ?_, frame'⟩
  simp only [processAttesterSlashing, ensure_of passes.slashableData, valid1, valid2,
    ensure_of trivial, walked, bind, Except.bind, pure, Except.pure]

/-- The attester slashings of a list succeed, one after the other, from a state `w` that has the
frame of `s`. The checks are made on `s`. Each slashing has a validator that it walks, that is
outside the set of the frame, that can be slashed on `s`, and that no earlier slashing of the
list walks. `s` has room, for each slashing, for one slashing of as many validators as
`attesting_indices` can hold.

The end state has the frame of `s`, for the set of the frame and every walked validator. -/
theorem attesterSlashings_foldlM_ok (ok : ChurnOk preset config) (fulu : .fusaka ≤ fork)
    (before : fork < .glamsterdam) (room : StateRoom config B p s) :
    ∀ (ass : List (AttesterSlashing preset fork)) (S : ValidatorIndex → Prop) (E R : Nat)
      (w : BeaconState preset fork),
      SlashFrame electra S E R s w →
      (∀ a ∈ ass, AttesterSlashingChecks config checks s a) →
      (∀ (pre : List (AttesterSlashing preset fork)) (a : AttesterSlashing preset fork)
        (post : List (AttesterSlashing preset fork)), ass = pre ++ a :: post →
        ∃ i ∈ slashingIndices a, ¬ S i ∧
          (∃ v, getValidator s i = .ok v ∧ isSlashableValidator v (getCurrentEpoch s) = true) ∧
          ∀ a' ∈ pre, i ∉ slashingIndices a') →
      exitStart electra s + E + ass.length * (maxAttesters preset fork * exitStep preset B) +
        config.MIN_VALIDATOR_WITHDRAWABILITY_DELAY.toNat < 2 ^ 64 →
      (∀ b, getBalance s p = .ok b →
        b.toNat + R + ass.length * (maxAttesters preset fork * rewardMax preset fork B) + B <
          2 ^ 64) →
      ∃ w', ass.foldlM (processAttesterSlashing config checks) w = .ok w' ∧
        SlashFrame electra (fun j => S j ∨ ∃ a ∈ ass, j ∈ slashingIndices a)
          (E + ass.length * (maxAttesters preset fork * exitStep preset B))
          (R + ass.length * (maxAttesters preset fork * rewardMax preset fork B)) s w'
  | [], S, E, R, w, frame, _, _, _, _ =>
    ⟨w, rfl, frame.mono (fun _ h => Or.inl h) (by simp) (by simp)⟩
  | a :: rest, S, E, R, w, frame, passes, fresh, exits, rewards => by
    -- The room one slashing takes is at most the room of a full one.
    have walked := length_slashingIndices_le before a
    have stepLe := Nat.mul_le_mul_right (exitStep preset B) walked
    have rewardLe := Nat.mul_le_mul_right (rewardMax preset fork B) walked
    generalize hstep : maxAttesters preset fork * exitStep preset B = step at exits stepLe ⊢
    generalize hreward :
      maxAttesters preset fork * rewardMax preset fork B = reward at rewards rewardLe ⊢
    simp only [List.length_cons, Nat.succ_mul] at exits rewards ⊢
    obtain ⟨i, hi, hS, can, -⟩ := fresh [] a rest rfl
    obtain ⟨w₁, computed, frame₁⟩ := processAttesterSlashing_ok_of_frame ok fulu before room
      frame (passes a (List.mem_cons_self ..)) ⟨i, hi, hS, can⟩ (by omega)
      (fun b hb => by have := rewards b hb; omega)
    have frame₁' : SlashFrame electra (fun j => S j ∨ j ∈ slashingIndices a) (E + step)
        (R + reward) s w₁ := frame₁.mono (fun _ h => h) (by omega) (by omega)
    obtain ⟨w₂, restOk, frame₂⟩ := attesterSlashings_foldlM_ok ok fulu before room rest
      (fun j => S j ∨ j ∈ slashingIndices a) (E + step) (R + reward) w₁ frame₁'
      (fun x hx => passes x (List.mem_cons_of_mem _ hx))
      (fun pre x post split => by
        obtain ⟨k, hk, hSk, cank, earlier⟩ := fresh (a :: pre) x post (by rw [split]; rfl)
        exact ⟨k, hk, fun h => h.elim hSk (earlier a (List.mem_cons_self ..)), cank,
          fun y hy => earlier y (List.mem_cons_of_mem _ hy)⟩)
      (by rw [hstep]; omega)
      (fun b hb => by rw [hreward]; have := rewards b hb; omega)
    rw [hstep, hreward] at frame₂
    refine ⟨w₂, ?_, frame₂.mono (fun j hj => ?_) (by omega) (by omega)⟩
    · simp only [List.foldlM_cons, computed, bind, Except.bind]
      exact restOk
    · rcases hj with (hj | hj) | ⟨x, hx, hj⟩
      · exact Or.inl hj
      · exact Or.inr ⟨a, List.mem_cons_self .., hj⟩
      · exact Or.inr ⟨x, List.mem_cons_of_mem _ hx, hj⟩

end EthLibOps
