module

public import EthLibOps.ExitQueue

/-!
# What a run of slashings and exits keeps

`SlashFrame electra S E R s s'` relates a state `s` to a state `s'` that slashings and exits of
the validators in `S` reach from it. It names what those operations keep, and bounds what they
move.

The slot, the fork versions, the genesis validators root, the proposer lookahead, the pending
partial withdrawals and the lengths of the registry and of the balances stay. The record of a
validator outside `S` stays. Every validator keeps its public key and its effective balance.

Four quantities can overflow, and the frame bounds each one:

* the epoch the next exit starts from moves by `E` epochs at most;
* each balance grows by `R` Gwei at most;
* the slashed balance of the current epoch, plus the effective balances not yet slashed, does
  not grow.

The frame is what the checks of a later operation read, so a check made on `s` still holds on
`s'` for a validator outside `S`.
-/

@[expose] public section

namespace EthLibOps

open EthLib EthLib.Base EthLib.Base.Uint64 EthLib.Consensus

variable {preset : Preset} {fork : Base.Fork} {config : Config}

/-! ## Lists -/

/-- Replacing one element of a list changes the sum of a map over it by that element alone. -/
theorem sum_map_set {α : Type} (f : α → Nat) :
    ∀ (l : List α) (i : Nat) (h : i < l.length) (x : α),
      ((l.set i x).map f).sum + f l[i] = (l.map f).sum + f x
  | _ :: l, 0, _, x => by
    simp only [List.set_cons_zero, List.map_cons, List.sum_cons, List.getElem_cons_zero]
    omega
  | _ :: l, i + 1, h, x => by
    have := sum_map_set f l i (by simpa using h) x
    simp only [List.set_cons_succ, List.map_cons, List.sum_cons, List.getElem_cons_succ]
    omega

/-- An element of a list is at most the sum of a map over the list. -/
theorem le_sum_map_of_getElem? {α : Type} (f : α → Nat) {l : List α} {i : Nat} {x : α}
    (h : l[i]? = some x) : f x ≤ (l.map f).sum :=
  List.le_sum_of_mem (List.mem_map_of_mem (List.mem_of_getElem? h))

/-- An array that agrees with another outside one position is the other with that position
set. -/
theorem toList_eq_set {α : Type} {a a' : Array α} {i : Nat} {x : α} (size : a'.size = a.size)
    (others : ∀ j, j ≠ i → a'[j]? = a[j]?) (self : a'[i]? = some x) :
    a'.toList = a.toList.set i x := by
  apply List.ext_getElem?
  intro j
  by_cases same : j = i
  · subst same
    have inside : j < a.size := size ▸ (Array.getElem?_eq_some_iff.mp self).1
    simp [inside, ← self]
  · have differ : i ≠ j := Ne.symm same
    simp [differ, others j same]

/-! ## The slashed balance -/

/-- The slashed balance of the current epoch: the entry of the slashings vector that
`slash_validator` adds to. -/
def slashedNow (s : BeaconState preset fork) : Nat :=
  match readCircular s.slashings.elements (getCurrentEpoch s) with
  | .ok amount => amount.toNat
  | .error _ => 0

/-- The effective balance a validator can still add to the slashed balance: none once it is
slashed. -/
def unslashedBalance (v : Validator) : Nat :=
  if v.slashed then 0 else v.effectiveBalance.toNat

/-- The effective balances of the validators that are not slashed. -/
def unslashedSum (s : BeaconState preset fork) : Nat :=
  (s.validators.data.toList.map unslashedBalance).sum

/-- The effective balances not yet slashed are at most all the effective balances. -/
theorem unslashedSum_le (s : BeaconState preset fork) : unslashedSum s ≤ effectiveBalanceSum s := by
  unfold unslashedSum effectiveBalanceSum
  generalize s.validators.data.toList = l
  induction l with
  | nil => simp
  | cons v rest ih =>
    simp only [List.map_cons, List.sum_cons]
    have : unslashedBalance v ≤ v.effectiveBalance.toNat := by
      unfold unslashedBalance
      split <;> omega
    omega

/-- A validator that is not slashed counts its effective balance among those not yet slashed. -/
theorem effectiveBalance_le_unslashedSum {s : BeaconState preset fork} {i : ValidatorIndex}
    {v : Validator} (found : getValidator s i = .ok v) (unslashed : v.slashed = false) :
    v.effectiveBalance.toNat ≤ unslashedSum s := by
  have entry : s.validators.data.toList[i.toNat]? = some v := by
    simpa using getValidator_eq_ok_iff.mp found
  have := le_sum_map_of_getElem? unslashedBalance entry
  simpa [unslashedSum, unslashedBalance, unslashed] using this

/-! ## The frame -/

/-- `SlashFrame electra S E R s s'`: what slashings and exits of the validators in `S` keep
between `s` and `s'`, and how far they move the quantities that can overflow. -/
structure SlashFrame (electra : .pectra ≤ fork) (S : ValidatorIndex → Prop) (E R : Nat)
    (s s' : BeaconState preset fork) : Prop where
  slot : s'.slot = s.slot
  forkVersions : s'.forkVersions = s.forkVersions
  genesisValidatorsRoot : s'.genesisValidatorsRoot = s.genesisValidatorsRoot
  proposerLookahead : s'.proposerLookahead = s.proposerLookahead
  pending : s'.pendingPartialWithdrawals = s.pendingPartialWithdrawals
  size : s'.validators.data.size = s.validators.data.size
  balancesSize : s'.balances.data.size = s.balances.data.size
  /-- The record of a validator outside `S` stays. -/
  record : ∀ j : ValidatorIndex, ¬ S j →
    s'.validators.data[j.toNat]? = s.validators.data[j.toNat]?
  /-- Every validator keeps its public key and its effective balance. -/
  fixed : ∀ (j : Nat) (v v' : Validator), s.validators.data[j]? = some v →
    s'.validators.data[j]? = some v' →
    v'.pubkey = v.pubkey ∧ v'.effectiveBalance = v.effectiveBalance
  /-- The epoch the next exit starts from moves by `E` epochs at most. -/
  start : exitStart electra s' ≤ exitStart electra s + E
  /-- Each balance grows by `R` Gwei at most. -/
  balance : ∀ (j : Nat) (b b' : Gwei), s.balances.data[j]? = some b →
    s'.balances.data[j]? = some b' → b'.toNat ≤ b.toNat + R
  /-- The slashed balance of the epoch plus the balances not yet slashed does not grow. -/
  slashed : slashedNow s' + unslashedSum s' ≤ slashedNow s + unslashedSum s

variable {electra : .pectra ≤ fork} {S S' : ValidatorIndex → Prop} {E E' R R' : Nat}
  {s v w : BeaconState preset fork}

/-- A state has the frame of itself, for any set and any two bounds. -/
theorem SlashFrame.refl (electra : .pectra ≤ fork) (S : ValidatorIndex → Prop) (E R : Nat)
    (s : BeaconState preset fork) : SlashFrame electra S E R s s :=
  ⟨rfl, rfl, rfl, rfl, rfl, rfl, rfl, fun _ _ => rfl,
    fun _ _ _ h h' => by cases h.symm.trans h'; exact ⟨rfl, rfl⟩, Nat.le_add_right _ _,
    fun _ _ _ h h' => by cases h.symm.trans h'; exact Nat.le_add_right _ _, Nat.le_refl _⟩

/-- A larger set and larger bounds give a weaker frame. -/
theorem SlashFrame.mono (h : SlashFrame electra S E R s w) (hS : ∀ j, S j → S' j) (hE : E ≤ E')
    (hR : R ≤ R') : SlashFrame electra S' E' R' s w :=
  ⟨h.slot, h.forkVersions, h.genesisValidatorsRoot, h.proposerLookahead, h.pending, h.size,
    h.balancesSize, fun j hj => h.record j (fun hsj => hj (hS j hsj)), h.fixed,
    Nat.le_trans h.start (by omega),
    fun j b b' hb hb' => Nat.le_trans (h.balance j b b' hb hb') (by omega), h.slashed⟩

/-- Frames compose. The sets and the bounds add up. -/
theorem SlashFrame.trans (h : SlashFrame electra S E R s v) (h' : SlashFrame electra S' E' R' v w) :
    SlashFrame electra (fun j => S j ∨ S' j) (E + E') (R + R') s w := by
  refine ⟨h'.slot.trans h.slot, h'.forkVersions.trans h.forkVersions,
    h'.genesisValidatorsRoot.trans h.genesisValidatorsRoot,
    h'.proposerLookahead.trans h.proposerLookahead, h'.pending.trans h.pending,
    h'.size.trans h.size, h'.balancesSize.trans h.balancesSize, fun j hj => ?_,
    fun j x z hx hz => ?_, ?_, fun j b b'' hb hb'' => ?_, Nat.le_trans h'.slashed h.slashed⟩
  · exact (h'.record j (fun hsj => hj (Or.inr hsj))).trans
      (h.record j (fun hsj => hj (Or.inl hsj)))
  · have inside : j < v.validators.data.size :=
      h.size ▸ (Array.getElem?_eq_some_iff.mp hx).1
    have hy : v.validators.data[j]? = some v.validators.data[j] :=
      Array.getElem?_eq_getElem inside
    obtain ⟨p1, e1⟩ := h.fixed j x _ hx hy
    obtain ⟨p2, e2⟩ := h'.fixed j _ z hy hz
    exact ⟨p2.trans p1, e2.trans e1⟩
  · have := h.start
    have := h'.start
    omega
  · have inside : j < v.balances.data.size :=
      h.balancesSize ▸ (Array.getElem?_eq_some_iff.mp hb).1
    have hb' : v.balances.data[j]? = some v.balances.data[j] := Array.getElem?_eq_getElem inside
    have := h.balance j b _ hb hb'
    have := h'.balance j _ b'' hb' hb''
    omega

/-- The frame keeps the current epoch. -/
theorem SlashFrame.currentEpoch (h : SlashFrame electra S E R s w) :
    getCurrentEpoch w = getCurrentEpoch s := by
  simp [getCurrentEpoch, h.slot]

/-- The frame keeps the sum of the effective balances. -/
theorem SlashFrame.effectiveBalanceSum_eq (h : SlashFrame electra S E R s w) :
    effectiveBalanceSum w = effectiveBalanceSum s := by
  unfold effectiveBalanceSum
  congr 1
  apply List.ext_getElem
  · simp [h.size]
  · intro j hw hs
    simp only [List.length_map, Array.length_toList] at hw hs
    simp only [List.getElem_map, Array.getElem_toList]
    exact congrArg UInt64.toNat
      (h.fixed j _ _ (Array.getElem?_eq_getElem hs) (Array.getElem?_eq_getElem hw)).2

/-- The frame keeps the match between the registry and the balances. -/
theorem SlashFrame.balancesMatch (h : SlashFrame electra S E R s w) (sizes : BalancesMatch s) :
    BalancesMatch w := by
  unfold BalancesMatch at sizes ⊢
  rw [h.balancesSize, h.size, sizes]

/-- The frame keeps a read of a validator outside `S`. -/
theorem SlashFrame.getValidator_eq (h : SlashFrame electra S E R s w) {j : ValidatorIndex}
    (hj : ¬ S j) : getValidator w j = getValidator s j := by
  simp only [getValidator, h.record j hj]

/-- A validator read on both sides of the frame has one public key and one effective balance. -/
theorem SlashFrame.getValidator_fixed (h : SlashFrame electra S E R s w) {j : ValidatorIndex}
    {x : Validator} (found : getValidator s j = .ok x) :
    ∃ y, getValidator w j = .ok y ∧ y.pubkey = x.pubkey ∧
      y.effectiveBalance = x.effectiveBalance := by
  have entry := getValidator_eq_ok_iff.mp found
  have inside : j.toNat < w.validators.data.size :=
    h.size ▸ (Array.getElem?_eq_some_iff.mp entry).1
  have entry' : w.validators.data[j.toNat]? = some w.validators.data[j.toNat] :=
    Array.getElem?_eq_getElem inside
  exact ⟨_, getValidator_eq_ok_iff.mpr entry', h.fixed _ _ _ entry entry'⟩

/-- From Fulu the proposer of the slot is an entry of the lookahead, which the frame keeps. -/
theorem SlashFrame.proposer_eq (h : SlashFrame electra S E R s w) (fulu : .fusaka ≤ fork) :
    getBeaconProposerIndex w = getBeaconProposerIndex s := by
  simp only [getBeaconProposerIndex, fulu, ↓reduceDIte, h.slot, h.proposerLookahead]

/-- An exit has the frame of the state it runs on, for the validator that exits. No balance
grows. -/
theorem ExitEffect.slashFrame {i : ValidatorIndex} {x : Validator}
    (found : getValidator s i = .ok x) (h : ExitEffect electra i x E s w) :
    SlashFrame electra (fun j => j = i) E 0 s w := by
  have entry := getValidator_eq_ok_iff.mp found
  obtain ⟨y, hy, shape⟩ := h.self
  refine ⟨h.slot, h.forkVersions, h.genesisValidatorsRoot, h.proposerLookahead, h.pending,
    h.size, by rw [h.balances], fun j hj => h.others _ (fun eq => hj (UInt64.toNat_inj.mp eq)),
    fun j a b ha hb => ?_, h.start, fun j b b' hb hb' => ?_, ?_⟩
  · by_cases same : j = i.toNat
    · subst same
      cases entry.symm.trans ha
      cases hy.symm.trans hb
      rw [shape]
      exact ⟨rfl, rfl⟩
    · cases ((h.others j same).symm.trans hb).symm.trans ha
      exact ⟨rfl, rfl⟩
  · rw [h.balances] at hb'
    cases hb.symm.trans hb'
    omega
  · -- The exit keeps the slashed flag and the effective balance of every validator.
    have list := toList_eq_set h.size h.others hy
    have inside : i.toNat < s.validators.data.toList.length := by
      simpa using (Array.getElem?_eq_some_iff.mp entry).1
    have value : s.validators.data.toList[i.toNat] = x := by
      have := (Array.getElem?_eq_some_iff.mp entry).2
      simpa using this
    have sum := sum_map_set unslashedBalance _ _ inside y
    rw [← list, value] at sum
    have same : unslashedBalance y = unslashedBalance x := by
      rw [shape]
      rfl
    have now : slashedNow w = slashedNow s := by
      simp [slashedNow, getCurrentEpoch, h.slot, h.slashings]
    unfold unslashedSum
    omega

end EthLibOps
