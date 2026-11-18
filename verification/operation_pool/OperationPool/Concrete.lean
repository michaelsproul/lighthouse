import OperationPool.ExclusionSet
import OperationPool.MaxCover

/-!
# Concrete iterators, closures and covers

`filter_limit_operations` and `maximum_cover` are generic over an iterator, over closures and
over a `MaxCover` implementation. Their theorems take each of those as a trait instance, with a
hypothesis for what it computes. This file gives instances for which the hypotheses hold by
construction, so the main theorem can state the two functions on plain data:

* the pool is a slice, walked by Aeneas's model of the slice iterator (`yields_slice`);
* a closure is a pure function (`pureFn`), called through Aeneas's `BuiltinFn`;
* a cover is `AttesterCover`, a model of `AttesterSlashingMaxCover` whose `MaxCover`
  implementation satisfies `MaxCoverModel` (`AttesterCover.model`).

Everything here is a model. That the Rust values behave as these do is TRUSTED: the pool's
iterator yields the pool entries, the closures compute functions and do not panic, and
`AttesterSlashingMaxCover` holds a map from validator index to effective balance.
-/

open Aeneas Aeneas.Std Result

namespace operation_pool

/-! ## Slices and pure closures -/

/-- A shared slice as an `IntoIterator`: Aeneas's model of `&[T]`. -/
abbrev sliceIntoIter (T : Type) : core.iter.traits.collect.IntoIterator (Slice T) T
    (core.slice.iter.Iter T) :=
  SharedSlice.Insts.CoreIterTraitsCollectIntoIteratorSharedIter T

/-- The closure that computes the pure function `f`. It does not panic. -/
def pureFn {α β : Type} (f : α → β) : α → Result β := fun a => ok (f a)

/-- A `Clone` implementation that returns its argument. -/
def cloneId (T : Type) : core.clone.Clone T := { clone := fun x => ok x }

/-- The slice iterator at position `i` yields the entries from `i` on. -/
theorem yields_slice_from {T : Type} (s : Slice T) :
    ∀ (n i : Nat), s.val.length - i = n →
      Yields (sliceIntoIter T).iteratorInst ⟨s, i⟩ (s.val.drop i) := by
  intro n
  induction n with
  | zero =>
    intro i hn
    have hnone : s.val[i]? = none := List.getElem?_eq_none (by omega)
    rw [List.drop_eq_nil_of_le (by omega)]
    exact Yields.done (it' := ⟨s, i⟩) (by
      show core.slice.iter.IteratorSliceIter.next ⟨s, i⟩ = _
      rw [slice_iter_next, hnone])
  | succ n ih =>
    intro i hn
    have hlt : i < s.val.length := by omega
    have hsome : s.val[i]? = some s.val[i] := List.getElem?_eq_getElem hlt
    rw [List.drop_eq_getElem_cons hlt]
    exact Yields.step (it' := ⟨s, i + 1⟩) (by
      show core.slice.iter.IteratorSliceIter.next ⟨s, i⟩ = _
      rw [slice_iter_next, hsome]) (ih (i + 1) (by omega))

/-- The slice iterator yields the entries of the slice, in order. -/
theorem yields_slice {T : Type} (s : Slice T) :
    Yields (sliceIntoIter T).iteratorInst ⟨s, 0⟩ s.val := by
  simpa using yields_slice_from s _ 0 rfl

/-! ## A model of `AttesterSlashingMaxCover` -/

/-- `AttesterSlashingMaxCover`: a slashing, and the map `effective_balances` from the validators
the slashing can still slash to their effective balances. The map is a list of entries. -/
structure AttesterCover (Im : Type) where
  /-- The slashing. -/
  slashing : Im
  /-- `effective_balances`, as a list of key and value. -/
  balances : List (U64 × U64)

namespace AttesterCover

variable {Im : Type}

/-- The cover has `u` as a key of `effective_balances`. -/
def covers (c : AttesterCover Im) (u : U64) : Prop := ∃ b, (u, b) ∈ c.balances

/-- `score`: `effective_balances.values().sum::<u64>() as usize`. The sum wraps, as it does in a
release build. -/
def score (c : AttesterCover Im) : Usize :=
  UScalar.cast .Usize (c.balances.foldl (fun acc kv => core.num.U64.wrapping_add acc kv.2) 0#u64)

/-- `update_covering_set`: `effective_balances.retain(|k, _| !covered.contains_key(k))`. -/
def retain (c : AttesterCover Im) (covered : List (U64 × U64)) : AttesterCover Im :=
  { c with balances := c.balances.filter fun kv => !(covered.map Prod.fst).contains kv.1 }

/-- The `MaxCover` implementation of `AttesterSlashingMaxCover`. The intermediate object is the
slashing, and the covering set is the map. -/
def maxCover (Im : Type) : max_cover.MaxCover (AttesterCover Im) Im Im (List (U64 × U64)) where
  corecloneCloneInst := cloneId _
  intermediate := fun c => ok c.slashing
  convert_to_object := fun i => ok i
  covering_set := fun c => ok c.balances
  update_covering_set := fun c _ covered => ok (c.retain covered)
  score := fun c => ok c.score

/-- A cover with a non-zero score has a key: the sum over no entry is zero. -/
theorem covers_of_score (c : AttesterCover Im) (h : c.score ≠ 0#usize) : ∃ u, c.covers u := by
  cases hb : c.balances with
  | nil =>
    exfalso
    apply h
    have hzero : UScalar.cast .Usize 0#u64 = 0#usize := by
      apply UScalar.eq_of_val_eq
      simp [UScalar.cast_val_eq]
    simpa [score, hb] using hzero
  | cons kv rest => exact ⟨kv.1, kv.2, by rw [hb]; exact List.mem_cons_self⟩

/-- `retain` removes the keys of `covered`, and no other key. -/
theorem covers_retain (c best : AttesterCover Im) (u : U64) :
    (c.retain best.balances).covers u ↔ c.covers u ∧ ¬ best.covers u := by
  unfold covers retain
  constructor
  · rintro ⟨b, hb⟩
    obtain ⟨hmem, hnot⟩ := List.mem_filter.mp hb
    refine ⟨⟨b, hmem⟩, ?_⟩
    rintro ⟨b', hb'⟩
    have hkey : u ∈ best.balances.map Prod.fst := List.mem_map.mpr ⟨(u, b'), hb', rfl⟩
    simp [hkey] at hnot
  · rintro ⟨⟨b, hb⟩, hnot⟩
    refine ⟨b, List.mem_filter.mpr ⟨hb, ?_⟩⟩
    have hkey : u ∉ best.balances.map Prod.fst := by
      intro hmem
      obtain ⟨kv, hkv, rfl⟩ := List.mem_map.mp hmem
      exact hnot ⟨kv.2, hkv⟩
    simp [hkey]

/-- The model implementation satisfies `MaxCoverModel`. An element is a validator index. -/
def model (Im : Type) : MaxCoverModel (maxCover Im) U64 where
  cov := covers
  score := score
  im := slashing
  set := balances
  update := fun c best => c.retain best.balances
  clone_eq := fun _ => rfl
  score_eq := fun _ => rfl
  intermediate_eq := fun _ => rfl
  covering_set_eq := fun _ => rfl
  update_eq := fun _ _ => rfl
  cov_of_score := covers_of_score
  update_cov := covers_retain
  update_im := fun _ _ => rfl

end AttesterCover

end operation_pool
