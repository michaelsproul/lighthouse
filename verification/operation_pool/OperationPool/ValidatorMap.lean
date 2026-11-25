import OperationPool.Funs

/-!
# The operations of a `ValidatorMap` are about distinct validators

`ValidatorMap<V>` in `beacon_node/operation_pool/src/validator_map.rs` holds the proposer
slashings of the op pool, and its voluntary exits. Its map is private, and three functions build
and write it:

* `new` is the empty map.
* `insert` reads the index of the validator from the operation, and stores the operation under
  that index.
* `retain` keeps the operations that a closure accepts.

`new` and `insert` are extracted. `retain` is one call of `HashMap::retain`, which
`OperationPool/FunsExternal.lean` models.

`Keyed inst m` is the invariant: the map has one entry per key, and each operation is stored
under the index of its validator. The empty map has it (`keyed_new`), and `insert` and `retain`
keep it (`Keyed.insert`, `Keyed.retain`). So the operations of a map are about pairwise distinct
validators (`Keyed.values_pairwise`).

Neither function changes an operation. The operations after `insert` are the new one and
operations the map had (`mem_values_of_insert`), and the operations after `retain` are
operations the map had (`mem_values_of_retain`).
-/

open Aeneas Aeneas.Std Result

namespace operation_pool.validator_map

/-- A bind that returns a value ran both of its parts. -/
theorem bind_eq_ok {α β : Type} {x : Result α} {f : α → Result β} {b : β}
    (h : Aeneas.Std.bind x f = ok b) : ∃ a, x = ok a ∧ f a = ok b := by
  cases x with
  | ret a => exact ⟨a, rfl, by rwa [bind_ok] at h⟩
  | vis i k =>
    rw [bind_vis] at h
    exact absurd h vis_not_ok
  | div =>
    rw [bind_div] at h
    exact absurd h div_not_ok

variable {V : Type} {inst : ValidatorIndexed V} {m m' : ValidatorMap V}

/-- The entries of a map: each validator index, with the operation stored under it. -/
def ValidatorMap.entries (m : ValidatorMap V) : List (U64 × V) := m.map.entries

/-- The operations of a map. `values()` yields each of them once, in an order that `std` does
not fix. -/
def ValidatorMap.values (m : ValidatorMap V) : List V := m.entries.map Prod.snd

/-- The map has one entry per key, and each operation is stored under the index of its
validator. -/
structure Keyed (inst : ValidatorIndexed V) (m : ValidatorMap V) : Prop where
  /-- No two entries have the same key. -/
  distinct : m.entries.Pairwise fun a b => a.1 ≠ b.1
  /-- The key of an entry is the index of the validator of its operation. -/
  indexed : ∀ entry ∈ m.entries, inst.validator_index entry.2 = ok entry.1

/-! ## `new` -/

/-- `ValidatorMap::new` returns a map with no entry. -/
theorem new_spec (V : Type) : ∃ m, ValidatorMap.new V = ok m ∧ m.entries = [] := by
  simp only [ValidatorMap.new, std.collections.hash.map.HashMapKVRandomStateGlobal.new, bind_ok]
  exact ⟨_, rfl, rfl⟩

/-- A map with no entry is keyed. -/
theorem Keyed.of_entries_nil (inst : ValidatorIndexed V) (empty : m.entries = []) :
    Keyed inst m :=
  ⟨by rw [empty]; exact List.Pairwise.nil, by rw [empty]; exact fun _ h => nomatch h⟩

/-- The map `ValidatorMap::new` returns is keyed. -/
theorem keyed_new (inst : ValidatorIndexed V) :
    ∃ m, ValidatorMap.new V = ok m ∧ m.entries = [] ∧ Keyed inst m := by
  obtain ⟨m, computed, empty⟩ := new_spec V
  exact ⟨m, computed, empty, .of_entries_nil inst empty⟩

/-! ## `insert` -/

/-- `ValidatorMap::insert` does not panic when the index of the validator is read. -/
theorem insert_ok (m : ValidatorMap V) {value : V} {index : U64}
    (read : inst.validator_index value = ok index) :
    ∃ old m', ValidatorMap.insert inst m value = ok (old, m') := by
  simp only [ValidatorMap.insert, read, std.collections.hash.map.HashMap.insert, bind_ok]
  exact ⟨_, _, rfl⟩

/-- What `ValidatorMap::insert` returns: the new operation is stored under the index of its
validator, and the other entries are entries the map had under other keys. -/
theorem insert_inv {value : V} {old : Option V}
    (computed : ValidatorMap.insert inst m value = ok (old, m')) :
    ∃ index rest, inst.validator_index value = ok index ∧
      m'.entries = (index, value) :: rest ∧ rest.Sublist m.entries ∧
      ∀ entry ∈ rest, entry.1 ≠ index := by
  unfold ValidatorMap.insert at computed
  obtain ⟨index, read, computed⟩ := bind_eq_ok computed
  simp only [std.collections.hash.map.HashMap.insert, bind_ok] at computed
  obtain ⟨-, rfl⟩ := Prod.mk.inj (Result.ok_injective computed)
  refine ⟨index, _, read, rfl, List.filter_sublist, fun entry member => ?_⟩
  simpa using (List.mem_filter.mp member).2

/-- `ValidatorMap::insert` keeps the invariant. -/
theorem Keyed.insert (h : Keyed inst m) {value : V} {old : Option V}
    (computed : ValidatorMap.insert inst m value = ok (old, m')) : Keyed inst m' := by
  obtain ⟨index, rest, read, entries, sub, fresh⟩ := insert_inv computed
  constructor
  · rw [entries]
    exact List.pairwise_cons.mpr
      ⟨fun entry member => (fresh entry member).symm, h.distinct.sublist sub⟩
  · intro entry member
    rw [entries] at member
    rcases List.mem_cons.mp member with rfl | member
    · exact read
    · exact h.indexed entry (sub.subset member)

/-- An operation of the map after `ValidatorMap::insert` is the new operation, or an operation
the map had. -/
theorem mem_values_of_insert {value : V} {old : Option V}
    (computed : ValidatorMap.insert inst m value = ok (old, m')) {w : V} (member : w ∈ m'.values) :
    w = value ∨ w ∈ m.values := by
  obtain ⟨index, rest, -, entries, sub, -⟩ := insert_inv computed
  unfold ValidatorMap.values at member ⊢
  rw [entries, List.map_cons] at member
  rcases List.mem_cons.mp member with rfl | member
  · exact Or.inl rfl
  · exact Or.inr ((sub.map Prod.snd).subset member)

/-! ## `retain` -/

/-- The entries that the model of `HashMap::retain` keeps are entries of the map, in order. -/
theorem retainEntries_sublist {F : Type}
    (keepInst : core.ops.function.FnMut F (U64 × V) Bool) :
    ∀ (entries : List (U64 × V)) (keep : F) (kept : List (U64 × V)),
      _root_.validator_map.retainEntries keepInst keep entries = ok kept → kept.Sublist entries
  | [], _, kept, computed => by
    rw [_root_.validator_map.retainEntries, Result.ok.injEq] at computed
    rw [← computed]
  | entry :: rest, keep, kept, computed => by
    rw [_root_.validator_map.retainEntries] at computed
    obtain ⟨⟨accepted, keep1⟩, -, computed⟩ := bind_eq_ok computed
    obtain ⟨rest1, restOk, computed⟩ := bind_eq_ok computed
    have ih := retainEntries_sublist keepInst rest keep1 rest1 restOk
    rw [Result.ok.injEq] at computed
    rw [← computed]
    split
    · exact ih.cons_cons entry
    · exact ih.cons entry

/-- The entries after `ValidatorMap::retain` are entries the map had. -/
theorem retain_sublist {F : Type} {keepInst : core.ops.function.FnMut F (U64 × V) Bool}
    {keep : F} (computed : _root_.validator_map.ValidatorMap.retain keepInst m keep = ok m') :
    m'.entries.Sublist m.entries := by
  unfold _root_.validator_map.ValidatorMap.retain at computed
  obtain ⟨entries, entriesOk, computed⟩ := bind_eq_ok computed
  rw [Result.ok.injEq] at computed
  rw [← computed]
  exact retainEntries_sublist keepInst _ keep entries entriesOk

/-- A map whose entries are entries of a keyed map is keyed. -/
theorem Keyed.of_sublist (h : Keyed inst m) (sub : m'.entries.Sublist m.entries) :
    Keyed inst m' :=
  ⟨h.distinct.sublist sub, fun entry member => h.indexed entry (sub.subset member)⟩

/-- `ValidatorMap::retain` keeps the invariant. -/
theorem Keyed.retain (h : Keyed inst m) {F : Type}
    {keepInst : core.ops.function.FnMut F (U64 × V) Bool} {keep : F}
    (computed : _root_.validator_map.ValidatorMap.retain keepInst m keep = ok m') :
    Keyed inst m' :=
  h.of_sublist (retain_sublist computed)

/-- An operation of the map after `ValidatorMap::retain` is an operation the map had. -/
theorem mem_values_of_retain {F : Type} {keepInst : core.ops.function.FnMut F (U64 × V) Bool}
    {keep : F} (computed : _root_.validator_map.ValidatorMap.retain keepInst m keep = ok m')
    {w : V} (member : w ∈ m'.values) : w ∈ m.values :=
  ((retain_sublist computed).map Prod.snd).subset member

/-! ## The operations of a keyed map -/

/-- The operations of a keyed map are about pairwise distinct validators. -/
theorem Keyed.values_pairwise (h : Keyed inst m) :
    m.values.Pairwise fun a b => inst.validator_index a ≠ inst.validator_index b := by
  unfold ValidatorMap.values
  rw [List.pairwise_map]
  refine h.distinct.imp_of_mem fun {a b} memberA memberB differ same => ?_
  rw [h.indexed a memberA, h.indexed b memberB, Result.ok.injEq] at same
  exact differ same

end operation_pool.validator_map
