import OperationPool.FilterLimit

/-!
# `maximum_cover` picks items that each cover something new

`maximum_cover` in `beacon_node/operation_pool/src/max_cover.rs` is the greedy algorithm the op
pool packs attestations and attester slashings with. Each item covers a set of elements. The
function picks the item with the best score, removes what it covers from the other items, and
repeats, at most `limit` times.

The function is generic over the item type `T: MaxCover`, so the theorem is too. `MaxCoverModel`
states what the proof assumes of the trait implementation: each method returns a value, and
`update_covering_set` removes exactly the elements the picked item covers.

`maximum_cover_spec` is the result. The function does not panic, and returns at most `limit`
items. Each returned item comes from an item `a` the iterator yielded, and `a` covers an element
that no item picked before it covered, when read on the yielded items.

For attester slashings an element is a validator the slashing can slash. The theorem then says
that each packed slashing slashes a validator that no slashing packed before it names.
-/

open Aeneas Aeneas.Std Result

namespace operation_pool

open max_cover

/-- What the proofs assume of a `MaxCover` implementation: a pure function for each method, and
the set `cov t` of elements that the item `t` covers. -/
structure MaxCoverModel {T Obj Im Set : Type} (inst : MaxCover T Obj Im Set) (U : Type) where
  /-- The elements the covering set of an item holds. -/
  cov : T → U → Prop
  /-- `score`, as a function. -/
  score : T → Usize
  /-- `intermediate`, as a function. -/
  im : T → Im
  /-- `covering_set`, as a function. -/
  set : T → Set
  /-- `update_covering_set`, as a function of the item and of the picked item. -/
  update : T → T → T
  /-- `clone` returns its argument. -/
  clone_eq : ∀ t, inst.corecloneCloneInst.clone t = ok t
  /-- `score` returns. -/
  score_eq : ∀ t, inst.score t = ok (score t)
  /-- `intermediate` returns. -/
  intermediate_eq : ∀ t, inst.intermediate t = ok (im t)
  /-- `covering_set` returns. -/
  covering_set_eq : ∀ t, inst.covering_set t = ok (set t)
  /-- `update_covering_set` returns. -/
  update_eq : ∀ t best, inst.update_covering_set t (im best) (set best) = ok (update t best)
  /-- An item with a non-zero score covers an element. -/
  cov_of_score : ∀ t, score t ≠ 0#usize → ∃ u, cov t u
  /-- `update_covering_set` removes the elements the picked item covers, and no other element. -/
  update_cov : ∀ t best u, cov (update t best) u ↔ cov t u ∧ ¬ cov best u
  /-- `update_covering_set` keeps the intermediate object. -/
  update_im : ∀ t best, im (update t best) = im t

section

variable {T Obj Im Set U : Type} {inst : MaxCover T Obj Im Set} (m : MaxCoverModel inst U)

/-- What `update_covering_sets` does to one entry: an available entry with a non-zero score gets
its covering set updated for the picked item `best`. Any other entry stays. -/
def MaxCoverModel.upd (best : T) (x : MaxCoverItem T) : MaxCoverItem T :=
  if x.available = true ∧ m.score x.item ≠ 0#usize then
    { x with item := m.update x.item best }
  else x

/-! ## `update_covering_sets` -/

/-- The loop of `update_covering_sets`, from position `iter.i`: it maps `upd` over the entries
from that position on. `back` is the continuation that the loop builds for the mutable borrow. -/
theorem update_covering_sets_loop_eq (best : T) :
    ∀ (n : Nat) (iter : core.slice.iter.IterMut (MaxCoverItem T))
      (back : core.slice.iter.IterMut (MaxCoverItem T) →
        core.slice.iter.IterMut (MaxCoverItem T)),
      iter.slice.val.length - iter.i = n →
      ∃ im, update_covering_sets_loop inst iter back best = ok (back im) ∧
        im.slice.val = iter.slice.val.take iter.i ++
          (iter.slice.val.drop iter.i).map (m.upd best) := by
  intro n
  induction n with
  | zero =>
    intro iter back hn
    have hlen : ¬ iter.i < iter.slice.val.length := by omega
    refine ⟨iter, ?_, ?_⟩
    · rw [update_covering_sets_loop]
      simp [core.slice.iter.IteratorIterMut.next, hlen]
    · have : iter.slice.val.length ≤ iter.i := by omega
      simp [List.take_of_length_le this, List.drop_eq_nil_of_le this]
  | succ n ih =>
    intro iter back hn
    have hlt : iter.i < iter.slice.len := by scalar_tac
    have hlt' : iter.i < iter.slice.val.length := by omega
    -- The entry at the current position, and the iterator after it.
    obtain ⟨im, him, hval⟩ := ih { iter with i := iter.i + 1 }
      (fun im => back { im with slice := im.slice.setAtNat iter.i (m.upd best iter.slice[iter.i]) })
      (by simp; omega)
    refine ⟨{ im with slice := im.slice.setAtNat iter.i (m.upd best iter.slice[iter.i]) }, ?_, ?_⟩
    · rw [update_covering_sets_loop]
      simp only [core.slice.iter.IteratorIterMut.next, hlt, ↓reduceDIte]
      generalize hx : iter.slice[iter.i] = x at him ⊢
      by_cases hav : x.available = true
      · by_cases hsc : (m.score x.item).val = 0
        · have hsc' : m.score x.item = 0#usize := by scalar_tac
          have hupd : m.upd best x = x := by simp [MaxCoverModel.upd, hsc']
          rw [hupd] at him ⊢
          simp [hav, m.score_eq, hsc, him]
        · have hsc' : m.score x.item ≠ 0#usize := by scalar_tac
          have hupd : m.upd best x = { x with item := m.update x.item best } := by
            simp [MaxCoverModel.upd, hav, hsc']
          rw [hupd] at him ⊢
          rw [hav] at him
          simp [hav, m.score_eq, hsc, m.intermediate_eq, m.covering_set_eq, m.update_eq, him]
      · have hupd : m.upd best x = x := by simp [MaxCoverModel.upd, hav]
        rw [hupd] at him ⊢
        simp [hav, him]
    · simp only [Slice.setAtNat, Slice.from_val, hval]
      have hget : iter.slice.val.drop iter.i =
          iter.slice[iter.i] :: iter.slice.val.drop (iter.i + 1) := by
        rw [List.drop_eq_getElem_cons hlt']
        rfl
      rw [hget, List.map_cons, List.set_append_left _ _ (by simp; omega)]
      rw [List.take_succ_eq_append_getElem hlt', List.set_append_right _ _ (by simp)]
      simp [List.length_take, Nat.min_eq_left (Nat.le_of_lt hlt')]


/-- `update_covering_sets` maps `upd` over the work list. It does not panic. -/
theorem update_covering_sets_eq (best : T) (s : Slice (MaxCoverItem T)) :
    ∃ s', update_covering_sets inst s best = ok s' ∧ s'.val = s.val.map (m.upd best) := by
  obtain ⟨im, him, hval⟩ := update_covering_sets_loop_eq m best _ { slice := s, i := 0 }
    (fun im => im) rfl
  refine ⟨im.slice, ?_, by simpa using hval⟩
  simp [update_covering_sets, core.slice.Slice.iter_mut, him]

/-! ## `best_item_index` -/

/-- `usize` addition of one returns the sum when it fits. -/
theorem usize_add_one_eq (c : Usize) (h : c.val + 1 ≤ Usize.max) :
    ∃ c', c + 1#usize = ok c' ∧ c'.val = c.val + 1 := by
  have heq := UScalar.add_equiv c 1#usize
  split at heq
  · rename_i z hz
    exact ⟨z, Result.match.isOk.mp hz, by simpa using heq.2.1⟩
  · exfalso
    apply heq
    simp only [UScalar.inBounds]
    scalar_tac
  · exact heq.elim

/-- `PickOk items r`: the index `r`, when there is one, names an entry of the work list that is
available and has a non-zero score. -/
def MaxCoverModel.PickOk (items : List (MaxCoverItem T)) (r : Option Usize) : Prop :=
  ∀ idx, r = some idx →
    ∃ x, items[idx.val]? = some x ∧ x.available = true ∧ m.score x.item ≠ 0#usize

/-- The loop of `best_item_index`, from position `iter.iter.i`: it does not panic, and it keeps
`PickOk`. -/
theorem best_item_index_loop_eq (s : Slice (MaxCoverItem T)) :
    ∀ (n : Nat) (iter : core.iter.adapters.enumerate.Enumerate
        (core.slice.iter.Iter (MaxCoverItem T)))
      (best_index : Option Usize) (best_score : Usize),
      iter.iter.slice = s → iter.count.val = iter.iter.i → s.val.length - iter.iter.i = n →
      m.PickOk s.val best_index →
      ∃ r, best_item_index_loop inst iter best_index best_score = ok r ∧ m.PickOk s.val r := by
  intro n
  induction n with
  | zero =>
    intro iter best_index best_score hs hcount hn hok
    have hlen : ¬ iter.iter.i < iter.iter.slice.val.length := by rw [hs]; omega
    refine ⟨best_index, ?_, hok⟩
    rw [best_item_index_loop]
    simp [core.iter.adapters.enumerate.IteratorEnumerate.next,
      core.slice.iter.IteratorSliceIter.next, hlen]
  | succ n ih =>
    intro iter best_index best_score hs hcount hn hok
    have hlt : iter.iter.i < iter.iter.slice.len := by rw [hs]; scalar_tac
    have hlt' : iter.iter.i < s.val.length := by omega
    obtain ⟨count', hadd, hcount'⟩ := usize_add_one_eq iter.count (by scalar_tac)
    -- The loop continues from the next position, whatever the entry is.
    have hnext : ∀ (bi : Option Usize) (bs : Usize), m.PickOk s.val bi →
        ∃ r, best_item_index_loop inst
          { iter := { iter.iter with i := iter.iter.i + 1 }, count := count' } bi bs = ok r ∧
          m.PickOk s.val r :=
      fun bi bs hbi => ih _ bi bs hs (by simp [hcount', hcount]) (by simp; omega) hbi
    rw [best_item_index_loop]
    simp only [core.iter.adapters.enumerate.IteratorEnumerate.next,
      core.slice.iter.IteratorSliceIter.next, hlt, ↓reduceDIte, hadd]
    generalize hx : iter.iter.slice[iter.iter.i] = x
    have hget : s.val[iter.count.val]? = some x := by
      rw [hcount, ← hx, List.getElem?_eq_getElem hlt']
      subst hs
      rfl
    by_cases hav : x.available = true
    · by_cases hsc : (m.score x.item).val = 0
      · obtain ⟨r, hr, hrok⟩ := hnext best_index best_score hok
        exact ⟨r, by simp [hav, m.score_eq, hsc, hr], hrok⟩
      · have hsc' : m.score x.item ≠ 0#usize := by scalar_tac
        by_cases hge : best_score.val ≤ (m.score x.item).val
        · obtain ⟨r, hr, hrok⟩ := hnext (some iter.count) (m.score x.item) (by
            intro idx hidx
            cases hidx
            exact ⟨x, hget, hav, hsc'⟩)
          exact ⟨r, by simp [hav, m.score_eq, hsc, hge, hr], hrok⟩
        · obtain ⟨r, hr, hrok⟩ := hnext best_index best_score hok
          exact ⟨r, by simp [hav, m.score_eq, hsc, hge, hr], hrok⟩
    · obtain ⟨r, hr, hrok⟩ := hnext best_index best_score hok
      exact ⟨r, by simp [hav, hr], hrok⟩

/-- `best_item_index` does not panic. The index it returns, when there is one, names an entry
that is available and has a non-zero score. -/
theorem best_item_index_eq (s : Slice (MaxCoverItem T)) :
    ∃ r, best_item_index inst s = ok r ∧ m.PickOk s.val r := by
  obtain ⟨r, hr, hok⟩ := best_item_index_loop_eq m s _
    { iter := { slice := s, i := 0 }, count := 0#usize } none 0#usize rfl rfl rfl
    (fun _ h => by cases h)
  refine ⟨r, ?_, hok⟩
  simp [best_item_index, core.slice.Slice.iter,
    core.iter.traits.iterator.Iterator.enumerate.trait_default,
    core.iter.traits.iterator.Iterator.enumerate.default, hr]

/-! ## The work list -/

variable {I It : Type} (iterInst : core.iter.traits.collect.IntoIterator I T It)

/-- The first loop of `maximum_cover`: it appends an available entry for each yielded item with a
non-zero score. -/
theorem maximum_cover_loop0_eq {it : It} {l : List T}
    (hyields : Yields iterInst.iteratorInst it l) (all_items : alloc.vec.Vec (MaxCoverItem T))
    (hlen : all_items.val.length + l.length ≤ Usize.max) :
    ∃ out, maximum_cover_loop0 iterInst inst it all_items = ok out ∧
      out.val = all_items.val ++
        (l.filter fun t => decide (m.score t ≠ 0#usize)).map
          (fun t => ({ item := t, available := true } : MaxCoverItem T)) := by
  induction hyields generalizing all_items with
  | done hnext =>
    refine ⟨all_items, ?_, by simp⟩
    rw [maximum_cover_loop0]
    simp [hnext]
  | @step it it' x xs hnext _ ih =>
    rw [maximum_cover_loop0]
    simp only [List.length_cons] at hlen
    by_cases hsc : (m.score x).val = 0
    · have hsc' : m.score x = 0#usize := by scalar_tac
      obtain ⟨out, hout, hval⟩ := ih all_items (by omega)
      exact ⟨out, by simp [hnext, m.score_eq, hsc, hout], by simp [hval, hsc]⟩
    · have hsc' : m.score x ≠ 0#usize := by scalar_tac
      obtain ⟨items1, hpush, hitems1⟩ := vec_push_eq all_items
        ({ item := x, available := true } : MaxCoverItem T) (by scalar_tac)
      obtain ⟨out, hout, hval⟩ := ih items1 (by rw [hitems1]; simp; omega)
      refine ⟨out, ?_, ?_⟩
      · simp [hnext, m.score_eq, hsc, MaxCoverItem.new, hpush, hout]
      · simp [hval, hitems1, hsc]

/-! ## The selection loop -/

/-- The property of the result. Each returned item `r` comes from a yielded item `a`: the two
have the same intermediate object, and `a` covers an element that no yielded item of an earlier
pick covers. `picked` lists the yielded items, in the order of the result. -/
def MaxCoverModel.Fresh (picked result : List T) : Prop :=
  ∀ (i : Nat) (a : T), picked[i]? = some a →
    (∀ r, result[i]? = some r → m.im r = m.im a) ∧
    ∃ u, m.cov a u ∧ ∀ (j : Nat) (a' : T), j < i → picked[j]? = some a' → ¬ m.cov a' u

/-- The invariant of the selection loop. `base` lists, for each entry of the work list, the
yielded item it came from. `picked` lists the yielded item of each result.

An available entry with a non-zero score covers what its yielded item covers, less what the
results cover. An entry with a zero score is never updated and never picked, so the invariant
says nothing about its covering set. -/
structure MaxCoverModel.CoverInv (l : List T) (items : List (MaxCoverItem T)) (base : List T)
    (result picked : List T) : Prop where
  base_len : base.length = items.length
  base_mem : ∀ a ∈ base, a ∈ l
  picked_len : picked.length = result.length
  picked_mem : ∀ a ∈ picked, a ∈ l
  im_eq : ∀ (k : Nat) (x : MaxCoverItem T) (a : T), items[k]? = some x → base[k]? = some a →
    m.im x.item = m.im a
  cov_eq : ∀ (k : Nat) (x : MaxCoverItem T) (a : T), items[k]? = some x → base[k]? = some a →
    x.available = true →
    m.score x.item ≠ 0#usize → ∀ u, m.cov x.item u ↔ m.cov a u ∧ ∀ r ∈ result, ¬ m.cov r u
  covered : ∀ a ∈ picked, ∀ u, m.cov a u → ∃ r ∈ result, m.cov r u
  fresh : m.Fresh picked result

/-- One round of the selection loop keeps the invariant. The round picks the entry at `k`, which
is available and has a non-zero score. It marks the entry unavailable, updates the other entries
and appends the picked item to the result. -/
theorem MaxCoverModel.CoverInv.step {l : List T} {items : List (MaxCoverItem T)}
    {base result picked : List T} (hinv : m.CoverInv l items base result picked) {k : Nat}
    {x : MaxCoverItem T} (hk : items[k]? = some x) (hav : x.available = true)
    (hsc : m.score x.item ≠ 0#usize) :
    ∃ a, a ∈ l ∧ m.CoverInv l
      ((items.set k { x with available := false }).map (m.upd x.item)) base
      (result ++ [x.item]) (picked ++ [a]) := by
  have hklt : k < items.length := (List.getElem?_eq_some_iff.mp hk).1
  obtain ⟨a, ha⟩ : ∃ a, base[k]? = some a :=
    ⟨base[k]'(by rw [hinv.base_len]; exact hklt), List.getElem?_eq_getElem _⟩
  have hamem : a ∈ l := hinv.base_mem a (List.mem_of_getElem? ha)
  have hxcov := hinv.cov_eq k x a hk ha hav hsc
  -- An entry of the new work list, in terms of the entry of the old one at the same position.
  have hentry : ∀ k' x', ((items.set k { x with available := false }).map (m.upd x.item))[k']? =
      some x' → ∃ z, items[k']? = some z ∧
        ((k' = k ∧ z = x ∧ x' = { x with available := false }) ∨
          (k' ≠ k ∧ x' = m.upd x.item z)) := by
    intro k' x' hx'
    rw [List.getElem?_map, Option.map_eq_some_iff] at hx'
    obtain ⟨y, hy, rfl⟩ := hx'
    by_cases hkk : k = k'
    · subst hkk
      rw [List.getElem?_set_self hklt] at hy
      cases hy
      exact ⟨x, hk, Or.inl ⟨rfl, rfl, by simp [MaxCoverModel.upd]⟩⟩
    · rw [List.getElem?_set_ne hkk] at hy
      exact ⟨y, hy, Or.inr ⟨fun h => hkk h.symm, rfl⟩⟩
  refine ⟨a, hamem, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · simp [hinv.base_len]
  · exact hinv.base_mem
  · simp [hinv.picked_len]
  · intro b hb
    rcases List.mem_append.mp hb with hb | hb
    · exact hinv.picked_mem b hb
    · rw [List.mem_singleton.mp hb]
      exact hamem
  · intro k' x' a' hx' ha'
    obtain ⟨z, hz, hcase⟩ := hentry k' x' hx'
    rcases hcase with ⟨_, rfl, rfl⟩ | ⟨_, rfl⟩
    · exact hinv.im_eq k' z a' hz ha'
    · unfold MaxCoverModel.upd
      split
      · rw [m.update_im]
        exact hinv.im_eq k' z a' hz ha'
      · exact hinv.im_eq k' z a' hz ha'
  · intro k' x' a' hx' ha' hav' hsc' u
    obtain ⟨z, hz, hcase⟩ := hentry k' x' hx'
    rcases hcase with ⟨_, _, rfl⟩ | ⟨_, rfl⟩
    · simp at hav'
    · unfold MaxCoverModel.upd at hav' hsc' ⊢
      split at hav' <;> rename_i hcond
      · rw [ite_eq_left hcond]
        have hz' := hinv.cov_eq k' z a' hz ha' hcond.1 hcond.2 u
        simp only [m.update_cov, hz', List.mem_append, List.mem_singleton]
        constructor
        · rintro ⟨⟨h1, h2⟩, h3⟩
          refine ⟨h1, fun r hr => ?_⟩
          rcases hr with hr | rfl
          · exact h2 r hr
          · exact h3
        · rintro ⟨h1, h2⟩
          exact ⟨⟨h1, fun r hr => h2 r (Or.inl hr)⟩, h2 _ (Or.inr rfl)⟩
      · rw [ite_eq_right hcond] at hsc'
        exact absurd ⟨hav', hsc'⟩ hcond
  · intro b hb u hu
    rcases List.mem_append.mp hb with hb | hb
    · obtain ⟨r, hr, hru⟩ := hinv.covered b hb u hu
      exact ⟨r, List.mem_append_left _ hr, hru⟩
    · rw [List.mem_singleton.mp hb] at hu
      by_cases hex : ∃ r ∈ result, m.cov r u
      · obtain ⟨r, hr, hru⟩ := hex
        exact ⟨r, List.mem_append_left _ hr, hru⟩
      · refine ⟨x.item, by simp, (hxcov u).mpr ⟨hu, fun r hr hru => hex ⟨r, hr, hru⟩⟩⟩
  · intro i b hb
    by_cases hi : i < picked.length
    · rw [List.getElem?_append_left hi] at hb
      obtain ⟨him, u, hu, hfresh⟩ := hinv.fresh i b hb
      refine ⟨fun r hr => him r ?_, u, hu, fun j a' hj ha' => hfresh j a' hj ?_⟩
      · rwa [List.getElem?_append_left (by rw [← hinv.picked_len]; exact hi)] at hr
      · rwa [List.getElem?_append_left (by omega)] at ha'
    · have hi' : i = picked.length := by
        have := (List.getElem?_eq_some_iff.mp hb).1
        simp at this
        omega
      subst hi'
      rw [List.getElem?_append_right (Nat.le_refl _)] at hb
      have hab : a = b := by simpa using hb
      subst hab
      refine ⟨fun r hr => ?_, ?_⟩
      · rw [hinv.picked_len, List.getElem?_append_right (Nat.le_refl _)] at hr
        have hxr : x.item = r := by simpa using hr
        subst hxr
        exact hinv.im_eq k x a hk ha
      · obtain ⟨u, hu⟩ := m.cov_of_score x.item hsc
        obtain ⟨hbu, hnone⟩ := (hxcov u).mp hu
        refine ⟨u, hbu, fun j a' hj ha' ha'u => ?_⟩
        rw [List.getElem?_append_left hj] at ha'
        obtain ⟨r, hr, hru⟩ := hinv.covered a' (List.mem_of_getElem? ha') u ha'u
        exact hnone r hr hru

omit m in
/-- `set_opt` with a value is `set`. -/
theorem list_set_opt_some {α : Type} (l : List α) (i : Nat) (y : α) :
    l.set_opt i (some y) = l.set i y := by
  induction l generalizing i with
  | nil => simp [List.set_opt]
  | cons hd tl ih =>
    cases i with
    | zero => simp [List.set_opt]
    | succ i => simp [List.set_opt, ih]

/-- `Range<usize>::next` on a range that is not empty: it yields the start and moves it by one. -/
theorem range_next_some (r : core.ops.range.Range Usize) (h : r.start.val < r.end.val) :
    ∃ s', core.iter.range.IteratorRange.next core.iter.range.StepUsize r =
        ok (some r.start, { r with start := s' }) ∧ s'.val = r.start.val + 1 := by
  have hfwd : r.start.val < UScalar.max .Usize := by scalar_tac
  simp only [core.iter.range.IteratorRange.next, core.iter.range.StepUsize,
    core.iter.range.UScalarStep, core.iter.range.UScalarStep.forward_checked]
  simp [core.cmp.impls.PartialOrdUsize.lt, h, hfwd]

/-- `Range<usize>::next` on an empty range yields nothing. -/
theorem range_next_none (r : core.ops.range.Range Usize) (h : r.end.val ≤ r.start.val) :
    core.iter.range.IteratorRange.next core.iter.range.StepUsize r = ok (none, r) := by
  have hnlt : ¬ r.start.val < r.end.val := by omega
  simp only [core.iter.range.IteratorRange.next, core.iter.range.StepUsize,
    core.iter.range.UScalarStep, core.iter.range.UScalarStep.forward_checked]
  simp [core.cmp.impls.PartialOrdUsize.lt, hnlt]

/-- The selection loop of `maximum_cover` does not panic. It runs at most `n` more rounds, and
the result it returns has the `Fresh` property. -/
theorem maximum_cover_loop1_eq (l : List T) :
    ∀ (n : Nat) (iter : core.ops.range.Range Usize)
      (all_items : alloc.vec.Vec (MaxCoverItem T)) (result : alloc.vec.Vec T)
      (base picked : List T),
      iter.end.val - iter.start.val = n → result.val.length ≤ iter.start.val →
      m.CoverInv l all_items.val base result.val picked →
      ∃ out picked', maximum_cover_loop1 iterInst inst iter all_items result = ok out ∧
        out.val.length ≤ result.val.length + n ∧ picked'.length = out.val.length ∧
        (∀ a ∈ picked', a ∈ l) ∧ m.Fresh picked' out.val := by
  intro n
  induction n with
  | zero =>
    intro iter all_items result base picked hn hres hinv
    refine ⟨result, picked, ?_, by omega, hinv.picked_len, hinv.picked_mem, hinv.fresh⟩
    rw [maximum_cover_loop1]
    simp [range_next_none iter (by omega)]
  | succ n ih =>
    intro iter all_items result base picked hn hres hinv
    obtain ⟨start', hnext, hstart'⟩ := range_next_some iter (by omega)
    obtain ⟨r, hr, hok⟩ := best_item_index_eq m (alloc.vec.Vec.deref all_items)
    rw [maximum_cover_loop1]
    simp only [hnext, hr]
    cases r with
    | none =>
      exact ⟨result, picked, by simp, by omega, hinv.picked_len, hinv.picked_mem, hinv.fresh⟩
    | some best_index =>
      obtain ⟨x, hx, hav, hsc⟩ := hok best_index rfl
      have hx' : all_items.val[best_index.val]? = some x := by
        simpa [alloc.vec.Vec.deref] using hx
      have hget : all_items.slice.val[best_index.val]? = some x := hx'
      -- The work list after the round, and the result with the picked item.
      obtain ⟨s4, hs4, hs4val⟩ := update_covering_sets_eq m x.item
        (all_items.slice.set_opt best_index (some { x with available := false }))
      obtain ⟨result1, hpush, hresult1⟩ := vec_push_eq result x.item (by scalar_tac)
      obtain ⟨a, _, hinv'⟩ := hinv.step m hx' hav hsc
      have hval2 : (({ slice := s4 } : alloc.vec.Vec (MaxCoverItem T))).val =
          (all_items.val.set best_index.val { x with available := false }).map
            (m.upd x.item) := by
        rw [show ({ slice := s4 } : alloc.vec.Vec (MaxCoverItem T)).val = s4.val from rfl, hs4val]
        simp only [Slice.set_opt, Slice.from_val, list_set_opt_some]
        rfl
      obtain ⟨out, picked', hout, hlen, hplen, hpmem, hfresh⟩ := ih
        { iter with start := start' } { slice := s4 } result1 base (picked ++ [a])
        (by simp; omega) (by rw [hresult1]; simp; omega) (by rw [hval2, hresult1]; exact hinv')
      refine ⟨out, picked', ?_, ?_, hplen, hpmem, hfresh⟩
      · simp [lift, alloc.vec.Vec.deref_mut, core.slice.Slice.get_mut, hget, m.clone_eq, hs4,
          hpush, hout]
      · rw [hresult1] at hlen
        simp at hlen
        omega

/-! ## `maximum_cover` -/

/-- `maximum_cover` does not panic on an iterator that yields `l`, and returns at most `limit`
items.

`picked` lists, for each returned item, the yielded item it came from. The two have the same
intermediate object. The yielded item covers an element that the yielded item of no earlier pick
covers (`Fresh`).

`hlen` bounds the number of yielded items by `usize::MAX`, the largest length of a `Vec`. -/
theorem maximum_cover_spec (items : I) {it : It} {l : List T}
    (hinto : iterInst.into_iter items = ok it) (hyields : Yields iterInst.iteratorInst it l)
    (hlen : l.length ≤ Usize.max) (limit : Usize) (label : Str) :
    ∃ out picked, maximum_cover iterInst inst items limit label = ok out ∧
      out.val.length ≤ limit.val ∧ picked.length = out.val.length ∧ (∀ a ∈ picked, a ∈ l) ∧
      m.Fresh picked out.val := by
  obtain ⟨all_items, hitems, hval⟩ := maximum_cover_loop0_eq m iterInst hyields
    (alloc.vec.Vec.new (MaxCoverItem T)) (by simpa using hlen)
  rw [show (alloc.vec.Vec.new (MaxCoverItem T)).val = [] from rfl, List.nil_append] at hval
  -- The work list starts with one available entry per yielded item with a non-zero score.
  have hinv : m.CoverInv l all_items.val (l.filter fun t => decide (m.score t ≠ 0#usize)) [] [] := by
    refine ⟨by simp [hval], fun a ha => (List.mem_filter.mp ha).1, rfl, by simp, ?_, ?_, by simp,
      fun i a h => by simp at h⟩
    · intro k x a hx ha
      rw [hval, List.getElem?_map, ha] at hx
      cases hx
      rfl
    · intro k x a hx ha _ _ u
      rw [hval, List.getElem?_map, ha] at hx
      cases hx
      simp
  obtain ⟨out, picked, hout, hlenout, hplen, hpmem, hfresh⟩ := maximum_cover_loop1_eq m iterInst l
    limit.val { start := 0#usize, «end» := limit } all_items (alloc.vec.Vec.new T) _ [] (by simp)
    (by simp) (by simpa using hinv)
  refine ⟨out, picked, ?_, by simpa using hlenout, hplen, hpmem, hfresh⟩
  simp [maximum_cover, hinto, hitems, metrics.set_max_cover_non_zero_items, hout]

end

end operation_pool
