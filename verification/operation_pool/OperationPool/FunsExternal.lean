-- Hand-written models of the external functions listed in `FunsExternal_Template.lean`.
--
-- Everything in this file is TRUSTED: each definition states what an external Rust function
-- computes, and is not checked against the Rust implementation. The models follow the documented
-- behaviour of `std`.
import Aeneas
import OperationPool.Types
open Aeneas Aeneas.Std Result ControlFlow Error
set_option linter.dupNamespace false
set_option linter.hashCommand false
set_option linter.unusedVariables false

open operation_pool

/-! ## `std::collections::HashSet`

The `Eq`, `Hash` and `BuildHasher` instances are ignored: membership is equality of values. -/

/-- The elements of a set. -/
def std.collections.hash.set.HashSet.toList {T : Type} {S : Type} {A : Type}
  (s : std.collections.hash.set.HashSet T S A) : List T := s

/-- The set with the given elements. -/
def std.collections.hash.set.HashSet.ofList {T : Type} {S : Type} {A : Type}
  (l : List T) : std.collections.hash.set.HashSet T S A := l

/-- [std::collections::hash::set::{HashSet<T, RandomState>}::with_capacity]

    The empty set; the capacity is only a hint. -/
@[rust_fun
  "std::collections::hash::set::{std::collections::hash::set::HashSet<@T, std::hash::random::RandomState, alloc::alloc::Global>}::with_capacity"]
def std.collections.hash.set.HashSetTRandomStateGlobal.with_capacity
  (T : Type) (_ : Std.Usize) :
  Result (std.collections.hash.set.HashSet T std.hash.random.RandomState
    alloc.alloc.Global) :=
  ok (std.collections.hash.set.HashSet.ofList [])

open Classical in
/-- [std::collections::hash::set::{HashSet<T, S, A>}::contains]

    Whether some element of the set borrows as the given value. -/
@[rust_fun
  "std::collections::hash::set::{std::collections::hash::set::HashSet<@T, @S, @A>}::contains"]
noncomputable def std.collections.hash.set.HashSet.contains
  {T : Type} {S : Type} {A : Type} {Q : Type} {Clause2_Hasher : Type}
  (corecmpEqInst : core.cmp.Eq T) (corehashHashInst : core.hash.Hash T)
  (corehashBuildHasherInst : core.hash.BuildHasher S Clause2_Hasher)
  (coreborrowBorrowInst : core.borrow.Borrow T Q) (corehashHashInst1 :
  core.hash.Hash Q) (corecmpEqInst1 : core.cmp.Eq Q)
  (s : std.collections.hash.set.HashSet T S A) (value : Q) : Result Bool :=
  ok (decide (∃ t ∈ s.toList, coreborrowBorrowInst.borrow t = ok value))

open Classical in
/-- [std::collections::hash::set::{HashSet<T, S, A>}::insert]

    Adds the value if it is not already present, and returns whether it was added. -/
@[rust_fun
  "std::collections::hash::set::{std::collections::hash::set::HashSet<@T, @S, @A>}::insert"]
noncomputable def std.collections.hash.set.HashSet.insert
  {T : Type} {S : Type} {A : Type} {Clause2_Hasher : Type} (corecmpEqInst :
  core.cmp.Eq T) (corehashHashInst : core.hash.Hash T) (corehashBuildHasherInst
  : core.hash.BuildHasher S Clause2_Hasher)
  (s : std.collections.hash.set.HashSet T S A) (value : T) :
  Result (Bool × (std.collections.hash.set.HashSet T S A)) :=
  if value ∈ s.toList then ok (false, s)
  else ok (true, std.collections.hash.set.HashSet.ofList (value :: s.toList))

/-! ## `std::collections::HashMap`

The `Eq`, `Hash` and `BuildHasher` instances are ignored: a key is found when it equals the key
of an entry. -/

/-- The entries of a map. -/
def std.collections.hash.map.HashMap.entries {K : Type} {V : Type} {S : Type} {A : Type}
  (m : std.collections.hash.map.HashMap K V S A) : List (K × V) := m

/-- The map with the given entries. -/
def std.collections.hash.map.HashMap.ofEntries {K : Type} {V : Type} {S : Type} {A : Type}
  (l : List (K × V)) : std.collections.hash.map.HashMap K V S A := l

/-- [std::collections::hash::map::{HashMap<K, V, RandomState>}::new]

    The empty map. -/
@[rust_fun
  "std::collections::hash::map::{std::collections::hash::map::HashMap<@K, @V, std::hash::random::RandomState, alloc::alloc::Global>}::new"]
def std.collections.hash.map.HashMapKVRandomStateGlobal.new
  (K : Type) (V : Type) :
  Result (std.collections.hash.map.HashMap K V std.hash.random.RandomState
    alloc.alloc.Global) :=
  ok (std.collections.hash.map.HashMap.ofEntries [])

open Classical in
/-- [std::collections::hash::map::{HashMap<K, V, S, A>}::insert]

    Stores the value under the key. The entry that the map had for the key, if any, is replaced,
    and its value is returned. -/
@[rust_fun
  "std::collections::hash::map::{std::collections::hash::map::HashMap<@K, @V, @S, @A>}::insert"]
noncomputable def std.collections.hash.map.HashMap.insert
  {K : Type} {V : Type} {S : Type} {A : Type} {Clause2_Hasher : Type}
  (corecmpEqInst : core.cmp.Eq K) (corehashHashInst : core.hash.Hash K)
  (corehashBuildHasherInst : core.hash.BuildHasher S Clause2_Hasher)
  (m : std.collections.hash.map.HashMap K V S A) (key : K) (value : V) :
  Result ((Option V) × (std.collections.hash.map.HashMap K V S A)) :=
  ok ((m.entries.find? fun entry => decide (entry.1 = key)).map Prod.snd,
    std.collections.hash.map.HashMap.ofEntries
      ((key, value) :: m.entries.filter fun entry => decide (entry.1 ≠ key)))

/-! ## `ValidatorMap::retain`

`extract.sh` leaves this method opaque, because Aeneas does not translate its body. -/

/-- The entries that `keep` accepts. `keep` is called once for each entry, and each call gets
the closure that the call before it returned. -/
def validator_map.retainEntries {V : Type} {F : Type}
  (inst : core.ops.function.FnMut F (Std.U64 × V) Bool) :
  F → List (Std.U64 × V) → Result (List (Std.U64 × V))
  | _, [] => ok []
  | keep, entry :: rest => do
    let (kept, keep1) ← inst.call_mut keep entry
    let rest1 ← validator_map.retainEntries inst keep1 rest
    ok (if kept then entry :: rest1 else rest1)

/-- [operation_pool::validator_map::{ValidatorMap<V>}::retain]

    ```rust
    self.map.retain(|index, value| keep(*index, value));
    ```

    `HashMap::retain` calls the closure once for each entry and keeps the entries it accepts. The
    closure gives `keep` the key and a shared reference to the value, so no value changes. `std`
    does not fix the order of the calls; the model makes them in the order of the entries. -/
def validator_map.ValidatorMap.retain
  {V : Type} {F : Type} (coreopsfunctionFnMutFPairU64Shared0VBoolInst :
  core.ops.function.FnMut F (Std.U64 × V) Bool)
  (self : validator_map.ValidatorMap V) (keep : F) :
  Result (validator_map.ValidatorMap V) := do
  let entries ←
    validator_map.retainEntries coreopsfunctionFnMutFPairU64Shared0VBoolInst keep self.map.entries
  ok { map := std.collections.hash.map.HashMap.ofEntries entries }

/-! ## Metrics -/

/-- [operation_pool::metrics::set_max_cover_non_zero_items]

    Sets a Prometheus gauge. The caller reads no result, and the model assumes that the call does
    not panic. -/
def metrics.set_max_cover_non_zero_items (_ : Str) (_ : Std.Usize) : Result Unit := ok ()

/-! The hashing functions below are never called by the extracted code; they only appear as
fields of trait instances. They are left opaque. -/

/-- [core::hash::impls::{impl core::hash::Hash for u64}::hash] -/
@[rust_fun "core::hash::impls::{core::hash::Hash<u64>}::hash"]
axiom U64.Insts.CoreHashHash.hash
  {H : Type} (HasherInst : core.hash.Hasher H) : Std.U64 → H → Result H

/-- [std::hash::random::{impl core::hash::Hasher for DefaultHasher}::finish] -/
@[rust_fun
  "std::hash::random::{core::hash::Hasher<std::hash::random::DefaultHasher>}::finish"]
axiom std.hash.random.DefaultHasher.Insts.CoreHashHasher.finish
  : std.hash.random.DefaultHasher → Result Std.U64

/-- [std::hash::random::{impl core::hash::Hasher for DefaultHasher}::write] -/
@[rust_fun
  "std::hash::random::{core::hash::Hasher<std::hash::random::DefaultHasher>}::write"]
axiom std.hash.random.DefaultHasher.Insts.CoreHashHasher.write
  :
  std.hash.random.DefaultHasher → Slice Std.U8 → Result
    std.hash.random.DefaultHasher

/-- [std::hash::random::{impl core::hash::BuildHasher for RandomState}::build_hasher] -/
@[rust_fun
  "std::hash::random::{core::hash::BuildHasher<std::hash::random::RandomState, std::hash::random::DefaultHasher>}::build_hasher"]
axiom
  std.hash.random.RandomState.Insts.CoreHashBuildHasherDefaultHasher.build_hasher
  : std.hash.random.RandomState → Result std.hash.random.DefaultHasher
