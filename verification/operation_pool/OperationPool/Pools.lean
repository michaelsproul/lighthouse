import OperationPool.Bridge.ToSpec

/-!
# A model of `get_slashings_and_exits`

`OperationPool::get_slashings_and_exits` reads `BeaconState<E>` and the three pools of `self`, so
it is not extracted. `getSlashingsAndExits` is a model of its body. It makes the calls of the
Rust, in order, on the extracted functions:

1. `filter_limit_operations` packs the proposer slashings;
2. `AttesterSlashingMaxCover::new` builds a cover for each attester slashing of the pool, with
   the proposers of the packed slashings as the set `to_be_slashed`;
3. `maximum_cover` picks covers;
4. the two `insert_*` functions build the exclusion set from the packed slashings;
5. `filter_limit_operations` packs the voluntary exits, with a filter that reads that set.

The data of the model:

* `PoolEntry T` is `SigVerifiedOp<T, E>`: an operation, and whether its signature is still valid
  on the fork of the packing state.
* A proposer slashing is the extracted Rust type. A voluntary exit is `RSignedVoluntaryExit`.
* `PooledAttesterSlashing` is an attester slashing of the pool. Rust's `AttesterSlashing<E>` is
  generic over `EthSpec` and is not extracted, so the model holds what the packing code reads,
  the attesting indices of the two attestations, next to the spec's form of the slashing.
* `Pools` holds the three pools, `Limits` the three limits, and `Packed` the three results.

Everything here is a model. That the Rust computes what `getSlashingsAndExits` computes is
TRUSTED. Each step quotes the Rust it models. `OperationPool/Closures.lean` has the models of the
three closures, and `OperationPool/Concrete.lean` the instances the extracted functions run on.
-/

set_option autoImplicit false

open Aeneas Aeneas.Std Result

namespace OperationPool

open OperationPool.Bridge
open operation_pool (IndexSet AttesterCover sliceIntoIter pureFn cloneId)
open EthLib.Consensus (Preset AttesterSlashing)

/-! ## The pools -/

/-- A pool entry, `SigVerifiedOp<T, E>`. -/
structure PoolEntry (T : Type) where
  /-- `as_inner()`: the operation. -/
  inner : T
  /-- `signature_is_still_valid(&state.fork())`, on the packing state. -/
  stillValid : Bool

/-- An attester slashing of the pool: the attesting indices of its two attestations, which are
what the packing code reads, and the spec's form of the slashing, which carries the same
indices. -/
structure PooledAttesterSlashing (preset : Preset) (fork : EthLib.Base.Fork) where
  /-- `attestation_1().attesting_indices`. -/
  attestingIndices1 : Slice U64
  /-- `attestation_2().attesting_indices`. -/
  attestingIndices2 : Slice U64
  /-- The spec's form of the slashing. -/
  spec : AttesterSlashing preset fork
  /-- The spec's first attestation names the Rust indices. -/
  indices1 : spec.attestation1.attestingIndices.data.toList = attestingIndices1.val.map u64ToSpec
  /-- The spec's second attestation names the Rust indices. -/
  indices2 : spec.attestation2.attestingIndices.data.toList = attestingIndices2.val.map u64ToSpec

/-- The two lists of attesting indices of a slashing. -/
def PooledAttesterSlashing.indices {preset : Preset} {fork : EthLib.Base.Fork}
    (slashing : PooledAttesterSlashing preset fork) : Slice U64 × Slice U64 :=
  (slashing.attestingIndices1, slashing.attestingIndices2)

/-- The three pools that `get_slashings_and_exits` reads: the values of
`self.proposer_slashings`, `self.attester_slashings` and `self.voluntary_exits`. -/
structure Pools (preset : Preset) (fork : EthLib.Base.Fork) where
  proposerSlashings : Slice (PoolEntry RProposerSlashing)
  attesterSlashings : Slice (PoolEntry (PooledAttesterSlashing preset fork))
  voluntaryExits : Slice (PoolEntry RSignedVoluntaryExit)

/-- The three limits: `E::MaxProposerSlashings`, `E::max_attester_slashings_electra()` and
`E::MaxVoluntaryExits`, as `usize`. -/
structure Limits where
  proposerSlashings : Usize
  attesterSlashings : Usize
  voluntaryExits : Usize

/-- What `get_slashings_and_exits` returns. -/
structure Packed (preset : Preset) (fork : EthLib.Base.Fork) where
  proposerSlashings : alloc.vec.Vec RProposerSlashing
  attesterSlashings : List (PooledAttesterSlashing preset fork)
  voluntaryExits : alloc.vec.Vec RSignedVoluntaryExit

namespace Packed

variable {preset : Preset} {fork : EthLib.Base.Fork}

/-- The spec's form of the packed operations. -/
def toSpec (packed : Packed preset fork) : EthLibOps.SlashingsAndExits preset fork where
  proposerSlashings := packed.proposerSlashings.val.map toSpecProposerSlashing
  attesterSlashings := packed.attesterSlashings.map (·.spec)
  voluntaryExits := packed.voluntaryExits.val.map toSpecVoluntaryExit

/-- Each packed list is within its limit. -/
def WithinLimits (packed : Packed preset fork) (limits : Limits) : Prop :=
  packed.proposerSlashings.val.length ≤ limits.proposerSlashings.val ∧
    packed.attesterSlashings.length ≤ limits.attesterSlashings.val ∧
    packed.voluntaryExits.val.length ≤ limits.voluntaryExits.val

end Packed

/-! ## Three small models -/

/-- A list as an iterator: `next` returns the head and keeps the tail. It stands for the
`flat_map` iterator of `get_attester_slashings`, which yields the covers in order. -/
def listIntoIter (T : Type) : core.iter.traits.collect.IntoIterator (List T) T (List T) where
  iteratorInst :=
    { next := fun items =>
        match items with
        | [] => ok (none, [])
        | item :: rest => ok (some item, rest) }
  into_iter := fun items => ok items

/-- The list iterator yields the entries of the list, in order. -/
theorem yields_list {T : Type} :
    ∀ items : List T, operation_pool.Yields (listIntoIter T).iteratorInst items items
  | [] => .done (it' := []) rfl
  | _ :: rest => .step (it' := rest) rfl (yields_list rest)

/-- `to_be_slashed.extend(keys)`: each key that the set does not hold is added. -/
def extendIndexSet (set : IndexSet) (keys : List U64) : IndexSet :=
  keys.foldl (fun set key =>
    if key ∈ set.toList then set
    else std.collections.hash.set.HashSet.ofList (key :: set.toList)) set

/-- The string literal `"attester_slashings"`, which `get_attester_slashings` gives to
`maximum_cover` as the label of a metric. Its bytes are not modelled. -/
opaque attesterSlashingsLabel : Str := Slice.new U8

/-! ## The model -/

variable {preset : Preset} {fork : EthLib.Base.Fork}

/-- `OperationPool::get_slashings_and_exits`, on a view of the state:

```rust
let proposer_slashings = filter_limit_operations(
    self.proposer_slashings.read().values(),
    |slashing| {
        slashing.signature_is_still_valid(&state.fork())
            && verify_proposer_slashing(slashing.as_inner(), state, VerifySignatures::False, spec)
                .is_ok()
    },
    |slashing| slashing.as_inner().clone(),
    E::MaxProposerSlashings::to_usize(),
);
let mut to_be_slashed = proposer_slashings.iter().map(|s| s.proposer_index()).collect();
let attester_slashings = self.get_attester_slashings(state, &mut to_be_slashed);
insert_proposer_slashing_indices(&mut to_be_slashed, &proposer_slashings);
for slashing in &attester_slashings {
    let slashing = slashing.to_ref();
    insert_common_attesting_indices(
        &mut to_be_slashed,
        slashing.attestation_1().attesting_indices_iter().as_slice(),
        slashing.attestation_2().attesting_indices_iter().as_slice(),
    );
}
let voluntary_exits = self.get_voluntary_exits(
    state,
    |exit| !to_be_slashed.contains(&exit.message.validator_index),
    spec,
);
(proposer_slashings, attester_slashings, voluntary_exits)
```

`get_attester_slashings` builds the covers, calls `maximum_cover`, and for each picked cover
extends `to_be_slashed` with the keys of its covering set and returns its slashing. -/
noncomputable def getSlashingsAndExits (view : StateView) (pools : Pools preset fork)
    (limits : Limits) : Result (Packed preset fork) := do
  let proposerSlashings ←
    operation_pool.filter_limit_operations (sliceIntoIter _) (BuiltinFn _ Bool)
      (BuiltinFn _ RProposerSlashing) (cloneId _) pools.proposerSlashings
      (proposerSlashingFilter view PoolEntry.stillValid headersOk PoolEntry.inner)
      (pureFn PoolEntry.inner) limits.proposerSlashings
  let proposers := proposerIndexSet proposerSlashings.val
  -- `get_attester_slashings`.
  let covers ←
    attesterCovers view PoolEntry.stillValid PoolEntry.inner PooledAttesterSlashing.indices
      proposers pools.attesterSlashings.val
  let picked ←
    operation_pool.max_cover.maximum_cover (listIntoIter _) (AttesterCover.maxCover _) covers
      limits.attesterSlashings attesterSlashingsLabel
  let toBeSlashed :=
    picked.val.foldl (fun set cover => extendIndexSet set (cover.balances.map Prod.fst)) proposers
  -- The exclusion set, from the packed slashings alone.
  let excluded ←
    operation_pool.exclusionSet toBeSlashed (alloc.vec.Vec.deref proposerSlashings)
      (picked.val.map fun cover => cover.slashing.indices)
  -- `get_voluntary_exits`.
  let voluntaryExits ←
    operation_pool.filter_limit_operations (sliceIntoIter _) (BuiltinFn _ Bool)
      (BuiltinFn _ RSignedVoluntaryExit) (cloneId _) pools.voluntaryExits
      (voluntaryExitFilter view excluded PoolEntry.stillValid fun entry => entry.inner.message)
      (pureFn PoolEntry.inner) limits.voluntaryExits
  ok
    { proposerSlashings
      attesterSlashings := picked.val.map (·.slashing)
      voluntaryExits }

end OperationPool
