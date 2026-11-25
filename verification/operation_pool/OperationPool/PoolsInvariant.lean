import OperationPool.PackedSlashingsAndExitsValid
import OperationPool.ValidatorMap

/-!
# `PoolsValid` is an invariant of the pool writers and of the state transition

`packed_slashings_and_exits_valid` assumes `PoolsValid config checks s pools`, on the packing
state `s`. This file proves that premise from a history of the pools.

`PoolMaps` is the three pools as the op pool holds them: two `ValidatorMap`s, for the proposer
slashings and the voluntary exits, and the set of attester slashings. `PoolInv config checks s
maps` is the invariant on the state `s`: the two maps are keyed by validator index, and each
entry whose signature is still valid passes the spec's signature checks on `s`.

`PoolStep` lists the moves of the pools and the state:

* `advance`: the state moves to one that extends it (`EthLibOps.Extends`). A run of slots and
  blocks within a fork is such a move (`PoolStep.advance_of_run`).
* `insertProposerSlashing`, `insertVoluntaryExit`: the extracted `ValidatorMap::insert`, for an
  operation that passes the spec's signature checks on the current state.
* `retainProposerSlashings`, `retainVoluntaryExits`: `ValidatorMap::retain`.
* `insertAttesterSlashing`, `pruneAttesterSlashings`: the two writes of the `HashSet` of
  attester slashings, on a list.

`PoolInv.step` proves that each move keeps the invariant, and `PoolInv.empty` that the empty
pools have it. `poolsValid_of_inv` gives `PoolsValid` for any pools that enumerate the maps, and
`packed_slashings_and_exits_valid_of_history` is the main theorem on a history.

What the moves assume, and the file does not prove:

* The premise of an insert, that the operation passes the checks on the current state. In
  Lighthouse `SigVerifiedOp::validate` makes those checks before an operation enters a pool.
* The two instances of `ValidatorIndexed` restate the two Rust impls.
* An entry's `stillValid` flag is fixed. That is right within a fork, where `state.fork()` does
  not change, and `Extends` keeps the fork versions.
-/

set_option autoImplicit false

namespace OperationPool

open EthLib EthLib.Consensus
open EthLibOps
open OperationPool.Bridge
open operation_pool.validator_map (ValidatorIndexed ValidatorMap Keyed)

section

variable {preset : Preset} {fork : Base.Fork} {config : Config} {checks : SignatureChecks}

/-! ## The validator of a pool entry -/

/-- `impl ValidatorIndexed for SigVerifiedOp<ProposerSlashing, E>`:

```rust
self.as_inner().proposer_index()
```

`ProposerSlashing::proposer_index` reads `signed_header_1.message.proposer_index`. -/
def proposerSlashingIndexed : ValidatorIndexed (PoolEntry RProposerSlashing) where
  validator_index entry := .ok entry.inner.signed_header_1.message.proposer_index

/-- `impl ValidatorIndexed for SigVerifiedOp<SignedVoluntaryExit, E>`:

```rust
self.as_inner().message.validator_index
```
-/
def voluntaryExitIndexed : ValidatorIndexed (PoolEntry RSignedVoluntaryExit) where
  validator_index entry := .ok entry.inner.message.validator_index

/-! ## The pools and their invariant -/

/-- The three pools, as the op pool holds them. -/
structure PoolMaps (preset : Preset) (fork : Base.Fork) where
  /-- `self.proposer_slashings`. -/
  proposerSlashings : ValidatorMap (PoolEntry RProposerSlashing)
  /-- `self.attester_slashings`, a `HashSet`: its entries, in some order. -/
  attesterSlashings : List (PoolEntry (PooledAttesterSlashing preset fork))
  /-- `self.voluntary_exits`. -/
  voluntaryExits : ValidatorMap (PoolEntry RSignedVoluntaryExit)

/-- The invariant of the pools, on the state `s`. -/
structure PoolInv (config : Config) (checks : SignatureChecks) (s : BeaconState preset fork)
    (maps : PoolMaps preset fork) : Prop where
  /-- Each proposer slashing is stored under the index of its proposer. -/
  proposersKeyed : Keyed proposerSlashingIndexed maps.proposerSlashings
  /-- Each voluntary exit is stored under the index of its validator. -/
  exitsKeyed : Keyed voluntaryExitIndexed maps.voluntaryExits
  /-- A proposer slashing with a signature that is still valid is signed on `s`. -/
  proposerSigned : ∀ entry ∈ maps.proposerSlashings.values, entry.stillValid = true →
    ProposerSlashingSigned config checks s (toSpecProposerSlashing entry.inner)
  /-- An attester slashing with a signature that is still valid passes the checks of
  `process_attester_slashing` that run before the walk, on `s`. -/
  attesterChecks : ∀ entry ∈ maps.attesterSlashings, entry.stillValid = true →
    AttesterSlashingChecks config checks s entry.inner.spec
  /-- A voluntary exit with a signature that is still valid is signed on `s`. -/
  exitSigned : ∀ entry ∈ maps.voluntaryExits.values, entry.stillValid = true →
    ExitSigned config checks s (toSpecVoluntaryExit entry.inner)

/-- Pools with no entry have the invariant, on any state. -/
theorem PoolInv.empty (s : BeaconState preset fork) {maps : PoolMaps preset fork}
    (proposerSlashings : maps.proposerSlashings.entries = [])
    (attesterSlashings : maps.attesterSlashings = [])
    (voluntaryExits : maps.voluntaryExits.entries = []) : PoolInv config checks s maps where
  proposersKeyed := .of_entries_nil _ proposerSlashings
  exitsKeyed := .of_entries_nil _ voluntaryExits
  proposerSigned entry member := by
    rw [ValidatorMap.values, proposerSlashings] at member
    exact nomatch member
  attesterChecks entry member := by
    rw [attesterSlashings] at member
    exact nomatch member
  exitSigned entry member := by
    rw [ValidatorMap.values, voluntaryExits] at member
    exact nomatch member

/-! ## The moves of the pools and the state -/

/-- One move of the pools and the state. -/
inductive PoolStep (config : Config) (checks : SignatureChecks) :
    BeaconState preset fork × PoolMaps preset fork →
    BeaconState preset fork × PoolMaps preset fork → Prop where
  /-- The state moves to a state that extends it. The pools do not change. -/
  | advance {s s' : BeaconState preset fork} {maps : PoolMaps preset fork} (h : Extends s s') :
      PoolStep config checks (s, maps) (s', maps)
  /-- `insert_proposer_slashing`: `ValidatorMap::insert`, for a slashing that is signed on the
  current state. -/
  | insertProposerSlashing {s : BeaconState preset fork} {maps : PoolMaps preset fork}
      {entry : PoolEntry RProposerSlashing} {old : Option (PoolEntry RProposerSlashing)}
      {map' : ValidatorMap (PoolEntry RProposerSlashing)}
      (signed : ProposerSlashingSigned config checks s (toSpecProposerSlashing entry.inner))
      (computed : ValidatorMap.insert proposerSlashingIndexed maps.proposerSlashings entry =
        .ok (old, map')) :
      PoolStep config checks (s, maps) (s, { maps with proposerSlashings := map' })
  /-- `insert_voluntary_exit`: `ValidatorMap::insert`, for an exit that is signed on the current
  state. -/
  | insertVoluntaryExit {s : BeaconState preset fork} {maps : PoolMaps preset fork}
      {entry : PoolEntry RSignedVoluntaryExit} {old : Option (PoolEntry RSignedVoluntaryExit)}
      {map' : ValidatorMap (PoolEntry RSignedVoluntaryExit)}
      (signed : ExitSigned config checks s (toSpecVoluntaryExit entry.inner))
      (computed : ValidatorMap.insert voluntaryExitIndexed maps.voluntaryExits entry =
        .ok (old, map')) :
      PoolStep config checks (s, maps) (s, { maps with voluntaryExits := map' })
  /-- `insert_attester_slashing`: `HashSet::insert`, for a slashing that passes the checks on
  the current state. The set then holds the slashing and entries it had. -/
  | insertAttesterSlashing {s : BeaconState preset fork} {maps : PoolMaps preset fork}
      {entry : PoolEntry (PooledAttesterSlashing preset fork)}
      {entries' : List (PoolEntry (PooledAttesterSlashing preset fork))}
      (passes : AttesterSlashingChecks config checks s entry.inner.spec)
      (held : ∀ other ∈ entries', other = entry ∨ other ∈ maps.attesterSlashings) :
      PoolStep config checks (s, maps) (s, { maps with attesterSlashings := entries' })
  /-- `prune_proposer_slashings`: `ValidatorMap::retain`, with any closure. -/
  | retainProposerSlashings {s : BeaconState preset fork} {maps : PoolMaps preset fork}
      {F : Type} {keepInst : Aeneas.Std.core.ops.function.FnMut F
        (Aeneas.Std.U64 × PoolEntry RProposerSlashing) Bool} {keep : F}
      {map' : ValidatorMap (PoolEntry RProposerSlashing)}
      (computed : validator_map.ValidatorMap.retain keepInst maps.proposerSlashings keep =
        .ok map') :
      PoolStep config checks (s, maps) (s, { maps with proposerSlashings := map' })
  /-- `prune_voluntary_exits`: `ValidatorMap::retain`, with any closure. -/
  | retainVoluntaryExits {s : BeaconState preset fork} {maps : PoolMaps preset fork}
      {F : Type} {keepInst : Aeneas.Std.core.ops.function.FnMut F
        (Aeneas.Std.U64 × PoolEntry RSignedVoluntaryExit) Bool} {keep : F}
      {map' : ValidatorMap (PoolEntry RSignedVoluntaryExit)}
      (computed : validator_map.ValidatorMap.retain keepInst maps.voluntaryExits keep =
        .ok map') :
      PoolStep config checks (s, maps) (s, { maps with voluntaryExits := map' })
  /-- `prune_attester_slashings`: `HashSet::retain`. The set then holds entries it had. -/
  | pruneAttesterSlashings {s : BeaconState preset fork} {maps : PoolMaps preset fork}
      {entries' : List (PoolEntry (PooledAttesterSlashing preset fork))}
      (held : ∀ other ∈ entries', other ∈ maps.attesterSlashings) :
      PoolStep config checks (s, maps) (s, { maps with attesterSlashings := entries' })

/-- A run of slots and blocks within a fork is a move of the state, for a state that a run from
a genesis state reaches. -/
theorem PoolStep.advance_of_run {g : ForkedBeaconState preset} {s s' : BeaconState preset fork}
    (maps : PoolMaps preset fork) (genesis : Genesis config checks g)
    (reached : Run config checks g ⟨fork, s⟩) (run : Run config checks ⟨fork, s⟩ ⟨fork, s'⟩) :
    PoolStep config checks (s, maps) (s', maps) :=
  .advance (extends_of_run genesis reached run)

/-- A move keeps the invariant. -/
theorem PoolInv.step {x y : BeaconState preset fork × PoolMaps preset fork}
    (h : PoolInv config checks x.1 x.2) (step : PoolStep config checks x y) :
    PoolInv config checks y.1 y.2 := by
  cases step with
  | advance later =>
    exact ⟨h.proposersKeyed, h.exitsKeyed,
      fun entry member valid => (h.proposerSigned entry member valid).of_extends later,
      fun entry member valid => (h.attesterChecks entry member valid).of_extends later,
      fun entry member valid => (h.exitSigned entry member valid).of_extends later⟩
  | insertProposerSlashing signed computed =>
    refine ⟨h.proposersKeyed.insert computed, h.exitsKeyed, fun entry member valid => ?_,
      h.attesterChecks, h.exitSigned⟩
    rcases operation_pool.validator_map.mem_values_of_insert computed member with rfl | member
    · exact signed
    · exact h.proposerSigned entry member valid
  | insertVoluntaryExit signed computed =>
    refine ⟨h.proposersKeyed, h.exitsKeyed.insert computed, h.proposerSigned, h.attesterChecks,
      fun entry member valid => ?_⟩
    rcases operation_pool.validator_map.mem_values_of_insert computed member with rfl | member
    · exact signed
    · exact h.exitSigned entry member valid
  | insertAttesterSlashing passes held =>
    refine ⟨h.proposersKeyed, h.exitsKeyed, h.proposerSigned, fun entry member valid => ?_,
      h.exitSigned⟩
    rcases held entry member with rfl | member
    · exact passes
    · exact h.attesterChecks entry member valid
  | retainProposerSlashings computed =>
    exact ⟨h.proposersKeyed.retain computed, h.exitsKeyed,
      fun entry member valid => h.proposerSigned entry
        (operation_pool.validator_map.mem_values_of_retain computed member) valid,
      h.attesterChecks, h.exitSigned⟩
  | retainVoluntaryExits computed =>
    exact ⟨h.proposersKeyed, h.exitsKeyed.retain computed, h.proposerSigned, h.attesterChecks,
      fun entry member valid => h.exitSigned entry
        (operation_pool.validator_map.mem_values_of_retain computed member) valid⟩
  | pruneAttesterSlashings held =>
    exact ⟨h.proposersKeyed, h.exitsKeyed, h.proposerSigned,
      fun entry member valid => h.attesterChecks entry (held entry member) valid, h.exitSigned⟩

/-- A sequence of moves keeps the invariant. -/
theorem PoolInv.steps {x y : BeaconState preset fork × PoolMaps preset fork}
    (h : PoolInv config checks x.1 x.2)
    (steps : Relation.ReflTransGen (PoolStep config checks) x y) :
    PoolInv config checks y.1 y.2 := by
  induction steps with
  | refl => exact h
  | tail _ step ih => exact ih.step step

/-! ## From the invariant to `PoolsValid` -/

/-- The slices that `get_slashings_and_exits` iterates hold the entries of the pools:
`self.proposer_slashings.read().values()`, `self.attester_slashings.read().iter()` and
`self.voluntary_exits.read().values()`. Each yields every entry once, in an order that `std`
does not fix, so each slice is a permutation of the entries. -/
structure Pools.Reads (pools : Pools preset fork) (maps : PoolMaps preset fork) : Prop where
  /-- The proposer slashings. -/
  proposerSlashings : pools.proposerSlashings.val.Perm maps.proposerSlashings.values
  /-- The attester slashings. -/
  attesterSlashings : pools.attesterSlashings.val.Perm maps.attesterSlashings
  /-- The voluntary exits. -/
  voluntaryExits : pools.voluntaryExits.val.Perm maps.voluntaryExits.values

variable {s : BeaconState preset fork} {maps : PoolMaps preset fork} {pools : Pools preset fork}

/-- Pools with the invariant are valid, in any order of their entries. -/
theorem poolsValid_of_inv (h : PoolInv config checks s maps) (reads : pools.Reads maps) :
    PoolsValid config checks s pools where
  proposersDistinct := by
    refine (reads.proposerSlashings.pairwise_iff fun differ => differ.symm).mpr ?_
    exact h.proposersKeyed.values_pairwise.imp fun differ same =>
      differ (congrArg Aeneas.Std.Result.ok same)
  exitsDistinct := by
    refine (reads.voluntaryExits.pairwise_iff fun differ => differ.symm).mpr ?_
    exact h.exitsKeyed.values_pairwise.imp fun differ same =>
      differ (congrArg Aeneas.Std.Result.ok same)
  proposerSigned entry member :=
    h.proposerSigned entry (reads.proposerSlashings.subset member)
  attesterChecks entry member :=
    h.attesterChecks entry (reads.attesterSlashings.subset member)
  exitSigned entry member := h.exitSigned entry (reads.voluntaryExits.subset member)

variable {limits : Limits} {view : StateView}

/-- The main theorem, on a history of the pools.

The pools had the invariant on some state, for instance as empty pools. A sequence of moves
then led to the packing state `s` and to the pools that `get_slashings_and_exits` reads. -/
theorem packed_slashings_and_exits_valid_of_history (hfork : IsFulu fork)
    (hroom : OperationsRoom preset config fork (2 ^ 58) limits.proposerSlashings.val
      limits.attesterSlashings.val limits.voluntaryExits.val)
    (hstate : PackingState config checks hfork view s)
    {start : BeaconState preset fork × PoolMaps preset fork}
    (hstart : PoolInv config checks start.1 start.2)
    (hsteps : Relation.ReflTransGen (PoolStep config checks) start (s, maps))
    (hreads : pools.Reads maps) :
    ∃ packed s', getSlashingsAndExits view pools limits = Aeneas.Std.Result.ok packed ∧
      processSlashingsAndExits config checks s packed.toSpec = .ok s' ∧
      packed.WithinLimits limits :=
  packed_slashings_and_exits_valid hfork hroom hstate
    (poolsValid_of_inv (hstart.steps hsteps) hreads)

end

end OperationPool
