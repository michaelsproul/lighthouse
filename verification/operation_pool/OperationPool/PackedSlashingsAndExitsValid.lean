import OperationPool.Pools
import OperationPool.PackedProposerSlashingsValid
import OperationPool.PackedAttesterSlashingsValid
import OperationPool.PackedExitsValid

/-!
# The packed slashings and voluntary exits of a block are valid together

`OperationPool::get_slashings_and_exits` packs the proposer slashings, the attester slashings
and the voluntary exits of one block. `process_operations` applies them in that order, and each
operation changes the state the next one reads.

`packed_slashings_and_exits_valid` is the main theorem of this project:

> `get_slashings_and_exits` does not panic. The spec processes what it returns, in order, from
> the packing state, and no step fails. Each returned list is within its limit.

Both sides are one function:

* `getSlashingsAndExits` (`OperationPool/Pools.lean`) is the model of the Rust function. It makes
  the calls of the Rust body on the extracted `filter_limit_operations`, `maximum_cover` and
  `insert_*` functions.
* `processSlashingsAndExits` (`EthLibOps/Operations.lean`) folds EthLib's
  `processProposerSlashing`, `processAttesterSlashing` and `processVoluntaryExit` over the three
  lists, as `process_operations` does.

`packed_operations_valid` states the same result on EthLib's `processOperations`: a block body
that holds the packed operations and no other operation passes `process_operations`.

The premises are four:

* `hfork`: the fork of the spec state is Fulu.
* `hroom`: `OperationsRoom`, a fact about constants. `operationsRoom_mainnet` and
  `operationsRoom_minimal` prove it for the limits of the two presets, and `OperationsRoom.mono`
  for smaller limits.
* `hstate`: `PackingState`, what the theorem assumes of the packing state.
* `hpools`: `PoolsValid`, what the theorem assumes of the three pools.

The fields of `PackingState` and `PoolsValid` are the trusted base. `README.md` lists them.

Two facts about `s` are not premises, because they follow from `PackingState.reachable`: the
state has one balance for each validator (`balancesMatch_of_reachable`), and the proposer of its
slot is a validator of the registry (EthLib's `Run.getBeaconProposerIndex_ok`).
-/

set_option autoImplicit false

namespace OperationPool

open EthLib EthLib.Consensus
open EthLibOps
open OperationPool.Bridge
open operation_pool (sliceIntoIter pureFn cloneId AttesterCover)

section

variable {preset : Preset} {fork : Base.Fork} {config : Config} {checks : SignatureChecks}

/-- A Rust index is one of the indices an attester slashing walks exactly when both Rust lists
of attesting indices hold it. `indices` holds the two Rust lists of the slashing `asl`. -/
theorem mem_slashingIndices_of_indices {asl : AttesterSlashing preset fork}
    {indices : Aeneas.Std.Slice Aeneas.Std.U64 × Aeneas.Std.Slice Aeneas.Std.U64}
    (h : asl.attestation1.attestingIndices.data.toList = indices.1.val.map u64ToSpec ∧
      asl.attestation2.attestingIndices.data.toList = indices.2.val.map u64ToSpec)
    (u : Aeneas.Std.U64) :
    u64ToSpec u ∈ slashingIndices asl ↔ u ∈ indices.1.val ∧ u ∈ indices.2.val := by
  have hmap : ∀ l : List Aeneas.Std.U64, u64ToSpec u ∈ l.map u64ToSpec ↔ u ∈ l := fun l =>
    ⟨fun hu => by
      obtain ⟨x, hx, hxu⟩ := List.mem_map.mp hu
      exact u64ToSpec_inj hxu ▸ hx,
    fun hu => List.mem_map.mpr ⟨u, hu, rfl⟩⟩
  rw [mem_slashingIndices, h.1, h.2, hmap, hmap]

/-- What the theorem assumes of the packing state `s`, the spec state that corresponds to the
Lighthouse state the op pool packs on. -/
structure PackingState (config : Config) (checks : SignatureChecks) (hfork : IsFulu fork)
    (view : StateView) (s : BeaconState preset fork) : Prop where
  /-- The state Lighthouse reads corresponds to `s`. -/
  corr : StateCorr config hfork.electra view s
  /-- A run of slots and blocks reaches `s` from a genesis state. -/
  reachable : ∃ g, Genesis config checks g ∧ Nonempty (Run config checks g ⟨fork, s⟩)
  /-- The slot and the earliest exit epoch are below `2 ^ 63`. -/
  time : TimeBound hfork.electra s
  /-- The state holds `2 ^ 58` Gwei at most. -/
  supply : SupplyCap (2 ^ 58) s

/-- What the theorem assumes of the three pools, on the packing state `s`. -/
structure PoolsValid (config : Config) (checks : SignatureChecks) (s : BeaconState preset fork)
    (pools : Pools preset fork) : Prop where
  /-- The pool holds one proposer slashing per proposer. -/
  proposersDistinct : pools.proposerSlashings.val.Pairwise fun a b =>
    a.inner.signed_header_1.message.proposer_index ≠
      b.inner.signed_header_1.message.proposer_index
  /-- The pool holds one voluntary exit per validator. -/
  exitsDistinct : pools.voluntaryExits.val.Pairwise fun a b =>
    a.inner.message.validator_index ≠ b.inner.message.validator_index
  /-- A proposer slashing with a signature that is still valid is signed on `s`. -/
  proposerSigned : ∀ entry ∈ pools.proposerSlashings.val, entry.stillValid = true →
    ProposerSlashingSigned config checks s (toSpecProposerSlashing entry.inner)
  /-- An attester slashing with a signature that is still valid passes the checks of
  `process_attester_slashing` that run before the walk, on `s`. -/
  attesterChecks : ∀ entry ∈ pools.attesterSlashings.val, entry.stillValid = true →
    AttesterSlashingChecks config checks s entry.inner.spec
  /-- A voluntary exit with a signature that is still valid is signed on `s`. -/
  exitSigned : ∀ entry ∈ pools.voluntaryExits.val, entry.stillValid = true →
    ExitSigned config checks s (toSpecVoluntaryExit entry.inner)

variable {limits : Limits} {view : StateView} {s : BeaconState preset fork}
  {pools : Pools preset fork}

/-- The proposer slashings, attester slashings and voluntary exits that
`get_slashings_and_exits` packs are valid: the call does not panic, the spec processes what it
returns, in that order, from the packing state `s`, and each returned list is within its
limit. -/
theorem packed_slashings_and_exits_valid (hfork : IsFulu fork)
    (hroom : OperationsRoom preset config fork (2 ^ 58) limits.proposerSlashings.val
      limits.attesterSlashings.val limits.voluntaryExits.val)
    (hstate : PackingState config checks hfork view s)
    (hpools : PoolsValid config checks s pools) :
    ∃ packed s', getSlashingsAndExits view pools limits = Aeneas.Std.Result.ok packed ∧
      processSlashingsAndExits config checks s packed.toSpec = .ok s' ∧
      packed.WithinLimits limits := by
  -- The state facts the three packing theorems take.
  obtain ⟨g, hgenesis, ⟨hrun⟩⟩ := hstate.reachable
  -- The proposer of the slot is a validator of the registry, as `s` is reachable.
  obtain ⟨p, hproposer, hproposerIn⟩ :
      ∃ p, getBeaconProposerIndex s = .ok p ∧ p.toNat < s.validators.data.size :=
    Run.getBeaconProposerIndex_ok hroom.slots hgenesis hrun hfork.fulu
  have hcorr := hstate.corr
  have hchurn := hroom.churn
  have hstateRoom : StateRoom config (2 ^ 58) p s := StateRoom.of_bounds hroom
    (balancesMatch_of_reachable hrun hgenesis) hproposer hproposerIn hstate.time hstate.supply
  have hexits := hroom.exits_of_timeBound hstate.time
  have hrewards := fun b (hb : getBalance s p = .ok b) =>
    hroom.rewards_of_supplyCap hstate.supply hb
  -- `step` and `reward` stand for the two products, so that the bounds below are linear.
  rw [Nat.add_mul, Nat.add_mul, Nat.mul_assoc] at hexits
  simp only [Nat.add_mul, Nat.mul_assoc] at hrewards
  generalize hstep : exitStep preset (2 ^ 58) = step at hexits
  generalize hreward : rewardMax preset fork (2 ^ 58) = reward at hrewards
  -- The proposer slashings. `keepP` is what the filter computes on an entry.
  choose keepP hkeepP hkeepPiff using
    proposerSlashingFilter_spec view (PoolEntry.stillValid (T := RProposerSlashing)) headersOk
      PoolEntry.inner
  obtain ⟨outP, s₁, houtP, hlenP, hrunP, hframeP⟩ := packed_proposer_slashings_valid
    (checks := checks)
    (sliceIntoIter (PoolEntry RProposerSlashing))
    (Aeneas.Std.BuiltinFn (PoolEntry RProposerSlashing) Bool)
    (Aeneas.Std.BuiltinFn (PoolEntry RProposerSlashing) RProposerSlashing)
    (cloneId (PoolEntry RProposerSlashing)) pools.proposerSlashings
    (proposerSlashingFilter view PoolEntry.stillValid headersOk PoolEntry.inner)
    (pureFn PoolEntry.inner) limits.proposerSlashings
    hfork hchurn rfl (operation_pool.yields_slice pools.proposerSlashings) keepP
    (fun op _ => hkeepP op) PoolEntry.inner (fun _ _ => rfl)
    (fun r => r.signed_header_1.message.proposer_index) toSpecProposerSlashing
    (fun _ _ => rfl) hpools.proposersDistinct hcorr
    (fun op hop hk => by
      obtain ⟨hvalid, hheaders, hcore⟩ := (hkeepPiff op).mp hk
      exact ⟨hcore, proposerSlashingWellFormed_of_headersOk hheaders,
        hpools.proposerSigned op hop hvalid⟩)
    hstateRoom (by rw [hstep]; omega)
    (fun b hb => by rw [hreward]; have := hrewards b hb; omega)
  rw [hstep, hreward] at hframeP
  -- The covers. `to_be_slashed` holds the proposers of the packed slashings.
  obtain ⟨covers, hcovers, hcoverLen, hcoverFrom⟩ := attesterCovers_spec view
    (PoolEntry.stillValid (T := PooledAttesterSlashing preset fork)) PoolEntry.inner
    PooledAttesterSlashing.indices (proposerIndexSet outP.val) pools.attesterSlashings.val
  -- The attester slashings.
  obtain ⟨outA, s₂, houtA, hlenA, _, hrunA, hframeA⟩ := packed_attester_slashings_valid
    (checks := checks)
    (listIntoIter (AttesterCover (PooledAttesterSlashing preset fork)))
    (AttesterCover.maxCover (PooledAttesterSlashing preset fork)) covers
    limits.attesterSlashings attesterSlashingsLabel
    hfork hchurn rfl (yields_list covers)
    (Nat.le_trans hcoverLen pools.attesterSlashings.property)
    (AttesterCover.model (PooledAttesterSlashing preset fork)) (fun slashing => slashing.spec)
    hcorr (fun u => u ∈ (proposerIndexSet outP.val).toList)
    (fun a ha u => by
      obtain ⟨op, _, _, hslashing, hcov⟩ := hcoverFrom a ha
      show a.covers u ↔ u64ToSpec u ∈ slashingIndices a.slashing.spec ∧ _
      rw [hcov u, hslashing, mem_slashingIndices_of_indices (indices := op.inner.indices)
        ⟨op.inner.indices1, op.inner.indices2⟩])
    (fun a ha => by
      obtain ⟨op, hop, hvalid, hslashing, _⟩ := hcoverFrom a ha
      show AttesterSlashingChecks config checks s a.slashing.spec
      rw [hslashing]
      exact hpools.attesterChecks op hop hvalid)
    hframeP
    (fun u hS => by
      obtain ⟨ps, hps, hidx⟩ := hS
      obtain ⟨r, hr, rfl⟩ := List.mem_map.mp hps
      exact mem_proposerIndexSet.mpr ⟨r, hr, u64ToSpec_inj hidx⟩)
    hstateRoom (by rw [hstep]; omega)
    (fun b hb => by rw [hreward]; have := hrewards b hb; omega)
  rw [hstep] at hframeA
  -- The exclusion set, and the exits. `keepE` is what the filter computes on an entry.
  obtain ⟨excl, hexcl⟩ := operation_pool.exclusionSet_ok
    (outA.val.foldl (fun set cover => extendIndexSet set (cover.balances.map Prod.fst))
      (proposerIndexSet outP.val))
    (Aeneas.Std.alloc.vec.Vec.deref outP) (outA.val.map fun cover => cover.slashing.indices)
  choose keepE hkeepE hkeepEiff using
    voluntaryExitFilter_spec view excl (PoolEntry.stillValid (T := RSignedVoluntaryExit))
      fun entry => entry.inner.message
  obtain ⟨out, s₃, hout, hlen, hrunE⟩ := packed_exits_valid_after_slashings (checks := checks)
    (sliceIntoIter (PoolEntry RSignedVoluntaryExit))
    (Aeneas.Std.BuiltinFn (PoolEntry RSignedVoluntaryExit) Bool)
    (Aeneas.Std.BuiltinFn (PoolEntry RSignedVoluntaryExit) RSignedVoluntaryExit)
    (cloneId (PoolEntry RSignedVoluntaryExit)) pools.voluntaryExits
    (voluntaryExitFilter view excl PoolEntry.stillValid fun entry => entry.inner.message)
    (pureFn PoolEntry.inner) limits.voluntaryExits hfork hchurn rfl
    (operation_pool.yields_slice pools.voluntaryExits) keepE (fun op _ => hkeepE op)
    PoolEntry.inner (fun _ _ => rfl) (fun exit => exit.message) toSpecVoluntaryExit
    (fun _ _ => ⟨rfl, rfl⟩) hpools.exitsDistinct hcorr hexcl
    (outP.val.map toSpecProposerSlashing) (outA.val.map fun cover => cover.slashing.spec)
    (fun ps hps => by
      -- Each spec slashing is the spec form of a packed Rust slashing, with the same proposer.
      obtain ⟨r, hr, rfl⟩ := List.mem_map.mp hps
      exact ⟨r, by simpa [Aeneas.Std.alloc.vec.Vec.deref] using hr, rfl⟩)
    (fun asl hasl => by
      obtain ⟨cover, hcover, rfl⟩ := List.mem_map.mp hasl
      exact ⟨cover.slashing.indices, List.mem_map.mpr ⟨cover, hcover, rfl⟩,
        cover.slashing.indices1, cover.slashing.indices2⟩)
    hframeA
    (fun op hop hk => by
      obtain ⟨hnot, hvalid, hcores⟩ := (hkeepEiff op).mp hk
      exact ⟨hnot, hcores, hpools.exitSigned op hop hvalid⟩)
    hstateRoom.toQueueRoom (by rw [hstep]; omega)
  -- The five calls are the model, and the three runs are the composite.
  rw [List.foldlM_toArray] at hrunP hrunA hrunE
  have hrunA' : (outA.val.map fun cover => cover.slashing.spec).foldlM
      (processAttesterSlashing config checks) s₁ = .ok s₂ := hrunA
  refine ⟨⟨outP, outA.val.map (·.slashing), out⟩, s₃, ?_, ?_, hlenP, by simpa using hlenA, hlen⟩
  · simp only [getSlashingsAndExits, houtP, hcovers, houtA, hexcl, hout, Aeneas.Std.bind_ok]
  · simp only [processSlashingsAndExits, Packed.toSpec, List.map_map, Function.comp_def, hrunP,
      hrunA', hrunE, Base.ok_bind]

/-- The same result on EthLib's `processOperations`: a block body that holds the packed slashings
and exits, and no other operation, passes `process_operations` on the packing state `s`. -/
theorem packed_operations_valid (hfork : IsFulu fork)
    (hroom : OperationsRoom preset config fork (2 ^ 58) limits.proposerSlashings.val
      limits.attesterSlashings.val limits.voluntaryExits.val)
    (hstate : PackingState config checks hfork view s)
    (hpools : PoolsValid config checks s pools) :
    ∃ packed, getSlashingsAndExits view pools limits = Aeneas.Std.Result.ok packed ∧
      packed.WithinLimits limits ∧
      ∀ (body : BeaconBlockBody preset fork) (parentSlot : Slot),
        HoldsOnly body packed.toSpec →
          ∃ s', processOperations config checks s body parentSlot = .ok s' := by
  obtain ⟨packed, s', hpacked, hrun, hwithin⟩ :=
    packed_slashings_and_exits_valid hfork hroom hstate hpools
  refine ⟨packed, hpacked, hwithin, fun body parentSlot holds => ⟨s', ?_⟩⟩
  rw [processOperations_of_holdsOnly hfork holds]
  exact hrun

end

end OperationPool
