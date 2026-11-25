# Slashing and voluntary-exit packing: proved valid against the consensus spec

Machine-checked proofs, in Lean 4, that the proposer slashings, the attester slashings and the
voluntary exits packed by `OperationPool::get_slashings_and_exits` are valid for inclusion in the
block. The consensus spec processes, in order and without an error:

- the packed proposer slashings, from the state they were packed on;
- then the packed attester slashings;
- then the packed voluntary exits.

The Rust is translated to Lean by [Charon](https://github.com/AeneasVerif/charon) and
[Aeneas](https://github.com/AeneasVerif/aeneas). The spec is
[EthLib](https://github.com/ethereum/EthLib)'s executable Lean model of the consensus
specification, at the Fulu fork. One Lean project depends on both.

## What is proved

Each theorem is a statement about the real `filter_limit_operations` and `maximum_cover`, and
about the spec's `process_proposer_slashing`, `process_attester_slashing` and
`process_voluntary_exit` (EthLib's `processProposerSlashing`, `processAttesterSlashing` and
`processVoluntaryExit`). Let `s` be the spec state that corresponds to the Lighthouse state the
op pool packs on.

A run of the spec is a fold: `ops.toArray.foldlM (processX config checks) s = .ok s'`. That is
the fold EthLib's `processOperations` makes over each list of a block body before Gloas.

**The main theorem.** `packed_slashings_and_exits_valid`
(`OperationPool/PackedSlashingsAndExitsValid.lean`):

```lean
theorem packed_slashings_and_exits_valid (hfork : IsFulu fork)
    (hroom : OperationsRoom preset config fork (2 ^ 58) limits.proposerSlashings.val
      limits.attesterSlashings.val limits.voluntaryExits.val)
    (hstate : PackingState config checks hfork view s)
    (hpools : PoolsValid config checks s pools) :
    ∃ packed s', getSlashingsAndExits view pools limits = ok packed ∧
      processSlashingsAndExits config checks s packed.toSpec = .ok s' ∧
      packed.WithinLimits limits
```

> `get_slashings_and_exits` does not panic. The spec processes what it returns, in order, from
> the packing state `s`, and no step fails. Each returned list is within its limit.

Each side is one function:

- `getSlashingsAndExits` (`OperationPool/Pools.lean`) is the model of the Rust function. Its body
  makes the calls of the Rust body, in order, on the extracted functions:
  `filter_limit_operations` for the proposer slashings, `AttesterSlashingMaxCover::new` and
  `maximum_cover` for the attester slashings, the two `insert_*` functions for the exclusion set,
  and `filter_limit_operations` for the exits.
- `processSlashingsAndExits` (`EthLibOps/Operations.lean`) folds `processProposerSlashing`, then
  `processAttesterSlashing`, then `processVoluntaryExit` over the three lists.

`packed_operations_valid` states the result on EthLib's `processOperations`: a block body that
holds the packed slashings and exits and no other operation (`HoldsOnly`) passes
`process_operations` on `s`. `processOperations_of_holdsOnly` is the link: on such a body, at
Fulu, `processOperations` is `processSlashingsAndExits`.

No premise of the theorem is a run of the spec. Every premise about the state is about the
packing state `s`.

The model runs the extracted functions on these instances:

- A pool is a slice of `PoolEntry`, walked by Aeneas's slice iterator
  (`OperationPool/Concrete.lean`). A `PoolEntry` is `SigVerifiedOp`: the operation, and
  `stillValid`, which is `signature_is_still_valid` on the fork of the packing state.
- A filter closure is a model that runs the extracted cores (`OperationPool/Closures.lean`).
  `proposerSlashingFilter` calls the core of `verify_proposer_slashing`. `voluntaryExitFilter`
  calls the two cores of `verify_exit`.
- A cover is `AttesterCover`, a model of `AttesterSlashingMaxCover`. The covers are the ones
  that `attesterCovers` builds: the model of `AttesterSlashingMaxCover::new`, which calls the
  extracted `Validator::is_slashable_at`, on each pool entry.

The spec's form of a packed operation is defined, not assumed (`OperationPool/Bridge/ToSpec.lean`):
`toSpecProposerSlashing` and `toSpecVoluntaryExit` convert field by field. `headersOk` is the
three checks of `verify_proposer_slashing` on the two headers, and
`proposerSlashingWellFormed_of_headersOk` proves that they are the spec's. An attester slashing of
the pool (`PooledAttesterSlashing`) holds its two lists of attesting indices and its spec form,
because Rust's `AttesterSlashing<E>` is not extracted.

The three theorems below are generic over the iterator, the closures and the `MaxCover`
implementation. The main theorem joins them.

**Proposer slashings.** `packed_proposer_slashings_valid`
(`OperationPool/PackedProposerSlashingsValid.lean`):

> The spec's `process_proposer_slashing` succeeds on each packed slashing in order, starting
> from `s`.

**Attester slashings.** `packed_attester_slashings_valid`
(`OperationPool/PackedAttesterSlashingsValid.lean`):

> The spec's `process_attester_slashing` succeeds on each packed slashing in order, starting
> from the state after the packed proposer slashings.

The spec rejects an attester slashing that slashes no validator. A slashing packed earlier in
the block can slash every validator that a later one names. `maximum_cover_spec`
(`OperationPool/MaxCover.lean`) is the fact about the greedy algorithm that rules this out:

> `maximum_cover` does not panic and returns at most `limit` items. Each returned item comes
> from an item the iterator yielded, and that item covers an element that no item picked before
> it covered.

`maximum_cover` is generic, and so is the theorem. It holds for attestation packing too.

**Exits.** `packed_exits_valid_after_slashings` (`OperationPool/PackedExitsValid.lean`):

> The spec's `process_voluntary_exit` succeeds on each packed exit in order, after the packed
> slashings of the block.

### The premises

| Premise | Meaning | Status |
|---|---|---|
| `hfork` | `IsFulu fork`: the fork of `s` is Fulu, or one of the forks that change the blob parameters alone | Assumed |
| `hroom` | `OperationsRoom`: on a state within the two bounds below, a block with as many slashings and exits as the three limits allow has room | Proved for the limits of mainnet and minimal at Fulu (`operationsRoom_mainnet`, `operationsRoom_minimal`), and for smaller limits by `OperationsRoom.mono` |
| `hstate` | `PackingState`: the four facts about `s` below | |
| `hpools` | `PoolsValid`: the five facts about the pools, listed under "What is trusted" | Proved from a history of the pools (`poolsValid_of_inv`), see "The invariant of the pools" |

The fields of `PackingState`:

| Field | Meaning | Status |
|---|---|---|
| `corr` | The Lighthouse state corresponds to `s` | Trusted, see below |
| `reachable` | A run of slots and blocks (EthLib's `Run`) reaches `s` from a genesis state (`Genesis`). The proof uses two consequences: `balances` has the registry's length, and the proposer of the slot is a validator of the registry | Assumed. EthLib's `reachable_balancesMatch` and `Run.getBeaconProposerIndex_ok` give the consequences |
| `time` | `TimeBound s`: the slot and the earliest exit epoch are below `2^63` | Assumed |
| `supply` | `SupplyCap (2^58) s`: the balances add up to `2^58` Gwei at most, and so do the effective balances. The slashed balance recorded for the current epoch is `2^58` Gwei at most | Assumed |

`TimeBound`, `SupplyCap` and `OperationsRoom` are defined in `EthLibOps/Bounds.lean`. They
replace the room formulas of the generic theorems, which cover the arithmetic of
`slash_validator` and `initiate_validator_exit` that packing never runs.

- The time bound has two clocks, because the slot does not bound the exit queue: each exit
  moves `earliest_exit_epoch` ahead of the current epoch.
- The supply cap bounds three sums. The proofs use these consequences: no balance and no
  effective balance exceeds the cap, and the slashed balance of the epoch has room for every
  effective balance that is not yet slashed. `2^58` Gwei is about 288 million ETH.
- Neither bound is an invariant of the state transition, since a processed slot moves the slot
  and a deposit adds Gwei. They are therefore separate from what every reachable state has.
- `OperationsRoom` reads constants alone. It counts one exit and one whistleblower reward for
  each proposer slashing and for each validator an attester slashing can name
  (`MAX_VALIDATORS_PER_COMMITTEE * MAX_COMMITTEES_PER_SLOT`, `131072` on mainnet), and one exit
  for each voluntary exit. `hroom` states it at the three limits the Rust passes. One of its
  fields is not about room: an epoch has at least one slot. EthLib's
  `Run.getBeaconProposerIndex_ok` needs it.
- EthLib anchors reachability at a Phase 0 genesis state, and a run crosses the fork upgrades.
  `Run.invariant` holds from any first state with EthLib's `Invariant`.
  `Run.getBeaconProposerIndex_ok` is stated from a genesis state alone, so a statement from
  another anchor state needs that theorem from the anchor as well.

### The invariant of the pools

`PoolsValid` is a premise of the main theorem. `OperationPool/PoolsInvariant.lean` proves it
from a history of the pools, and states the main theorem on that history:

```lean
theorem packed_slashings_and_exits_valid_of_history (hfork : IsFulu fork)
    (hroom : OperationsRoom preset config fork (2 ^ 58) limits.proposerSlashings.val
      limits.attesterSlashings.val limits.voluntaryExits.val)
    (hstate : PackingState config checks hfork view s)
    (hstart : PoolInv config checks start.1 start.2)
    (hsteps : Relation.ReflTransGen (PoolStep config checks) start (s, maps))
    (hreads : pools.Reads maps) :
    ∃ packed s', getSlashingsAndExits view pools limits = ok packed ∧
      processSlashingsAndExits config checks s packed.toSpec = .ok s' ∧
      packed.WithinLimits limits
```

> The pools had the invariant on some state, for instance as empty pools. Moves of the pools
> and of the state then led to the packing state `s` and to the pools that
> `get_slashings_and_exits` reads. Then the conclusion of the main theorem holds.

- `PoolMaps` is the three pools as the op pool holds them: two `ValidatorMap`s, for the proposer
  slashings and the voluntary exits, and the entries of the `HashSet` of attester slashings.
- `PoolInv config checks s maps` is the invariant on the state `s`. Each map is keyed: it has one
  entry per key, and each operation is stored under the index of its validator. Each entry whose
  signature is still valid passes the spec's signature checks on `s`. Empty pools have the
  invariant (`PoolInv.empty`).
- `PoolStep` is one move. `PoolInv.step` proves that each move keeps the invariant.
- `Pools.Reads pools maps` says that each slice `get_slashings_and_exits` iterates is a
  permutation of the entries of its pool. `poolsValid_of_inv` gives `PoolsValid` from the
  invariant, for any such pools.

| Move | What it is | What it assumes |
|---|---|---|
| `advance` | The state moves to a state that extends it (`Extends`) | Nothing for a run of slots and blocks within the fork (`PoolStep.advance_of_run`) |
| `insertProposerSlashing`, `insertVoluntaryExit` | The extracted `ValidatorMap::insert` | The operation passes the spec's signature checks on the current state |
| `retainProposerSlashings`, `retainVoluntaryExits` | `ValidatorMap::retain`, with any closure | The model of `retain` |
| `insertAttesterSlashing` | `HashSet::insert`, on a list | The slashing passes `AttesterSlashingChecks` on the current state |
| `pruneAttesterSlashings` | `HashSet::retain`, on a list | Nothing |

`Extends s s'` (`EthLibOps/Stability.lean`) names what the signature checks read of a state. The
public keys of `s` are a prefix of the public keys of `s'`, and the two states have the same fork
versions and the same genesis validators root. A check that passes on `s` then passes on `s'`
(`ProposerSlashingSigned.of_extends`, `ExitSigned.of_extends`,
`AttesterSlashingChecks.of_extends`). `extends_of_run` proves `Extends` for a run of slots and
blocks between two states of one fork, with EthLib's theorems that a step never overwrites a
public key or the genesis validators root.

Limits of this result:

- **One fork.** The moves are between states of one fork. An entry's `stillValid` flag is fixed,
  which is right while `state.fork()` does not change. Entries that a pool carries over the Fulu
  upgrade enter through `hstart`.
- **One direction.** `Extends` needs the registry of the later state to hold the registry of the
  earlier one. A reorg to a state with a shorter registry is not an `advance`. An attester
  slashing can name a validator that is missing there, and Lighthouse does not check that again
  at packing.
- **The insert premise.** That an inserted operation passes the checks on the current state is
  assumed. In Lighthouse `SigVerifiedOp::validate` makes the checks, with BLS, before an
  operation enters a pool.
- **The attester slashings.** Their pool is a `HashSet` with no key logic, so its two writers are
  modelled on a list and not extracted.

### Supporting theorems

| Theorem | File | Statement |
|---|---|---|
| `filter_limit_operations_eq` | `OperationPool/FilterLimit.lean` | The packed list is `((pool.filter keep).take limit).map f`; no panic |
| `maximum_cover_spec` | `OperationPool/MaxCover.lean` | Each item `maximum_cover` returns covers an element that no earlier pick covered; no panic |
| `verify_proposer_slashing_validator_checks_ok_iff` | `StateProcessing/VerifyProposerSlashing.lean` | Returns `Ok` exactly when the proposer exists and its record passes `SlashableChecks`; no panic |
| `verify_exit_validator_checks_ok_iff` | `StateProcessing/VerifyExit.lean` | Returns `Ok` exactly when the validator exists and its record passes `ExitChecks`; no panic |
| `verify_exit_pending_balance_ok_iff` | `StateProcessing/VerifyExit.lean` | Returns `Ok` exactly when the pending balance, if computed, is zero; no panic |
| `exclusionSet_covers` | `OperationPool/ExclusionSet.lean` | The exclusion set holds each packed proposer slashing's proposer and each index common to the two attestations of a packed attester slashing |
| `proposerSlashingGates_of_coreOk` | `OperationPool/Bridge/StateCorr.lean` | A proposer the `verify_proposer_slashing` core accepts passes the spec's `ProposerSlashingGates` |
| `exitGates_of_coresOk` | `OperationPool/Bridge/StateCorr.lean` | An exit both `verify_exit` cores accept passes the spec's `ExitGates` |
| `proposerSlashingFilter_spec` | `OperationPool/Closures.lean` | The model of the proposer-slashing filter accepts an entry exactly when the signature is still valid, the headers pass, and the core accepts the proposer; no panic |
| `voluntaryExitFilter_spec` | `OperationPool/Closures.lean` | The model of the exit filter accepts an entry exactly when the validator is outside the exclusion set, the signature is still valid, and both cores accept the exit; no panic |
| `attesterCovers_spec` | `OperationPool/Closures.lean` | Each cover the model of `AttesterSlashingMaxCover::new` builds covers exactly the validators that both attestations name, that `Validator::is_slashable_at` accepts, and that `to_be_slashed` does not hold; no panic |
| `yields_slice` | `OperationPool/Concrete.lean` | Aeneas's slice iterator yields the entries of the slice, in order |
| `AttesterCover.model` | `OperationPool/Concrete.lean` | The model of `AttesterSlashingMaxCover` satisfies `MaxCoverModel` |
| `exclusionSet_ok` | `OperationPool/ExclusionSet.lean` | The two `insert_*` functions do not panic |
| `isSlashableValidator_of_slashableOn` | `OperationPool/Bridge/StateCorr.lean` | A validator `Validator::is_slashable_at` accepts is slashable by the spec's `is_slashable_validator` |
| `packed_operations_valid` | `OperationPool/PackedSlashingsAndExitsValid.lean` | A block body that holds the packed slashings and exits and no other operation passes `processOperations` |
| `proposerSlashingWellFormed_of_headersOk` | `OperationPool/Bridge/ToSpec.lean` | A proposer slashing whose headers pass the three checks of `verify_proposer_slashing` is well formed for the spec |
| `yields_list` | `OperationPool/Pools.lean` | The list iterator yields the entries of the list, in order |
| `packed_exits_valid` | `OperationPool/PackedExitsValid.lean` | The packed exits succeed from any state with the slash frame of the packing state |
| `packed_exits_valid_direct` | `OperationPool/PackedExitsValid.lean` | The packed exits succeed from the packing state itself |
| `keyed_new`, `Keyed.insert`, `Keyed.retain` | `OperationPool/ValidatorMap.lean` | The map `ValidatorMap::new` returns is keyed, and `insert` and `retain` keep a map keyed |
| `insert_ok` | `OperationPool/ValidatorMap.lean` | `ValidatorMap::insert` does not panic |
| `Keyed.values_pairwise` | `OperationPool/ValidatorMap.lean` | The operations of a keyed map are about pairwise distinct validators |
| `mem_values_of_insert`, `mem_values_of_retain` | `OperationPool/ValidatorMap.lean` | The operations after `insert` are the new one and operations the map had. The operations after `retain` are operations the map had |
| `PoolInv.empty`, `PoolInv.step`, `PoolInv.steps` | `OperationPool/PoolsInvariant.lean` | Empty pools have the invariant, and each move keeps it |
| `poolsValid_of_inv` | `OperationPool/PoolsInvariant.lean` | Pools that enumerate maps with the invariant are valid |

The three generic theorems state the room with one bound `B` on every effective balance:
`exitStep preset B` epochs and `rewardMax preset fork B` Gwei for each validator that exits or is
slashed.

The spec-side theorems are in `EthLibOps/`, a library of this project that imports EthLib alone.
EthLib has the definitions and many lemmas about them, and no theorem that a slashing or an exit
succeeds from its checks. `EthLibOps` proves those.

| Theorem | File | Statement |
|---|---|---|
| `churnQueue_ok`, `initiateValidatorExit_ok_of_room` | `ExitQueue.lean` | `initiate_validator_exit` cannot fail for a validator of the registry when the exit queue has room, and it moves the queue by `exitStep` of the validator's effective balance at most |
| `SlashFrame`, `SlashFrame.trans` | `SlashFrame.lean` | What slashings and exits keep between two states, and the bounds on what they move. Frames compose |
| `slashValidator_ok_frame` | `SlashValidator.lean` | `slash_validator` cannot fail for a validator that is not yet slashed, given the list lengths, the proposer and the rooms. The state after it has the slash frame of the state before it |
| `StateRoom`, `slashValidator_ok_of_frame` | `Room.lean` | The same, on a state that has the slash frame of the state the room is read on |
| `processProposerSlashing_ok_of_frame` | `ProposerSlashings.lean` | A slashing that is well formed, passes `ProposerSlashingGates` and `ProposerSlashingSigned` cannot fail |
| `proposerSlashings_foldlM_ok` | `ProposerSlashings.lean` | Slashings that name pairwise distinct proposers and pass the checks on a state succeed in order |
| `slashCandidates_ok` | `AttesterSlashings.lean` | The walk of `process_attester_slashing` cannot fail, and slashes a validator when one it walks is still slashable |
| `processAttesterSlashing_ok_of_frame` | `AttesterSlashings.lean` | A slashing that passes `AttesterSlashingChecks` and walks one slashable validator cannot fail, given room for every walked validator |
| `attesterSlashings_foldlM_ok` | `AttesterSlashings.lean` | Slashings succeed in order after a run of slashings, when each one walks a validator that is slashable on the start state and that no earlier slashing can slash |
| `mem_slashingIndices` | `AttesterSlashings.lean` | The validators a slashing walks are the validators both attestations name |
| `processVoluntaryExit_ok_of_frame` | `VoluntaryExits.lean` | An exit that passes `ExitGates` and `ExitSigned` cannot fail when the exit queue has room |
| `voluntaryExits_foldlM_ok` | `VoluntaryExits.lean` | Exits that name pairwise distinct validators and pass the gates succeed in order, from any state with the slash frame of the state they pass the gates on |
| `StateRoom.of_bounds`, `OperationsRoom.exits_of_timeBound`, `OperationsRoom.rewards_of_supplyCap` | `Bounds.lean` | Within `TimeBound` and `SupplyCap`, the state has the room a slashing reads, and the exit queue and each balance have room for the operations of a block |
| `processOperations_of_holdsOnly` | `Operations.lean` | At Fulu, `processOperations` on a block body that holds the slashings and exits `ops` and no other operation is `processSlashingsAndExits` on `ops` |
| `operationsRoom_mainnet`, `operationsRoom_minimal` | `Bounds.lean` | `OperationsRoom` holds at Fulu for the block limits of each preset, at a cap of `2^58` Gwei |
| `balancesMatch_of_reachable` | `Bounds.lean` | A state that a run reaches from a genesis state has one balance for each validator. This is EthLib's `reachable_balancesMatch` |
| `extends_of_run` | `Stability.lean` | A run of slots and blocks within a fork reaches a state that extends its first state, for a first state that a run from genesis reaches |
| `ProposerSlashingSigned.of_extends`, `ExitSigned.of_extends`, `AttesterSlashingChecks.of_extends` | `Stability.lean` | A signature check that passes on a state passes on a state that extends it |

The proofs use these theorems of EthLib, among others: `slashValidator_total_of_room`,
`slashValidator_ok`, `balance_slashValidator`, `isValidIndexedAttestation_of_keys`,
`isValidIndexedAttestation_lt_size`, `mem_sortedSet`, `getTotalActiveBalance_exists_ok` and
`reachable_balancesMatch`, `Run.getBeaconProposerIndex_ok`,
`validatorKeys_prefix_and_distinct_of_chainSteps`, `Run.genesisValidatorsRoot`,
`forkVersions_eq_of_fork` and `isValidIndexedAttestation_of_keys`.

No proof uses `sorry`. `lake build Audit` prints the axioms of each theorem: Lean's `propext`,
`Classical.choice` and `Quot.sound`, plus the opaque external declarations listed under "What is
trusted". The `EthLibOps` theorems use the three standard axioms only.

## Why the property holds

Packing checks each operation against the unmodified state. Block processing applies the
operations in order, each one writing the state the next one reads.

For the proposer slashings:

1. Each packed slashing passed `verify_proposer_slashing` on the packing state
   (`filter_limit_operations_eq`, `proposerSlashingFilter_spec`,
   `verify_proposer_slashing_validator_checks_ok_iff`, `proposerSlashingGates_of_coreOk`).
2. The packed slashings name pairwise distinct proposers, and a slashing of one validator leaves
   the checks of the others alone (`SlashFrame`, `proposerSlashings_foldlM_ok`).
3. `slash_validator`, which packing never runs, cannot fail when the exit queue and the
   proposer's balance have room (`slashValidator_ok_frame`).

For the attester slashings, after the proposer slashings:

1. Each `AttesterSlashingMaxCover` covers the validators its slashing can slash on the packing
   state: the validators both attestations name, that are slashable, and that no packed
   proposer slashing names (`attesterCovers_spec`).
2. `maximum_cover` picks only items with a non-zero score, and after each pick it removes what
   the pick covers from every other item that can still be picked. So each picked slashing can
   slash a validator that no slashing picked before it can slash (`maximum_cover_spec`).
3. A slashing changes the record of the validators it slashes and of no other validator
   (`SlashFrame`). That validator is therefore still slashable when its slashing runs, and the
   spec's `slashed_any` assertion holds (`slashCandidates_ok`,
   `attesterSlashings_foldlM_ok`).
4. The checks before the walk read the registry's length, the public keys and the fork. No
   slashing changes them (`SlashFrame.isValidIndexedAttestation`).
5. Each `slash_validator` call of the walk cannot fail when the exit queue and the proposer's
   balance have room (`slashValidator_ok_of_frame`).

For the exits, after the slashings:

1. Each packed exit passed `verify_exit` on the packing state (`filter_limit_operations_eq`,
   `voluntaryExitFilter_spec`, `verify_exit_*_ok_iff`, `exitGates_of_coresOk`).
2. The packed exits name pairwise distinct validators, and an exit of one validator leaves the
   checks of the others alone (`voluntaryExits_foldlM_ok`).
3. No packed exit names a validator the packed slashings can slash (`exclusionSet_covers`), and
   a slashing changes nothing else an exit reads (`SlashFrame`).
4. `initiate_validator_exit`, which packing never runs, cannot fail when the exit queue has
   room (`processVoluntaryExit_ok_of_frame`).

## The Rust that is extracted

Nine functions, each free of `EthSpec`, that Aeneas translates:

| Function | Crate | Role |
|---|---|---|
| `filter_limit_operations` | `operation_pool` | The packing loop, generic over the iterator and the two closures. It packs the proposer slashings and the exits |
| `maximum_cover` | `operation_pool` | The greedy max cover, generic over `T: MaxCover`. It packs the attester slashings and the attestations. Its helpers `best_item_index` and `update_covering_sets` are extracted with it |
| `insert_proposer_slashing_indices` | `operation_pool` | Adds the packed proposer slashings' proposers to the exclusion set |
| `insert_common_attesting_indices` | `operation_pool` | Adds the indices common to an attester slashing's two attestations |
| `verify_proposer_slashing_validator_checks` | `state_processing` | The check of `verify_proposer_slashing` on the proposer's record |
| `verify_exit_validator_checks` | `state_processing` | The checks of `verify_exit` on the validator record |
| `verify_exit_pending_balance` | `state_processing` | The pending-withdrawal check of `verify_exit` |
| `ValidatorMap::new` | `operation_pool` | The empty map of operations keyed by validator index |
| `ValidatorMap::insert` | `operation_pool` | Reads the validator index from the operation and stores the operation under it. It writes the pools of proposer slashings and of voluntary exits |

`Validator::is_slashable_at` and `Validator::is_active_at` are extracted with the cores that
call them. The model of `AttesterSlashingMaxCover::new` calls the first one too.

`maximum_cover` was a chain of iterator adaptors (`filter`, `max_by_key`, `for_each`), for which
the Aeneas library has no model. It is now three written-out loops with the same behaviour: the
same items are kept, the last item with the best score wins a tie as with `max_by_key`, and the
same items are updated. Three changes come with it. The trait `MaxCover` lost the `Clone` bounds
on its associated types: no code used them, and Aeneas gave the four `Clone` clauses of the
trait one field name, which Lean rejects. The gauge update moved to
`metrics::set_max_cover_non_zero_items`, which stays opaque. The selection pass computes each
score once, where `filter` and `max_by_key` computed it twice.

The proposer-slashing filter of `get_slashings_and_exits` calls
`verify_proposer_slashing(.., VerifySignatures::False, ..)`, as the exit filter calls
`verify_exit`. It called `Validator::is_slashable_at` directly before. The result is the same
for a pool entry, and the extracted core is now on the packing path.

`get_slashings_and_exits` already held the validators to be slashed in `to_be_slashed`. The two
`insert_*` functions add the same validators again from the packed slashings alone, so the proof
of the exits does not depend on the max cover. They add the whole intersection of the two
attestations, where the max cover adds its slashable members. A validator in the intersection
that is not slashable is slashed already, not yet active, or withdrawable, and has no valid
exit, so the packed exits are the same on any reachable state.

The pools of proposer slashings and of voluntary exits were `HashMap<u64, SigVerifiedOp<..>>`
fields of `OperationPool`, and each caller computed the key. They are now `ValidatorMap`s
(`beacon_node/operation_pool/src/validator_map.rs`). The map is a private field, so `insert` and
`retain` are the only functions that write it, and `insert` computes the key through the trait
`ValidatorIndexed`. `FromIterator`, which the persistence code uses, is a loop of `insert`. The
`HashMap` calls are the ones the code made before.

`ValidatorMap::retain` is not extracted. Its body passes a closure to `HashMap::retain`, whose
closure takes `&mut V`, and Aeneas emits Lean for that closure that does not type-check.
`extract.sh` keeps the method opaque, and `OperationPool/FunsExternal.lean` models it.

## What is trusted

Not proved, and to be checked by inspection:

| Item | Where it enters |
|---|---|
| Charon, Aeneas and the Lean kernel | Translation and checking |
| The external models: `HashSet` as a list of distinct values (`with_capacity`, `insert`, `contains`), `HashMap` as a list of entries with one entry per key (`new`, `insert`), `ValidatorMap::retain` as the entries its closure accepts, `metrics::set_max_cover_non_zero_items` as a call that returns, and the opaque external types and hashing functions | `OperationPool/TypesExternal.lean`, `OperationPool/FunsExternal.lean`, `StateProcessing/TypesExternal.lean`, `ExternalTypes.lean` |
| The models of `OperationPool/Concrete.lean` stand for the Rust values. A pool's iterator yields each pool entry once, as the slice iterator does on a slice of the entries. A mapping closure clones the operation of the entry and does not panic | `getSlashingsAndExits`: `sliceIntoIter`, `pureFn`, `cloneId` |
| The models of `OperationPool/Closures.lean` compute what the Rust closures compute. Each definition quotes the Rust it models. The two filters are `proposerSlashingFilter` and `voluntaryExitFilter`. `attesterCovers` is what the `flat_map` iterator of `get_attester_slashings` yields, and `attesterCoverNew` is `AttesterSlashingMaxCover::new` with `get_slashable_indices_modular` written out. `proposerIndexSet` is the set `to_be_slashed` that `new` reads | The form of the main theorem |
| The reads of the models are the reads of the Rust. `verify_proposer_slashing<E>`, `verify_exit<E>`, `get_slashable_indices_modular` and `get_effective_balance` read the validator at an index with `state.validators().get(index)`, and the models read `StateView.validatorAt`. The epoch is `state.current_epoch()`, the two constants come from the chain spec, and the pending balance is `get_pending_balance_to_withdraw(index).ok()` | `StateView` |
| `AttesterCover` stands for `AttesterSlashingMaxCover`: the slashing, and `effective_balances` as a list of key and value. Its methods are those of the Rust: `score` is the sum of the values, which wraps as in a release build; `update_covering_set` is `retain` on the keys the picked cover does not have; `clone` returns its argument | `AttesterCover`, `AttesterCover.maxCover` |
| `getSlashingsAndExits` makes the calls of the Rust body, with the same arguments. `to_be_slashed` holds the proposers of the packed proposer slashings when the covers are built. `get_attester_slashings` extends it with the keys of each picked cover (`extendIndexSet`, a model of `HashSet::extend`) and returns `convert_to_object(cover.intermediate())` for each picked cover. The exclusion set is built from the packed slashings, and the exit filter reads that set. The `flat_map` iterator of the covers is a list iterator (`listIntoIter`), and the label of the metric is an uninterpreted constant | `getSlashingsAndExits` (`OperationPool/Pools.lean`) |
| The three limits are the preset's: `E::MaxProposerSlashings`, `E::max_attester_slashings_electra()` and `E::MaxVoluntaryExits` | `limits`, `hroom` |
| An attester slashing passes the spec's checks before the walk on the state it is inserted on: the two attestations are slashable, and each is a valid indexed attestation (indices not empty, sorted, in the registry, aggregate signature valid). `SigVerifiedOp` is built only after `verify_attester_slashing`. That the checks pass on later states is proved (`AttesterSlashingChecks.of_extends`), for states whose registry extends the earlier one. Lighthouse does not check again at packing that every attesting index is in the registry of the packing state: `new` looks up the indices that both attestations name, and no other | The premise `passes` of `PoolStep.insertAttesterSlashing` |
| The pool of attester slashings is a `HashSet`. Its writers are `insert_attester_slashing` (`HashSet::insert`) and `prune_attester_slashings` (`HashSet::retain`), modelled on the list of its entries | `PoolStep.insertAttesterSlashing`, `PoolStep.pruneAttesterSlashings` |
| A `PoolEntry` stands for `SigVerifiedOp<T, E>`: `inner` is `as_inner()`, and `stillValid` is `signature_is_still_valid(&state.fork())` on the packing state. The flag is a field of the entry, so a history of the pools keeps it fixed, which is right within a fork. `RSignedVoluntaryExit` is written by hand from the Rust struct, which no extracted function mentions. `Slot` is the `u64` it wraps | `PoolEntry`, `RSignedVoluntaryExit`, `OperationPool/TypesExternal.lean` |
| The entries of the two keyed pools name pairwise distinct validators. Proved for pools with a history (`Keyed.values_pairwise`). What is trusted in its place: the two instances of `ValidatorIndexed` restate the Rust impls (`proposer_index()` and `message.validator_index`), and the private field of `ValidatorMap` has no writer but `insert` and `retain` | `proposerSlashingIndexed`, `voluntaryExitIndexed` (`OperationPool/PoolsInvariant.lean`) |
| The slices that `get_slashings_and_exits` iterates are the entries of the pools, each once: `values()` on the two maps and `iter()` on the set | `Pools.Reads` |
| The history of the pools is a chain of `PoolStep`s. In particular the states on which operations are validated, and the packing state, form a chain in which each state extends the one before (`Extends`) | `hsteps` of `packed_slashings_and_exits_valid_of_history` |
| `headersOk` is the three checks of `verify_proposer_slashing<E>` on the two headers: same slot, same proposer index, and `header_1 != header_2`, which compares field by field. `toSpecProposerSlashing` and `toSpecVoluntaryExit` describe the spec's form of a Rust value: the same integers and the same 32-byte roots. A signature is an opaque Rust type, and `signatureBytes` is an uninterpreted function for its 96 bytes | `OperationPool/Bridge/ToSpec.lean` |
| The Lighthouse state corresponds to the spec state: same validator epochs and slashed flags, same current epoch, and a pending balance that was computed without overflow | `PackingState.corr` (`StateCorr`) |
| A proposer slashing or an exit has valid signatures on the state it is inserted on. `SigVerifiedOp` is built only by `validate`, which checks the signatures on that state. That the signatures stay valid on later states is proved (`ProposerSlashingSigned.of_extends`, `ExitSigned.of_extends`) | The premise `signed` of `PoolStep.insertProposerSlashing` and `PoolStep.insertVoluntaryExit` |
| The spec's form of an attester slashing of the pool names the attesting indices that the Rust reads. Rust's `AttesterSlashing<E>` is not extracted, so the model type holds both and the two equations | `PooledAttesterSlashing` (`indices1`, `indices2`) |
| EthLib's model is the consensus spec. It is checked against the consensus spec tests. The signature checks are the `SignatureChecks` the theorem is stated for: any pair of check functions, of which `SignatureChecks.bls` is the real one | The spec-side theorems |

Assumptions about the state, by design:

- The packing state is a Fulu state that a run of the spec reaches from a genesis state
  (`hfork`, `PackingState.reachable`). This is where the proof assumes that Lighthouse's state
  transition is the spec's.
- The state is within the time bound and the supply cap (`PackingState.time`,
  `PackingState.supply`).

## Scope

- **Fork.** The spec side is Fulu: `IsFulu fork`, from the Fulu upgrade and before Gloas. EthLib
  states each rule for every fork, and the proofs here take the Fulu branch. Gloas overrides the
  exit churn, voids builder payments in `slash_validator`, and runs the parent's execution
  requests before the exits; the Gloas filter for those is proved in `../block_production`.
- **The `MaxCover` implementation.** `maximum_cover` is extracted and proved. The methods of
  `AttesterSlashingMaxCover` and its constructor are generic over `EthSpec` and are not
  extracted. `AttesterCover` models the methods, and `attesterCoverNew` models the constructor.
- **The closures.** The three closures of `get_slashings_and_exits` read `BeaconState<E>` and
  are not extracted. Their models call the extracted cores, and the rest of each model is
  written by hand from the Rust.
- **Steps before the operations.** A block runs its header, withdrawals, execution payload,
  RANDAO and eth1 data before the proposer slashings, and its attestations before the exits.
  None of them writes what a slashing or an exit reads, apart from balances, but that is not
  proved here. `SlashFrame` is the interface for adding them.
- **Checked sums.** EthLib raises an error on every sum and product that reaches `2^64`, as the
  pyspec does. The theorems therefore cover the two sums of `slash_validator` that an earlier
  version of this work, on a model that wraps, left out: `epoch + EPOCHS_PER_SLASHINGS_VECTOR`,
  and the slashings entry plus the effective balance. `TimeBound` gives room for the first, and
  the third field of `SupplyCap` for the second.
- **The room EthLib asks for.** `slashValidator_total_of_room` (EthLib) asks that the proposer's
  balance has room for the whole effective balance of the slashed validator, where the reward
  is a fraction of it. `OperationsRoom` carries that extra `2^58` Gwei.
- **The spec's loops.** `processSlashingsAndExits` is three folds, the ones EthLib's
  `processOperations` makes over those lists of a block body before Gloas.
  `packed_operations_valid` covers a body with no other operation. A real block also holds
  attestations, which run between the slashings and the exits. They are not covered:
  `SlashFrame` is the interface for adding them.
- **Completeness.** The theorems say every packed operation is valid. They do not say that
  every valid operation is packed, or that the max cover picks a best set of slashings.

## Layout

```
ExternalTypes.lean            opaque external types shared by both crates (hand-written, trusted)
EthLibOps/                    proofs on EthLib's spec alone; no Rust
  ExitQueue.lean              `initiate_validator_exit` succeeds when the exit queue has room
  SlashFrame.lean             what slashings and exits keep, and the bounds on what they move
  SlashValidator.lean         `slash_validator` succeeds and has the slash frame
  Room.lean                   the room read on the packing state; the single steps from a frame
  ProposerSlashings.lean      the proposer slashings of a block succeed
  AttesterSlashings.lean      the attester slashings of a block succeed
  VoluntaryExits.lean         the voluntary exits of a block succeed
  Bounds.lean                 `TimeBound`, `SupplyCap`, `OperationsRoom`; mainnet and minimal
  Operations.lean             `processSlashingsAndExits`, and its link to `processOperations`
  Stability.lean              the signature checks of an operation hold on later states
OperationPool/
  Types.lean, Funs.lean       generated from `operation_pool`
  TypesExternal.lean          hand-written models of external types (trusted)
  FunsExternal.lean           hand-written models of external functions (trusted)
  FilterLimit.lean            proof: the packing loop
  MaxCover.lean               proof: the greedy max cover
  ExclusionSet.lean           proof: the exclusion set
  Concrete.lean               slice pools, pure closures and the model of a cover
  Closures.lean               the filter closures and `AttesterSlashingMaxCover::new`, modelled
                              on the extracted cores
  Bridge/Scalars.lean         Rust `u64` to the spec's `UInt64`
  Bridge/StateCorr.lean       the state correspondence; Rust checks to spec gates
  Bridge/ToSpec.lean          the spec's form of a packed operation; the header checks
  Pools.lean                  the pools, and the model of `get_slashings_and_exits`
  PackedProposerSlashingsValid.lean   the proposer slashings
  PackedAttesterSlashingsValid.lean   the attester slashings, after the proposer slashings
  PackedExitsValid.lean               the exits, after the slashings
  PackedSlashingsAndExitsValid.lean   main theorem, and the same on `processOperations`
  ValidatorMap.lean           proof: `ValidatorMap` keeps one operation per validator
  PoolsInvariant.lean         `PoolsValid` from a history of the pools
StateProcessing/
  Types.lean, Funs.lean       generated from `state_processing`
  TypesExternal.lean          hand-written opaque external types (trusted)
  VerifyExit.lean             proof: the `verify_exit` cores
  VerifyProposerSlashing.lean proof: the `verify_proposer_slashing` core
Audit.lean                    prints the axioms of the main theorems
extract.sh                    regenerates the generated files
```

The `*_Template.lean` files are generated by Aeneas and list what the hand-written external
files have to provide.

## Dependencies

Both sit under `../deps`, which Git ignores, and this repository holds neither. The Lean files
of this directory are all that the repository adds.

- `aeneas-release-2e3d448/`: the Aeneas release `nightly-2026.11.23-2e3d448`, on Lean v4.36.0,
  unpacked from `aeneas-linux-x86_64.tar.gz`. It bundles the `aeneas` and `charon` binaries and
  the Lean library.
- `ethlib/`: a checkout of [EthLib](https://github.com/ethereum/EthLib) on Lean v4.36.0, at
  commit `24f2ccdae` of `master`. EthLib is pure Lean and needs no native library.

No script fetches them yet. These commands do, from this directory:

```sh
mkdir -p ../deps/aeneas-release-2e3d448
curl -L https://github.com/AeneasVerif/aeneas/releases/download/nightly-2026.11.23-2e3d448/aeneas-linux-x86_64.tar.gz \
  | tar -xz -C ../deps/aeneas-release-2e3d448
git clone https://github.com/ethereum/EthLib ../deps/ethlib
git -C ../deps/ethlib checkout 24f2ccdaeb9fb79a4af73c794f856c17c84a16db
```

Aeneas and EthLib both require Mathlib `v4.36.0`, so the project has one copy. EthLib changes
fast: a later commit can rename or restate the theorems that `EthLibOps` uses, so the build is
checked against that one commit.

## Regenerating and checking

```sh
./extract.sh       # Rust -> LLBC -> {OperationPool,StateProcessing}/{Types,Funs}.lean
lake build         # checks the generated code, the models and the proofs
lake build Audit   # prints the axioms each main theorem depends on
```

The first build needs the Mathlib cache: `lake exe cache get`. It then builds the part of EthLib
that the proofs import, about 1900 modules, in a few minutes. To check one file,
`lake env lean OperationPool/FilterLimit.lean` takes a few seconds once its imports are built.

Every library sets `autoImplicit = false`, so a name that does not resolve in a statement is an
error and not a new variable.

The files of `EthLibOps` are Lean modules, as EthLib's are, so that they see what EthLib
exports and nothing else. The files of `OperationPool` import Aeneas, which is not a module
library, and are plain files.

`extract.sh` runs Charon once per crate. Aeneas emits a function of another crate only when an
extracted body calls it, so each crate's functions are extracted from that crate. Neither crate
builds on its own, and the script unifies Cargo features across the workspace to make them
build.
