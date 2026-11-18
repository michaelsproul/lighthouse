# Slashing and voluntary-exit packing: proved valid against the consensus spec

Machine-checked proofs, in Lean 4, that the proposer slashings, the attester slashings and the
voluntary exits packed by `OperationPool::get_slashings_and_exits` are valid for inclusion in the
block. The consensus spec processes, in order and without an error:

- the packed proposer slashings, from the state they were packed on;
- then the packed attester slashings;
- then the packed voluntary exits.

The Rust is translated to Lean by [Charon](https://github.com/AeneasVerif/charon) and
[Aeneas](https://github.com/AeneasVerif/aeneas). The spec is the Fulu fork of
[Etheorem](https://github.com/etheorem/etheorem), an executable Lean model of the consensus
specification. One Lean project depends on both.

## What is proved

Each theorem is a statement about the real `filter_limit_operations` and `maximum_cover`, and
about the spec's `process_proposer_slashing`, `process_attester_slashing` and
`process_voluntary_exit`. Let `s` be the spec state that corresponds to the Lighthouse state the
op pool packs on.

**The main theorem.** `packed_slashings_and_exits_valid`
(`OperationPool/PackedSlashingsAndExitsValid.lean`) follows the body of
`get_slashings_and_exits` call by call:

> `filter_limit_operations` returns the proposer slashings `outP`, and the spec processes them
> in order from `s` and reaches `s₁`. `maximum_cover` returns the covers `outA`, and the spec
> processes their slashings in order from `s₁` and reaches `s₂`. The two `insert_*` functions
> build the exclusion set `excl` from `outP` and `outA`. `filter_limit_operations`, with a
> filter that rejects the validators of `excl`, returns the exits `out`, and the spec processes
> them in order from `s₂`. No call panics, and each list is within its limit.

No hypothesis of this theorem is a run of the spec. Every hypothesis about the state is about
the packing state `s`.

The extracted functions are generic over an iterator, over closures and over a `MaxCover`
implementation. The main theorem states them on concrete instances
(`OperationPool/Concrete.lean`): a pool is a slice walked by Aeneas's slice iterator, a closure
is a pure function, and a cover is `AttesterCover`, a model of `AttesterSlashingMaxCover`. The
facts that the generic theorems take as hypotheses about the instances hold for these by
construction. The three theorems below are the generic ones the main theorem joins.

**Proposer slashings.** `packed_proposer_slashings_valid_uniform`
(`OperationPool/PackedProposerSlashingsValid.lean`):

> The spec's `process_proposer_slashing` succeeds on each packed slashing in order, starting
> from `s`.

**Attester slashings.** `packed_attester_slashings_valid_uniform`
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

**Exits.** `packed_exits_valid_after_slashings_uniform` (`OperationPool/PackedExitsValid.lean`):

> The spec's `process_voluntary_exit` succeeds on each packed exit in order, after the packed
> slashings of the block.

### The hypotheses about the state

| Hypothesis | Meaning | Status |
|---|---|---|
| `hcorr` | The Lighthouse state corresponds to `s` | Trusted, see below |
| `hreach`, `hanchors` | `ReachableState`: `s` is reached from an anchor state by the spec's `process_slots` and `process_block`, and each anchor has `SimpleStateInvariants`. The proof uses one consequence: `balances` has the registry's length | Assumed. Etheorem's `ReachableState.simpleStateInvariants` gives the consequence |
| `hproposer` | The proposer of the slot is a validator in the registry | Assumed. `process_block_header` checks it earlier in the block |
| `htime` | `TimeBound s`: the slot and the earliest exit epoch are below `2^63` | Assumed |
| `hsupply` | `SupplyCap (2^58) s`: the balances add up to `2^58` Gwei at most, and so do the effective balances | Assumed |
| `[PresetValid]`, `[ConfigValid]` | The preset and the config are valid. The proof uses `ConfigValid.operationsRoom`: within the two bounds above, a block with as many slashings and exits as the preset allows has room | Proved for mainnet and minimal, by Etheorem's instances |

`TimeBound` and `SupplyCap` are defined in Etheorem (`Proofs/Fulu/StateBounds.lean`), and
`OperationsRoom` in `Proofs/Fulu/ConfigValid.lean`. They replace the room formulas of the generic
theorems, which cover the arithmetic of `slash_validator` and `initiate_validator_exit` that
packing never runs.

- The time bound has two clocks, because the slot does not bound the exit queue: each exit
  moves `earliest_exit_epoch` ahead of the current epoch.
- The supply cap is a sum, as a supply is. The proofs use two consequences: no balance and no
  effective balance exceeds the cap. `2^58` Gwei is about 288 million ETH.
- Neither bound is an invariant of the state transition, since a processed slot moves the slot
  and a deposit adds Gwei. They are therefore separate from `SimpleStateInvariants`, which
  holds what every reachable state has.
- `ConfigValid.operationsRoom` reads constants alone. It counts one exit and one whistleblower
  reward for each proposer slashing and for each validator an attester slashing can name
  (`MAX_VALIDATORS_PER_COMMITTEE * MAX_COMMITTEES_PER_SLOT`, `131072` on mainnet), and one exit
  for each voluntary exit, at the block limits of the preset. `hlimits` says that the three
  limits the Rust passes are at most those.

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
| `yields_slice` | `OperationPool/Concrete.lean` | Aeneas's slice iterator yields the entries of the slice, in order |
| `AttesterCover.model` | `OperationPool/Concrete.lean` | The model of `AttesterSlashingMaxCover` satisfies `MaxCoverModel` |
| `exclusionSet_ok` | `OperationPool/ExclusionSet.lean` | The two `insert_*` functions do not panic |
| `isSlashableValidator_of_slashableOn` | `OperationPool/Bridge/StateCorr.lean` | A validator `Validator::is_slashable_at` accepts is slashable by the spec's `is_slashable_validator` |
| `packed_proposer_slashings_valid` | `OperationPool/PackedProposerSlashingsValid.lean` | The packed proposer slashings succeed, with the room stated per slashing |
| `packed_attester_slashings_valid` | `OperationPool/PackedAttesterSlashingsValid.lean` | The packed attester slashings succeed, with the room stated per walked validator |
| `packed_exits_valid` | `OperationPool/PackedExitsValid.lean` | The packed exits succeed from any state with the exit view of the packing state |
| `packed_exits_valid_after_slashings` | `OperationPool/PackedExitsValid.lean` | The packed exits succeed after slashings whose runs are given |

The spec-side theorems live in Etheorem, on branch `voluntary-exit-proofs`, under
`packages/EthCLSpecs/EthCLSpecs/Proofs/Fulu/`. `qsort_perm` is in
`packages/EthCLLib/EthCLLib/Proofs/QSort.lean`.

| Theorem | File | Statement |
|---|---|---|
| `slashValidator_succeeds` | `ProposerSlashings.lean` | `slash_validator` cannot fail for a validator of the registry, given the list lengths, the proposer and the two rooms |
| `processProposerSlashing_succeeds` | `ProposerSlashings.lean` | A slashing that is well formed, passes `ProposerSlashingGates` and `ProposerSlashingSigned` cannot fail, under the same premises |
| `proposerSlashings_succeed` | `ProposerSlashings.lean` | Slashings that name pairwise distinct proposers and pass the checks on a state succeed in order from it |
| `processAttesterSlashing_succeeds` | `AttesterSlashings.lean` | A slashing that passes `AttesterSlashingChecks` and walks one slashable validator cannot fail, given room for every walked validator |
| `attesterSlashings_succeed` | `AttesterSlashings.lean` | Slashings succeed in order after a run of slashings, when each one walks a validator that is slashable on the start state and that no earlier slashing can slash |
| `mem_attesterSlashingIndices` | `AttesterSlashings.lean` | The validators a slashing walks are the validators both attestations name |
| `qsort_perm` | `QSort.lean` | `Array.qsort` returns a permutation of its input |
| `OperationsRoom.exits_of_timeBound`, `OperationsRoom.rewards_of_supplyCap` | `StateBounds.lean` | Within `TimeBound` and `SupplyCap`, the exit queue and each balance have room for the operations of a block |
| `ConfigValid.operationsRoom` | `ConfigValid.lean` | A field of `ConfigValid`: `OperationsRoom` holds for the block limits of the preset at a cap of `2^58` Gwei. The instances for mainnet and minimal prove it |
| `ReachableState.simpleStateInvariants` | `ReachableInvariants.lean` | Every reachable state has `SimpleStateInvariants`, once every anchor does |
| `processVoluntaryExit_succeeds` | `VoluntaryExits.lean` | An exit that passes `ExitGates` and `ExitSigned` cannot fail when the exit queue has room |
| `voluntaryExits_succeed_of_exitView` | `VoluntaryExits.lean` | Exits that name pairwise distinct validators and pass the gates succeed in order, from any state with the same `ExitView` |
| `exitView_of_slashings` | `VoluntaryExits/Slashings.lean` | The proposer and attester slashings of a block keep the exit view for the validators they cannot slash |

No proof uses `sorry`. `./lake.sh build Audit` prints the axioms of each theorem: Lean's
`propext`, `Classical.choice` and `Quot.sound`, plus the opaque external declarations listed
under "What is trusted". The Etheorem theorems use the three standard axioms only.

## Why the property holds

Packing checks each operation against the unmodified state. Block processing applies the
operations in order, each one writing the state the next one reads.

For the proposer slashings:

1. Each packed slashing passed `verify_proposer_slashing` on the packing state
   (`filter_limit_operations_eq`, `verify_proposer_slashing_validator_checks_ok_iff`,
   `proposerSlashingGates_of_coreOk`).
2. The packed slashings name pairwise distinct proposers, and a slashing of one validator leaves
   the checks of the others alone (`SlashFrame`, `proposerSlashings_succeed`).
3. `slash_validator`, which packing never runs, cannot fail when the exit queue and the
   proposer's balance have room (`slashValidator_succeeds`).

For the attester slashings, after the proposer slashings:

1. Each `AttesterSlashingMaxCover` covers the validators its slashing can slash on the packing
   state: the validators both attestations name, that are slashable, and that no packed
   proposer slashing names (`hcover`, trusted).
2. `maximum_cover` picks only items with a non-zero score, and after each pick it removes what
   the pick covers from every other item that can still be picked. So each picked slashing can
   slash a validator that no slashing picked before it can slash (`maximum_cover_spec`).
3. A slashing changes the record of the validators it slashes and of no other validator
   (`SlashFrame`). That validator is therefore still slashable when its slashing runs, and the
   spec's `slashed_any` assertion holds (`processAttesterSlashing_succeeds`,
   `attesterSlashings_succeed`).
4. The checks before the walk read the registry's length, the public keys and the fork. No
   slashing changes them (`AttesterSlashingChecks.of_slashFrame`).
5. Each `slash_validator` call of the walk cannot fail when the exit queue and the proposer's
   balance have room (`slashValidator_succeeds`).

For the exits, after the slashings:

1. Each packed exit passed `verify_exit` on the packing state (`filter_limit_operations_eq`,
   `verify_exit_*_ok_iff`, `exitGates_of_coresOk`).
2. The packed exits name pairwise distinct validators, and an exit of one validator leaves the
   checks of the others alone (`voluntaryExits_succeed_of_exitView`).
3. No packed exit names a validator the packed slashings can slash (`exclusionSet_covers`), and
   a slashing changes nothing else an exit reads (`exitView_of_slashings`).
4. `initiate_validator_exit`, which packing never runs, cannot fail when the exit queue has
   room (`processVoluntaryExit_succeeds`).

## The Rust that is extracted

Seven functions, each free of `EthSpec`, that Aeneas translates:

| Function | Crate | Role |
|---|---|---|
| `filter_limit_operations` | `operation_pool` | The packing loop, generic over the iterator and the two closures. It packs the proposer slashings and the exits |
| `maximum_cover` | `operation_pool` | The greedy max cover, generic over `T: MaxCover`. It packs the attester slashings and the attestations. Its helpers `best_item_index` and `update_covering_sets` are extracted with it |
| `insert_proposer_slashing_indices` | `operation_pool` | Adds the packed proposer slashings' proposers to the exclusion set |
| `insert_common_attesting_indices` | `operation_pool` | Adds the indices common to an attester slashing's two attestations |
| `verify_proposer_slashing_validator_checks` | `state_processing` | The check of `verify_proposer_slashing` on the proposer's record |
| `verify_exit_validator_checks` | `state_processing` | The checks of `verify_exit` on the validator record |
| `verify_exit_pending_balance` | `state_processing` | The pending-withdrawal check of `verify_exit` |

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

## What is trusted

Not proved, and to be checked by inspection:

| Item | Where it enters |
|---|---|
| Charon, Aeneas and the Lean kernel | Translation and checking |
| The external models: `HashSet` as a list of distinct values (`with_capacity`, `insert`, `contains`), `metrics::set_max_cover_non_zero_items` as a call that returns, and the opaque external types and hashing functions | `OperationPool/TypesExternal.lean`, `OperationPool/FunsExternal.lean`, `StateProcessing/TypesExternal.lean`, `ExternalTypes.lean` |
| The models of `OperationPool/Concrete.lean` stand for the Rust values. A pool's iterator yields each pool entry once, as the slice iterator does on a slice of the entries, and the attester-slashing iterator yields the covers that `AttesterSlashingMaxCover::new` builds. A closure computes a function of its argument and does not panic | The form of the main theorem: `sliceIntoIter`, `pureFn`, `cloneId` |
| `AttesterCover` stands for `AttesterSlashingMaxCover`: the slashing, and `effective_balances` as a list of key and value. Its methods are those of the Rust: `score` is the sum of the values, which wraps as in a release build; `update_covering_set` is `retain` on the keys the picked cover does not have; `clone` returns its argument | `AttesterCover`, `AttesterCover.maxCover` |
| The body of `get_slashings_and_exits` makes the four calls of the conclusion, with these arguments: the exclusion set is built from the packed slashings, and the exit filter is `!to_be_slashed.contains(index)` and then the checks of `get_voluntary_exits` | The form of the conclusion; `initial`, `indicesA` |
| The three limits are the preset's: `E::MaxProposerSlashings`, `E::max_attester_slashings_electra()` and `E::MaxVoluntaryExits` | `hlimits` |
| What a cover covers. `AttesterSlashingMaxCover::new` runs `get_slashable_indices_modular` with the closure `is_slashable_at(epoch) && !to_be_slashed.contains(index)`, so the keys of `effective_balances` are the validators that both attestations name, that are in the registry and slashable, and that `to_be_slashed` does not hold. `to_be_slashed` holds the proposers of the packed proposer slashings at that point | `hcoverA` (`SlashableOn`) |
| A slashing of the attester-slashing pool passes the spec's checks before the walk, on the packing state: the two attestations are slashable, and each is a valid indexed attestation (indices not empty, sorted, in the registry, aggregate signature valid). `SigVerifiedOp` is built only after `verify_attester_slashing`, and `signature_is_still_valid` checks the fork versions. Lighthouse does not check again at packing that every attesting index is in the registry of the packing state: `new` looks up the indices that both attestations name, and no other | `hchecksA` (`AttesterSlashingChecks`) |
| `get_attester_slashings` returns, for each picked cover, `convert_to_object(cover.intermediate())` | `toSpecA`, the conclusion |
| The exit pool's `HashMap` is keyed by `message.validator_index`, and the proposer-slashing pool's by `proposer_index()` (`insert_voluntary_exit`, `insert_proposer_slashing`, `persistence.rs`), so the entries name pairwise distinct validators | `hkeys`, `hkeysP` |
| What the two filter closures check. Exits: `filter(exit) && signature_is_still_valid && verify_exit(..).is_ok()`. Proposer slashings: `signature_is_still_valid && verify_proposer_slashing(..).is_ok()`. Both mappings clone the operation | `hkeepE`, `hkeepP` |
| `verify_exit<E>` calls the two cores with the validator at the exit's index, the state's current epoch, the chain spec's constants, and the pending balance when it was computed | `hkeepE` (`CoresOk`), `StateView` |
| `verify_proposer_slashing<E>` checks that the two headers have the same slot and the same proposer index and differ, and then calls the core with the validator at that index and the state's current epoch | `hkeepP` (`ProposerCoreOk`, `ProposerSlashingWellFormed`) |
| Two headers that differ have different hash tree roots. Lighthouse compares the headers field by field; Etheorem's model compares their hash tree roots. The two agree unless SHA-256 collides | `hkeepP` (`ProposerSlashingWellFormed.differ`) |
| The Lighthouse state corresponds to the spec state: same validator epochs and slashed flags, same current epoch, and a pending balance that was computed without overflow | `hcorr` (`StateCorr`) |
| An operation in a pool has valid signatures for the packing state. `SigVerifiedOp` is built only after a signature check, and a validator's public key never changes. For an exit, the Fulu domain is fixed at the Capella fork version. For a proposer slashing, `signature_is_still_valid` checks that the fork version at the header's epoch is the one the signature was checked under | `hkeepE` (`ExitSigned`), `hkeepP` (`ProposerSlashingSigned`) |
| The spec's forms of the packed operations carry the same indices and epochs as the Rust values | `hmessage`, `hindexP`, `hindicesA` |
| Etheorem's Fulu model is the consensus spec, with a valid preset and config (`PresetValid`, `ConfigValid`, proved for `mainnet` and `minimal`) | The spec-side theorems |

Assumptions about the state, by design:

- The packing state is a reachable state of the spec (`hreach`, `hanchors`). This is where the
  proof assumes that Lighthouse's state transition is the spec's.
- The proposer of the slot is in the registry (`hproposer`). Etheorem's reachable-state
  invariants do not cover the proposer lookahead yet.
- The state is within the time bound and the supply cap (`htime`, `hsupply`).

## Scope

- **Fork.** The spec side is Fulu. Gloas overrides the exit churn, voids builder payments in
  `slash_validator`, and runs the parent's execution requests before the exits; the Gloas filter
  for those is proved in `../block_production`.
- **The `MaxCover` implementation.** `maximum_cover` is extracted and proved. The methods of
  `AttesterSlashingMaxCover` and its constructor are generic over `EthSpec` and are not
  extracted. `AttesterCover` models the methods, and `hcoverA` states what the constructor
  computes.
- **Steps before the operations.** A block runs its header, withdrawals, execution payload,
  RANDAO and eth1 data before the proposer slashings, and its attestations before the exits.
  None of them writes what a slashing or an exit reads, apart from balances, but that is not
  proved here. `SlashFrame` and `ExitView` are the interfaces for adding them.
- **Sums that wrap in the model.** Etheorem's `slash_validator` wraps on two sums where the
  pyspec raises: `epoch + EPOCHS_PER_SLASHINGS_VECTOR`, and the slashings entry plus the
  effective balance. The theorems follow the model, so they say nothing about a state where one
  of those sums passes `2^64`. This work found the gap; Etheorem's `DISCREPANCIES.md` records
  it.
- **The spec's loops.** Each fold is stated as `ForM.forM ops process_op`.
  `forIn_voluntaryExits_eq` (Etheorem) proves that the `for` loop over `body.voluntary_exits`
  is that fold. The same fact for the two kinds of slashing, and the split of
  `process_operations` into its loops, are proved in Etheorem for Gloas
  (`processOperations_eq_seq`) and not yet ported to Fulu.
- **Completeness.** The theorems say every packed operation is valid. They do not say that
  every valid operation is packed, or that the max cover picks a best set of slashings.

## Layout

```
ExternalTypes.lean            opaque external types shared by both crates (hand-written, trusted)
OperationPool/
  Types.lean, Funs.lean       generated from `operation_pool`
  TypesExternal.lean          hand-written models of external types (trusted)
  FunsExternal.lean           hand-written models of external functions (trusted)
  FilterLimit.lean            proof: the packing loop
  MaxCover.lean               proof: the greedy max cover
  ExclusionSet.lean           proof: the exclusion set
  Concrete.lean               slice pools, pure closures and the model of a cover
  Bridge/Scalars.lean         Rust `u64` to the spec's `UInt64`
  Bridge/StateCorr.lean       the state correspondence; Rust checks to spec gates
  PackedProposerSlashingsValid.lean   the proposer slashings
  PackedAttesterSlashingsValid.lean   the attester slashings, after the proposer slashings
  PackedExitsValid.lean               the exits, after slashings whose runs are given
  PackedSlashingsAndExitsValid.lean   main theorem: the three kinds, in order
StateProcessing/
  Types.lean, Funs.lean       generated from `state_processing`
  TypesExternal.lean          hand-written opaque external types (trusted)
  VerifyExit.lean             proof: the `verify_exit` cores
  VerifyProposerSlashing.lean proof: the `verify_proposer_slashing` core
Audit.lean                    prints the axioms of the main theorems
extract.sh                    regenerates the generated files
lake.sh                       runs `lake` for this project
```

The `*_Template.lean` files are generated by Aeneas and list what the hand-written external
files have to provide.

## Dependencies

Both sit under `../deps`, which Git ignores. No script fetches them yet.

- `aeneas-release-5a392f4/`: the Aeneas release `nightly-2026.11.09-5a392f4`, the last one on
  Lean v4.35.0, unpacked from `aeneas-linux-x86_64.tar.gz`. It bundles the `aeneas` and `charon`
  binaries and the Lean library.
- `etheorem-lean4.35/`: a checkout of Etheorem on branch `voluntary-exit-proofs` of
  `github.com/michaelsproul/etheorem`, at a commit whose `ConfigValid` has the field
  `operationsRoom`. The branch sets `lean-toolchain` to v4.35.0. The
  checkout also needs the vendored native sources, which Etheorem's `just hazmat-*-vendor`
  recipes fetch.

## Regenerating and checking

```sh
./extract.sh          # Rust -> LLBC -> {OperationPool,StateProcessing}/{Types,Funs}.lean
./lake.sh build       # checks the generated code, the models and the proofs
./lake.sh build Audit # prints the axioms each main theorem depends on
```

Use `./lake.sh` in place of `lake`. Etheorem's native libraries link only when Lake starts
inside the Etheorem checkout, and the script takes care of that. To check one file that does not
import Etheorem, `./lake.sh env lean OperationPool/FilterLimit.lean` takes a few seconds. A file
that imports Etheorem has to go through `./lake.sh build <Module>`.

The first build needs the Mathlib cache: `./lake.sh exe cache get`.

`extract.sh` runs Charon once per crate. Aeneas emits a function of another crate only when an
extracted body calls it, so each crate's functions are extracted from that crate. Neither crate
builds on its own, and the script unifies Cargo features across the workspace to make them
build.
