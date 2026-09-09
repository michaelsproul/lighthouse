# Gloas block production properties

Run the bounded default campaign (16 generated chains plus deterministic coverage tests):

```sh
cargo test --release -p beacon_chain --test beacon_chain_tests gloas_block_production::
```

The tests select Gloas explicitly; `FORK_NAME` and the `fork_from_env` feature are unnecessary.
They use `BeaconChainHarness` with its in-process mock execution engine on loopback. No
execution client, devnet, or external endpoint is required.

For a longer campaign or a reproducible random seed:

```sh
PROPTEST_CASES=1000 PROPTEST_RNG_SEED=12345 \
  cargo test --release -p beacon_chain --test beacon_chain_tests \
  gloas_block_production::produced_blocks_pass_transition_and_gossip
```

Proptest shrinks the validator configuration and action sequence, reports the failing input,
and saves regression seeds in `beacon_node/beacon_chain/proptest-regressions/`.
Re-running the same command replays saved failures before generating new cases. Commit
regression seeds for production failures. `PROPTEST_MAX_SHRINK_ITERS` controls the shrinking
budget (128 by default); set it to `0` when first investigating a slow failure. Use
`RUST_LOG=error` to reduce logging and `-- --nocapture` to see failures immediately.

Each scenario starts from a valid genesis with 48 known signing keys and random BLS,
execution, or compounding withdrawal credentials and balances. A sequence of 2–12 actions
then varies slot gaps, cached versus unadvanced production states, envelope delivery, blob
presence, attestation participation, sync committee contributions, payload attestations,
and operation-pool contents. Execution
requests are injected into the mock EL, committed in the produced envelope, and processed
by a later child when its parent is full. Withheld envelopes exercise empty-parent builds.
Some scenarios also register a builder at genesis and prepend 32 fully attested blocks to
finalize its deposit. Every warm-up block passes the same checks. Random bids arrive on
gossip or through the real builder HTTP client backed by a local mock server; their values
span both sides of the mock EL's local payload value. This exercises external winners and
local fallback. The mock builder's payload comes from the same mock EL. Gossip bids are
submitted when the head snapshot has the reception epoch's RANDAO mix; longer gaps still
exercise local and direct builds. A separate reception clock models preferences arriving
before the proposal without rewinding chain time. All bid/preference validation checks run.
Rejected gossip bids are left out of the cache so production can fall back to the local
payload. Bid acceptance correctness is outside this production property's scope.

Operation tuples are `(kind, validator)`: kinds `0`–`3` select voluntary exits, proposer
slashings, attester slashings, and BLS credential changes. Request tuples are
`(kind, source, target, amount_eth)`: kinds `0`–`6` select deposit top-ups, full exits,
partial withdrawals, switches to compounding, consolidations, builder deposits, and builder
exits. Builder requests use the separate builder signing key. Request lists respect the
preset limits. Operation eligibility is checked against the evolving state before gossip
import; ineligible choices are no-ops.
Requests may be consensus no-ops, as with real execution requests. Operations in produced
blocks are never modified or repaired by the harness.
An active, unslashed half of the validator set is retained so chains can continue. Slots
still assigned to a slashed proposer by Gloas lookahead are modeled as missed proposals.
The parent is resolved again after slashing imports or epoch changes, since these can
change the fork-choice head. Produced blocks are checked even if fork choice prefers
another branch afterward.

For every block, the test calls the public Gloas production entry point, signs its output,
then independently advances a clone of the parent state with caches dropped. It runs
`per_block_processing` with individual signature verification and block-root verification,
compares the computed state root with both the block and production state, and verifies
the envelope. It also runs block gossip verification and normal import, envelope gossip
verification (including withheld envelopes), and normal envelope/column import when
delivery is selected. Production errors and failures validating the produced block or
envelope fail the property.

The deterministic coverage scenario requires nonempty exits, both slashing families,
credential changes, attestations, payload attestations, sync aggregates, full withdrawals,
queued partial withdrawals, queued consolidations, and deferred requests.
It includes a voluntary exit overlapping a parent's full-exit request, an empty parent,
blobs, and epoch crossings. Other cases cover direct/gossip builder selection, deferred
builder exits, and both parent state cache conditions. Shortened validator exit eligibility
and withdrawal delays, plus a lower activation/exit churn cap leaving churn available for
consolidations in a small validator set, keep these tests practical. The mock EL supplies
execution validity; these properties exercise consensus production and validation, not
EVM execution. Random testing checks
the generated cases and is not an exhaustive proof over all possible beacon states.
