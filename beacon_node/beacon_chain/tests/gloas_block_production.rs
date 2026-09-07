//! Stateful properties for the production Gloas block builder. See
//! `gloas_block_production.md` for replay and longer campaigns.

use beacon_chain::{
    AvailabilityProcessingStatus, NotifyExecutionLayer, ProduceBlockVerification,
    graffiti_calculator::GraffitiSettings,
    observed_operations::ObservationOutcome,
    payload_bid_verification::gossip_verified_bid::GossipVerifiedPayloadBid,
    payload_envelope_verification::EnvelopeSource,
    proposer_preferences_verification::gossip_verified_proposer_preferences::GossipVerifiedProposerPreferences,
    test_utils::{
        BeaconChainHarness, EphemeralHarnessType, PayloadAttestationVote, RelativeSyncCommittee,
    },
};
use bls::Keypair;
use eth2::types::{
    BuilderConfig, BuilderEntry, BuilderPubkeys, EmptyMetadata, ForkVersionedResponse, RequestAuth,
    RequestAuthData, SignedRequestAuth,
};
use execution_layer::test_utils::generate_genesis_header;
use genesis::{InteropGenesisBuilder, bls_withdrawal_credentials};
use proptest::prelude::*;
use slot_clock::{SlotClock, TestingSlotClock};
use state_processing::{
    BlockSignatureStrategy, ConsensusContext, VerifyBlockRoot, VerifySignatures,
    envelope_processing::verify_execution_payload_envelope, per_block_processing,
    state_advance::complete_state_advance,
};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use types::*;

type E = MinimalEthSpec;
type Harness = BeaconChainHarness<EphemeralHarnessType<E>>;
const VALIDATORS: usize = 48;
const GWEI_PER_ETH: u64 = 1_000_000_000;
static KEYS: LazyLock<Vec<Keypair>> =
    LazyLock::new(|| types::test_utils::generate_deterministic_keypairs(VALIDATORS + 1));

#[derive(Clone, Debug)]
struct Scenario {
    // Credential kind (BLS, execution, compounding), and excess balance in ETH.
    validators: Vec<(u8, u16)>,
    builders: bool,
    steps: Vec<Step>,
}

#[derive(Clone, Debug)]
struct Step {
    slot_delta: u64,
    cache_advanced_state: bool,
    deliver_envelope: bool,
    blobs: bool,
    participation: usize,
    sync: bool,
    payload_votes: bool,
    builder: u8,
    bid_value: u64,
    operations: Vec<(u8, usize)>,
    requests: Vec<(u8, usize, usize, u16)>,
}

fn scenarios() -> impl Strategy<Value = Scenario> {
    let step = (
        prop_oneof![4 => Just(1), 1 => 2u64..=9],
        any::<bool>(),
        any::<bool>(),
        any::<bool>(),
        0usize..=VALIDATORS,
        any::<bool>(),
        any::<bool>(),
        0u8..3,
        prop_oneof![
            Just(0),
            Just(9_999_999),
            Just(10_000_000),
            10_000_001u64..20_000_001
        ],
        prop::collection::vec((0u8..4, 0usize..VALIDATORS), 0..5),
        prop::collection::vec(
            (0u8..7, 0usize..VALIDATORS, 0usize..VALIDATORS, 1u16..65),
            0..6,
        ),
    )
        .prop_map(
            |(
                slot_delta,
                cache_advanced_state,
                deliver_envelope,
                blobs,
                participation,
                sync,
                payload_votes,
                builder,
                bid_value,
                operations,
                requests,
            )| Step {
                slot_delta,
                cache_advanced_state,
                deliver_envelope,
                blobs,
                participation,
                sync,
                payload_votes,
                builder,
                bid_value,
                operations,
                requests,
            },
        );
    (
        prop::collection::vec(
            (0u8..3, prop_oneof![Just(0), 1u16..65, Just(2016)]),
            VALIDATORS,
        ),
        prop::collection::vec(step, 2..13),
        prop_oneof![3 => Just(false), 1 => Just(true)],
    )
        .prop_map(|(validators, steps, builders)| Scenario {
            validators,
            steps,
            builders,
        })
}

fn harness(scenario: &Scenario) -> Harness {
    // Pin the fork independently of FORK_NAME, including when running the whole test binary.
    let mut spec = ForkName::Gloas.make_genesis_spec(E::default_spec());
    // Exercise exits and withdrawals in short chains without hundreds of warm-up epochs.
    spec.shard_committee_period = 0;
    spec.min_validator_withdrawability_delay = Epoch::new(1);
    // Leave some churn for consolidations in a small validator set.
    spec.max_per_epoch_activation_exit_churn_limit = 64 * GWEI_PER_ETH;
    spec.target_aggregators_per_committee = VALIDATORS as u64;
    let balances = scenario.validators.clone();
    let credentials = scenario.validators.clone();
    let genesis = InteropGenesisBuilder::new()
        .set_initial_balance_fn(Box::new(move |index| {
            (32 + u64::from(balances[index].1)) * GWEI_PER_ETH
        }))
        .set_withdrawal_credentials_fn(Box::new(move |index, pubkey, spec| {
            if credentials[index].0 == 0 {
                bls_withdrawal_credentials(pubkey, spec)
            } else {
                let mut bytes = [0; 32];
                bytes[0] = if credentials[index].0 == 1 {
                    spec.eth1_address_withdrawal_prefix_byte
                } else {
                    spec.compounding_withdrawal_prefix_byte
                };
                bytes[12..].fill(index as u8 + 1);
                Hash256::from(bytes)
            }
        }));
    let mut genesis_state = genesis
        .set_opt_execution_payload_header(generate_genesis_header::<E>(&spec))
        .build_genesis_state(
            &KEYS[..VALIDATORS],
            beacon_chain::test_utils::HARNESS_GENESIS_TIME,
            Hash256::repeat_byte(0x42),
            &spec,
        )
        .unwrap();
    if scenario.builders {
        genesis_state
            .add_builder_to_registry(
                KEYS[VALIDATORS].pk.clone().into(),
                types::consts::gloas::PAYLOAD_BUILDER_VERSION,
                builder_credentials(&spec),
                100 * GWEI_PER_ETH,
                Slot::new(0),
                &spec,
            )
            .unwrap();
    }
    Harness::builder(E::default())
        .spec(Arc::new(spec))
        .keypairs(KEYS[..VALIDATORS].to_vec())
        .withdrawal_keypairs(KEYS.iter().take(VALIDATORS).cloned().map(Some).collect())
        .initial_mutator(Box::new(|builder| {
            let client = builder_client::BuilderHttpClient::new(None, false).unwrap();
            builder.builders(Some(Arc::new(builder_client::Builders::new(Arc::new(
                client,
            )))))
        }))
        .genesis_state_ephemeral_store(genesis_state)
        .mock_execution_layer()
        .build()
}

fn queue_operations(harness: &Harness, state: &BeaconState<E>, step: &Step) {
    for &(kind, index) in &step.operations {
        let validator = state.get_validator(index).unwrap();
        match kind {
            0 if index < VALIDATORS / 2
                && validator.is_active_at(state.current_epoch())
                && validator.exit_epoch == harness.spec.far_future_epoch
                && state.get_pending_balance_to_withdraw(index).unwrap() == 0 =>
            {
                let exit = harness.make_voluntary_exit(index as u64, state.current_epoch());
                if let ObservationOutcome::New(exit) = harness
                    .chain
                    .verify_voluntary_exit_for_gossip(exit)
                    .unwrap()
                {
                    harness.chain.import_voluntary_exit(exit);
                }
            }
            // Retain an unslashed half of the validator set so the scenario can keep proposing.
            1 | 2 if index < VALIDATORS / 2 && validator.is_slashable_at(state.current_epoch()) => {
                if kind == 1 {
                    let slashing = harness.make_proposer_slashing(index as u64);
                    if let ObservationOutcome::New(slashing) = harness
                        .chain
                        .verify_proposer_slashing_for_gossip(slashing)
                        .unwrap()
                    {
                        harness.chain.import_proposer_slashing(slashing);
                    }
                } else {
                    let slashing = harness.make_attester_slashing(vec![index as u64]);
                    if let ObservationOutcome::New(slashing) = harness
                        .chain
                        .verify_attester_slashing_for_gossip(slashing)
                        .unwrap()
                    {
                        harness.chain.import_attester_slashing(slashing);
                    }
                }
            }
            3 if validator.withdrawal_credentials.as_slice()[0]
                == harness.spec.bls_withdrawal_prefix_byte =>
            {
                let change = harness.make_bls_to_execution_change(
                    index as u64,
                    Address::repeat_byte(index as u8 + 1),
                );
                if let ObservationOutcome::New(change) = harness
                    .chain
                    .verify_bls_to_execution_change_for_gossip(change)
                    .unwrap()
                {
                    harness.chain.import_bls_to_execution_change(
                        change,
                        operation_pool::ReceivedPreCapella::No,
                    );
                }
            }
            _ => {}
        }
    }
}

fn execution_requests(
    harness: &Harness,
    state: &BeaconState<E>,
    step: &Step,
    next_deposit_index: &mut u64,
) -> ExecutionRequestsGloas<E> {
    let mut requests = ExecutionRequestsGloas::default();
    for &(kind, source, target, amount) in &step.requests {
        let validator = state.get_validator(source).unwrap();
        // Keep half the validators active as well as unslashed; all-exited chains cannot
        // produce blocks and are outside this producer-validity property.
        if source >= VALIDATORS / 2 && (kind == 1 || (kind == 4 && source != target)) {
            continue;
        }
        if kind == 5
            && requests.builder_deposits.len() < E::max_builder_deposit_requests_per_payload()
        {
            let message = DepositMessage {
                pubkey: KEYS[VALIDATORS].pk.clone().into(),
                withdrawal_credentials: builder_credentials(&harness.spec),
                amount: u64::from(amount) * GWEI_PER_ETH,
            };
            let domain = harness.spec.compute_domain(
                Domain::BuilderDeposit,
                harness.spec.genesis_fork_version,
                Hash256::ZERO,
            );
            requests.builder_deposits.push(BuilderDepositRequest {
                pubkey: message.pubkey,
                withdrawal_credentials: message.withdrawal_credentials,
                amount: message.amount,
                signature: KEYS[VALIDATORS]
                    .sk
                    .sign(message.signing_root(domain))
                    .into(),
            });
        } else if kind == 6
            && requests.builder_exits.len() < E::max_builder_exit_requests_per_payload()
        {
            requests.builder_exits.push(BuilderExitRequest {
                source_address: Address::repeat_byte(0x42),
                pubkey: KEYS[VALIDATORS].pk.clone().into(),
            });
        } else if kind == 0 {
            let data = DepositData {
                pubkey: validator.pubkey,
                withdrawal_credentials: validator.withdrawal_credentials,
                amount: u64::from(amount) * GWEI_PER_ETH,
                signature: bls::SignatureBytes::empty(),
            };
            requests.deposits.push(DepositRequest {
                pubkey: data.pubkey,
                withdrawal_credentials: data.withdrawal_credentials,
                amount: data.amount,
                signature: data.create_signature(&KEYS[source].sk, &harness.spec),
                index: *next_deposit_index,
            });
            *next_deposit_index += 1;
        } else if let Some(source_address) =
            validator.get_execution_withdrawal_address(&harness.spec)
        {
            match kind {
                1 | 2 if requests.withdrawals.len() < E::max_withdrawal_requests_per_payload() => {
                    requests.withdrawals.push(WithdrawalRequest {
                        source_address,
                        validator_pubkey: validator.pubkey,
                        amount: if kind == 1 {
                            harness.spec.full_exit_request_amount
                        } else {
                            u64::from(amount) * GWEI_PER_ETH
                        },
                    })
                }
                3 | 4
                    if requests.consolidations.len()
                        < E::max_consolidation_requests_per_payload() =>
                {
                    requests.consolidations.push(ConsolidationRequest {
                        source_address,
                        source_pubkey: validator.pubkey,
                        target_pubkey: if kind == 3 {
                            validator.pubkey
                        } else {
                            state.get_validator(target).unwrap().pubkey
                        },
                    })
                }
                _ => {}
            }
        }
    }
    requests
}

fn builder_credentials(spec: &ChainSpec) -> Hash256 {
    let mut bytes = [0; 32];
    bytes[0] = spec.builder_withdrawal_prefix_byte;
    bytes[12..].fill(0x42);
    Hash256::from(bytes)
}

#[derive(Default, Debug)]
struct Coverage {
    blocks: usize,
    exits: usize,
    proposer_slashings: usize,
    attester_slashings: usize,
    bls_changes: usize,
    attestations: usize,
    payload_attestations: usize,
    withdrawals: usize,
    full_withdrawals: usize,
    pending_partial_withdrawals: usize,
    pending_consolidations: usize,
    sync_aggregates: usize,
    parent_requests: usize,
    empty_parents: usize,
    gossip_builds: usize,
    direct_builds: usize,
    advanced_states: usize,
    unadvanced_states: usize,
}

async fn run_scenario(scenario: &Scenario) -> Coverage {
    let harness = harness(scenario);
    let mut coverage = Coverage::default();
    let mut next_deposit_index = 0;
    // Finalize builder deposits through real transitions; do not forge a finalized checkpoint.
    let warmup = if scenario.builders {
        vec![baseline_step(); 4 * E::slots_per_epoch() as usize]
    } else {
        vec![]
    };
    for (step_index, step) in warmup.iter().chain(&scenario.steps).enumerate() {
        let mut slot = harness.get_current_slot().max(harness.head_slot()) + step.slot_delta;
        harness.set_current_slot(slot);
        harness.chain.recompute_head_at_current_slot().await;
        queue_operations(&harness, &harness.get_current_state(), step);
        // Slashing imports can change fork-choice weights. Resolve the head again before
        // preparing a state, and whenever skipping a slashed proposer's slot crosses an epoch.
        let (parent_state, mut pre_state, proposer) = loop {
            harness.set_current_slot(slot);
            harness.chain.recompute_head_at_current_slot().await;
            let parent_state = harness.get_current_state();
            let mut pre_state = parent_state.clone();
            pre_state.drop_all_caches().unwrap();
            pre_state.build_caches(&harness.spec).unwrap();
            complete_state_advance(&mut pre_state, None, slot, None, &harness.spec).unwrap();
            pre_state.build_caches(&harness.spec).unwrap();
            let proposer = pre_state
                .get_beacon_proposer_index(slot, &harness.spec)
                .unwrap();
            if !pre_state.get_validator(proposer).unwrap().slashed {
                break (parent_state, pre_state, proposer);
            }
            // Gloas lookahead may still assign a slashed proposer. Model its missed proposal.
            slot += 1;
        };
        let parent_root = harness.head_block_root();
        assert_eq!(pre_state.get_latest_block_root(Hash256::ZERO), parent_root);
        let requests = execution_requests(&harness, &parent_state, step, &mut next_deposit_index);
        {
            let mut generator = harness.execution_block_generator();
            generator.set_generate_blobs(step.blobs);
            generator.set_min_blob_count(1);
            generator.set_next_execution_requests(ExecutionRequests::Gloas(requests.clone()));
        }
        // Model preferences/bids received in the preceding slot without rewinding chain time.
        // Use the real gossip verifiers with a separate reception clock.
        let reception_clock = TestingSlotClock::new(
            Slot::new(0),
            Duration::from_secs(parent_state.genesis_time()),
            harness.spec.get_slot_duration(),
        );
        reception_clock.set_slot((slot - 1).as_u64());
        let randao = harness.sign_randao_reveal(&pre_state, proposer, slot);
        let mut external_payload = None;
        let mut builder_config = BuilderConfig::empty();
        let mut builder_server = None;
        let mut builder_mock = None;
        if step.builder != 0
            && scenario.builders
            && pre_state.is_active_builder(0, &harness.spec).unwrap()
            && harness.chain.canonical_head.cached_head().head_payload_status() != proto_array::PayloadStatus::Pending
            // Gossip checks RANDAO against the head snapshot at the receiving slot's epoch.
            // After an epoch of missed blocks, exercise local/direct production until gossip
            // can accept a bid again; do not inject a bid through an unchecked cache wrapper.
            && (step.builder != 1 || (slot - 1).epoch(E::slots_per_epoch()) == parent_state.current_epoch())
        {
            let head = harness.chain.canonical_head.cached_head();
            let full = harness
                .chain
                .canonical_head
                .fork_choice_read_lock()
                .should_build_on_full(&head.head_block_root(), head.head_payload_status(), slot)
                .unwrap();
            let parent_bid = pre_state.latest_execution_payload_bid().unwrap();
            let parent_hash = if full {
                parent_bid.block_hash
            } else {
                parent_bid.parent_block_hash
            };
            let (mut bid, local) = harness
                .chain
                .clone()
                .produce_execution_payload_bid(
                    &pre_state,
                    head.snapshot.execution_envelope.clone(),
                    slot,
                    step.bid_value,
                    0,
                    parent_hash,
                )
                .await
                .unwrap();
            let epoch = slot.epoch(E::slots_per_epoch());
            let preferences = ProposerPreferences {
                dependent_root: pre_state
                    .proposer_shuffling_decision_root_at_epoch(
                        epoch,
                        head.head_block_root(),
                        &harness.spec,
                    )
                    .unwrap(),
                proposal_slot: slot,
                validator_index: proposer as u64,
                fee_recipient: bid.message.fee_recipient,
                target_gas_limit: bid.message.gas_limit,
            };
            let domain = harness.spec.get_domain(
                epoch,
                Domain::ProposerPreferences,
                &pre_state.fork(),
                pre_state.genesis_validators_root(),
            );
            let signature = KEYS[proposer].sk.sign(preferences.signing_root(domain));
            let mut preferences_context = harness
                .chain
                .proposer_preferences_gossip_verification_context();
            preferences_context.slot_clock = &reception_clock;
            let verified = GossipVerifiedProposerPreferences::new(
                Arc::new(SignedProposerPreferences {
                    message: preferences,
                    signature,
                }),
                &preferences_context,
            )
            .unwrap();
            harness
                .chain
                .gossip_verified_proposer_preferences_cache
                .insert_preferences(verified);
            let domain = harness.spec.get_domain(
                epoch,
                Domain::BeaconBuilder,
                &pre_state.fork(),
                pre_state.genesis_validators_root(),
            );
            bid.signature = KEYS[VALIDATORS].sk.sign(bid.message.signing_root(domain));
            if step.builder == 1 {
                let mut bid_context = harness.chain.payload_bid_gossip_verification_context();
                bid_context.slot_clock = &reception_clock;
                let verified = GossipVerifiedPayloadBid::new(Arc::new(bid), &bid_context).unwrap();
                harness
                    .chain
                    .gossip_verified_payload_bid_cache
                    .observe_bid(verified);
            } else {
                let mut server = mockito::Server::new_async().await;
                let response = ForkVersionedResponse {
                    version: ForkName::Gloas,
                    metadata: EmptyMetadata {},
                    data: bid,
                };
                builder_mock = Some(
                    server
                        .mock(
                            "POST",
                            mockito::Matcher::Regex(
                                r"^/eth/v1/builder/execution_payload_bid/.+$".into(),
                            ),
                        )
                        .with_status(200)
                        .with_header("content-type", "application/json")
                        .with_header("eth-consensus-version", "gloas")
                        .with_body(serde_json::to_string(&response).unwrap())
                        .create_async()
                        .await,
                );
                builder_config
                    .builders
                    .push(BuilderEntry {
                        url: server.url().parse().unwrap(),
                        auth: SignedRequestAuth {
                            message: RequestAuth {
                                data: RequestAuthData::new(server.url().into_bytes()).unwrap(),
                                slot,
                            },
                            signature: bls::Signature::empty(),
                        },
                        builder_pubkeys: BuilderPubkeys::default(),
                        max_execution_payment: 0,
                        min_bid: 0,
                        builder_boost_factor: 100,
                    })
                    .unwrap();
                builder_server = Some(server);
            }
            external_payload = Some(local.payload_data);
            harness
                .execution_block_generator()
                .set_next_execution_requests(ExecutionRequests::Gloas(requests.clone()));
        }
        harness.set_current_slot(slot);
        assert_eq!(harness.head_block_root(), parent_root);
        if let Some(signal) = &harness.chain.fork_choice_signal_tx {
            signal.notify_fork_choice_complete(slot).unwrap();
        }
        if step.cache_advanced_state {
            // Simulate the state advance timer winning the race with the block request.
            let mut advanced_state = pre_state.clone();
            let root = advanced_state.update_tree_hash_cache().unwrap();
            advanced_state.apply_pending_mutations().unwrap();
            harness
                .chain
                .store
                .state_cache
                .lock()
                .put_state(root, parent_root, &advanced_state)
                .unwrap();
            coverage.advanced_states += 1;
        } else {
            coverage.unadvanced_states += 1;
        }
        let (block, mut produced_state, _, _, payload, builder_url) = harness
            .chain
            .produce_block_with_verification_gloas(
                randao,
                slot,
                GraffitiSettings::new(None, None),
                ProduceBlockVerification::VerifyRandao,
                builder_config,
            )
            .await
            .unwrap_or_else(|error| {
                panic!("production at step {step_index}, slot {slot}: {error:?}")
            });
        let block = Arc::new(block.sign(
            &KEYS[proposer].sk,
            &produced_state.fork(),
            produced_state.genesis_validators_root(),
            &harness.spec,
        ));
        let block_root = block.canonical_root();
        if let Some(mock) = builder_mock {
            mock.assert_async().await;
        }
        drop(builder_server);
        let envelope = if let Some((envelope, _, _)) = payload {
            assert_eq!(
                block
                    .message()
                    .body()
                    .signed_execution_payload_bid()
                    .unwrap()
                    .message
                    .builder_index,
                types::consts::gloas::BUILDER_INDEX_SELF_BUILD
            );
            envelope
        } else {
            let data = external_payload.expect("external build must have a matching mock payload");
            if builder_url.is_some() {
                coverage.direct_builds += 1;
            } else {
                coverage.gossip_builds += 1;
            }
            Arc::new(ExecutionPayloadEnvelope {
                payload: data.payload,
                execution_requests: data.execution_requests,
                builder_index: data.builder_index,
                beacon_block_root: block_root,
                parent_beacon_block_root: block.message().parent_root(),
            })
        };
        assert_eq!(
            envelope.execution_requests, requests,
            "mock EL must carry generated requests"
        );
        let domain = harness.spec.get_domain(
            slot.epoch(E::slots_per_epoch()),
            Domain::BeaconBuilder,
            &produced_state.fork(),
            produced_state.genesis_validators_root(),
        );
        let signer = if envelope.builder_index == types::consts::gloas::BUILDER_INDEX_SELF_BUILD {
            proposer
        } else {
            VALIDATORS
        };
        let signature = KEYS[signer].sk.sign(envelope.signing_root(domain));
        let envelope = Arc::new(SignedExecutionPayloadEnvelope {
            message: Arc::unwrap_or_clone(envelope),
            signature,
        });

        // Rebuild once more in case an advanced clone was shared with production's state cache.
        pre_state.drop_all_caches().unwrap();
        pre_state.build_caches(&harness.spec).unwrap();
        per_block_processing(
            &mut pre_state,
            &block,
            BlockSignatureStrategy::VerifyIndividual,
            VerifyBlockRoot::True,
            &mut ConsensusContext::new(slot),
            &harness.spec,
        )
        .unwrap_or_else(|error| {
            panic!("state transition at step {step_index}, slot {slot}: {error:?}")
        });
        let state_root = pre_state.update_tree_hash_cache().unwrap();
        assert_eq!(
            state_root,
            block.state_root(),
            "transition root at step {step_index}"
        );
        assert_eq!(
            state_root,
            produced_state.update_tree_hash_cache().unwrap(),
            "producer state at step {step_index}"
        );
        verify_execution_payload_envelope(
            &pre_state,
            &envelope,
            VerifySignatures::True,
            state_root,
            &harness.spec,
        )
        .unwrap();

        let body = block.message().body();
        coverage.blocks += 1;
        coverage.exits += body.voluntary_exits().len();
        coverage.proposer_slashings += body.proposer_slashings().len();
        coverage.attester_slashings += body.attester_slashings_len();
        coverage.bls_changes += body.bls_to_execution_changes().unwrap().len();
        coverage.attestations += body.attestations_len();
        coverage.payload_attestations += body.payload_attestations().unwrap().len();
        coverage.withdrawals += envelope.message.payload.withdrawals.len();
        coverage.full_withdrawals += envelope
            .message
            .payload
            .withdrawals
            .iter()
            .filter(|withdrawal| {
                pre_state
                    .get_validator(withdrawal.validator_index as usize)
                    .is_ok_and(|validator| {
                        validator.withdrawable_epoch <= pre_state.current_epoch()
                    })
            })
            .count();
        coverage.pending_partial_withdrawals +=
            pre_state.pending_partial_withdrawals().unwrap().len();
        coverage.pending_consolidations += pre_state.pending_consolidations().unwrap().len();
        coverage.sync_aggregates += usize::from(body.sync_aggregate().unwrap().num_set_bits() > 0);
        let parent_requests = body.parent_execution_requests().unwrap();
        coverage.parent_requests += parent_requests.deposits.len()
            + parent_requests.withdrawals.len()
            + parent_requests.consolidations.len()
            + parent_requests.builder_deposits.len()
            + parent_requests.builder_exits.len();
        let bid = &body.signed_execution_payload_bid().unwrap().message;
        if bid.parent_block_hash
            == parent_state
                .latest_execution_payload_bid()
                .unwrap()
                .parent_block_hash
        {
            coverage.empty_parents += 1;
        }

        let verified_block = harness
            .chain
            .verify_block_for_gossip(block.clone())
            .await
            .unwrap_or_else(|error| {
                panic!("block gossip at step {step_index}, slot {slot}: {error:?}")
            });
        let imported = harness
            .chain
            .process_block(
                block_root,
                verified_block,
                NotifyExecutionLayer::Yes,
                BlockImportSource::Lookup,
                || Ok(()),
            )
            .await
            .unwrap();
        assert!(
            matches!(imported, AvailabilityProcessingStatus::Imported(_, root) if root == block_root)
        );
        // Verify even envelopes we withhold, so every locally produced artifact has an oracle.
        let verified_envelope = harness
            .chain
            .verify_envelope_for_gossip(envelope, EnvelopeSource::Gossip)
            .await
            .unwrap();
        if step.deliver_envelope {
            if block.num_expected_blobs() > 0 {
                harness.process_gossip_columns(&block, None).await;
            }
            let imported = harness
                .chain
                .process_execution_payload_envelope(
                    block_root,
                    verified_envelope,
                    NotifyExecutionLayer::Yes,
                    BlockImportSource::Lookup,
                    || Ok(()),
                )
                .await
                .unwrap();
            assert!(
                matches!(imported, AvailabilityProcessingStatus::Imported(_, root) if root == block_root)
            );
        }
        harness.chain.recompute_head_at_current_slot().await;
        // Attest to the produced block even if fork choice currently prefers another branch.
        let state = pre_state;
        let validators: Vec<_> = (0..step.participation)
            .filter(|index| {
                state
                    .get_validator(*index)
                    .unwrap()
                    .is_active_at(state.current_epoch())
            })
            .collect();
        harness.attest_block(&state, state_root, block_root.into(), &block, &validators);
        if step.sync {
            let current_period = slot
                .epoch(E::slots_per_epoch())
                .sync_committee_period(&harness.spec)
                .unwrap();
            let next_period = (slot + 1)
                .epoch(E::slots_per_epoch())
                .sync_committee_period(&harness.spec)
                .unwrap();
            harness.sync_committee_sign_block(
                &state,
                block_root,
                slot,
                if current_period == next_period {
                    RelativeSyncCommittee::Current
                } else {
                    RelativeSyncCommittee::Next
                },
            );
        }
        if step.payload_votes {
            let (messages, _) = harness.make_payload_attestation_messages(
                &state,
                block_root,
                slot,
                vec![PayloadAttestationVote {
                    validator_count: E::ptc_size(),
                    payload_present: step.deliver_envelope,
                    blob_data_available: step.deliver_envelope,
                }],
            );
            harness
                .import_payload_attestation_messages(messages)
                .unwrap();
        }
    }
    coverage
}

fn run(scenario: &Scenario) -> Coverage {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(run_scenario(scenario))
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: std::env::var("PROPTEST_CASES").map(|value| value.parse().expect("PROPTEST_CASES must be a number")).unwrap_or(16),
        max_shrink_iters: std::env::var("PROPTEST_MAX_SHRINK_ITERS").map(|value| value.parse().expect("PROPTEST_MAX_SHRINK_ITERS must be a number")).unwrap_or(128),
        ..ProptestConfig::default()
    })]
    #[test]
    fn produced_blocks_pass_transition_and_gossip(scenario in scenarios()) {
        let coverage = run(&scenario);
        prop_assert_eq!(coverage.blocks, scenario.steps.len() + if scenario.builders { 4 * E::slots_per_epoch() as usize } else { 0 });
    }
}

// This exercises each operation family even if a short random campaign happens to choose
// empty lists. In particular, the second block's voluntary exit overlaps its parent's
// full-exit request, and the third block must build on an empty parent.
#[test]
fn operation_coverage() {
    let baseline = baseline_step();
    let mut steps = vec![baseline.clone(); 16];
    steps[0].requests = vec![
        (0, 0, 0, 1), // deposit top-up
        (1, 1, 0, 1), // full exit
        (2, 2, 0, 1), // partial withdrawal
        (3, 4, 0, 1), // switch to compounding
        (4, 7, 8, 1), // consolidation
    ];
    steps[1].operations = vec![(0, 1), (0, 10), (1, 11), (2, 12), (3, 0)];
    steps[1].deliver_envelope = false;
    steps[2].blobs = true;
    for step in &mut steps[8..] {
        step.slot_delta = E::slots_per_epoch();
    }
    let coverage = run(&Scenario {
        validators: (0..VALIDATORS)
            .map(|index| ((index % 3) as u8, 8))
            .collect(),
        builders: false,
        steps,
    });
    assert_eq!(coverage.blocks, 16);
    assert!(coverage.exits > 0, "{coverage:?}");
    assert!(coverage.proposer_slashings > 0, "{coverage:?}");
    assert!(coverage.attester_slashings > 0, "{coverage:?}");
    assert!(coverage.bls_changes > 0, "{coverage:?}");
    assert!(coverage.attestations > 0, "{coverage:?}");
    assert!(coverage.payload_attestations > 0, "{coverage:?}");
    assert!(coverage.withdrawals > 0, "{coverage:?}");
    assert!(coverage.full_withdrawals > 0, "{coverage:?}");
    assert!(coverage.pending_partial_withdrawals > 0, "{coverage:?}");
    assert!(coverage.pending_consolidations > 0, "{coverage:?}");
    assert!(coverage.sync_aggregates > 0, "{coverage:?}");
    assert!(coverage.parent_requests >= 5, "{coverage:?}");
    assert!(coverage.empty_parents > 0, "{coverage:?}");
}

fn baseline_step() -> Step {
    Step {
        slot_delta: 1,
        cache_advanced_state: false,
        deliver_envelope: true,
        blobs: false,
        participation: VALIDATORS,
        sync: true,
        payload_votes: true,
        builder: 0,
        bid_value: 20_000_000,
        operations: vec![],
        requests: vec![],
    }
}

#[test]
fn parent_state_cache_coverage() {
    let mut steps = vec![baseline_step(); 4];
    steps[1].cache_advanced_state = true;
    steps[2].slot_delta = E::slots_per_epoch();
    steps[3].cache_advanced_state = true;
    steps[3].slot_delta = E::slots_per_epoch();
    let coverage = run(&Scenario {
        validators: vec![(1, 1); VALIDATORS],
        builders: false,
        steps,
    });
    assert_eq!(coverage.advanced_states, 2);
    assert_eq!(coverage.unadvanced_states, 2);
    assert_eq!(coverage.blocks, 4);
}

#[test]
fn builder_coverage() {
    let mut steps = vec![baseline_step(); 5];
    steps[0].builder = 1;
    steps[0].requests = vec![(1, 1, 0, 1), (2, 2, 0, 1)];
    steps[1].builder = 2;
    steps[1].blobs = true;
    steps[1].operations = vec![(0, 1)];
    steps[1].deliver_envelope = false;
    steps[2].builder = 1;
    steps[2].slot_delta = 2;
    steps[3].builder = 2;
    steps[3].bid_value = 0;
    let coverage = run(&Scenario {
        validators: (0..VALIDATORS)
            .map(|index| ((index % 3) as u8, 8))
            .collect(),
        builders: true,
        steps,
    });
    assert_eq!(coverage.gossip_builds, 2, "{coverage:?}");
    assert_eq!(coverage.direct_builds, 1, "{coverage:?}");
}

#[test]
fn builder_exit_before_bid_selection() {
    for builder in [1, 2] {
        let mut steps = vec![baseline_step(); 3];
        steps[0].requests = vec![(5, 0, 0, 1), (6, 0, 0, 1)];
        steps[1].builder = builder;
        let coverage = run(&Scenario {
            validators: vec![(1, 1); VALIDATORS],
            builders: true,
            steps,
        });
        assert_eq!(
            coverage.gossip_builds + coverage.direct_builds,
            0,
            "{coverage:?}"
        );
        assert!(coverage.parent_requests >= 2, "{coverage:?}");
    }
}
