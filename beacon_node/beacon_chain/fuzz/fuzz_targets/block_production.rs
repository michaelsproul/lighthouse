#![no_main]

use arbitrary::Unstructured;
use beacon_chain::attestation_verification::Error as AttestationError;
use beacon_chain::beacon_proposer_cache::compute_proposer_duties_from_head;
use beacon_chain::builder::BeaconChainBuilder;
use beacon_chain::slot_clock::SlotClock;
use beacon_chain::test_utils::{
    test_spec, AttestationStrategy, BeaconChainHarness, BlockStrategy, EphemeralHarnessType,
};
use beacon_chain::{
    historical_blocks::HistoricalBlockError, migrate::MigratorConfig, BeaconChain,
    BeaconChainError, BeaconChainTypes, BeaconSnapshot, ChainConfig, ServerSentEventHandler,
    WhenSlotSkipped,
};
use lazy_static::lazy_static;
use libfuzzer_sys::fuzz_target;
use logging::test_logger;
use rand::Rng;
use state_processing::BlockReplayer;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::convert::TryInto;
use std::sync::Arc;
use std::time::Duration;
use store::{
    iter::{BlockRootsIterator, StateRootsIterator},
    HotColdDB, LevelDB, StoreConfig,
};
use tokio::runtime::Runtime;
use tree_hash::TreeHash;
use types::test_utils::{SeedableRng, XorShiftRng};
use types::*;

pub const NUM_HONEST_NODES: usize = 9;
pub const TOTAL_VALIDATORS: usize = 80;
pub const ATTACKER_VALIDATORS: usize = TOTAL_VALIDATORS / 10;
pub const HONEST_VALIDATORS: usize = TOTAL_VALIDATORS - ATTACKER_VALIDATORS;

pub const TICKS_PER_SLOT: usize = 3;
pub const MAX_DELAY_TICKS: usize = TICKS_PER_SLOT * 2;

pub const ATTESTATION_TICK: usize = TICKS_PER_SLOT / 3;
pub const AGGREGATE_TICK: usize = 2 * ATTESTATION_TICK;

lazy_static! {
    /// A cached set of keys.
    static ref KEYPAIRS: Vec<Keypair> = types::test_utils::generate_deterministic_keypairs(TOTAL_VALIDATORS);
}

type E = MinimalEthSpec;
type TestHarness = BeaconChainHarness<EphemeralHarnessType<E>>;

fn get_harness() -> TestHarness {
    let spec = test_spec::<E>();

    let harness = BeaconChainHarness::builder(MinimalEthSpec)
        .spec(spec)
        .keypairs(KEYPAIRS.to_vec())
        .fresh_ephemeral_store()
        .mock_execution_layer()
        .build();

    harness.advance_slot();
    harness
}

pub enum Message {
    Attestation(Attestation<E>),
    Block(SignedBeaconBlock<E>),
}

struct Node {
    harness: TestHarness,
    /// Queue of ordered `(tick, message)` pairs.
    ///
    /// Each `message` will be delivered to the node at `tick`.
    message_queue: VecDeque<(usize, Message)>,
    /// Validator indices assigned to this node.
    validators: Vec<usize>,
}

impl Node {
    fn deliver_message(&self, message: Message) {
        match message {
            Message::Attestation(att) => {
                let _ = self.harness.process_unaggregated_attestation(att);
            }
            Message::Block(block) => {
                let _ = self.harness.process_block_result(block);
            }
        }
    }
}

#[derive(arbitrary::Arbitrary)]
enum AttackerAction {
    ChangeParentBlock,
    ChangeAttestationTarget,
    ChangeDelays,
    DoNothing,
}

fuzz_target!(|data: &[u8]| {
    let rt = Runtime::new().unwrap();
    if let Err(arbitrary::Error::EmptyChoose | arbitrary::Error::IncorrectFormat) =
        rt.block_on(run(data))
    {
        panic!("bad arbitrary usage");
    }
});

async fn run(data: &[u8]) -> arbitrary::Result<()> {
    let mut u = Unstructured::new(data);
    let spec = test_spec::<E>();
    let slots_per_epoch = E::slots_per_epoch() as usize;

    // Create honest nodes.
    let validators_per_node = HONEST_VALIDATORS / NUM_HONEST_NODES;
    let mut honest_nodes = (0..NUM_HONEST_NODES)
        .map(|i| {
            let harness = get_harness();
            let validators = (i * validators_per_node..(i + 1) * validators_per_node).collect();
            Node {
                harness,
                message_queue: VecDeque::new(),
                validators,
            }
        })
        .collect::<Vec<_>>();

    // Set up attacker values.
    let attacker = Node {
        harness: get_harness(),
        message_queue: VecDeque::new(),
        validators: (TOTAL_VALIDATORS - ATTACKER_VALIDATORS..TOTAL_VALIDATORS).collect(),
    };
    let mut attacker_parent_block = 0;
    let mut attacker_attestation_target = 0;

    // Simulation parameters.
    let mut tick = 0;
    let mut current_time = *attacker.harness.chain.slot_clock.genesis_duration();
    let tick_duration = Duration::from_secs(spec.seconds_per_slot) / TICKS_PER_SLOT as u32;

    let mut all_blocks = vec![attacker.harness.head_block_root()];
    let mut message_delays = vec![0; NUM_HONEST_NODES];

    // Generate events while the input is non-empty.
    while !u.is_empty() {
        // Generate attacker action.
        let attacker_action = u.arbitrary()?;
        match attacker_action {
            AttackerAction::ChangeParentBlock => {
                attacker_parent_block = u.choose_index(all_blocks.len())?;
            }
            AttackerAction::ChangeAttestationTarget => {
                attacker_attestation_target = u.choose_index(all_blocks.len())?;
            }
            AttackerAction::ChangeDelays => {
                message_delays = (0..NUM_HONEST_NODES)
                    .map(|_| u.int_in_range(0..=MAX_DELAY_TICKS))
                    .collect::<Result<_, _>>()?;
            }
            AttackerAction::DoNothing => {}
        }

        // Slot start activities for honest nodes.
        let current_slot = attacker.harness.chain.slot_clock.now().unwrap();
        let current_epoch = current_slot.epoch(E::slots_per_epoch());

        if tick % TICKS_PER_SLOT == 0 {
            let mut new_blocks = vec![];

            // Produce block(s).
            for node in &mut honest_nodes {
                let (proposers, _, _, _) =
                    compute_proposer_duties_from_head(current_epoch, &node.harness.chain).unwrap();
                let current_slot_proposer = proposers[current_slot.as_usize() % slots_per_epoch];

                if !node.validators.contains(&current_slot_proposer) {
                    continue;
                }

                let head_state = node.harness.get_current_state();
                let (block, block_root) = node.harness.make_block(head_state, current_slot).await;
                new_blocks.push((block, block_root));
            }

            // New honest blocks get delivered instantly.
            for (block, _) in new_blocks {
                let block_root = block.canonical_root();
                for node in &honest_nodes {
                    node.deliver_message(Message::Block(block.clone()));
                }
                attacker.deliver_message(Message::Block(block));
                all_blocks.push(block_root);
            }
        }

        // Slot start activities for the attacker.
        if tick % TICKS_PER_SLOT == 0 {
            let new_blocks = vec![];
            let chain = &attacker.harness.chain;

            // Produce block(s).
            for (head_block_root, _) in chain.heads() {
                let head_block = chain.get_blinded_block(&head_block_root).unwrap().unwrap();
                let state_root = head_block.state_root();
                let mut head_state = chain
                    .get_state(&state_root, Some(head_block.slot()))
                    .unwrap()
                    .unwrap();

                ensure_state_is_in_epoch(&mut head_state, state_root, current_epoch, &spec)
                    .unwrap();

                let current_slot_proposer = head_state
                    .get_beacon_proposer_index(current_slot, &spec)
                    .unwrap();

                if !attacker.validators.contains(&current_slot_proposer) {
                    continue;
                }

                // FIXME(sproul): pull up tips from prior canonical blocks
                // FIXME(sproul): keep going here
                let (block, block_root) =
                    attacker.harness.make_block(head_state, current_slot).await;
                new_blocks.push((block, block_root));
            }
        }

        // Increment clock on each node and deliver messages.
        tick += 1;
        current_time += tick_duration;

        for node in &mut honest_nodes {
            node.harness.chain.slot_clock.set_current_time(current_time);

            loop {
                match node.message_queue.front() {
                    Some((mtick, _)) if *mtick == tick => {
                        let (_, message) = node.message_queue.pop_front().unwrap();
                        node.deliver_message(message);
                    }
                    _ => break,
                }
            }
        }
    }
    println!("finished a run that generated {} blocks", all_blocks.len());
    Ok(())
}
