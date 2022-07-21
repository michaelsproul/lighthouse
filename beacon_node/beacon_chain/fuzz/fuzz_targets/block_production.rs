#![no_main]

use arbitrary::Unstructured;
use beacon_chain::beacon_proposer_cache::compute_proposer_duties_from_head;
use beacon_chain::slot_clock::SlotClock;
use beacon_chain::test_utils::{test_spec, BeaconChainHarness, EphemeralHarnessType};
use lazy_static::lazy_static;
use libfuzzer_sys::fuzz_target;
use state_processing::state_advance::complete_state_advance;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ops::ControlFlow;
use std::time::Duration;
use tokio::runtime::Runtime;
use types::*;

pub const NUM_HONEST_NODES: usize = 9;
pub const TOTAL_VALIDATORS: usize = 80;
pub const ATTACKER_VALIDATORS: usize = TOTAL_VALIDATORS / 10;
pub const HONEST_VALIDATORS: usize = TOTAL_VALIDATORS - ATTACKER_VALIDATORS;

pub const TICKS_PER_SLOT: usize = 3;
pub const MAX_DELAY_TICKS: usize = TICKS_PER_SLOT * 2;

pub const ATTESTATION_TICK: usize = TICKS_PER_SLOT / 3;
pub const AGGREGATE_TICK: usize = 2 * ATTESTATION_TICK;

pub const MIN_ATTACKER_PROPOSERS_PER_SLOT: u32 = 0;
pub const MAX_ATTACKER_PROPOSERS_PER_SLOT: u32 = 4;

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
    async fn deliver_message(&self, message: Message) {
        match message {
            Message::Attestation(att) => match self.harness.process_unaggregated_attestation(att) {
                Ok(()) => (),
                Err(_) => (),
            },
            Message::Block(block) => {
                self.harness
                    .process_block_result(block)
                    .await
                    .expect("blocks should always apply");
            }
        }
    }
}

fuzz_target!(|data: &[u8]| {
    let rt = Runtime::new().unwrap();
    if let Err(arbitrary::Error::EmptyChoose | arbitrary::Error::IncorrectFormat) =
        rt.block_on(run(data))
    {
        panic!("bad arbitrary usage");
    }
});

#[derive(Default)]
struct Hydra {
    states: HashMap<Hash256, BeaconState<E>>,
}

impl Hydra {
    fn update(&mut self, harness: &TestHarness, current_epoch: Epoch, spec: &ChainSpec) {
        let finalized_checkpoint = harness
            .chain
            .canonical_head
            .cached_head()
            .finalized_checkpoint();
        let finalized_slot = finalized_checkpoint.epoch.start_slot(E::slots_per_epoch());

        // Pull up every block on every viable chain that descends from finalization.
        for (head_block_root, _) in harness.chain.heads() {
            let relevant_block_roots = harness
                .chain
                .rev_iter_block_roots_from(head_block_root)
                .unwrap()
                .map(Result::unwrap)
                .take_while(|(_, slot)| *slot >= finalized_slot)
                .map(|(block_root, _)| block_root)
                .collect::<Vec<_>>();

            // Discard this head if it isn't descended from the finalized checkpoint (special case
            // for genesis).
            if relevant_block_roots.last() != Some(&finalized_checkpoint.root)
                && finalized_slot != 0
            {
                continue;
            }

            for block_root in relevant_block_roots {
                self.ensure_block(harness, block_root, current_epoch, spec);
            }
        }

        // Prune all stale heads.
        self.prune(finalized_checkpoint);
    }

    fn ensure_block(
        &mut self,
        harness: &TestHarness,
        block_root: Hash256,
        current_epoch: Epoch,
        spec: &ChainSpec,
    ) {
        let state = self.states.entry(block_root).or_insert_with(|| {
            let block = harness
                .chain
                .get_blinded_block(&block_root)
                .unwrap()
                .unwrap();
            let mut state = harness.get_hot_state(block.state_root().into()).unwrap();
            state.build_all_caches(spec).unwrap();
            state
        });
        if state.current_epoch() != current_epoch {
            complete_state_advance(
                state,
                None,
                current_epoch.start_slot(E::slots_per_epoch()),
                spec,
            )
            .unwrap();
        }
    }

    fn prune(&mut self, finalized_checkpoint: Checkpoint) {
        self.states.retain(|_, state| {
            state.finalized_checkpoint() == finalized_checkpoint
                || state.finalized_checkpoint().epoch == 0
        })
    }

    fn num_heads(&self) -> usize {
        self.states.len()
    }

    fn proposer_heads_at_slot(
        &self,
        slot: Slot,
        validator_indices: &[usize],
        spec: &ChainSpec,
    ) -> BTreeMap<usize, Vec<(Hash256, &BeaconState<E>)>> {
        let mut proposer_heads = BTreeMap::new();

        for (block_root, state) in &self.states {
            let proposer = state.get_beacon_proposer_index(slot, spec).unwrap();
            if validator_indices.contains(&proposer) {
                proposer_heads
                    .entry(proposer)
                    .or_insert_with(Vec::new)
                    .push((*block_root, state));
            }
        }

        // Sort vecs to establish deterministic ordering.
        for (_, proposal_opps) in &mut proposer_heads {
            proposal_opps.sort_by_key(|(block_root, _)| *block_root);
        }

        proposer_heads
    }
}

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
    let mut hydra = Hydra::default();

    // Simulation parameters.
    let mut tick = 0;
    let mut current_time = *attacker.harness.chain.slot_clock.genesis_duration();
    let tick_duration = Duration::from_secs(spec.seconds_per_slot) / TICKS_PER_SLOT as u32;

    let mut all_blocks = vec![(attacker.harness.head_block_root(), Slot::new(0))];

    // Generate events while the input is non-empty.
    while !u.is_empty() {
        // Slot start activities for honest nodes.
        let current_slot = attacker.harness.chain.slot_clock.now().unwrap();
        let current_epoch = current_slot.epoch(E::slots_per_epoch());

        if tick % TICKS_PER_SLOT == 0 && tick != 0 {
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
                let (block, _) = node.harness.make_block(head_state, current_slot).await;
                new_blocks.push(block);
            }

            // New honest blocks get delivered instantly.
            for block in new_blocks {
                let block_root = block.canonical_root();
                let slot = block.slot();
                for node in &honest_nodes {
                    node.deliver_message(Message::Block(block.clone())).await;
                }
                attacker.deliver_message(Message::Block(block)).await;
                all_blocks.push((block_root, slot));
            }
        }

        // Slot start activities for the attacker.
        if tick % TICKS_PER_SLOT == 0 && tick != 0 {
            hydra.update(&attacker.harness, current_epoch, &spec);
            println!(
                "number of hydra heads at slot {}: {}",
                current_slot,
                hydra.num_heads()
            );
            let proposer_heads =
                hydra.proposer_heads_at_slot(current_slot, &attacker.validators, &spec);
            println!("number of attacker proposers: {}", proposer_heads.len());

            if !proposer_heads.is_empty() {
                let mut proposers = proposer_heads.iter();
                let mut selected_proposals = vec![];

                u.arbitrary_loop(
                    Some(MIN_ATTACKER_PROPOSERS_PER_SLOT),
                    Some(std::cmp::min(
                        MAX_ATTACKER_PROPOSERS_PER_SLOT,
                        proposer_heads.len() as u32,
                    )),
                    |ux| {
                        let (proposer_index, head_choices) = proposers.next().unwrap();
                        let (block_root, state_ref) = ux.choose(&head_choices)?;
                        let state: BeaconState<E> = (*state_ref).clone();

                        println!("proposing a block from {proposer_index} on {block_root:?}");
                        selected_proposals.push((block_root, state));
                        Ok(ControlFlow::Continue(()))
                    },
                )?;

                for (_parent_block_root, state) in selected_proposals {
                    let (block, _) = attacker.harness.make_block(state, current_slot).await;

                    // FIXME(sproul): delay delivery
                    let block_root = block.canonical_root();
                    let slot = block.slot();
                    for node in &honest_nodes {
                        node.deliver_message(Message::Block(block.clone())).await;
                    }
                    attacker.deliver_message(Message::Block(block)).await;
                    all_blocks.push((block_root, slot));
                }
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
                        node.deliver_message(message).await;
                    }
                    _ => break,
                }
            }
        }
        attacker
            .harness
            .chain
            .slot_clock
            .set_current_time(current_time);
    }
    println!(
        "finished a run that generated {} blocks at slots: {:?}",
        all_blocks.len(),
        all_blocks.iter().map(|(_, slot)| slot).collect::<Vec<_>>()
    );
    Ok(())
}
