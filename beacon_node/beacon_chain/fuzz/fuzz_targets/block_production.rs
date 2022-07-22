#![no_main]

use arbitrary::Unstructured;
use beacon_chain::beacon_proposer_cache::compute_proposer_duties_from_head;
use beacon_chain::slot_clock::SlotClock;
use beacon_chain::test_utils::{test_spec, BeaconChainHarness, EphemeralHarnessType};
use beacon_chain_fuzz::Hydra;
use libfuzzer_sys::fuzz_target;
use std::collections::VecDeque;
use std::ops::ControlFlow;
use std::time::Duration;
use tokio::runtime::Runtime;
use types::{test_utils::generate_deterministic_keypairs, *};

const DEBUG_LOGS: bool = false;

type E = MinimalEthSpec;
type TestHarness = BeaconChainHarness<EphemeralHarnessType<E>>;

fn get_harness(keypairs: &[Keypair]) -> TestHarness {
    let spec = test_spec::<E>();

    let harness = BeaconChainHarness::builder(MinimalEthSpec)
        .spec(spec)
        .keypairs(keypairs.to_vec())
        .fresh_ephemeral_store()
        .mock_execution_layer()
        .build();
    harness
}

pub struct Config {
    pub num_honest_nodes: usize,
    pub total_validators: usize,
    pub attacker_validators: usize,
    pub ticks_per_slot: usize,
    pub min_attacker_proposers_per_slot: usize,
    pub max_attacker_proposers_per_slot: usize,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            num_honest_nodes: 6,
            total_validators: 90,
            attacker_validators: 30,
            ticks_per_slot: 3,
            min_attacker_proposers_per_slot: 0,
            max_attacker_proposers_per_slot: 4,
        }
    }
}

impl Config {
    pub fn is_valid(&self) -> bool {
        self.ticks_per_slot % 3 == 0
            && self.honest_validators() % self.num_honest_nodes == 0
            && self.max_attacker_proposers_per_slot >= self.min_attacker_proposers_per_slot
    }

    pub fn honest_validators(&self) -> usize {
        self.total_validators - self.attacker_validators
    }

    pub fn honest_validators_per_node(&self) -> usize {
        self.honest_validators() / self.num_honest_nodes
    }

    pub fn attestation_tick(&self) -> usize {
        self.ticks_per_slot / 3
    }

    pub fn is_block_proposal_tick(&self, tick: usize) -> bool {
        tick % self.ticks_per_slot == 0 && tick != 0
    }

    pub fn is_attestation_tick(&self, tick: usize) -> bool {
        tick % self.ticks_per_slot == self.attestation_tick()
    }

    pub fn min_attacker_proposers(&self, available: usize) -> Option<u32> {
        Some(std::cmp::min(self.min_attacker_proposers_per_slot, available) as u32)
    }

    pub fn max_attacker_proposers(&self, available: usize) -> Option<u32> {
        Some(std::cmp::min(self.max_attacker_proposers_per_slot, available) as u32)
    }

    pub fn tick_duration(&self, spec: &ChainSpec) -> Duration {
        Duration::from_secs(spec.seconds_per_slot) / self.ticks_per_slot as u32
    }
}

#[derive(Clone)]
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

async fn deliver_all(message: &Message, nodes: &[Node]) {
    for node in nodes {
        node.deliver_message(message.clone()).await;
    }
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
    let config = Config::default();
    let rt = Runtime::new().unwrap();
    if let Err(arbitrary::Error::EmptyChoose | arbitrary::Error::IncorrectFormat) =
        rt.block_on(run(data, config))
    {
        panic!("bad arbitrary usage");
    }
});

async fn run(data: &[u8], conf: Config) -> arbitrary::Result<()> {
    assert!(conf.is_valid());

    let mut u = Unstructured::new(data);
    let spec = test_spec::<E>();
    let slots_per_epoch = E::slots_per_epoch() as usize;

    let keypairs = generate_deterministic_keypairs(conf.total_validators);

    // Create honest nodes.
    let validators_per_node = conf.honest_validators_per_node();
    let mut honest_nodes = (0..conf.num_honest_nodes)
        .map(|i| {
            let harness = get_harness(&keypairs);
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
        harness: get_harness(&keypairs),
        message_queue: VecDeque::new(),
        validators: (conf.honest_validators()..conf.total_validators).collect(),
    };
    let mut hydra = Hydra::default();

    // Simulation parameters.
    let mut tick = 0;
    let mut current_time = *attacker.harness.chain.slot_clock.genesis_duration();
    let tick_duration = conf.tick_duration(&spec);

    let mut all_blocks = vec![(attacker.harness.head_block_root(), Slot::new(0))];

    // Generate events while the input is non-empty.
    while !u.is_empty() {
        let current_slot = attacker.harness.chain.slot_clock.now().unwrap();
        let current_epoch = current_slot.epoch(E::slots_per_epoch());

        // Slot start activities for honest nodes.
        if conf.is_block_proposal_tick(tick) {
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

        // Unaggregated attestations from the honest nodes.
        if conf.is_attestation_tick(tick) {
            let mut new_attestations = vec![];
            for node in &honest_nodes {
                let head = node.harness.chain.canonical_head.cached_head();
                let attestations = node.harness.make_unaggregated_attestations(
                    &node.validators,
                    &head.snapshot.beacon_state,
                    head.head_state_root(),
                    head.head_block_root().into(),
                    current_slot,
                );
                new_attestations.extend(
                    attestations
                        .into_iter()
                        .flat_map(|atts| atts.into_iter().map(|(att, _)| att)),
                );
            }
            for attestation in new_attestations {
                let message = Message::Attestation(attestation);
                deliver_all(&message, &honest_nodes).await;
                attacker.deliver_message(message).await;
            }
        }

        // Slot start activities for the attacker.
        if conf.is_block_proposal_tick(tick) {
            hydra.update(&attacker.harness, current_epoch, &spec);
            let proposer_heads =
                hydra.proposer_heads_at_slot(current_slot, &attacker.validators, &spec);
            if DEBUG_LOGS {
                println!(
                    "number of hydra heads at slot {}: {}, attacker proposers: {}",
                    current_slot,
                    hydra.num_heads(),
                    proposer_heads.len(),
                );
            }

            if !proposer_heads.is_empty() {
                let mut proposers = proposer_heads.iter();
                let mut selected_proposals = vec![];

                u.arbitrary_loop(
                    conf.min_attacker_proposers(proposers.len()),
                    conf.max_attacker_proposers(proposers.len()),
                    |ux| {
                        let (_, head_choices) = proposers.next().unwrap();
                        let (block_root, state_ref) = ux.choose(&head_choices)?;
                        let state: BeaconState<E> = (*state_ref).clone();

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
        "finished a run that generated {} blocks up to slot {}",
        all_blocks.len(),
        all_blocks.iter().map(|(_, slot)| slot).max().unwrap()
    );
    Ok(())
}
