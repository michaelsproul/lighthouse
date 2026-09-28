use super::*;
use types::MainnetEthSpec;

type E = MainnetEthSpec;

const R: u64 = 1;
const A: u64 = 2;
const A1: u64 = 3;
const B: u64 = 4;
const B1: u64 = 5;

fn root(block: u64) -> Hash256 {
    Hash256::from_low_u64_be(block)
}

fn hash(block: u64) -> ExecutionBlockHash {
    ExecutionBlockHash::from_root(root(block))
}

struct TestRig {
    fork_choice: ProtoArrayForkChoice,
    spec: ChainSpec,
    checkpoint: Checkpoint,
    balances: JustifiedBalances,
    equivocators: BTreeSet<u64>,
}

impl TestRig {
    // R -> A -> A1 and R -> B -> B1. Include a direct vote on A as well as
    // votes on both leaves to distinguish direct weight from descendant weight.
    fn new() -> Self {
        Self::with_anchor_status(ExecutionStatus::Valid(hash(R)))
    }

    fn with_anchor_status(anchor_status: ExecutionStatus) -> Self {
        let mut spec = E::default_spec();
        spec.gloas_fork_epoch = None;
        let checkpoint = Checkpoint {
            epoch: Epoch::new(0),
            root: root(R),
        };
        let shuffling = AttestationShufflingId::from_components(Epoch::new(0), root(R));
        let mut fork_choice = ProtoArrayForkChoice::new::<E>(
            Slot::new(0),
            Slot::new(0),
            root(R),
            checkpoint,
            checkpoint,
            shuffling.clone(),
            shuffling.clone(),
            anchor_status,
            None,
            None,
            0,
            &spec,
        )
        .unwrap();

        for (block, parent, slot, execution_status) in [
            (A, R, 1, ExecutionStatus::Optimistic(hash(A))),
            (A1, A, 2, ExecutionStatus::Optimistic(hash(A1))),
            (B, R, 1, ExecutionStatus::Valid(hash(B))),
            (B1, B, 2, ExecutionStatus::Valid(hash(B1))),
        ] {
            fork_choice
                .process_block::<E>(
                    Block {
                        slot: Slot::new(slot),
                        root: root(block),
                        parent_root: Some(root(parent)),
                        state_root: root(block),
                        target_root: root(R),
                        current_epoch_shuffling_id: shuffling.clone(),
                        next_epoch_shuffling_id: shuffling.clone(),
                        justified_checkpoint: checkpoint,
                        finalized_checkpoint: checkpoint,
                        execution_status,
                        unrealized_justified_checkpoint: Some(checkpoint),
                        unrealized_finalized_checkpoint: Some(checkpoint),
                        execution_payload_parent_hash: None,
                        execution_payload_block_hash: None,
                        proposer_index: Some(0),
                        payload_received: false,
                    },
                    Slot::new(3),
                    &spec,
                    Duration::ZERO,
                )
                .unwrap();
        }

        let mut rig = Self {
            fork_choice,
            spec,
            checkpoint,
            balances: JustifiedBalances::from_effective_balances(vec![32, 31, 16]).unwrap(),
            equivocators: BTreeSet::new(),
        };
        for (validator, block) in [A1, B1, A].into_iter().enumerate() {
            rig.vote(validator, block, 2);
        }
        assert_eq!(rig.head(), root(A1));
        rig.assert_weights([79, 48, 32, 31, 31]);
        rig
    }

    fn vote(&mut self, validator: usize, block: u64, slot: u64) {
        self.fork_choice
            .process_attestation(validator, root(block), Slot::new(slot), false)
            .unwrap();
    }

    fn head(&mut self) -> Hash256 {
        self.fork_choice
            .find_head::<E>(
                self.checkpoint,
                self.checkpoint,
                &self.balances,
                Hash256::zero(),
                &self.equivocators,
                Slot::new(5),
                &self.spec,
            )
            .unwrap()
            .root()
    }

    fn invalidate(&mut self) {
        self.fork_choice
            .process_execution_payload_invalidation::<E>(
                &InvalidationOperation::InvalidateOne { head_hash: hash(A) },
                self.checkpoint,
            )
            .unwrap();
        for block in [A, A1] {
            assert_eq!(
                self.fork_choice
                    .get_block(&root(block))
                    .unwrap()
                    .execution_status,
                ExecutionStatus::Invalid(hash(block))
            );
        }
    }

    fn reset(&mut self) {
        self.fork_choice
            .set_all_blocks_to_optimistic::<E>(&self.equivocators)
            .unwrap();
    }

    fn assert_weights(&self, expected: [u64; 5]) {
        for (block, weight) in [R, A, A1, B, B1].into_iter().zip(expected) {
            assert_eq!(
                self.fork_choice.get_weight(&root(block)),
                Some(weight),
                "weight of block {block}"
            );
        }
    }

    fn round_trip(&self) -> ProtoArrayForkChoice {
        ProtoArrayForkChoice::from_bytes(&self.fork_choice.as_bytes(), self.balances.clone())
            .unwrap()
    }
}

#[test]
fn optimistic_reset_restores_invalid_subtree() {
    for settle_invalidation in [false, true] {
        let mut rig = TestRig::new();
        rig.invalidate();
        if settle_invalidation {
            assert_eq!(rig.head(), root(B1));
            rig.assert_weights([31, 0, 0, 31, 31]);
        } else {
            rig.assert_weights([79, 48, 32, 31, 31]);
        }

        rig.reset();
        rig.assert_weights([79, 48, 32, 31, 31]);
        for block in [R, A, A1, B, B1] {
            assert_eq!(
                rig.fork_choice
                    .get_block(&root(block))
                    .unwrap()
                    .execution_status,
                ExecutionStatus::Optimistic(hash(block))
            );
        }
        assert_eq!(rig.head(), root(A1));
        rig.assert_weights([79, 48, 32, 31, 31]);
    }
}

#[test]
fn optimistic_reset_applies_pending_votes() {
    for (moves, expected, head) in [
        (vec![(0, B1)], [79, 16, 0, 63, 63], B1),
        (vec![(1, A1)], [79, 79, 63, 0, 0], A1),
        (vec![(0, B1), (1, A1)], [79, 47, 31, 32, 32], A1),
        (vec![(3, A1)], [87, 56, 40, 31, 31], A1),
    ] {
        let mut rig = TestRig::new();
        rig.balances = JustifiedBalances::from_effective_balances(vec![32, 31, 16, 8]).unwrap();
        rig.head();
        for &(validator, block) in &moves {
            rig.vote(validator, block, 3);
        }
        // The target was still eligible when each pending vote was registered.
        rig.invalidate();
        rig.reset();
        rig.assert_weights(expected);
        for (validator, block) in moves {
            let vote = rig.fork_choice.votes.0.get(validator).unwrap();
            assert_eq!(vote.current_root, root(block));
            assert_eq!(vote.current_slot, Slot::new(3));
        }
        assert_eq!(rig.head(), root(head));
        rig.assert_weights(expected);
    }
}

#[test]
fn optimistic_reset_moves_vote_out_of_zeroed_subtree() {
    let mut rig = TestRig::new();
    rig.invalidate();
    rig.head();
    rig.assert_weights([31, 0, 0, 31, 31]);
    rig.vote(0, B1, 3);
    rig.reset();
    rig.assert_weights([79, 16, 0, 63, 63]);
    assert_eq!(rig.head(), root(B1));
    rig.assert_weights([79, 16, 0, 63, 63]);
}

#[test]
fn optimistic_reset_excludes_equivocators() {
    for (equivocators, expected) in [
        (BTreeSet::from([0]), [47, 16, 0, 31, 31]),
        (BTreeSet::from([1]), [48, 48, 32, 0, 0]),
        (BTreeSet::from([0, 1, 2]), [0, 0, 0, 0, 0]),
    ] {
        for settle_slashing in [false, true] {
            for settle_invalidation in [false, true] {
                let mut rig = TestRig::new();
                if settle_slashing {
                    rig.equivocators = equivocators.clone();
                    rig.head();
                }
                rig.invalidate();
                if settle_invalidation {
                    rig.head();
                }
                // For a pending slashing, do not let the invalidation-settling
                // head calculation process the slashing as well.
                rig.equivocators = equivocators.clone();
                rig.reset();
                rig.assert_weights(expected);
                for &validator in &equivocators {
                    assert_eq!(
                        rig.fork_choice
                            .votes
                            .0
                            .get(validator as usize)
                            .unwrap()
                            .current_root,
                        Hash256::zero()
                    );
                    rig.vote(validator as usize, B1, 4);
                }
                rig.reset();
                rig.assert_weights(expected);
                rig.head();
                rig.assert_weights(expected);
            }
        }
    }
}

#[test]
fn optimistic_reset_discards_equivocators_pending_vote() {
    let mut rig = TestRig::new();
    rig.vote(0, B1, 3);
    rig.equivocators.insert(0);
    rig.invalidate();
    rig.reset();
    rig.assert_weights([47, 16, 0, 31, 31]);
    assert_eq!(rig.head(), root(B1));
    rig.assert_weights([47, 16, 0, 31, 31]);
}

#[test]
fn optimistic_reset_survives_persistence_with_pending_updates() {
    for (equivocators, expected, head) in [
        (BTreeSet::new(), [79, 16, 0, 63, 63], B1),
        (BTreeSet::from([0]), [47, 16, 0, 31, 31], B1),
        (BTreeSet::from([1]), [48, 16, 0, 32, 32], B1),
    ] {
        let mut rig = TestRig::new();
        rig.vote(0, B1, 3);
        rig.equivocators = equivocators;
        rig.invalidate();
        let restored = rig.round_trip();
        assert!(restored == rig.fork_choice);
        rig.fork_choice = restored;
        rig.reset();
        rig.assert_weights(expected);
        assert_eq!(rig.head(), root(head));
        rig.assert_weights(expected);
    }
}

#[test]
fn optimistic_reset_is_idempotent() {
    let mut rig = TestRig::new();
    rig.vote(0, B1, 3);
    rig.equivocators.insert(1);
    rig.invalidate();
    rig.reset();
    rig.assert_weights([48, 16, 0, 32, 32]);
    let snapshot = rig.round_trip();

    rig.reset();
    assert!(rig.fork_choice == snapshot);
    assert_eq!(rig.head(), root(B1));
    assert!(rig.fork_choice == snapshot);
    rig.reset();
    assert!(rig.fork_choice == snapshot);
    assert_eq!(rig.head(), root(B1));
}

#[test]
fn optimistic_reset_handles_pruned_votes() {
    for move_pruned_vote in [false, true] {
        let mut rig = TestRig::new();
        // Only the R -> A -> A1 path has votes. B and B1 remain in the array
        // after pruning, but become disconnected and should keep zero weight.
        rig.vote(0, R, 3);
        rig.vote(1, A1, 3);
        rig.head();
        rig.fork_choice.set_prune_threshold(0);
        rig.fork_choice.maybe_prune(root(A)).unwrap();
        rig.checkpoint.root = root(A);
        assert!(!rig.fork_choice.proto_array.indices.contains_key(&root(R)));
        if move_pruned_vote {
            rig.vote(0, A1, 4);
        }
        rig.reset();
        let (ancestor_weight, leaf_weight) = if move_pruned_vote { (79, 63) } else { (47, 31) };
        for _ in 0..2 {
            assert_eq!(rig.fork_choice.get_weight(&root(R)), None);
            for (block, expected) in [(A, ancestor_weight), (A1, leaf_weight), (B, 0), (B1, 0)] {
                assert_eq!(rig.fork_choice.get_weight(&root(block)), Some(expected));
            }
            assert_eq!(rig.head(), root(A1));
        }
    }
}

#[test]
fn optimistic_reset_preserves_subsequent_delta_accounting() {
    for (balances, new_vote, expected, head) in [
        (vec![31, 32, 0], None, [63, 31, 31, 32, 32], B1),
        (vec![31], None, [31, 31, 31, 0, 0], A1),
        (vec![31, 32, 0, 8], Some((3, A1)), [71, 39, 39, 32, 32], A1),
        (vec![31, 32, 0], Some((0, B1)), [63, 0, 0, 63, 63], B1),
    ] {
        let mut rig = TestRig::new();
        rig.invalidate();
        rig.head();
        rig.reset();
        rig.assert_weights([79, 48, 32, 31, 31]);
        if let Some((validator, block)) = new_vote {
            rig.vote(validator, block, 3);
        }
        rig.balances = JustifiedBalances::from_effective_balances(balances).unwrap();
        assert_eq!(rig.head(), root(head));
        rig.assert_weights(expected);
        assert_eq!(rig.head(), root(head));
        rig.assert_weights(expected);
    }
}

#[test]
fn optimistic_reset_without_invalid_blocks_preserves_weights() {
    for anchor_status in [
        ExecutionStatus::Valid(hash(R)),
        ExecutionStatus::irrelevant(),
    ] {
        let mut rig = TestRig::with_anchor_status(anchor_status);
        rig.reset();
        rig.assert_weights([79, 48, 32, 31, 31]);
        for block in [R, A, A1, B, B1] {
            let expected = if block == R && !anchor_status.is_execution_enabled() {
                anchor_status
            } else {
                ExecutionStatus::Optimistic(hash(block))
            };
            assert_eq!(
                rig.fork_choice
                    .get_block(&root(block))
                    .unwrap()
                    .execution_status,
                expected
            );
        }
        assert_eq!(rig.head(), root(A1));
        rig.assert_weights([79, 48, 32, 31, 31]);
    }
}
