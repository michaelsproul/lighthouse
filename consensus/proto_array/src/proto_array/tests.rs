use super::*;
use types::MainnetEthSpec;

struct FilterTest {
    array: ProtoArray,
    spec: ChainSpec,
    justified: Checkpoint,
    finalized: Checkpoint,
    current_slot: Slot,
}

impl FilterTest {
    fn new() -> Self {
        let mut spec = MainnetEthSpec::default_spec();
        spec.gloas_fork_epoch = Some(Epoch::new(0));
        let mut test = Self {
            array: ProtoArray {
                prune_threshold: 0,
                nodes: vec![],
                indices: HashMap::new(),
                children: vec![],
            },
            spec,
            justified: Checkpoint {
                epoch: Epoch::new(1),
                root: root(1),
            },
            finalized: Checkpoint::default(),
            current_slot: Slot::new(128),
        };
        test.add_block(0, None, 0, PayloadStatus::Empty, false);
        test
    }

    fn add_block(
        &mut self,
        id: u64,
        parent: Option<u64>,
        voting_source_epoch: u64,
        parent_status: PayloadStatus,
        payload_received: bool,
    ) {
        let slot = Slot::new(id.saturating_mul(32));
        let shuffling_id = AttestationShufflingId::from_components(Epoch::new(0), root(0));
        let justified_checkpoint = Checkpoint {
            epoch: Epoch::new(voting_source_epoch),
            root: root(voting_source_epoch),
        };
        self.array
            .on_block::<MainnetEthSpec>(
                Block {
                    slot,
                    root: root(id),
                    parent_root: parent.map(root),
                    state_root: root(0),
                    target_root: root(0),
                    current_epoch_shuffling_id: shuffling_id.clone(),
                    next_epoch_shuffling_id: shuffling_id,
                    justified_checkpoint,
                    finalized_checkpoint: self.finalized,
                    unrealized_justified_checkpoint: Some(justified_checkpoint),
                    unrealized_finalized_checkpoint: Some(self.finalized),
                    execution_status: ExecutionStatus::irrelevant(),
                    execution_payload_block_hash: Some(ExecutionBlockHash::from_root(root(id))),
                    execution_payload_parent_hash: Some(ExecutionBlockHash::from_root(
                        if parent_status == PayloadStatus::Full {
                            root(parent.unwrap())
                        } else {
                            root(99)
                        },
                    )),
                    proposer_index: Some(0),
                    payload_received,
                },
                slot,
                &self.spec,
                Duration::ZERO,
            )
            .unwrap();
        self.node_mut(id).payload_received = payload_received;
    }

    fn node_mut(&mut self, id: u64) -> &mut ProtoNodeV29 {
        let index = *self.array.indices.get(&root(id)).unwrap();
        self.array
            .nodes
            .get_mut(index)
            .unwrap()
            .as_v29_mut()
            .unwrap()
    }

    fn filtered(&self) -> HashSet<(usize, PayloadStatus)> {
        self.array
            .get_filtered_node_tree::<MainnetEthSpec>(
                *self.array.indices.get(&self.justified.root).unwrap(),
                self.current_slot,
                self.justified,
                self.finalized,
            )
            .unwrap()
    }

    fn assert_head(&self, id: u64, status: PayloadStatus) {
        let balances = JustifiedBalances::default();
        assert_eq!(
            self.array
                .find_head::<MainnetEthSpec>(
                    &self.justified.root,
                    self.current_slot,
                    self.justified,
                    self.finalized,
                    root(0),
                    &balances,
                    &self.spec,
                )
                .unwrap(),
            (root(id), status)
        );
        assert_eq!(
            self.array
                .get_canonical_payload_status::<MainnetEthSpec>(
                    root(id),
                    self.current_slot,
                    self.justified,
                    self.finalized,
                    root(0),
                    &balances,
                    &self.spec,
                )
                .unwrap(),
            status
        );
    }

    fn assert_leaves(&self, expected: Vec<(Hash256, PayloadStatus, u64)>) {
        let mut actual = self
            .array
            .filtered_node_tree_leaves_and_weights::<MainnetEthSpec>(
                &self.justified.root,
                self.current_slot,
                self.justified,
                self.finalized,
                root(0),
                &JustifiedBalances::default(),
                &self.spec,
            )
            .unwrap();
        actual.sort_by_key(|(root, status, _)| (*root, *status as u8));
        assert_eq!(actual, expected);
    }
}

fn root(id: u64) -> Hash256 {
    Hash256::from_low_u64_be(id)
}

#[test]
fn childless_unviable_variants_are_pruned() {
    for child_status in [PayloadStatus::Empty, PayloadStatus::Full] {
        let mut test = FilterTest::new();
        // B fails FFG, but its child K passes. Only the variant extended by K survives.
        test.add_block(1, Some(0), 0, PayloadStatus::Empty, true);
        test.add_block(2, Some(1), 1, child_status, false);
        // Make the childless variant heavier so an unfiltered walk would select it.
        let parent = test.node_mut(1);
        parent.weight = 10;
        match child_status {
            PayloadStatus::Empty => parent.full_payload_weight = 10,
            PayloadStatus::Full => parent.empty_payload_weight = 10,
            PayloadStatus::Pending => unreachable!(),
        }
        assert_eq!(
            test.filtered(),
            HashSet::from([
                (1, PayloadStatus::Pending),
                (1, child_status),
                (2, PayloadStatus::Pending),
                (2, PayloadStatus::Empty),
            ])
        );
        test.assert_head(2, PayloadStatus::Empty);
        test.assert_leaves(vec![(root(2), PayloadStatus::Empty, 0)]);
    }
}

#[test]
fn viable_childless_sibling_survives_unviable_descendants() {
    for child_status in [PayloadStatus::Empty, PayloadStatus::Full] {
        let mut test = FilterTest::new();
        // B passes FFG, but K fails. The variant with K as its only child is pruned,
        // while B's childless sibling remains viable and must become head.
        test.add_block(1, Some(0), 1, PayloadStatus::Empty, true);
        test.add_block(2, Some(1), 0, child_status, false);
        let parent = test.node_mut(1);
        parent.weight = 10;
        let sibling_status = match child_status {
            PayloadStatus::Empty => {
                parent.empty_payload_weight = 10;
                PayloadStatus::Full
            }
            PayloadStatus::Full => {
                parent.full_payload_weight = 10;
                PayloadStatus::Empty
            }
            PayloadStatus::Pending => unreachable!(),
        };
        assert_eq!(
            test.filtered(),
            HashSet::from([(1, PayloadStatus::Pending), (1, sibling_status)])
        );
        test.assert_head(1, sibling_status);
        test.assert_leaves(vec![(root(1), sibling_status, 0)]);
    }
}

#[test]
fn both_viable_childless_variants_are_retained() {
    let mut test = FilterTest::new();
    test.add_block(1, Some(0), 1, PayloadStatus::Empty, true);
    let node = test.node_mut(1);
    node.weight = 10;
    node.full_payload_weight = 10;
    assert_eq!(
        test.filtered(),
        HashSet::from([
            (1, PayloadStatus::Pending),
            (1, PayloadStatus::Empty),
            (1, PayloadStatus::Full),
        ])
    );
    test.assert_head(1, PayloadStatus::Full);
    test.assert_leaves(vec![
        (root(1), PayloadStatus::Empty, 0),
        (root(1), PayloadStatus::Full, 10),
    ]);
}

#[test]
fn unreceived_full_variant_and_its_descendants_are_unreachable() {
    let mut test = FilterTest::new();
    test.add_block(1, Some(0), 1, PayloadStatus::Empty, false);
    test.add_block(2, Some(1), 1, PayloadStatus::Full, false);
    assert_eq!(
        test.filtered(),
        HashSet::from([(1, PayloadStatus::Pending), (1, PayloadStatus::Empty)])
    );
    test.assert_head(1, PayloadStatus::Empty);
    test.assert_leaves(vec![(root(1), PayloadStatus::Empty, 0)]);
}

#[test]
fn empty_filtered_tree_returns_empty_justified_head() {
    let mut test = FilterTest::new();
    test.add_block(1, Some(0), 0, PayloadStatus::Empty, true);
    test.add_block(2, Some(1), 0, PayloadStatus::Empty, false);
    // A viable node outside the justified subtree must not suppress the fallback.
    test.add_block(3, Some(0), 1, PayloadStatus::Empty, false);
    let node = test.node_mut(1);
    node.weight = 10;
    node.full_payload_weight = 10;
    assert!(test.filtered().is_empty());
    test.assert_head(1, PayloadStatus::Empty);
    // The compliance helper walks from PENDING even when the filtered tree is empty.
    test.assert_leaves(vec![(root(1), PayloadStatus::Pending, 10)]);
}
