use parking_lot::RwLock;
use ssz_derive::{Decode, Encode};
use std::collections::{HashMap, HashSet};
use std::iter::{self, FromIterator};
use types::{Hash256, Slot};

// FIXME(sproul): docs
#[derive(Debug)]
pub struct BlockTree {
    nodes: RwLock<HashMap<Hash256, Node>>,
}

#[derive(Debug, PartialEq)]
pub enum BlockTreeError {
    PrevUnknown(Hash256),
}

#[derive(Debug, Clone, Encode, Decode)]
struct Node {
    previous: Hash256,
    slot: Slot,
}

impl BlockTree {
    pub fn new(root_hash: Hash256, root_slot: Slot) -> Self {
        Self {
            nodes: RwLock::new(HashMap::from_iter(iter::once((
                root_hash,
                Node {
                    previous: Hash256::zero(),
                    slot: root_slot,
                },
            )))),
        }
    }

    pub fn is_known_block_root(&self, block_root: &Hash256) -> bool {
        self.nodes.read().contains_key(block_root)
    }

    pub fn add_block_root(
        &self,
        block_root: Hash256,
        prev_block_root: Hash256,
        block_slot: Slot,
    ) -> Result<(), BlockTreeError> {
        let mut nodes = self.nodes.write();
        if nodes.contains_key(&prev_block_root) {
            nodes.insert(
                block_root,
                Node {
                    previous: prev_block_root,
                    slot: block_slot,
                },
            );
            Ok(())
        } else {
            Err(BlockTreeError::PrevUnknown(prev_block_root))
        }
    }

    pub fn iter_from(&self, block_root: Hash256) -> BlockTreeIter {
        BlockTreeIter {
            tree: self,
            current_block_root: block_root,
        }
    }

    pub fn prune_to(&self, finalized_root: Hash256, heads: impl Iterator<Item = Hash256>) {
        let mut keep = HashSet::new();
        keep.insert(finalized_root);

        for head_block_root in heads {
            // Iterate backwards until we reach a portion of the chain that we've already decided
            // to keep.
            self.iter_from(head_block_root)
                .take_while(|(block_root, _)| keep.insert(*block_root))
                .count();
        }

        self.nodes
            .write()
            .retain(|block_root, _| keep.contains(block_root));
    }

    pub fn as_ssz_container(&self) -> SszBlockTree {
        SszBlockTree {
            nodes: Vec::from_iter(self.nodes.read().clone()),
        }
    }
}

#[derive(Debug)]
pub struct BlockTreeIter<'a> {
    tree: &'a BlockTree,
    current_block_root: Hash256,
}

impl<'a> Iterator for BlockTreeIter<'a> {
    type Item = (Hash256, Slot);

    fn next(&mut self) -> Option<Self::Item> {
        // Genesis
        if self.current_block_root.is_zero() {
            None
        } else {
            let block_root = self.current_block_root;
            self.tree.nodes.read().get(&block_root).map(|node| {
                self.current_block_root = node.previous;
                (block_root, node.slot)
            })
        }
    }
}

// Serializable version of `BlockTree` that can be persisted to disk.
#[derive(Debug, Clone, Encode, Decode)]
pub struct SszBlockTree {
    nodes: Vec<(Hash256, Node)>,
}

impl Into<BlockTree> for SszBlockTree {
    fn into(self) -> BlockTree {
        BlockTree {
            nodes: RwLock::new(HashMap::from_iter(self.nodes)),
        }
    }
}

// FIXME(sproul): more tests!
#[cfg(test)]
mod test {
    use super::*;

    fn int_hash(x: u64) -> Hash256 {
        Hash256::from_low_u64_be(x)
    }

    #[test]
    fn single_chain() {
        let block_tree = BlockTree::new(int_hash(1), Slot::new(1));
        for i in 2..100 {
            block_tree
                .add_block_root(int_hash(i), int_hash(i - 1), Slot::new(i))
                .expect("add_block_root ok");

            let expected = (1..i + 1)
                .rev()
                .map(|j| (int_hash(j), Slot::new(j)))
                .collect::<Vec<_>>();

            assert_eq!(
                block_tree.iter_from(int_hash(i)).collect::<Vec<_>>(),
                expected
            );

            // Still OK after pruning.
            block_tree.prune_to(int_hash(1), vec![int_hash(i)].into_iter());

            assert_eq!(
                block_tree.iter_from(int_hash(i)).collect::<Vec<_>>(),
                expected
            );
        }
    }

    #[test]
    fn iter_zero() {
        let block_tree = BlockTree::new(int_hash(0), Slot::new(0));
        assert_eq!(block_tree.iter_from(int_hash(0)).count(), 0);
    }
}
