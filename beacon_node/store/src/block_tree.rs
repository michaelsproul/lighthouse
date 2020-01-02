use parking_lot::RwLock;
use std::collections::HashMap;
use std::iter::{self, FromIterator};
use types::{Hash256, Slot};

#[derive(Debug)]
pub struct BlockTree {
    nodes: RwLock<HashMap<Hash256, Node>>,
}

#[derive(Debug)]
pub enum BlockTreeError {
    PrevUnknown(Hash256),
}

#[derive(Debug)]
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

// FIXME(sproul): tests, don't forget!
// FIXME(sproul): pruning
