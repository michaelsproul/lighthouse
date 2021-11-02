use types::{Checkpoint, Epoch, Hash256};

#[derive(Clone, PartialEq, Debug)]
pub enum Error {
    FinalizedNodeUnknown(Hash256),
    JustifiedNodeUnknown(Hash256),
    InvalidFinalizedRootChange,
    InvalidNodeIndex(usize),
    InvalidParentIndex(usize),
    InvalidBestChildIndex(usize),
    InvalidJustifiedIndex(usize),
    InvalidBestDescendant(usize),
    InvalidParentDelta(usize),
    InvalidNodeDelta(usize),
    DeltaOverflow(usize),
    IndexOverflow(&'static str),
    InvalidDeltaLen {
        deltas: usize,
        indices: usize,
    },
    RevertedFinalizedEpoch {
        current_finalized_epoch: Epoch,
        new_finalized_epoch: Epoch,
    },
    InvalidBestNode {
        start_root: Hash256,
        justified_checkpoint: Checkpoint,
        finalized_checkpoint: Checkpoint,
        head_root: Hash256,
        head_justified_checkpoint: Checkpoint,
        head_finalized_checkpoint: Checkpoint,
    },
    InvalidAncestorOfValidPayload {
        ancestor_block_root: Hash256,
        ancestor_payload_block_hash: Hash256,
    },
}
