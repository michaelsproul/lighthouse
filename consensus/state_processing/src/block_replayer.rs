use crate::{
    per_block_processing, per_slot_processing, BlockProcessingError, BlockSignatureStrategy,
    SlotProcessingError,
};
use std::marker::PhantomData;
use types::{BeaconState, ChainSpec, EthSpec, Hash256, SignedBeaconBlock, Slot};

type PreBlockHook<'a, E, Error> =
    Box<dyn FnMut(&mut BeaconState<E>, &SignedBeaconBlock<E>) -> Result<(), Error> + 'a>;
type PostBlockHook<'a, E, Error> = PreBlockHook<'a, E, Error>;

type StateRootIterDefault<Error> = std::iter::Empty<Result<(Hash256, Slot), Error>>;

pub struct BlockReplayer<
    'a,
    Spec: EthSpec,
    Error = BlockReplayError,
    StateRootIter = StateRootIterDefault<Error>,
> {
    state: BeaconState<Spec>,
    spec: &'a ChainSpec,
    state_root_strategy: StateRootStrategy,
    block_sig_strategy: BlockSignatureStrategy,
    pre_block_hook: Option<PreBlockHook<'a, Spec, Error>>,
    post_block_hook: Option<PostBlockHook<'a, Spec, Error>>,
    state_root_iter: Option<StateRootIter>,
    pub state_root_miss: bool,
    _phantom: PhantomData<Error>,
}

#[derive(Debug)]
pub enum BlockReplayError {
    NoBlocks,
    SlotProcessing(SlotProcessingError),
    BlockProcessing(BlockProcessingError),
}

impl From<SlotProcessingError> for BlockReplayError {
    fn from(e: SlotProcessingError) -> Self {
        Self::SlotProcessing(e)
    }
}

impl From<BlockProcessingError> for BlockReplayError {
    fn from(e: BlockProcessingError) -> Self {
        Self::BlockProcessing(e)
    }
}

/// Defines how state roots should be computed during block replay.
#[derive(PartialEq)]
pub enum StateRootStrategy {
    /// Perform all transitions faithfully to the specification.
    Accurate,
    /// Don't compute state roots, eventually computing an invalid beacon state that can only be
    /// used for obtaining shuffling.
    Inconsistent,
}

impl<'a, E, Error, StateRootIter> BlockReplayer<'a, E, Error, StateRootIter>
where
    E: EthSpec,
    StateRootIter: Iterator<Item = Result<(Hash256, Slot), Error>>,
    Error: From<BlockReplayError>,
{
    pub fn new(state: BeaconState<E>, spec: &'a ChainSpec) -> Self {
        Self {
            state,
            spec,
            state_root_strategy: StateRootStrategy::Accurate,
            block_sig_strategy: BlockSignatureStrategy::NoVerification,
            pre_block_hook: None,
            post_block_hook: None,
            state_root_iter: None,
            state_root_miss: false,
            _phantom: PhantomData,
        }
    }

    pub fn pre_block_hook(mut self, hook: PreBlockHook<'a, E, Error>) -> Self {
        self.pre_block_hook = Some(hook);
        self
    }

    pub fn post_block_hook(mut self, hook: PostBlockHook<'a, E, Error>) -> Self {
        self.post_block_hook = Some(hook);
        self
    }

    pub fn state_root_iter(mut self, iter: StateRootIter) -> Self {
        self.state_root_iter = Some(iter);
        self
    }

    fn get_state_root(
        &mut self,
        slot: Slot,
        blocks: &[SignedBeaconBlock<E>],
        i: usize,
    ) -> Result<Option<Hash256>, Error> {
        // If we don't care about state roots then return immediately.
        if self.state_root_strategy == StateRootStrategy::Inconsistent {
            return Ok(Some(Hash256::zero()));
        }

        // If a state root iterator is configured, use it to find the root.
        if let Some(ref mut state_root_iter) = self.state_root_iter {
            let opt_root = state_root_iter
                .take_while(|res| res.as_ref().map_or(true, |(_, s)| *s <= slot))
                .find(|res| res.as_ref().map_or(true, |(_, s)| *s == slot))
                .transpose()?;

            if let Some((root, _)) = opt_root {
                return Ok(Some(root));
            }
        }

        // Otherwise try to source a root from the previous block.
        if i > 0 {
            if let Some(prev_block) = blocks.get(i - 1) {
                if prev_block.slot() == slot {
                    return Ok(Some(prev_block.state_root()));
                }
            }
        }

        self.state_root_miss = true;
        Ok(None)
    }

    pub fn apply_blocks(&mut self, blocks: Vec<SignedBeaconBlock<E>>) -> Result<(), Error> {
        for (i, block) in blocks.iter().enumerate() {
            if block.slot() <= self.state.slot() {
                continue;
            }

            while self.state.slot() < block.slot() {
                let state_root = self.get_state_root(self.state.slot(), &blocks, i)?;

                per_slot_processing(&mut self.state, state_root, &self.spec)
                    .map_err(BlockReplayError::from)?;
            }

            if let Some(ref mut pre_block_hook) = self.pre_block_hook {
                pre_block_hook(&mut self.state, &block)?;
            }

            // FIXME(sproul): block root
            per_block_processing(
                &mut self.state,
                block,
                None,
                self.block_sig_strategy,
                &self.spec,
            )
            .map_err(BlockReplayError::from)?;
        }

        Ok(())
    }
}

impl<'a, E, Error> BlockReplayer<'a, E, Error, StateRootIterDefault<Error>>
where
    E: EthSpec,
    Error: From<BlockReplayError>,
{
    pub fn no_state_root_iter(self) -> Self {
        self
    }
}
