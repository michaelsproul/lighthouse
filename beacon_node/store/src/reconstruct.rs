//! Implementation of historic state reconstruction (given complete block history).
use crate::hot_cold_store::{ColdStateSummary, HotColdDB, HotColdDBError};
use crate::{Error, KeyValueStore, LevelDB, StoreItem};
use itertools::{process_results, Itertools};
use state_processing::{per_block_processing, per_slot_processing, BlockSignatureStrategy};
use std::sync::Arc;
use types::{EthSpec, Hash256};

impl<E> HotColdDB<E, LevelDB<E>, LevelDB<E>>
where
    E: EthSpec,
{
    pub fn reconstruct_historic_states(self: Arc<Self>) -> Result<(), Error> {
        let mut anchor = if let Some(anchor) = self.get_anchor_info() {
            anchor
        } else {
            // Nothing to do, history is complete.
            return Ok(());
        };
        // FIXME(sproul): check oldest_block_parent is 0

        let slots_per_restore_point = self.config.slots_per_restore_point;

        // Iterate blocks from the state lower limit to the upper limit.
        let lower_limit_slot = anchor.state_lower_limit;
        let upper_limit_state = {
            let split = self.split.read_recursive();
            self.get_restore_point(
                anchor.state_upper_limit.as_u64() / slots_per_restore_point,
                &split,
            )?
        };
        let upper_limit_slot = upper_limit_state.slot();

        // Use a dummy root, as we never read the block for the upper limit state.
        let upper_limit_block_root = Hash256::repeat_byte(0xff);

        let block_root_iter = Self::forwards_block_roots_iterator(
            self.clone(),
            lower_limit_slot,
            upper_limit_state,
            upper_limit_block_root,
            &self.spec,
        )?;

        // The state to be advanced.
        let mut state = self
            .load_cold_state_by_slot(lower_limit_slot)?
            .ok_or(HotColdDBError::MissingLowerLimitState(lower_limit_slot))?;

        state.build_all_caches(&self.spec)?;

        process_results(block_root_iter, |iter| -> Result<(), Error> {
            let mut io_batch = vec![];

            let mut prev_state_root = None;

            for ((prev_block_root, _), (block_root, slot)) in iter.tuple_windows() {
                let is_skipped_slot = prev_block_root == block_root;

                let block = if is_skipped_slot {
                    None
                } else {
                    Some(
                        self.get_block(&block_root)?
                            .ok_or(Error::BlockNotFound(block_root))?,
                    )
                };

                // Advance state to slot.
                per_slot_processing(&mut state, prev_state_root.take(), &self.spec)
                    .map_err(HotColdDBError::BlockReplaySlotError)?;

                // Apply block.
                if let Some(block) = block {
                    per_block_processing(
                        &mut state,
                        &block,
                        Some(block_root),
                        BlockSignatureStrategy::NoVerification,
                        &self.spec,
                    )
                    .map_err(HotColdDBError::BlockReplayBlockError)?;

                    prev_state_root = Some(block.state_root());
                }

                let state_root = prev_state_root
                    .ok_or(())
                    .or_else(|_| state.update_tree_hash_cache())?;

                // Stage state for storage in freezer DB.
                io_batch.push(ColdStateSummary { slot: state.slot() }.as_kv_store_op(state_root));
                self.store_cold_state(&state_root, &state, &mut io_batch)?;

                // If the slot lies on an epoch boundary, commit the batch and update the anchor.
                if slot % slots_per_restore_point == 0 || slot + 1 == upper_limit_slot {
                    self.cold_db.do_atomically(std::mem::take(&mut io_batch))?;

                    // Update anchor.
                    let old_anchor = Some(anchor.clone());

                    if slot + 1 == upper_limit_slot {
                        // The two limits have met in the middle! We're done!
                        self.compare_and_set_anchor_info(old_anchor, None)?;

                        return Ok(());
                    } else {
                        // The lower limit has been raised, store it.
                        anchor.state_lower_limit = slot;

                        self.compare_and_set_anchor_info(old_anchor, Some(anchor.clone()))?;
                    }
                }
            }

            // FIXME(sproul): error here, should always exit via `return` above
            Ok(())
        })??;

        Ok(())
    }
}
