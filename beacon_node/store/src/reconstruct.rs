//! Implementation of historic state reconstruction (given complete block history).
use crate::hot_cold_store::{HotColdDB, HotColdDBError};
use crate::{Error, ItemStore, KeyValueStore};
use itertools::{process_results, Itertools};
use slog::info;
use state_processing::{per_block_processing, per_slot_processing, BlockSignatureStrategy};
use std::sync::Arc;
use types::{EthSpec, Hash256};

impl<E, Hot, Cold> HotColdDB<E, Hot, Cold>
where
    E: EthSpec,
    Hot: KeyValueStore<E> + ItemStore<E>,
    Cold: KeyValueStore<E> + ItemStore<E>,
{
    pub fn reconstruct_historic_states(self: &Arc<Self>, lock_mutex: bool) -> Result<(), Error> {
        // Do not run historic state reconstruction in parallel with the database migration
        // that is triggered upon finalization. It simplifies our reasoning if the split point is
        // assumed not to advance for the duration of this function.
        let _migration_mutex = if lock_mutex {
            Some(self.lock_migration_mutex())
        } else {
            None
        };

        let mut anchor = if let Some(anchor) = self.get_anchor_info() {
            anchor
        } else {
            // Nothing to do, history is complete.
            return Ok(());
        };

        // Check that all historic blocks are known.
        if anchor.oldest_block_slot != 0 {
            return Err(Error::MissingHistoricBlocks {
                oldest_block_slot: anchor.oldest_block_slot,
            });
        }

        info!(
            self.log,
            "Reconstructing historic states";
            "start_slot" => anchor.state_lower_limit,
        );

        let slots_per_restore_point = self.slots_per_restore_point();

        // Iterate blocks from the state lower limit to the upper limit.
        let lower_limit_slot = anchor.state_lower_limit;
        let split = self.get_split_info();
        let upper_limit_state = self.get_restore_point(
            anchor.state_upper_limit.as_u64() / slots_per_restore_point,
            &split,
        )?;
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
                self.store_cold_state(&state_root, &state, &mut io_batch)?;

                // If the slot lies on an epoch boundary, commit the batch and update the anchor.
                if slot % slots_per_restore_point == 0 || slot + 1 == upper_limit_slot {
                    info!(
                        self.log,
                        "State reconstruction in progress";
                        "slot" => slot,
                        "remaining" => upper_limit_slot - 1 - slot
                    );

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

            // Should always reach the `upper_limit_slot` and return early above.
            Err(Error::StateReconstructionDidNotComplete)
        })??;

        // Check that the split point wasn't mutated during the state reconstruction process.
        // It shouldn't, due to the migration mutex, so this is just a paranoid check.
        let latest_split = self.get_split_info();
        if split != latest_split {
            return Err(Error::SplitPointModified(latest_split.slot, split.slot));
        }

        Ok(())
    }

    pub fn unindex(
        self: &Arc<Self>,
        slots_per_restore_point: u64,
        lock_mutex: bool,
    ) -> Result<(), Error> {
        let lock = if lock_mutex {
            Some(self.lock_migration_mutex())
        } else {
            None
        };

        // First update the anchor info. In the case of a crash the database will not read
        // the dangling restore points. In future we could do both operations as part of the same
        // atomic transaction.
        let split = self.get_split_info();
        let prev_anchor = self.get_anchor_info();
        if prev_anchor.map_or(false, |anchor| anchor.oldest_block_slot != 0) {
            return Err(Error::UnableToUnindex {
                oldest_block_slot: anchor.oldest_block_slot,
            });
        }
        let new_anchor = AnchorInfo {
            anchor_slot: Slot::new(0),
            oldest_block_slot: Slot::new(0),
            oldest_block_parent: Hash256::zero(),
            state_upper_limit: Self::next_restore_point_slot(split.slot, slots_per_restore_point),
            state_lower_limit: Slot::new(0),
        };
        self.compare_and_set_anchor_info(prev_anchor, Some(new_anchor))?;

        // Delete all existing restore points.

        Ok(())
    }

    pub fn reindex(
        self: &Arc<Self>,
        slots_per_restore_point: u64,
        lock_mutex: bool,
    ) -> Result<(), Error> {
        // Check legitimacy of new value.
        Self::verify_slots_per_restore_point(slots_per_restore_point)?;

        // Lock the migration mutex to prevent concurrent migrations or reconstructions.
        let lock = if lock_mutex {
            Some(self.lock_migration_mutex())
        } else {
            None
        };

        // Examine the current anchor to determine which of the following cases we're in:
        //
        // 1. Complete database with a different SPRP (the simple case)
        // 2. Complete database with the same SPRP (no-op)
        // 3. Partial database with matching SPRP (proceed with state reconstruction)
        // 4. Partial database with mismatched SPRP or other fields (error)
        let current_slots_per_restore_point = self.slots_per_restore_point();
        match self.get_anchor_info() {
            // Case (1): continue.
            None if slots_per_restore_point != current_slots_per_restore_point => {}
            // Case (2): return early.
            None => {
                info!(self.log, "No need to re-index database");
                return Ok(());
            }
            Some(anchor) => {
                let split = self.get_split_info();
                // Case (3): resume previous reindexing.
                // FIXME(sproul): consider requiring `anchor_slot == 0`.
                if anchor.oldest_block_slot == 0
                    && anchor.state_upper_limit
                        == Self::next_restore_point_slot(split.slot, slots_per_restore_point)
                    && slots_per_restore_point == current_slots_per_restore_point
                {
                    info!(self.log, "Resuming reindexing");
                    self.reconstruct_historic_states(false)?;
                    drop(lock);
                    return Ok(());
                }
                // Case (4): some kind of mess, don't do anything (unindexing recommended).
                else {
                    return Err(Error::UnableToReindex {
                        new_slots_per_restore_point: slots_per_restore_point,
                        current_slots_per_restore_point,
                        split_slot: split.slot,
                        oldest_block_slot: anchor.oldest_block_slot,
                        state_upper_limit_slot: anchor.state_upper_limit,
                    });
                }
            }
        }

        // If we've made it to here we know we're in the "nice" case where we have a complete
        // database with a different SPRP that we need to modify. Proceed by:
        //
        // 1. Unindexing the database to delete all of the existing historic states.
        // 2. Updating the `slots_per_restore_point` value.
        // 3. Reconstructing the states with the new `slots_per_restore_point` value.
        info!(
            self.log,
            "Reindexing freezer database";
            "from" => format!("{} slots per restore point", current_slots_per_restore_point),
            "to" => format!("{} slots per restore point", slots_per_restore_point),
        );
        self.unindex(false)?;

        self.set_slots_per_restore_point(slots_per_restore_point)?;

        self.reconstruct_historic_states(false)?;

        drop(lock);
        Ok(())
    }
}
