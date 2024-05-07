use crate::{DBColumn, Error, HotColdDB, ItemStore};
use std::fmt::Debug;
use types::{EthSpec, SignedBlindedBeaconBlock};

fn assert_invariant<T: Debug>(condition: bool, description: &str, tag: T) -> Result<(), Error> {
    if !condition {
        Err(Error::InvariantError(format!("{description}: {tag:?}")))
    } else {
        Ok(())
    }
}

impl<E: EthSpec, Hot: ItemStore<E>, Cold: ItemStore<E>> HotColdDB<E, Hot, Cold> {
    /// All blocks in the hot DB more recent than the split point, have a state summary in the hot
    /// DB matching their state_root.
    pub fn invariant_hot_block_state_summaries(&self) -> Result<(), Error> {
        let split = self.split.read_recursive();
        let mut count = 0;
        for res in self.hot_db.iter_column(DBColumn::BeaconBlock) {
            let (block_root, block_bytes) = res?;
            let block =
                SignedBlindedBeaconBlock::<E>::from_ssz_bytes(&block_bytes, self.get_chain_spec())?;
            let state_root = block.state_root();

            count += 1;

            if count % 100 == 0 {
                slog::debug!(self.log, "Processed {} blocks", count);
            }

            // Block is prior to split slot, skip it.
            if block.slot() < split.slot {
                continue;
            }

            let Some(state_summary) = self.load_hot_state_summary(&state_root)? else {
                return assert_invariant(
                    false,
                    "hot state summary not found for state root",
                    state_root,
                );
            };

            assert_invariant(
                state_summary.slot == block.slot(),
                "hot state summary slot for state root",
                state_root,
            )?;
            assert_invariant(
                state_summary.latest_block_root == block_root,
                "hot state summary latest_block_root for state root",
                state_root,
            )?;
        }
        Ok(())
    }

    // Hot blocks stored in the DB have the correct block_root.
}
