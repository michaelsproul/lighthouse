use super::*;
use crate::case_result::compare_beacon_state_results_without_caches;
use crate::decode::{ssz_decode_file_with, ssz_decode_state, yaml_decode_file};
use serde::Deserialize;
use state_processing::{
    BlockSignatureStrategy, ConsensusContext, VerifyBlockRoot, per_block_processing,
    per_slot_processing,
};
use std::sync::Arc;
use store::{DatabaseBlock, HotColdDB, KeyValueStore, MemoryStore, StoreConfig};
use types::{BeaconState, Hash256, RelativeEpoch, SignedBeaconBlock};

use super::sanity_blocks::Metadata;

#[derive(Debug, Clone, Deserialize)]
#[serde(bound = "E: EthSpec")]
pub struct SanityBlocksDB<E: EthSpec> {
    pub metadata: Metadata,
    pub pre: BeaconState<E>,
    pub blocks: Vec<SignedBeaconBlock<E>>,
    pub post: Option<BeaconState<E>>,
}

impl<E: EthSpec> LoadCase for SanityBlocksDB<E> {
    fn load_from_dir(path: &Path, fork_name: ForkName) -> Result<Self, Error> {
        let spec = &testing_spec::<E>(fork_name);
        let metadata: Metadata = yaml_decode_file(&path.join("meta.yaml"))?;
        let pre = ssz_decode_state(&path.join("pre.ssz_snappy"), spec)?;
        let blocks = (0..metadata.blocks_count)
            .map(|i| {
                let filename = format!("blocks_{}.ssz_snappy", i);
                ssz_decode_file_with(&path.join(filename), |bytes| {
                    SignedBeaconBlock::from_ssz_bytes(bytes, spec)
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let post_file = path.join("post.ssz_snappy");
        let post = if post_file.is_file() {
            Some(ssz_decode_state(&post_file, spec)?)
        } else {
            None
        };

        Ok(Self {
            metadata,
            pre,
            blocks,
            post,
        })
    }
}

impl<E: EthSpec> Case for SanityBlocksDB<E> {
    fn description(&self) -> String {
        self.metadata.description.clone().unwrap_or_default()
    }

    fn result(&self, _case_index: usize, fork_name: ForkName) -> Result<(), Error> {
        self.metadata.bls_setting.unwrap_or_default().check()?;

        // Skip failure cases — there's no valid post-state to round-trip.
        if self.post.is_none() {
            return Err(Error::SkippedKnownFailure);
        }

        let spec = Arc::new(testing_spec::<E>(fork_name));
        let mut state = self.pre.clone();
        let mut expected = self.post.clone();
        state.build_caches(&spec).unwrap();

        // Create an ephemeral HotColdDB.
        let db = HotColdDB::<E, MemoryStore<E>, MemoryStore<E>>::open_ephemeral(
            StoreConfig::default(),
            spec.clone(),
        )
        .map_err(|e| Error::InternalError(format!("Failed to open ephemeral DB: {:?}", e)))?;

        let pre_slot = state.slot();

        // Initialize the anchor info so put_state won't fail with AnchorUninitialized.
        let anchor_op = db
            .init_anchor_info(Hash256::ZERO, pre_slot, pre_slot, true)
            .map_err(|e| Error::InternalError(format!("Failed to init anchor: {:?}", e)))?;
        db.hot_db
            .do_atomically(vec![anchor_op])
            .map_err(|e| Error::InternalError(format!("Failed to commit anchor: {:?}", e)))?;

        // Compute pre-state root and set the split point.
        let pre_state_root = state
            .update_tree_hash_cache()
            .map_err(|e| Error::InternalError(format!("Failed to hash pre-state: {:?}", e)))?;
        db.set_split(pre_slot, pre_state_root, Hash256::ZERO);

        // Store the pre-state.
        db.put_state(&pre_state_root, &state)
            .map_err(|e| Error::InternalError(format!("Failed to store pre-state: {:?}", e)))?;

        // Track stored blocks and states for later round-trip verification.
        let mut stored_blocks: Vec<(Hash256, SignedBeaconBlock<E>)> = Vec::new();
        let mut stored_states: Vec<(Hash256, types::Slot)> = Vec::new();

        // Process each block: advance state, apply block, store block and state.
        for signed_block in &self.blocks {
            let block = signed_block.message();
            while state.slot() < block.slot() {
                per_slot_processing(&mut state, None, &spec).unwrap();
            }

            state
                .build_committee_cache(RelativeEpoch::Current, &spec)
                .unwrap();

            let mut ctxt = ConsensusContext::new(state.slot());
            per_block_processing(
                &mut state,
                signed_block,
                BlockSignatureStrategy::VerifyIndividual,
                VerifyBlockRoot::True,
                &mut ctxt,
                &spec,
            )
            .map_err(|e| Error::InternalError(format!("Block processing failed: {:?}", e)))?;

            let block_root = signed_block.canonical_root();
            db.put_block(&block_root, signed_block.clone())
                .map_err(|e| Error::InternalError(format!("Failed to store block: {:?}", e)))?;
            stored_blocks.push((block_root, signed_block.clone()));

            let state_root = state
                .update_tree_hash_cache()
                .map_err(|e| Error::InternalError(format!("Failed to hash state: {:?}", e)))?;
            db.put_state(&state_root, &state)
                .map_err(|e| Error::InternalError(format!("Failed to store state: {:?}", e)))?;
            stored_states.push((state_root, state.slot()));
        }

        // Verify block round-trips.
        for (block_root, original_block) in &stored_blocks {
            let db_block = db
                .try_get_full_block(block_root)
                .map_err(|e| Error::InternalError(format!("Failed to get block: {:?}", e)))?
                .ok_or_else(|| {
                    Error::InternalError(format!("Block not found for root {:?}", block_root))
                })?;
            match db_block {
                DatabaseBlock::Full(retrieved_block) => {
                    if retrieved_block != *original_block {
                        return Err(Error::InternalError(
                            "Retrieved block does not match stored block".to_string(),
                        ));
                    }
                }
                DatabaseBlock::Blinded(_) => {
                    return Err(Error::InternalError(
                        "Expected full block but got blinded block".to_string(),
                    ));
                }
            }
        }

        // Verify state round-trips.
        for (state_root, slot) in &stored_states {
            let mut retrieved_state = db
                .get_state(state_root, Some(*slot), false)
                .map_err(|e| Error::InternalError(format!("Failed to get state: {:?}", e)))?
                .ok_or_else(|| {
                    Error::InternalError(format!(
                        "State not found for root {:?} at slot {}",
                        state_root, slot
                    ))
                })?;
            let retrieved_root = retrieved_state.update_tree_hash_cache().map_err(|e| {
                Error::InternalError(format!("Failed to hash retrieved state: {:?}", e))
            })?;
            if retrieved_root != *state_root {
                return Err(Error::InternalError(format!(
                    "State root mismatch: expected {:?}, got {:?}",
                    state_root, retrieved_root
                )));
            }
        }

        // Verify final state matches expected post-state.
        let mut result: Result<BeaconState<E>, String> = Ok(state);
        compare_beacon_state_results_without_caches(&mut result, &mut expected)
    }
}
