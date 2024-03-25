use crate::config::{OnDiskStoreConfig, StoreConfig};
use crate::forwards_iter::{HybridForwardsBlockRootsIterator, HybridForwardsStateRootsIterator};
use crate::hdiff::{HDiff, HDiffBuffer, HierarchyModuli, StorageStrategy};
use crate::hot_state_iter::HotStateRootIter;
use crate::impls::{
    beacon_state::{get_full_state, store_full_state},
    frozen_block_slot::FrozenBlockSlot,
};
use crate::iter::{BlockRootsIterator, ParentRootBlockIterator, RootsIterator};
use crate::leveldb_store::{BytesKey, LevelDB};
use crate::memory_store::MemoryStore;
use crate::metadata::{
    AnchorInfo, BlobInfo, CompactionTimestamp, SchemaVersion, ANCHOR_INFO_KEY, BLOB_INFO_KEY,
    COMPACTION_TIMESTAMP_KEY, CONFIG_KEY, CURRENT_SCHEMA_VERSION, SCHEMA_VERSION_KEY, SPLIT_KEY,
    STATE_UPPER_LIMIT_NO_RETAIN,
};
use crate::metrics;
use crate::state_cache::{PutStateOutcome, StateCache};
use crate::{
    get_key_for_col, DBColumn, DatabaseBlock, Error, ItemStore, KeyValueStoreOp, StoreItem,
    StoreOp, ValidatorPubkeyCache,
};
use itertools::process_results;
use leveldb::iterator::LevelDBIterator;
use lru::LruCache;
use parking_lot::{Mutex, RwLock};
use promise_cache::{Promise, PromiseCache};
use safe_arith::SafeArith;
use serde::{Deserialize, Serialize};
use slog::{debug, error, info, trace, warn, Logger};
use ssz::{Decode, Encode};
use ssz_derive::{Decode, Encode};
use state_processing::{
    block_replayer::PreSlotHook, AllCaches, BlockProcessingError, BlockReplayer,
    SlotProcessingError, StateProcessingStrategy,
};
use std::cmp::min;
use std::collections::VecDeque;
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tailcall::tailcall;
use types::blob_sidecar::BlobSidecarList;
use types::*;
use zstd::{Decoder, Encoder};

pub const MAX_PARENT_STATES_TO_CACHE: u64 = 1;

/// On-disk database that stores finalized states efficiently.
///
/// Stores vector fields like the `block_roots` and `state_roots` separately, and only stores
/// intermittent "restore point" states pre-finalization.
#[derive(Debug)]
pub struct HotColdDB<E: EthSpec, Hot: ItemStore<E>, Cold: ItemStore<E>> {
    /// The slot and state root at the point where the database is split between hot and cold.
    ///
    /// States with slots less than `split.slot` are in the cold DB, while states with slots
    /// greater than or equal are in the hot DB.
    pub(crate) split: RwLock<Split>,
    /// The starting slots for the range of blocks & states stored in the database.
    anchor_info: RwLock<Option<AnchorInfo>>,
    /// The starting slots for the range of blobs stored in the database.
    blob_info: RwLock<BlobInfo>,
    pub(crate) config: StoreConfig,
    pub(crate) hierarchy: HierarchyModuli,
    /// Cold database containing compact historical data.
    pub cold_db: Cold,
    /// Database containing blobs.
    pub blobs_db: Cold,
    /// Hot database containing duplicated but quick-to-access recent data.
    pub hot_db: Hot,
    /// LRU cache of deserialized blocks and blobs. Updated whenever a block or blob is loaded.
    block_cache: Mutex<BlockCache<E>>,
    /// Cache of beacon states.
    ///
    /// LOCK ORDERING: this lock must always be locked *after* the `split` if both are required.
    state_cache: Mutex<StateCache<E>>,
    state_promise_cache: PromiseCache<Hash256, BeaconState<E>>,
    /// Immutable validator cache.
    pub immutable_validators: Arc<RwLock<ValidatorPubkeyCache<E, Hot, Cold>>>,
    /// LRU cache of replayed states.
    // FIXME(sproul): re-enable historic state cache
    #[allow(dead_code)]
    historic_state_cache: Mutex<LruCache<Slot, BeaconState<E>>>,
    /// Cache of hierarchical diff buffers.
    diff_buffer_cache: Mutex<LruCache<Slot, Arc<HDiffBuffer>>>,
    diff_buffer_promise_cache: PromiseCache<Slot, (Slot, Arc<HDiffBuffer>)>,
    /// Chain spec.
    pub(crate) spec: ChainSpec,
    /// Logger.
    pub log: Logger,
    /// Mere vessel for E.
    _phantom: PhantomData<E>,
}

#[derive(Debug)]
struct BlockCache<E: EthSpec> {
    block_cache: LruCache<Hash256, SignedBeaconBlock<E>>,
    blob_cache: LruCache<Hash256, BlobSidecarList<E>>,
}

impl<E: EthSpec> BlockCache<E> {
    pub fn new(size: NonZeroUsize) -> Self {
        Self {
            block_cache: LruCache::new(size),
            blob_cache: LruCache::new(size),
        }
    }
    pub fn put_block(&mut self, block_root: Hash256, block: SignedBeaconBlock<E>) {
        self.block_cache.put(block_root, block);
    }
    pub fn put_blobs(&mut self, block_root: Hash256, blobs: BlobSidecarList<E>) {
        self.blob_cache.put(block_root, blobs);
    }
    pub fn get_block<'a>(&'a mut self, block_root: &Hash256) -> Option<&'a SignedBeaconBlock<E>> {
        self.block_cache.get(block_root)
    }
    pub fn get_blobs<'a>(&'a mut self, block_root: &Hash256) -> Option<&'a BlobSidecarList<E>> {
        self.blob_cache.get(block_root)
    }
    pub fn delete_block(&mut self, block_root: &Hash256) {
        let _ = self.block_cache.pop(block_root);
    }
    pub fn delete_blobs(&mut self, block_root: &Hash256) {
        let _ = self.blob_cache.pop(block_root);
    }
    pub fn delete(&mut self, block_root: &Hash256) {
        let _ = self.block_cache.pop(block_root);
        let _ = self.blob_cache.pop(block_root);
    }
}

#[derive(Debug, PartialEq)]
pub enum HotColdDBError {
    UnsupportedSchemaVersion {
        target_version: SchemaVersion,
        current_version: SchemaVersion,
    },
    /// Recoverable error indicating that the database freeze point couldn't be updated
    /// due to the finalized block not lying on an epoch boundary (should be infrequent).
    FreezeSlotUnaligned(Slot),
    FreezeSlotError {
        current_split_slot: Slot,
        proposed_split_slot: Slot,
    },
    MissingStateToFreeze(Hash256),
    MissingRestorePointHash(u64),
    MissingRestorePointState(Slot),
    MissingRestorePoint(Hash256),
    MissingColdStateSummary(Hash256),
    MissingHotStateSummary(Hash256),
    MissingEpochBoundaryState(Hash256),
    MissingPrevState(Hash256),
    MissingSplitState(Hash256, Slot),
    MissingStateDiff(Hash256),
    MissingHDiff(Slot),
    MissingExecutionPayload(Hash256),
    MissingFullBlockExecutionPayloadPruned(Hash256, Slot),
    MissingAnchorInfo,
    MissingFrozenBlockSlot(Hash256),
    MissingFrozenBlock(Slot),
    MissingPathToBlobsDatabase,
    BlobsPreviouslyInDefaultStore,
    HotStateSummaryError(BeaconStateError),
    RestorePointDecodeError(ssz::DecodeError),
    BlockReplayBeaconError(BeaconStateError),
    BlockReplaySlotError(SlotProcessingError),
    BlockReplayBlockError(BlockProcessingError),
    MissingLowerLimitState(Slot),
    InvalidSlotsPerRestorePoint {
        slots_per_restore_point: u64,
        slots_per_historical_root: u64,
        slots_per_epoch: u64,
    },
    ZeroEpochsPerBlobPrune,
    BlobPruneLogicError,
    RestorePointBlockHashError(BeaconStateError),
    IterationError {
        unexpected_key: BytesKey,
    },
    FinalizedStateNotInHotDatabase {
        split_slot: Slot,
        request_slot: Slot,
        block_root: Hash256,
    },
    Rollback,
}

impl<E: EthSpec> HotColdDB<E, MemoryStore<E>, MemoryStore<E>> {
    pub fn open_ephemeral(
        config: StoreConfig,
        spec: ChainSpec,
        log: Logger,
    ) -> Result<HotColdDB<E, MemoryStore<E>, MemoryStore<E>>, Error> {
        config.verify::<E>()?;

        let hierarchy = config.hierarchy_config.to_moduli()?;

        let block_cache_size = config.block_cache_size;
        let state_cache_size = config.state_cache_size;
        let historic_state_cache_size = config.historic_state_cache_size;
        let diff_buffer_cache_size = config.diff_buffer_cache_size;

        let db = HotColdDB {
            split: RwLock::new(Split::default()),
            anchor_info: RwLock::new(None),
            blob_info: RwLock::new(BlobInfo::default()),
            cold_db: MemoryStore::open(),
            blobs_db: MemoryStore::open(),
            hot_db: MemoryStore::open(),
            block_cache: Mutex::new(BlockCache::new(block_cache_size)),
            state_cache: Mutex::new(StateCache::new(state_cache_size)),
            state_promise_cache: PromiseCache::new(),
            immutable_validators: Arc::new(RwLock::new(Default::default())),
            historic_state_cache: Mutex::new(LruCache::new(historic_state_cache_size)),
            diff_buffer_cache: Mutex::new(LruCache::new(diff_buffer_cache_size)),
            diff_buffer_promise_cache: PromiseCache::new(),
            config,
            hierarchy,
            spec,
            log,
            _phantom: PhantomData,
        };

        Ok(db)
    }
}

impl<E: EthSpec> HotColdDB<E, LevelDB<E>, LevelDB<E>> {
    /// Open a new or existing database, with the given paths to the hot and cold DBs.
    ///
    /// The `migrate_schema` function is passed in so that the parent `BeaconChain` can provide
    /// context and access `BeaconChain`-level code without creating a circular dependency.
    pub fn open(
        hot_path: &Path,
        cold_path: &Path,
        blobs_db_path: &Path,
        migrate_schema: impl FnOnce(Arc<Self>, SchemaVersion, SchemaVersion) -> Result<(), Error>,
        config: StoreConfig,
        spec: ChainSpec,
        log: Logger,
    ) -> Result<Arc<Self>, Error> {
        config.verify::<E>()?;

        let hierarchy = config.hierarchy_config.to_moduli()?;

        let block_cache_size = config.block_cache_size;
        let state_cache_size = config.state_cache_size;
        let historic_state_cache_size = config.historic_state_cache_size;
        let diff_buffer_cache_size = config.diff_buffer_cache_size;

        let mut db = HotColdDB {
            split: RwLock::new(Split::default()),
            anchor_info: RwLock::new(None),
            blob_info: RwLock::new(BlobInfo::default()),
            cold_db: LevelDB::open(cold_path)?,
            blobs_db: LevelDB::open(blobs_db_path)?,
            hot_db: LevelDB::open(hot_path)?,
            block_cache: Mutex::new(BlockCache::new(block_cache_size)),
            state_cache: Mutex::new(StateCache::new(state_cache_size)),
            state_promise_cache: PromiseCache::new(),
            immutable_validators: Arc::new(RwLock::new(Default::default())),
            historic_state_cache: Mutex::new(LruCache::new(historic_state_cache_size)),
            diff_buffer_cache: Mutex::new(LruCache::new(diff_buffer_cache_size)),
            diff_buffer_promise_cache: PromiseCache::new(),
            config,
            hierarchy,
            spec,
            log,
            _phantom: PhantomData,
        };

        // Load the config from disk but don't error on a failed read because the config itself may
        // need migrating.
        if db.load_config().is_err() {
            // We expect this failure when migrating to tree-states for the first time, before
            // the new config is written. We need to set it here before the DB gets Arc'd for the
            // schema migration. This can be deleted when the v24 schema upgrade is deleted.
            db.config.linear_blocks = false;
        }

        // Load the previous split slot from the database (if any). This ensures we can
        // stop and restart correctly. This needs to occur *before* running any migrations
        // because some migrations load states and depend on the split.
        if let Some(split) = db.load_split()? {
            *db.split.write() = split;
            *db.anchor_info.write() = db.load_anchor_info()?;

            info!(
                db.log,
                "Hot-Cold DB initialized";
                "split_slot" => split.slot,
                "split_state" => ?split.state_root
            );
        }

        // Open separate blobs directory if configured and same configuration was used on previous
        // run.
        let blob_info = db.load_blob_info()?;
        let deneb_fork_slot = db
            .spec
            .deneb_fork_epoch
            .map(|epoch| epoch.start_slot(E::slots_per_epoch()));
        let new_blob_info = match &blob_info {
            Some(blob_info) => {
                // If the oldest block slot is already set do not allow the blob DB path to be
                // changed (require manual migration).
                if blob_info.oldest_blob_slot.is_some() && !blob_info.blobs_db {
                    return Err(HotColdDBError::BlobsPreviouslyInDefaultStore.into());
                }
                // Set the oldest blob slot to the Deneb fork slot if it is not yet set.
                // Always initialize `blobs_db` to true, we no longer support storing the blobs
                // in the freezer DB, because the UX is strictly worse for relocating the DB.
                let oldest_blob_slot = blob_info.oldest_blob_slot.or(deneb_fork_slot);
                BlobInfo {
                    oldest_blob_slot,
                    blobs_db: true,
                }
            }
            // First start.
            None => BlobInfo {
                // Set the oldest blob slot to the Deneb fork slot if it is not yet set.
                oldest_blob_slot: deneb_fork_slot,
                blobs_db: true,
            },
        };
        db.compare_and_set_blob_info_with_write(<_>::default(), new_blob_info.clone())?;
        info!(
            db.log,
            "Blob DB initialized";
            "path" => ?blobs_db_path,
            "oldest_blob_slot" => ?new_blob_info.oldest_blob_slot,
        );

        // Ensure that the schema version of the on-disk database matches the software.
        // If the version is mismatched, an automatic migration will be attempted.
        let db = Arc::new(db);
        if let Some(schema_version) = db.load_schema_version()? {
            debug!(
                db.log,
                "Attempting schema migration";
                "from_version" => schema_version.as_u64(),
                "to_version" => CURRENT_SCHEMA_VERSION.as_u64(),
            );
            migrate_schema(db.clone(), schema_version, CURRENT_SCHEMA_VERSION)?;
        } else {
            db.store_schema_version(CURRENT_SCHEMA_VERSION)?;
        }

        // Ensure that any on-disk config is compatible with the supplied config.
        if let Some(disk_config) = db.load_config()? {
            let split = db.get_split_info();
            let anchor = db.get_anchor_info();
            db.config
                .check_compatibility(&disk_config, &split, anchor.as_ref())?;

            // Inform user if hierarchy config is changing.
            if db.config.hierarchy_config != disk_config.hierarchy_config {
                info!(
                    db.log,
                    "Updating historic state config";
                    "previous_config" => ?disk_config.hierarchy_config,
                    "new_config" => ?db.config.hierarchy_config,
                );
            }
        }
        db.store_config()?;

        // Load validator pubkey cache.
        let pubkey_cache = ValidatorPubkeyCache::load_from_store(&db)?;
        *db.immutable_validators.write() = pubkey_cache;

        // Run a garbage collection pass.
        db.remove_garbage()?;

        // If configured, run a foreground compaction pass.
        if db.config.compact_on_init {
            info!(db.log, "Running foreground compaction");
            db.compact()?;
            info!(db.log, "Foreground compaction complete");
        }

        Ok(db)
    }

    /// Return an iterator over the state roots of all temporary states.
    pub fn iter_temporary_state_roots(&self) -> impl Iterator<Item = Result<Hash256, Error>> + '_ {
        let column = DBColumn::BeaconStateTemporary;
        let start_key =
            BytesKey::from_vec(get_key_for_col(column.into(), Hash256::zero().as_bytes()));

        let keys_iter = self.hot_db.keys_iter();
        keys_iter.seek(&start_key);

        keys_iter
            .take_while(move |key| key.matches_column(column))
            .map(move |bytes_key| {
                bytes_key.remove_column(column).ok_or_else(|| {
                    HotColdDBError::IterationError {
                        unexpected_key: bytes_key,
                    }
                    .into()
                })
            })
    }
}

enum CurrentState<E: EthSpec> {
    State(BeaconState<E>),
    Buffer(Slot, HDiffBuffer),
}

impl<E: EthSpec> CurrentState<E> {
    fn slot(&self) -> Slot {
        match self {
            Self::State(state) => state.slot(),
            Self::Buffer(slot, _) => *slot,
        }
    }

    fn into_state(self, spec: &ChainSpec) -> Result<BeaconState<E>, crate::hdiff::Error> {
        match self {
            Self::State(state) => Ok(state),
            Self::Buffer(_, buffer) => buffer.into_state(spec),
        }
    }

    fn into_buffer(self) -> HDiffBuffer {
        match self {
            Self::State(state) => HDiffBuffer::from_state(state),
            Self::Buffer(_, buffer) => buffer,
        }
    }
}

/// - diffs_to_apply: diffs in slot-descending order
/// - promises_to_resolve: promises in slot-descending order
/// - state_root_iter: in slot-descending order
#[tailcall]
pub fn get_hot_state_and_apply<E: EthSpec, Hot: ItemStore<E>, Cold: ItemStore<E>>(
    store: &HotColdDB<E, Hot, Cold>,
    state_root: Hash256,
    final_state_slot: Option<Slot>,
    mut diffs_to_apply: Vec<(Slot, Slot, HDiff)>,
    mut blocks_to_apply: Vec<SignedBlindedBeaconBlock<E>>,
    mut state_root_iter: Vec<(Hash256, Slot)>,
    mut latest_block: Option<SignedBlindedBeaconBlock<E>>,
    mut promises_to_resolve: Vec<promise_cache::Sender<BeaconState<E>>>,
) -> Result<Option<BeaconState<E>>, Error> {
    // Check state cache.
    let split = store.split.read_recursive();
    let mut cached_state: Option<BeaconState<E>> =
        store.state_cache.lock().get_by_state_root(state_root);
    let mut promise = None;

    if cached_state.is_none() {
        match store.state_promise_cache.get_or_create_promise(state_root) {
            Promise::Wait(recv) => {
                if let Ok(state) = recv.recv() {
                    let s: BeaconState<E> = state;
                    cached_state = Some(s);
                }
            }
            Promise::Ready(state) => {
                cached_state = Some(state);
            }
            Promise::Compute(sender) => {
                promise = Some(sender);
            }
        }
    }

    // If the state is the finalized state, load it from disk. This should only be necessary
    // once during start-up, after which point the finalized state will be cached.
    if cached_state.is_none() && state_root == split.state_root {
        let (split_state, _) = store.load_hot_state_full(&state_root)?;
        cached_state = Some(split_state);
    }

    if let Some(start_state) = cached_state {
        // Base case: apply accumulated diffs and blocks.
        let mut current_state = CurrentState::State(start_state);
        loop {
            if diffs_to_apply
                .last()
                .map_or(false, |(diff_base_slot, _, _)| {
                    *diff_base_slot == current_state.slot()
                })
            {
                let (_, target_slot, diff) = diffs_to_apply.pop().unwrap();
                let mut buffer = current_state.into_buffer();
                diff.apply(&mut buffer).unwrap();
                current_state = CurrentState::Buffer(target_slot, buffer);
            } else if let Some(block) = blocks_to_apply.pop() {
                // Take state roots until reaching the slot of the current state.
                let mut state_roots = vec![];
                while state_root_iter
                    .last()
                    .map_or(false, |(_, slot)| *slot <= block.slot())
                {
                    if let Some(elem) = state_root_iter.pop() {
                        state_roots.push(elem);
                    }
                }

                // Replay blocks.
                let target_slot = block.slot();
                let state = current_state.into_state(&store.spec).unwrap();
                // TODO: resolve promises & cache in pre-slot hook?
                let replayed_state = store
                    .replay_blocks(
                        state,
                        vec![block],
                        target_slot,
                        state_roots.into_iter().map(Ok),
                        None,
                    )
                    .unwrap();
                current_state = CurrentState::State(replayed_state);
            } else {
                assert!(diffs_to_apply.is_empty());
                let state = current_state.into_state(&store.spec).unwrap();
                let advanced_state = if let Some(target_slot) = final_state_slot {
                    store
                        .replay_blocks(
                            state,
                            vec![],
                            target_slot,
                            state_root_iter.into_iter().rev().map(Ok),
                            None,
                        )
                        .unwrap()
                } else {
                    state
                };
                current_state = CurrentState::State(advanced_state);
                break;
            }
        }
        Ok(Some(current_state.into_state(&store.spec).unwrap()))
    } else {
        if let Some(sender) = promise {
            promises_to_resolve.push(sender);
        }

        // Load hot state summary.
        let Some(state_summary) = store.load_hot_state_summary(&state_root)? else {
            return Ok(None);
        };

        state_root_iter.push((state_root, state_summary.slot));

        // Ensure the latest block for this state is loaded. If it has been deleted, then the state
        // is considered deleted too and will be cleaned up in a future finalization migration.
        if latest_block.is_none() && state_summary.latest_block_root != split.block_root {
            if let Some(block) = store.get_hot_blinded_block(&state_summary.latest_block_root)? {
                latest_block = Some(block);
            } else {
                return Ok(None);
            }
        }

        // If the state is a diff state, load the diff and apply it to the diff base state.
        let base_state_root = if !state_summary.diff_base_state_root.is_zero()
            && state_summary.diff_base_slot >= split.slot
        {
            let diff = store.load_hot_state_diff(state_root)?;
            diffs_to_apply.push((state_summary.diff_base_slot, state_summary.slot, diff));
            latest_block = None;
            state_summary.diff_base_state_root
        }
        // Otherwise if there is no diff, iterate back until the most recently applied block.
        else {
            latest_block = if let Some(block) = latest_block {
                if block.slot() == state_summary.slot {
                    // Stage this block for application.
                    blocks_to_apply.push(block);
                    None
                } else {
                    Some(block)
                }
            } else {
                None
            };
            state_summary.prev_state_root
        };

        debug!(
            store.log,
            "recursing from {} at {} to {}, diffs: {}, blocks: {}, latest_block? {}",
            state_root,
            state_summary.slot,
            base_state_root,
            diffs_to_apply.len(),
            blocks_to_apply.len(),
            latest_block.is_some()
        );
        get_hot_state_and_apply(
            store,
            base_state_root,
            final_state_slot.or(Some(state_summary.slot)),
            diffs_to_apply,
            blocks_to_apply,
            state_root_iter,
            latest_block,
            promises_to_resolve,
        )
    }
}

impl<E: EthSpec, Hot: ItemStore<E>, Cold: ItemStore<E>> HotColdDB<E, Hot, Cold> {
    pub fn update_finalized_state(
        &self,
        state_root: Hash256,
        block_root: Hash256,
        state: BeaconState<E>,
    ) -> Result<(), Error> {
        self.state_cache
            .lock()
            .update_finalized_state(state_root, block_root, state)
    }

    pub fn state_cache_len(&self) -> usize {
        self.state_cache.lock().len()
    }

    /// Store a block and update the LRU cache.
    pub fn put_block(
        &self,
        block_root: &Hash256,
        block: SignedBeaconBlock<E>,
    ) -> Result<(), Error> {
        // Store on disk.
        let mut ops = Vec::with_capacity(2);

        let block = self.block_as_kv_store_ops(block_root, block, &mut ops)?;
        self.hot_db.do_atomically(ops)?;

        // Update cache.
        self.block_cache.lock().put_block(*block_root, block);
        Ok(())
    }

    /// Prepare a signed beacon block for storage in the database.
    ///
    /// Return the original block for re-use after storage. It's passed by value so it can be
    /// cracked open and have its payload extracted.
    pub fn block_as_kv_store_ops(
        &self,
        key: &Hash256,
        block: SignedBeaconBlock<E>,
        ops: &mut Vec<KeyValueStoreOp>,
    ) -> Result<SignedBeaconBlock<E>, Error> {
        // Split block into blinded block and execution payload.
        let (blinded_block, payload) = block.into();

        // Store blinded block.
        self.blinded_block_as_kv_store_ops(key, &blinded_block, ops);

        // Store execution payload if present.
        if let Some(ref execution_payload) = payload {
            ops.push(execution_payload.as_kv_store_op(*key));
        }

        // Re-construct block. This should always succeed.
        blinded_block
            .try_into_full_block(payload)
            .ok_or(Error::AddPayloadLogicError)
    }

    /// Prepare a signed beacon block for storage in the datbase *without* its payload.
    pub fn blinded_block_as_kv_store_ops(
        &self,
        key: &Hash256,
        blinded_block: &SignedBeaconBlock<E, BlindedPayload<E>>,
        ops: &mut Vec<KeyValueStoreOp>,
    ) {
        let db_key = get_key_for_col(DBColumn::BeaconBlock.into(), key.as_bytes());
        ops.push(KeyValueStoreOp::PutKeyValue(
            db_key,
            blinded_block.as_ssz_bytes(),
        ));
    }

    pub fn try_get_full_block(
        &self,
        block_root: &Hash256,
        slot: Option<Slot>,
    ) -> Result<Option<DatabaseBlock<E>>, Error> {
        metrics::inc_counter(&metrics::BEACON_BLOCK_GET_COUNT);

        // Check the cache.
        if let Some(block) = self.block_cache.lock().get_block(block_root) {
            metrics::inc_counter(&metrics::BEACON_BLOCK_CACHE_HIT_COUNT);
            return Ok(Some(DatabaseBlock::Full(block.clone())));
        }

        // Load the blinded block.
        let Some(blinded_block) = self.get_blinded_block(block_root, slot)? else {
            return Ok(None);
        };

        // If the block is after the split point then we should have the full execution payload
        // stored in the database. If it isn't but payload pruning is disabled, try to load it
        // on-demand.
        //
        // Hold the split lock so that it can't change while loading the payload.
        let split = self.split.read_recursive();

        let block = if blinded_block.message().execution_payload().is_err()
            || blinded_block.slot() >= split.slot
        {
            // Re-constructing the full block should always succeed here.
            let full_block = self.make_full_block(block_root, blinded_block)?;

            // Add to cache.
            self.block_cache
                .lock()
                .put_block(*block_root, full_block.clone());

            DatabaseBlock::Full(full_block)
        } else if !self.config.prune_payloads {
            // If payload pruning is disabled there's a chance we may have the payload of
            // this finalized block. Attempt to load it but don't error in case it's missing.
            let fork_name = blinded_block.fork_name(&self.spec)?;
            if let Some(payload) = self.get_execution_payload(block_root, fork_name)? {
                DatabaseBlock::Full(
                    blinded_block
                        .try_into_full_block(Some(payload))
                        .ok_or(Error::AddPayloadLogicError)?,
                )
            } else {
                DatabaseBlock::Blinded(blinded_block)
            }
        } else {
            DatabaseBlock::Blinded(blinded_block)
        };
        drop(split);

        Ok(Some(block))
    }

    /// Fetch a full block with execution payload from the store.
    pub fn get_full_block(
        &self,
        block_root: &Hash256,
        slot: Option<Slot>,
    ) -> Result<Option<SignedBeaconBlock<E>>, Error> {
        match self.try_get_full_block(block_root, slot)? {
            Some(DatabaseBlock::Full(block)) => Ok(Some(block)),
            Some(DatabaseBlock::Blinded(block)) => Err(
                HotColdDBError::MissingFullBlockExecutionPayloadPruned(*block_root, block.slot())
                    .into(),
            ),
            None => Ok(None),
        }
    }

    /// Convert a blinded block into a full block by loading its execution payload if necessary.
    pub fn make_full_block(
        &self,
        block_root: &Hash256,
        blinded_block: SignedBeaconBlock<E, BlindedPayload<E>>,
    ) -> Result<SignedBeaconBlock<E>, Error> {
        if blinded_block.message().execution_payload().is_ok() {
            let fork_name = blinded_block.fork_name(&self.spec)?;
            let execution_payload = self
                .get_execution_payload(block_root, fork_name)?
                .ok_or(HotColdDBError::MissingExecutionPayload(*block_root))?;
            blinded_block.try_into_full_block(Some(execution_payload))
        } else {
            blinded_block.try_into_full_block(None)
        }
        .ok_or(Error::AddPayloadLogicError)
    }

    pub fn get_blinded_block(
        &self,
        block_root: &Hash256,
        slot: Option<Slot>,
    ) -> Result<Option<SignedBlindedBeaconBlock<E>>, Error> {
        // If linear_blocks is disabled, all blocks are in the hot DB.
        if !self.config.linear_blocks {
            return self.get_hot_blinded_block(block_root);
        }

        let split = self.get_split_info();
        if let Some(slot) = slot {
            // FIXME(sproul): this split block_root condition looks wrong
            if (slot < split.slot || slot == 0) && *block_root != split.block_root {
                // To the freezer DB.
                self.get_cold_blinded_block_by_slot(slot)
            } else {
                self.get_hot_blinded_block(block_root)
            }
        } else {
            match self.get_hot_blinded_block(block_root)? {
                Some(block) => Ok(Some(block)),
                None => self.get_cold_blinded_block_by_root(block_root),
            }
        }
    }

    pub fn get_hot_blinded_block(
        &self,
        block_root: &Hash256,
    ) -> Result<Option<SignedBlindedBeaconBlock<E>>, Error> {
        self.get_block_with(block_root, |bytes| {
            SignedBeaconBlock::from_ssz_bytes(bytes, &self.spec)
        })
    }

    pub fn get_cold_blinded_block_by_root(
        &self,
        block_root: &Hash256,
    ) -> Result<Option<SignedBlindedBeaconBlock<E>>, Error> {
        // Load slot.
        if let Some(FrozenBlockSlot(block_slot)) = self.cold_db.get(block_root)? {
            self.get_cold_blinded_block_by_slot(block_slot)
        } else {
            Ok(None)
        }
    }

    pub fn get_cold_blinded_block_by_slot(
        &self,
        slot: Slot,
    ) -> Result<Option<SignedBlindedBeaconBlock<E>>, Error> {
        let Some(bytes) = self.cold_db.get_bytes(
            DBColumn::BeaconBlockFrozen.into(),
            &slot.as_u64().to_be_bytes(),
        )?
        else {
            return Ok(None);
        };

        let mut ssz_bytes = Vec::with_capacity(self.config.estimate_decompressed_size(bytes.len()));
        let mut decoder = Decoder::new(&*bytes).map_err(Error::Compression)?;
        decoder
            .read_to_end(&mut ssz_bytes)
            .map_err(Error::Compression)?;
        Ok(Some(SignedBeaconBlock::from_ssz_bytes(
            &ssz_bytes, &self.spec,
        )?))
    }

    pub fn put_cold_blinded_block(
        &self,
        block_root: &Hash256,
        block: &SignedBlindedBeaconBlock<E>,
    ) -> Result<(), Error> {
        let mut ops = Vec::with_capacity(2);
        self.blinded_block_as_cold_kv_store_ops(block_root, block, &mut ops)?;
        self.cold_db.do_atomically(ops)
    }

    pub fn blinded_block_as_cold_kv_store_ops(
        &self,
        block_root: &Hash256,
        block: &SignedBlindedBeaconBlock<E>,
        kv_store_ops: &mut Vec<KeyValueStoreOp>,
    ) -> Result<(), Error> {
        // Write the block root to slot mapping.
        let slot = block.slot();
        kv_store_ops.push(FrozenBlockSlot(slot).as_kv_store_op(*block_root));

        // Write the slot to block root mapping.
        kv_store_ops.push(KeyValueStoreOp::PutKeyValue(
            get_key_for_col(
                DBColumn::BeaconBlockRoots.into(),
                &slot.as_u64().to_be_bytes(),
            ),
            block_root.as_bytes().to_vec(),
        ));

        // Write the block keyed by slot.
        let db_key = get_key_for_col(
            DBColumn::BeaconBlockFrozen.into(),
            &slot.as_u64().to_be_bytes(),
        );

        let ssz_bytes = block.as_ssz_bytes();
        let mut compressed_value =
            Vec::with_capacity(self.config.estimate_compressed_size(ssz_bytes.len()));
        let mut encoder = Encoder::new(&mut compressed_value, self.config.compression_level)
            .map_err(Error::Compression)?;
        encoder.write_all(&ssz_bytes).map_err(Error::Compression)?;
        encoder.finish().map_err(Error::Compression)?;

        kv_store_ops.push(KeyValueStoreOp::PutKeyValue(db_key, compressed_value));

        Ok(())
    }

    /// Fetch a block from the store, ignoring which fork variant it *should* be for.
    pub fn get_block_any_variant<Payload: AbstractExecPayload<E>>(
        &self,
        block_root: &Hash256,
    ) -> Result<Option<SignedBeaconBlock<E, Payload>>, Error> {
        self.get_block_with(block_root, SignedBeaconBlock::any_from_ssz_bytes)
    }

    /// Fetch a block from the store using a custom decode function.
    ///
    /// This is useful for e.g. ignoring the slot-indicated fork to forcefully load a block as if it
    /// were for a different fork.
    pub fn get_block_with<Payload: AbstractExecPayload<E>>(
        &self,
        block_root: &Hash256,
        decoder: impl FnOnce(&[u8]) -> Result<SignedBeaconBlock<E, Payload>, ssz::DecodeError>,
    ) -> Result<Option<SignedBeaconBlock<E, Payload>>, Error> {
        self.hot_db
            .get_bytes(DBColumn::BeaconBlock.into(), block_root.as_bytes())?
            .map(|block_bytes| decoder(&block_bytes))
            .transpose()
            .map_err(|e| e.into())
    }

    /// Load the execution payload for a block from disk.
    /// This method deserializes with the proper fork.
    pub fn get_execution_payload(
        &self,
        block_root: &Hash256,
        fork_name: ForkName,
    ) -> Result<Option<ExecutionPayload<E>>, Error> {
        let column = ExecutionPayload::<E>::db_column().into();
        let key = block_root.as_bytes();

        match self.hot_db.get_bytes(column, key)? {
            Some(bytes) => Ok(Some(ExecutionPayload::from_ssz_bytes(&bytes, fork_name)?)),
            None => Ok(None),
        }
    }

    /// Load the execution payload for a block from disk.
    /// DANGEROUS: this method just guesses the fork.
    pub fn get_execution_payload_dangerous_fork_agnostic(
        &self,
        block_root: &Hash256,
    ) -> Result<Option<ExecutionPayload<E>>, Error> {
        self.get_item(block_root)
    }

    /// Check if the execution payload for a block exists on disk.
    pub fn execution_payload_exists(&self, block_root: &Hash256) -> Result<bool, Error> {
        self.get_item::<ExecutionPayload<E>>(block_root)
            .map(|payload| payload.is_some())
    }

    /// Store an execution payload in the hot database.
    pub fn put_execution_payload(
        &self,
        block_root: &Hash256,
        execution_payload: &ExecutionPayload<E>,
    ) -> Result<(), Error> {
        self.hot_db
            .do_atomically(vec![execution_payload.as_kv_store_op(*block_root)])
    }

    /// Check if the blobs for a block exists on disk.
    pub fn blobs_exist(&self, block_root: &Hash256) -> Result<bool, Error> {
        self.blobs_db
            .key_exists(DBColumn::BeaconBlob.into(), block_root.as_bytes())
    }

    /// Determine whether a block exists in the database (hot *or* cold).
    pub fn block_exists(&self, block_root: &Hash256) -> Result<bool, Error> {
        Ok(self
            .hot_db
            .key_exists(DBColumn::BeaconBlock.into(), block_root.as_bytes())?
            || self
                .cold_db
                .key_exists(DBColumn::BeaconBlock.into(), block_root.as_bytes())?)
    }

    /// Delete a block from the store and the block cache.
    pub fn delete_block(&self, block_root: &Hash256) -> Result<(), Error> {
        self.block_cache.lock().delete(block_root);
        self.hot_db
            .key_delete(DBColumn::BeaconBlock.into(), block_root.as_bytes())?;
        self.hot_db
            .key_delete(DBColumn::ExecPayload.into(), block_root.as_bytes())?;
        self.blobs_db
            .key_delete(DBColumn::BeaconBlob.into(), block_root.as_bytes())
    }

    pub fn put_blobs(&self, block_root: &Hash256, blobs: BlobSidecarList<E>) -> Result<(), Error> {
        self.blobs_db.put_bytes(
            DBColumn::BeaconBlob.into(),
            block_root.as_bytes(),
            &blobs.as_ssz_bytes(),
        )?;
        self.block_cache.lock().put_blobs(*block_root, blobs);
        Ok(())
    }

    pub fn blobs_as_kv_store_ops(
        &self,
        key: &Hash256,
        blobs: BlobSidecarList<E>,
        ops: &mut Vec<KeyValueStoreOp>,
    ) {
        let db_key = get_key_for_col(DBColumn::BeaconBlob.into(), key.as_bytes());
        ops.push(KeyValueStoreOp::PutKeyValue(db_key, blobs.as_ssz_bytes()));
    }

    pub fn put_state_summary(
        &self,
        state_root: &Hash256,
        summary: HotStateSummary,
    ) -> Result<(), Error> {
        self.hot_db.put(state_root, &summary).map_err(Into::into)
    }

    /// Store a state in the store.
    pub fn put_state(&self, state_root: &Hash256, state: &BeaconState<E>) -> Result<(), Error> {
        let mut ops: Vec<KeyValueStoreOp> = Vec::new();
        if state.slot() < self.get_split_slot() {
            self.store_cold_state(state_root, state, &mut ops)?;
            self.cold_db.do_atomically(ops)
        } else {
            self.store_hot_state(state_root, state, &mut ops)?;
            self.hot_db.do_atomically(ops)
        }
    }

    /// Fetch a state from the store.
    ///
    /// If `slot` is provided then it will be used as a hint as to which database should
    /// be checked. Importantly, if the slot hint is provided and indicates a slot that lies
    /// in the freezer database, then only the freezer database will be accessed and `Ok(None)`
    /// will be returned if the provided `state_root` doesn't match the state root of the
    /// frozen state at `slot`. Consequently, if a state from a non-canonical chain is desired, it's
    /// best to set `slot` to `None`, or call `load_hot_state` directly.
    pub fn get_state(
        &self,
        state_root: &Hash256,
        slot: Option<Slot>,
    ) -> Result<Option<BeaconState<E>>, Error> {
        metrics::inc_counter(&metrics::BEACON_STATE_GET_COUNT);

        if let Some(slot) = slot {
            if slot < self.get_split_slot() {
                // Although we could avoid a DB lookup by shooting straight for the
                // frozen state using `load_cold_state_by_slot`, that would be incorrect
                // in the case where the caller provides a `state_root` that's off the canonical
                // chain. This way we avoid returning a state that doesn't match `state_root`.
                self.load_cold_state(state_root)
            } else {
                self.get_hot_state(state_root)
            }
        } else {
            match self.get_hot_state(state_root)? {
                Some(state) => Ok(Some(state)),
                None => self.load_cold_state(state_root),
            }
        }
    }

    /// Get a state with `latest_block_root == block_root` advanced through to at most `slot`.
    ///
    /// See `Self::get_advanced_hot_state` for information about `max_slot`.
    ///
    /// ## Warning
    ///
    /// The returned state **is not a valid beacon state**, it can only be used for obtaining
    /// shuffling to process attestations. At least the following components of the state will be
    /// broken/invalid:
    ///
    /// - `state.state_roots`
    /// - `state.block_roots`
    pub fn get_inconsistent_state_for_attestation_verification_only(
        &self,
        block_root: &Hash256,
        max_slot: Slot,
        state_root: Hash256,
    ) -> Result<Option<(Hash256, BeaconState<E>)>, Error> {
        metrics::inc_counter(&metrics::BEACON_STATE_GET_COUNT);
        self.get_advanced_hot_state_with_strategy(
            *block_root,
            max_slot,
            state_root,
            StateProcessingStrategy::Inconsistent,
        )
    }

    /// Get a state with `latest_block_root == block_root` advanced through to at most `max_slot`.
    ///
    /// The `state_root` argument is used to look up the block's un-advanced state in case an
    /// advanced state is not found.
    ///
    /// Return the `(result_state_root, state)` satisfying:
    ///
    /// - `result_state_root == state.canonical_root()`
    /// - `state.slot() <= max_slot`
    /// - `state.get_latest_block_root(result_state_root) == block_root`
    ///
    /// Presently this is only used to avoid loading the un-advanced split state, but in future will
    /// be expanded to return states from an in-memory cache.
    pub fn get_advanced_hot_state(
        &self,
        block_root: Hash256,
        max_slot: Slot,
        state_root: Hash256,
    ) -> Result<Option<(Hash256, BeaconState<E>)>, Error> {
        if let Some(cached) = self
            .state_cache
            .lock()
            .get_by_block_root(block_root, max_slot)
        {
            return Ok(Some(cached));
        }
        self.get_advanced_hot_state_with_strategy(
            block_root,
            max_slot,
            state_root,
            StateProcessingStrategy::Accurate,
        )
    }

    /// Same as `get_advanced_hot_state` but taking a `StateProcessingStrategy`.
    // FIXME(sproul): delete the state processing strategy stuff again
    pub fn get_advanced_hot_state_with_strategy(
        &self,
        block_root: Hash256,
        max_slot: Slot,
        state_root: Hash256,
        _state_processing_strategy: StateProcessingStrategy,
    ) -> Result<Option<(Hash256, BeaconState<E>)>, Error> {
        // Hold a read lock on the split point so it can't move while we're trying to load the
        // state.
        let split = self.split.read_recursive();

        // Sanity check max-slot against the split slot.
        if max_slot < split.slot {
            return Err(HotColdDBError::FinalizedStateNotInHotDatabase {
                split_slot: split.slot,
                request_slot: max_slot,
                block_root,
            }
            .into());
        }

        let state_root = if block_root == split.block_root && split.slot <= max_slot {
            split.state_root
        } else {
            state_root
        };
        let opt_state = self
            .load_hot_state(&state_root)?
            .map(|(state, _block_root)| (state_root, state));
        drop(split);
        Ok(opt_state)
    }

    /// Delete a state, ensuring it is removed from the LRU cache, as well as from on-disk.
    ///
    /// It is assumed that all states being deleted reside in the hot DB, even if their slot is less
    /// than the split point. You shouldn't delete states from the finalized portion of the chain
    /// (which are frozen, and won't be deleted), or valid descendents of the finalized checkpoint
    /// (which will be deleted by this function but shouldn't be).
    pub fn delete_state(&self, state_root: &Hash256, slot: Slot) -> Result<(), Error> {
        self.do_atomically_with_block_and_blobs_cache(vec![StoreOp::DeleteState(
            *state_root,
            Some(slot),
        )])
    }

    pub fn forwards_block_roots_iterator(
        &self,
        start_slot: Slot,
        end_state: BeaconState<E>,
        end_block_root: Hash256,
    ) -> Result<impl Iterator<Item = Result<(Hash256, Slot), Error>> + '_, Error> {
        HybridForwardsBlockRootsIterator::new(
            self,
            DBColumn::BeaconBlockRoots,
            start_slot,
            None,
            || Ok((end_state, end_block_root)),
        )
    }

    pub fn forwards_block_roots_iterator_until(
        &self,
        start_slot: Slot,
        end_slot: Slot,
        get_state: impl FnOnce() -> Result<(BeaconState<E>, Hash256), Error>,
    ) -> Result<HybridForwardsBlockRootsIterator<E, Hot, Cold>, Error> {
        HybridForwardsBlockRootsIterator::new(
            self,
            DBColumn::BeaconBlockRoots,
            start_slot,
            Some(end_slot),
            get_state,
        )
    }

    pub fn forwards_state_roots_iterator(
        &self,
        start_slot: Slot,
        end_state_root: Hash256,
        end_state: BeaconState<E>,
    ) -> Result<impl Iterator<Item = Result<(Hash256, Slot), Error>> + '_, Error> {
        HybridForwardsStateRootsIterator::new(
            self,
            DBColumn::BeaconStateRoots,
            start_slot,
            None,
            || Ok((end_state, end_state_root)),
        )
    }

    pub fn forwards_state_roots_iterator_until(
        &self,
        start_slot: Slot,
        end_slot: Slot,
        get_state: impl FnOnce() -> Result<(BeaconState<E>, Hash256), Error>,
    ) -> Result<HybridForwardsStateRootsIterator<E, Hot, Cold>, Error> {
        HybridForwardsStateRootsIterator::new(
            self,
            DBColumn::BeaconStateRoots,
            start_slot,
            Some(end_slot),
            get_state,
        )
    }

    pub fn put_item<I: StoreItem>(&self, key: &Hash256, item: &I) -> Result<(), Error> {
        self.hot_db.put(key, item)
    }

    pub fn get_item<I: StoreItem>(&self, key: &Hash256) -> Result<Option<I>, Error> {
        self.hot_db.get(key)
    }

    pub fn item_exists<I: StoreItem>(&self, key: &Hash256) -> Result<bool, Error> {
        self.hot_db.exists::<I>(key)
    }

    /// Convert a batch of `StoreOp` to a batch of `KeyValueStoreOp`.
    pub fn convert_to_kv_batch(
        &self,
        batch: Vec<StoreOp<E>>,
    ) -> Result<Vec<KeyValueStoreOp>, Error> {
        let mut key_value_batch = Vec::with_capacity(batch.len());
        for op in batch {
            match op {
                StoreOp::PutBlock(block_root, block) => {
                    self.block_as_kv_store_ops(
                        &block_root,
                        block.as_ref().clone(),
                        &mut key_value_batch,
                    )?;
                }

                StoreOp::PutBlobs(block_root, blobs) => {
                    self.blobs_as_kv_store_ops(&block_root, blobs, &mut key_value_batch);
                }

                StoreOp::PutState(state_root, state) => {
                    self.store_hot_state(&state_root, state, &mut key_value_batch)?;
                }

                StoreOp::PutStateTemporaryFlag(state_root) => {
                    key_value_batch.push(TemporaryFlag.as_kv_store_op(state_root));
                }

                StoreOp::DeleteStateTemporaryFlag(state_root) => {
                    let db_key =
                        get_key_for_col(TemporaryFlag::db_column().into(), state_root.as_bytes());
                    key_value_batch.push(KeyValueStoreOp::DeleteKey(db_key));
                }

                StoreOp::DeleteBlock(block_root) => {
                    let key = get_key_for_col(DBColumn::BeaconBlock.into(), block_root.as_bytes());
                    key_value_batch.push(KeyValueStoreOp::DeleteKey(key));
                }

                StoreOp::DeleteBlobs(block_root) => {
                    let key = get_key_for_col(DBColumn::BeaconBlob.into(), block_root.as_bytes());
                    key_value_batch.push(KeyValueStoreOp::DeleteKey(key));
                }

                StoreOp::DeleteState(state_root, slot) => {
                    let state_summary_key =
                        get_key_for_col(DBColumn::BeaconStateSummary.into(), state_root.as_bytes());
                    key_value_batch.push(KeyValueStoreOp::DeleteKey(state_summary_key));

                    if slot.map_or(true, |slot| slot % E::slots_per_epoch() == 0) {
                        // Delete full state if any.
                        let state_key =
                            get_key_for_col(DBColumn::BeaconState.into(), state_root.as_bytes());
                        key_value_batch.push(KeyValueStoreOp::DeleteKey(state_key));

                        // Delete diff too.
                        let diff_key = get_key_for_col(
                            DBColumn::BeaconStateDiff.into(),
                            state_root.as_bytes(),
                        );
                        key_value_batch.push(KeyValueStoreOp::DeleteKey(diff_key));
                    }
                }
                StoreOp::DeleteExecutionPayload(block_root) => {
                    let key = get_key_for_col(DBColumn::ExecPayload.into(), block_root.as_bytes());
                    key_value_batch.push(KeyValueStoreOp::DeleteKey(key));
                }

                StoreOp::KeyValueOp(kv_op) => {
                    key_value_batch.push(kv_op);
                }
            }
        }
        Ok(key_value_batch)
    }

    pub fn do_atomically_with_block_and_blobs_cache(
        &self,
        batch: Vec<StoreOp<E>>,
    ) -> Result<(), Error> {
        let mut blobs_to_delete = Vec::new();
        let (blobs_ops, hot_db_ops): (Vec<StoreOp<E>>, Vec<StoreOp<E>>) =
            batch.into_iter().partition(|store_op| match store_op {
                StoreOp::PutBlobs(_, _) => true,
                StoreOp::DeleteBlobs(block_root) => {
                    match self.get_blobs(block_root) {
                        Ok(Some(blob_sidecar_list)) => {
                            blobs_to_delete.push((*block_root, blob_sidecar_list));
                        }
                        Err(e) => {
                            error!(
                                self.log, "Error getting blobs";
                                "block_root" => %block_root,
                                "error" => ?e
                            );
                        }
                        _ => (),
                    }
                    true
                }
                StoreOp::PutBlock(_, _) | StoreOp::DeleteBlock(_) => false,
                _ => false,
            });

        // Update database whilst holding a lock on cache, to ensure that the cache updates
        // atomically with the database.
        let mut guard = self.block_cache.lock();

        let blob_cache_ops = blobs_ops.clone();
        // Try to execute blobs store ops.
        self.blobs_db
            .do_atomically(self.convert_to_kv_batch(blobs_ops)?)?;

        let hot_db_cache_ops = hot_db_ops.clone();
        // Try to execute hot db store ops.
        let tx_res = match self.convert_to_kv_batch(hot_db_ops) {
            Ok(kv_store_ops) => self.hot_db.do_atomically(kv_store_ops),
            Err(e) => Err(e),
        };
        // Rollback on failure
        if let Err(e) = tx_res {
            error!(
                self.log,
                "Database write failed";
                "error" => ?e,
                "action" => "reverting blob DB changes"
            );
            let mut blob_cache_ops = blob_cache_ops;
            for op in blob_cache_ops.iter_mut() {
                let reverse_op = match op {
                    StoreOp::PutBlobs(block_root, _) => StoreOp::DeleteBlobs(*block_root),
                    StoreOp::DeleteBlobs(_) => match blobs_to_delete.pop() {
                        Some((block_root, blobs)) => StoreOp::PutBlobs(block_root, blobs),
                        None => return Err(HotColdDBError::Rollback.into()),
                    },
                    _ => return Err(HotColdDBError::Rollback.into()),
                };
                *op = reverse_op;
            }
            self.blobs_db
                .do_atomically(self.convert_to_kv_batch(blob_cache_ops)?)?;
            return Err(e);
        }

        for op in hot_db_cache_ops {
            match op {
                StoreOp::PutBlock(block_root, block) => {
                    guard.put_block(block_root, (*block).clone());
                }

                StoreOp::PutBlobs(_, _) => (),

                StoreOp::PutState(_, _) => (),

                StoreOp::PutStateTemporaryFlag(_) => (),

                StoreOp::DeleteStateTemporaryFlag(_) => (),

                StoreOp::DeleteBlock(block_root) => {
                    guard.delete_block(&block_root);
                    self.state_cache.lock().delete_block_states(&block_root);
                }

                StoreOp::DeleteState(state_root, _) => {
                    self.state_cache.lock().delete_state(&state_root)
                }

                StoreOp::DeleteBlobs(_) => (),

                StoreOp::DeleteExecutionPayload(_) => (),

                StoreOp::KeyValueOp(_) => (),
            }
        }

        for op in blob_cache_ops {
            match op {
                StoreOp::PutBlobs(block_root, blobs) => {
                    guard.put_blobs(block_root, blobs);
                }

                StoreOp::DeleteBlobs(block_root) => {
                    guard.delete_blobs(&block_root);
                }

                _ => (),
            }
        }

        drop(guard);

        Ok(())
    }

    /// Store a post-finalization state efficiently in the hot database.
    ///
    /// On an epoch boundary, store a full state. On an intermediate slot, store
    /// just a backpointer to the nearest epoch boundary.
    pub fn store_hot_state(
        &self,
        state_root: &Hash256,
        state: &BeaconState<E>,
        ops: &mut Vec<KeyValueStoreOp>,
    ) -> Result<(), Error> {
        // Put the state in the cache.
        // FIXME(sproul): could optimise out the block root
        let block_root = state.get_latest_block_root(*state_root);

        // Avoid storing states in the database if they already exist in the state cache.
        // The exception to this is the finalized state, which must exist in the cache before it
        // is stored on disk.
        if let PutStateOutcome::Duplicate =
            self.state_cache
                .lock()
                .put_state(*state_root, block_root, state)?
        {
            debug!(
                self.log,
                "Skipping storage of cached state";
                "slot" => state.slot()
            );
            return Ok(());
        }

        // Store a summary of the state.
        // We store one even for the epoch boundary states, as we may need their slots
        // when doing a look up by state root.
        let diff_base_slot = self.state_diff_slot(state.slot());

        let hot_state_summary = HotStateSummary::new(state_root, state, diff_base_slot)?;
        let op = hot_state_summary.as_kv_store_op(*state_root);
        ops.push(op);

        // On an epoch boundary, consider storing:
        //
        // 1. A full state, if the state is the split state or a fork boundary state.
        // 2. A state diff, if the state is a multiple of `epochs_per_state_diff` after the
        //    split state.
        if state.slot() % E::slots_per_epoch() == 0 {
            if self.is_stored_as_full_state(*state_root, state.slot())? {
                info!(
                    self.log,
                    "Storing full state on epoch boundary";
                    "slot" => state.slot(),
                    "state_root" => ?state_root,
                );
                self.store_full_state_in_batch(state_root, state, ops)?;
            } else if let Some(base_slot) = diff_base_slot {
                debug!(
                    self.log,
                    "Storing state diff on boundary";
                    "slot" => state.slot(),
                    "base_slot" => base_slot,
                    "state_root" => ?state_root,
                );
                let diff_base_state_root = hot_state_summary.diff_base_state_root;
                let diff_base_state = self.get_hot_state(&diff_base_state_root)?.ok_or(
                    HotColdDBError::MissingEpochBoundaryState(diff_base_state_root),
                )?;

                let compute_diff_timer =
                    metrics::start_timer(&metrics::BEACON_STATE_DIFF_COMPUTE_TIME);

                let base_buffer = HDiffBuffer::from_state(diff_base_state);
                let target_buffer = HDiffBuffer::from_state(state.clone());
                let diff = HDiff::compute(&base_buffer, &target_buffer)?;
                drop(compute_diff_timer);
                ops.push(diff.as_kv_store_op(*state_root));
            }
        }

        Ok(())
    }

    pub fn store_full_state(
        &self,
        state_root: &Hash256,
        state: &BeaconState<E>,
    ) -> Result<(), Error> {
        let mut ops = Vec::with_capacity(4);
        self.store_full_state_in_batch(state_root, state, &mut ops)?;
        self.hot_db.do_atomically(ops)
    }

    pub fn store_full_state_in_batch(
        &self,
        state_root: &Hash256,
        state: &BeaconState<E>,
        ops: &mut Vec<KeyValueStoreOp>,
    ) -> Result<(), Error> {
        store_full_state(state_root, state, ops, &self.config)
    }

    /// Get a post-finalization state from the database or store.
    pub fn get_hot_state(&self, state_root: &Hash256) -> Result<Option<BeaconState<E>>, Error> {
        get_hot_state_and_apply(
            self,
            *state_root,
            None,
            vec![],
            vec![],
            vec![],
            None,
            vec![],
        )
        /*
        if let Some(state) = self.state_cache.lock().get_by_state_root(*state_root) {
            return Ok(Some(state));
        }
        warn!(
            self.log,
            "State cache missed";
            "state_root" => ?state_root,
        );

        Ok(self.state_promise_cache.get_or_compute(state_root, || {
            let state_from_disk = self.load_hot_state(state_root)?;

            if let Some((state, block_root)) = state_from_disk {
                self.state_cache
                    .lock()
                    .put_state(*state_root, block_root, &state)?;
                Ok(Some(state))
            } else {
                Ok(None)
            }
        })?)
        */
    }

    /// Load a post-finalization state from the hot database.
    ///
    /// Use a combination of state diffs and replayed blocks as appropriate.
    ///
    /// Return the `(state, latest_block_root)` if found.
    pub fn load_hot_state(
        &self,
        state_root: &Hash256,
    ) -> Result<Option<(BeaconState<E>, Hash256)>, Error> {
        let _timer = metrics::start_timer(&metrics::BEACON_HOT_STATE_READ_TIMES);
        metrics::inc_counter(&metrics::BEACON_STATE_HOT_GET_COUNT);

        // If the state is the finalized state, load it from disk. This should only be necessary
        // once during start-up, after which point the finalized state will be cached.
        if *state_root == self.get_split_info().state_root {
            return self.load_hot_state_full(state_root).map(Some);
        }

        let Some(target_summary) = self.load_hot_state_summary(state_root)? else {
            return Ok(None);
        };

        let target_slot = target_summary.slot;
        let target_latest_block_root = target_summary.latest_block_root;

        // Load the latest block, and use it to confirm the validity of this state.
        if self
            .get_blinded_block(&target_summary.latest_block_root, None)?
            .is_none()
        {
            // Dangling state, will be deleted fully once finalization advances past it.
            debug!(
                self.log,
                "Ignoring state load for dangling state";
                "state_root" => ?state_root,
                "slot" => target_slot,
                "latest_block_root" => ?target_summary.latest_block_root,
            );
            return Ok(None);
        }

        // Take a read lock on the split point while we load data from prior states. We need
        // to prevent the finalization migration from deleting the state summaries and state diffs
        // that we are iterating back through.
        let split_read_lock = self.split.read_recursive();

        // Backtrack until we reach a state that is in the cache, or in the worst case
        // the finalized state (this should only be reachable on first start-up).
        let state_summary_iter = HotStateRootIter::new(self, target_slot, *state_root);

        // State and state root of the state upon which blocks and diffs will be replayed.
        let mut base_state = None;

        // State diffs to be replayed on top of `base_state`.
        // Each element is `(summary, state_root, diff)` such that applying `diff` to the
        // state with `summary.diff_base_state_root` yields the state with `state_root`.
        let mut state_diffs = VecDeque::new();

        // State roots for all slots between `base_state` and the `target_slot`. Depending on how
        // the diffs fall, some of these roots may not be needed.
        let mut state_roots = VecDeque::new();

        for res in state_summary_iter {
            let (prior_state_root, prior_summary) = res?;

            state_roots.push_front(Ok((prior_state_root, prior_summary.slot)));

            // Check if this state is in the cache.
            if let Some(state) = self.state_cache.lock().get_by_state_root(prior_state_root) {
                debug!(
                    self.log,
                    "Found cached base state for replay";
                    "base_state_root" => ?prior_state_root,
                    "base_slot" => prior_summary.slot,
                    "target_state_root" => ?state_root,
                    "target_slot" => target_slot,
                );
                base_state = Some((prior_state_root, state));
                break;
            }

            // If the prior state is the split state and it isn't cached then load it in
            // entirety from disk. This should only happen on first start up.
            if prior_state_root == split_read_lock.state_root || prior_summary.slot == 0 {
                debug!(
                    self.log,
                    "Using split state as base state for replay";
                    "base_state_root" => ?prior_state_root,
                    "base_slot" => prior_summary.slot,
                    "target_state_root" => ?state_root,
                    "target_slot" => target_slot,
                );
                let (split_state, _) = self.load_hot_state_full(&prior_state_root)?;
                base_state = Some((prior_state_root, split_state));
                break;
            }

            // If there's a state diff stored at this slot, load it and store it for application.
            if !prior_summary.diff_base_state_root.is_zero() {
                let diff = self.load_hot_state_diff(prior_state_root)?;
                state_diffs.push_front((prior_summary, prior_state_root, diff));
            }
        }

        let (_, mut state) = base_state.ok_or(Error::NoBaseStateFound(*state_root))?;

        // Finished reading information about prior states, allow the split point to update.
        drop(split_read_lock);

        // Construct a mutable iterator for the state roots, which will be iterated through
        // consecutive calls to `replay_blocks`.
        let mut state_roots_iter = state_roots.into_iter();

        // This hook caches states from block replay so that they may be reused.
        let state_cacher_hook = |opt_state_root: Option<Hash256>, state: &mut BeaconState<_>| {
            // Ensure all caches are built before attempting to cache.
            state.update_tree_hash_cache()?;
            state.build_all_caches(&self.spec)?;

            if let Some(state_root) = opt_state_root {
                // Cache
                if state.slot() + MAX_PARENT_STATES_TO_CACHE >= target_slot
                    || state.slot() % E::slots_per_epoch() == 0
                {
                    let slot = state.slot();
                    let latest_block_root = state.get_latest_block_root(state_root);
                    if let PutStateOutcome::New =
                        self.state_cache
                            .lock()
                            .put_state(state_root, latest_block_root, state)?
                    {
                        debug!(
                            self.log,
                            "Cached ancestor state";
                            "state_root" => ?state_root,
                            "slot" => slot,
                        );
                    }
                }
            } else {
                debug!(
                    self.log,
                    "Block replay state root miss";
                    "slot" => state.slot(),
                );
            }
            Ok(())
        };

        // Apply the diffs, and replay blocks atop the base state to reach the target state.
        while state.slot() < target_slot {
            // Drop unncessary diffs.
            state_diffs.retain(|(summary, diff_root, _)| {
                let keep = summary.diff_base_slot >= state.slot();
                if !keep {
                    debug!(
                        self.log,
                        "Ignoring irrelevant state diff";
                        "diff_state_root" => ?diff_root,
                        "diff_base_slot" => summary.diff_base_slot,
                        "current_state_slot" => state.slot(),
                    );
                }
                keep
            });

            // Get the next diff that will be applicable, taking the highest slot diff in case of
            // multiple diffs which are applicable at the same base slot, which can happen if the
            // diff frequency has changed.
            let mut next_state_diff: Option<(HotStateSummary, Hash256, HDiff)> = None;
            while let Some((summary, _, _)) = state_diffs.front() {
                if next_state_diff.as_ref().map_or(true, |(current, _, _)| {
                    summary.diff_base_slot == current.diff_base_slot
                }) {
                    next_state_diff = state_diffs.pop_front();
                } else {
                    break;
                }
            }

            // Replay blocks to get to the next diff's base state, or to the target state if there
            // is no next diff to apply.
            if next_state_diff
                .as_ref()
                .map_or(true, |(next_summary, _, _)| {
                    next_summary.diff_base_slot != state.slot()
                })
            {
                let (next_slot, latest_block_root) = next_state_diff
                    .as_ref()
                    .map(|(summary, _, _)| (summary.diff_base_slot, summary.latest_block_root))
                    .unwrap_or_else(|| (target_summary.slot, target_latest_block_root));
                debug!(
                    self.log,
                    "Replaying blocks";
                    "from_slot" => state.slot(),
                    "to_slot" => next_slot,
                    "latest_block_root" => ?latest_block_root,
                );
                let blocks =
                    self.load_blocks_to_replay(state.slot(), next_slot, latest_block_root)?;

                state = self.replay_blocks(
                    state,
                    blocks,
                    next_slot,
                    &mut state_roots_iter,
                    Some(Box::new(state_cacher_hook)),
                )?;

                state.update_tree_hash_cache()?;
                state.build_all_caches(&self.spec)?;
            }

            // Apply state diff. Block replay should have ensured that the diff is now applicable.
            if let Some((summary, to_root, diff)) = next_state_diff {
                let block_root = summary.latest_block_root;
                debug!(
                    self.log,
                    "Applying state diff";
                    "from_root" => ?summary.diff_base_state_root,
                    "from_slot" => summary.diff_base_slot,
                    "to_root" => ?to_root,
                    "to_slot" => summary.slot,
                    "block_root" => ?block_root,
                );
                assert_eq!(summary.diff_base_slot, state.slot());

                let t = std::time::Instant::now();
                let pre_state = state.clone();
                let mut base_buffer = HDiffBuffer::from_state(pre_state.clone());
                diff.apply(&mut base_buffer)?;
                state = base_buffer.into_state(&self.spec)?;
                let application_ms = t.elapsed().as_millis();

                // Rebase state before adding it to the cache, to ensure it uses minimal memory.
                let t = std::time::Instant::now();
                state.rebase_on(&pre_state, &self.spec)?;
                let rebase_ms = t.elapsed().as_millis();

                let t = std::time::Instant::now();
                state.update_tree_hash_cache()?;
                let tree_hash_ms = t.elapsed().as_millis();

                let t = std::time::Instant::now();
                state.build_all_caches(&self.spec)?;
                let cache_ms = t.elapsed().as_millis();

                debug!(
                    self.log,
                    "State diff applied";
                    "application_ms" => application_ms,
                    "rebase_ms" => rebase_ms,
                    "tree_hash_ms" => tree_hash_ms,
                    "cache_ms" => cache_ms,
                    "slot" => state.slot()
                );

                // Add state to the cache, it is by definition an epoch boundary state and likely
                // to be useful.
                self.state_cache
                    .lock()
                    .put_state(to_root, block_root, &state)?;
            }
        }

        Ok(Some((state, target_latest_block_root)))
    }

    /// Determine if the `state_root` at `slot` should be stored as a full state.
    ///
    /// This is dependent on the database's current split point, so may change from `false` to
    /// `true` after a finalization update. It cannot change from `true` to `false` for a state in
    /// the hot database as the split state will be migrated to the freezer.
    ///
    /// All fork boundary states are also stored as full states.
    pub fn is_stored_as_full_state(&self, state_root: Hash256, slot: Slot) -> Result<bool, Error> {
        let split = self.get_split_info();

        if slot >= split.slot {
            Ok(state_root == split.state_root
                || self.spec.fork_activated_at_slot::<E>(slot).is_some()
                || slot == 0)
        } else {
            Err(Error::SlotIsBeforeSplit { slot })
        }
    }

    /// Determine if a state diff should be stored at `slot`.
    ///
    /// If `Some(base_slot)` is returned then a state diff should be constructed for the state
    /// at `slot` based on the ancestor state at `base_slot`. The frequency of state diffs stored
    /// on disk is determined by the `epochs_per_state_diff` parameter.
    pub fn state_diff_slot(&self, slot: Slot) -> Option<Slot> {
        let split = self.get_split_info();
        let slots_per_epoch = E::slots_per_epoch();

        if slot % slots_per_epoch != 0 {
            return None;
        }

        let epochs_since_split = slot.saturating_sub(split.slot).epoch(slots_per_epoch);

        (epochs_since_split > 0 && epochs_since_split % self.config.epochs_per_state_diff == 0)
            .then(|| slot.saturating_sub(self.config.epochs_per_state_diff * slots_per_epoch))
    }

    pub fn load_hot_state_full(
        &self,
        state_root: &Hash256,
    ) -> Result<(BeaconState<E>, Hash256), Error> {
        let pubkey_cache = self.immutable_validators.read();
        let validator_pubkeys = |i: usize| pubkey_cache.get_validator_pubkey(i);
        let mut state = get_full_state(
            &self.hot_db,
            state_root,
            validator_pubkeys,
            &self.config,
            &self.spec,
        )?
        .ok_or(HotColdDBError::MissingEpochBoundaryState(*state_root))?;

        // Do a tree hash here so that the cache is fully built.
        state.update_tree_hash_cache()?;
        state.build_all_caches(&self.spec)?;

        let latest_block_root = state.get_latest_block_root(*state_root);
        Ok((state, latest_block_root))
    }

    pub fn load_hot_state_diff(&self, state_root: Hash256) -> Result<HDiff, Error> {
        self.hot_db
            .get(&state_root)?
            .ok_or(HotColdDBError::MissingStateDiff(state_root).into())
    }

    pub fn store_cold_state_summary(
        &self,
        state_root: &Hash256,
        slot: Slot,
        ops: &mut Vec<KeyValueStoreOp>,
    ) -> Result<(), Error> {
        ops.push(ColdStateSummary { slot }.as_kv_store_op(*state_root));
        ops.push(KeyValueStoreOp::PutKeyValue(
            get_key_for_col(
                DBColumn::BeaconStateRoots.into(),
                &slot.as_u64().to_be_bytes(),
            ),
            state_root.as_bytes().to_vec(),
        ));
        Ok(())
    }

    /// Store a pre-finalization state in the freezer database.
    pub fn store_cold_state(
        &self,
        state_root: &Hash256,
        state: &BeaconState<E>,
        ops: &mut Vec<KeyValueStoreOp>,
    ) -> Result<(), Error> {
        self.store_cold_state_summary(state_root, state.slot(), ops)?;

        let slot = state.slot();
        match self.hierarchy.storage_strategy(slot)? {
            StorageStrategy::ReplayFrom(from) => {
                debug!(
                    self.log,
                    "Storing cold state";
                    "strategy" => "replay",
                    "from_slot" => from,
                    "slot" => state.slot(),
                );
            }
            StorageStrategy::Snapshot => {
                debug!(
                    self.log,
                    "Storing cold state";
                    "strategy" => "snapshot",
                    "slot" => state.slot(),
                );
                self.store_cold_state_as_snapshot(state, ops)?;
            }
            StorageStrategy::DiffFrom(from) => {
                debug!(
                    self.log,
                    "Storing cold state";
                    "strategy" => "diff",
                    "from_slot" => from,
                    "slot" => state.slot(),
                );
                self.store_cold_state_as_diff(state, from, ops)?;
            }
        }

        Ok(())
    }

    pub fn store_cold_state_as_snapshot(
        &self,
        state: &BeaconState<E>,
        ops: &mut Vec<KeyValueStoreOp>,
    ) -> Result<(), Error> {
        let bytes = state.as_ssz_bytes();
        let mut compressed_value =
            Vec::with_capacity(self.config.estimate_compressed_size(bytes.len()));
        let mut encoder = Encoder::new(&mut compressed_value, self.config.compression_level)
            .map_err(Error::Compression)?;
        encoder.write_all(&bytes).map_err(Error::Compression)?;
        encoder.finish().map_err(Error::Compression)?;

        let key = get_key_for_col(
            DBColumn::BeaconStateSnapshot.into(),
            &state.slot().as_u64().to_be_bytes(),
        );
        ops.push(KeyValueStoreOp::PutKeyValue(key, compressed_value));
        Ok(())
    }

    pub fn load_cold_state_bytes_as_snapshot(&self, slot: Slot) -> Result<Option<Vec<u8>>, Error> {
        match self.cold_db.get_bytes(
            DBColumn::BeaconStateSnapshot.into(),
            &slot.as_u64().to_be_bytes(),
        )? {
            Some(bytes) => {
                let mut ssz_bytes =
                    Vec::with_capacity(self.config.estimate_decompressed_size(bytes.len()));
                let mut decoder = Decoder::new(&*bytes).map_err(Error::Compression)?;
                decoder
                    .read_to_end(&mut ssz_bytes)
                    .map_err(Error::Compression)?;
                Ok(Some(ssz_bytes))
            }
            None => Ok(None),
        }
    }

    pub fn load_cold_state_as_snapshot(&self, slot: Slot) -> Result<Option<BeaconState<E>>, Error> {
        Ok(self
            .load_cold_state_bytes_as_snapshot(slot)?
            .map(|bytes| BeaconState::from_ssz_bytes(&bytes, &self.spec))
            .transpose()?)
    }

    pub fn store_cold_state_as_diff(
        &self,
        state: &BeaconState<E>,
        from_slot: Slot,
        ops: &mut Vec<KeyValueStoreOp>,
    ) -> Result<(), Error> {
        // Load diff base state bytes.
        let (_, base_buffer) = self.load_hdiff_buffer_for_slot(from_slot)?;
        let target_buffer = HDiffBuffer::from_state(state.clone());
        let diff = HDiff::compute(&base_buffer, &target_buffer)?;
        let diff_bytes = diff.as_ssz_bytes();

        let key = get_key_for_col(
            DBColumn::BeaconStateDiff.into(),
            &state.slot().as_u64().to_be_bytes(),
        );
        ops.push(KeyValueStoreOp::PutKeyValue(key, diff_bytes));
        Ok(())
    }

    /// Try to load a pre-finalization state from the freezer database.
    ///
    /// Return `None` if no state with `state_root` lies in the freezer.
    pub fn load_cold_state(&self, state_root: &Hash256) -> Result<Option<BeaconState<E>>, Error> {
        match self.load_cold_state_slot(state_root)? {
            Some(slot) => self.load_cold_state_by_slot(slot),
            None => Ok(None),
        }
    }

    /// Load a pre-finalization state from the freezer database.
    ///
    /// Will reconstruct the state if it lies between restore points.
    pub fn load_cold_state_by_slot(&self, slot: Slot) -> Result<Option<BeaconState<E>>, Error> {
        let (base_slot, hdiff_buffer) = self.load_hdiff_buffer_for_slot(slot)?;
        let base_state = hdiff_buffer.as_state(&self.spec)?;
        debug_assert_eq!(base_slot, base_state.slot());

        if base_state.slot() == slot {
            return Ok(Some(base_state));
        }

        let blocks = self.load_cold_blocks(base_state.slot() + 1, slot)?;

        // Include state root for base state as it is required by block processing.
        let state_root_iter =
            self.forwards_state_roots_iterator_until(base_state.slot(), slot, || {
                panic!("FIXME(sproul): unreachable state root iter miss")
            })?;

        self.replay_blocks(base_state, blocks, slot, state_root_iter, None)
            .map(Some)
    }

    fn load_hdiff_for_slot(&self, slot: Slot) -> Result<HDiff, Error> {
        self.cold_db
            .get_bytes(
                DBColumn::BeaconStateDiff.into(),
                &slot.as_u64().to_be_bytes(),
            )?
            .map(|bytes| HDiff::from_ssz_bytes(&bytes))
            .ok_or(HotColdDBError::MissingHDiff(slot))?
            .map_err(Into::into)
    }

    /// Returns `HDiffBuffer` for the specified slot, or `HDiffBuffer` for the `ReplayFrom` slot if
    /// the diff for the specified slot is not stored.
    fn load_hdiff_buffer_for_slot(&self, slot: Slot) -> Result<(Slot, Arc<HDiffBuffer>), Error> {
        if let Some(buffer) = self.diff_buffer_cache.lock().get(&slot) {
            debug!(
                self.log,
                "Hit diff buffer cache";
                "slot" => slot
            );
            return Ok((slot, buffer.clone()));
        }

        Ok(self.diff_buffer_promise_cache.get_or_compute(&slot, || {
            // Load buffer for the previous state.
            // This amount of recursion (<10 levels) should be OK.
            let t = std::time::Instant::now();
            let (_buffer_slot, buffer) = match self.hierarchy.storage_strategy(slot)? {
                // Base case.
                StorageStrategy::Snapshot => {
                    let state = self
                        .load_cold_state_as_snapshot(slot)?
                        .ok_or(Error::MissingSnapshot(slot))?;
                    let buffer = Arc::new(HDiffBuffer::from_state(state));

                    self.diff_buffer_cache.lock().put(slot, buffer.clone());
                    debug!(
                        self.log,
                        "Added diff buffer to cache";
                        "load_time_ms" => t.elapsed().as_millis(),
                        "slot" => slot
                    );

                    return Ok((slot, buffer));
                }
                // Recursive case.
                StorageStrategy::DiffFrom(from) => self.load_hdiff_buffer_for_slot(from)?,
                StorageStrategy::ReplayFrom(from) => return self.load_hdiff_buffer_for_slot(from),
            };

            // Load diff and apply it to buffer.
            let diff = self.load_hdiff_for_slot(slot)?;
            let buffer = Arc::new(diff.apply_to_parts(buffer.state(), buffer.balances().to_vec())?);

            self.diff_buffer_cache.lock().put(slot, buffer.clone());
            debug!(
                self.log,
                "Added diff buffer to cache";
                "load_time_ms" => t.elapsed().as_millis(),
                "slot" => slot
            );

            Ok((slot, buffer))
        })?)
    }

    /// Load cold blocks between `start_slot` and `end_slot` inclusive.
    pub fn load_cold_blocks(
        &self,
        start_slot: Slot,
        end_slot: Slot,
    ) -> Result<Vec<SignedBlindedBeaconBlock<E>>, Error> {
        process_results(
            (start_slot.as_u64()..=end_slot.as_u64())
                .map(Slot::new)
                .map(|slot| self.get_cold_blinded_block_by_slot(slot)),
            |iter| iter.flatten().collect(),
        )
    }

    /// Load the blocks between `start_slot` and `end_slot` by backtracking from `end_block_hash`.
    ///
    /// Blocks are returned in slot-ascending order, suitable for replaying on a state with slot
    /// equal to `start_slot`, to reach a state with slot equal to `end_slot`.
    pub fn load_blocks_to_replay(
        &self,
        start_slot: Slot,
        end_slot: Slot,
        end_block_hash: Hash256,
    ) -> Result<Vec<SignedBeaconBlock<E, BlindedPayload<E>>>, Error> {
        let mut blocks = ParentRootBlockIterator::new(self, end_block_hash)
            .map(|result| result.map(|(_, block)| block))
            // Include the block at the end slot (if any), it needs to be
            // replayed in order to construct the canonical state at `end_slot`.
            .filter(|result| {
                result
                    .as_ref()
                    .map_or(true, |block| block.slot() <= end_slot)
            })
            // Include the block at the start slot (if any). Whilst it doesn't need to be
            // applied to the state, it contains a potentially useful state root.
            //
            // Return `true` on an `Err` so that the `collect` fails, unless the error is a
            // `BlockNotFound` error and some blocks are intentionally missing from the DB.
            // This complexity is unfortunately necessary to avoid loading the parent of the
            // oldest known block -- we can't know that we have all the required blocks until we
            // load a block with slot less than the start slot, which is impossible if there are
            // no blocks with slot less than the start slot.
            .take_while(|result| match result {
                Ok(block) => block.slot() >= start_slot,
                Err(Error::BlockNotFound(_)) => {
                    self.get_oldest_block_slot() == self.spec.genesis_slot
                }
                Err(_) => true,
            })
            .collect::<Result<Vec<_>, _>>()?;
        blocks.reverse();
        Ok(blocks)
    }

    /// Replay `blocks` on top of `state` until `target_slot` is reached.
    ///
    /// Will skip slots as necessary. The returned state is not guaranteed
    /// to have any caches built, beyond those immediately required by block processing.
    pub fn replay_blocks(
        &self,
        state: BeaconState<E>,
        blocks: Vec<SignedBeaconBlock<E, BlindedPayload<E>>>,
        target_slot: Slot,
        state_root_iter: impl Iterator<Item = Result<(Hash256, Slot), Error>>,
        pre_slot_hook: Option<PreSlotHook<E, Error>>,
    ) -> Result<BeaconState<E>, Error> {
        let mut block_replayer = BlockReplayer::new(state, &self.spec)
            .no_signature_verification()
            .minimal_block_root_verification()
            .state_root_iter(state_root_iter);

        if let Some(pre_slot_hook) = pre_slot_hook {
            block_replayer = block_replayer.pre_slot_hook(pre_slot_hook);
        }

        block_replayer
            .apply_blocks(blocks, Some(target_slot))
            .map(|block_replayer| {
                if block_replayer.state_root_miss() {
                    warn!(
                        self.log,
                        "State root cache miss during block replay";
                        "slot" => target_slot,
                    );
                }
                block_replayer.into_state()
            })
    }

    /// Fetch blobs for a given block from the store.
    pub fn get_blobs(&self, block_root: &Hash256) -> Result<Option<BlobSidecarList<E>>, Error> {
        // Check the cache.
        if let Some(blobs) = self.block_cache.lock().get_blobs(block_root) {
            metrics::inc_counter(&metrics::BEACON_BLOBS_CACHE_HIT_COUNT);
            return Ok(Some(blobs.clone()));
        }

        match self
            .blobs_db
            .get_bytes(DBColumn::BeaconBlob.into(), block_root.as_bytes())?
        {
            Some(ref blobs_bytes) => {
                let blobs = BlobSidecarList::from_ssz_bytes(blobs_bytes)?;
                self.block_cache
                    .lock()
                    .put_blobs(*block_root, blobs.clone());
                Ok(Some(blobs))
            }
            None => Ok(None),
        }
    }

    /// Get a reference to the `ChainSpec` used by the database.
    pub fn get_chain_spec(&self) -> &ChainSpec {
        &self.spec
    }

    /// Get a reference to the `Logger` used by the database.
    pub fn logger(&self) -> &Logger {
        &self.log
    }

    /// Fetch a copy of the current split slot from memory.
    pub fn get_split_slot(&self) -> Slot {
        self.split.read_recursive().slot
    }

    /// Fetch a copy of the current split slot from memory.
    pub fn get_split_info(&self) -> Split {
        *self.split.read_recursive()
    }

    pub fn set_split(&self, slot: Slot, state_root: Hash256, block_root: Hash256) {
        *self.split.write() = Split {
            slot,
            state_root,
            block_root,
        };
    }

    /// Load the database schema version from disk.
    fn load_schema_version(&self) -> Result<Option<SchemaVersion>, Error> {
        self.hot_db.get(&SCHEMA_VERSION_KEY)
    }

    /// Store the database schema version.
    pub fn store_schema_version(&self, schema_version: SchemaVersion) -> Result<(), Error> {
        self.hot_db.put(&SCHEMA_VERSION_KEY, &schema_version)
    }

    /// Store the database schema version atomically with additional operations.
    pub fn store_schema_version_atomically(
        &self,
        schema_version: SchemaVersion,
        mut ops: Vec<KeyValueStoreOp>,
    ) -> Result<(), Error> {
        let column = SchemaVersion::db_column().into();
        let key = SCHEMA_VERSION_KEY.as_bytes();
        let db_key = get_key_for_col(column, key);
        let op = KeyValueStoreOp::PutKeyValue(db_key, schema_version.as_store_bytes());
        ops.push(op);

        self.hot_db.do_atomically(ops)
    }

    /// Initialise the anchor info for checkpoint sync starting from `block`.
    pub fn init_anchor_info(
        &self,
        block: BeaconBlockRef<'_, E>,
        retain_historic_states: bool,
    ) -> Result<KeyValueStoreOp, Error> {
        let anchor_slot = block.slot();

        // Set the `state_upper_limit` to the slot of the *next* checkpoint.
        let next_snapshot_slot = self.hierarchy.next_snapshot_slot(anchor_slot)?;
        let state_upper_limit = if !retain_historic_states {
            STATE_UPPER_LIMIT_NO_RETAIN
        } else {
            next_snapshot_slot
        };
        let anchor_info = if state_upper_limit == 0 && anchor_slot == 0 {
            // Genesis archive node: no anchor because we *will* store all states.
            None
        } else {
            Some(AnchorInfo {
                anchor_slot,
                oldest_block_slot: anchor_slot,
                oldest_block_parent: block.parent_root(),
                state_upper_limit,
                state_lower_limit: self.spec.genesis_slot,
            })
        };
        self.compare_and_set_anchor_info(None, anchor_info)
    }

    /// Get a clone of the store's anchor info.
    ///
    /// To do mutations, use `compare_and_set_anchor_info`.
    pub fn get_anchor_info(&self) -> Option<AnchorInfo> {
        self.anchor_info.read_recursive().clone()
    }

    /// Atomically update the anchor info from `prev_value` to `new_value`.
    ///
    /// Return a `KeyValueStoreOp` which should be written to disk, possibly atomically with other
    /// values.
    ///
    /// Return an `AnchorInfoConcurrentMutation` error if the `prev_value` provided
    /// is not correct.
    pub fn compare_and_set_anchor_info(
        &self,
        prev_value: Option<AnchorInfo>,
        new_value: Option<AnchorInfo>,
    ) -> Result<KeyValueStoreOp, Error> {
        let mut anchor_info = self.anchor_info.write();
        if *anchor_info == prev_value {
            let kv_op = self.store_anchor_info_in_batch(&new_value);
            *anchor_info = new_value;
            Ok(kv_op)
        } else {
            Err(Error::AnchorInfoConcurrentMutation)
        }
    }

    /// As for `compare_and_set_anchor_info`, but also writes the anchor to disk immediately.
    pub fn compare_and_set_anchor_info_with_write(
        &self,
        prev_value: Option<AnchorInfo>,
        new_value: Option<AnchorInfo>,
    ) -> Result<(), Error> {
        let kv_store_op = self.compare_and_set_anchor_info(prev_value, new_value)?;
        self.hot_db.do_atomically(vec![kv_store_op])
    }

    /// Load the anchor info from disk, but do not set `self.anchor_info`.
    fn load_anchor_info(&self) -> Result<Option<AnchorInfo>, Error> {
        self.hot_db.get(&ANCHOR_INFO_KEY)
    }

    /// Store the given `anchor_info` to disk.
    ///
    /// The argument is intended to be `self.anchor_info`, but is passed manually to avoid issues
    /// with recursive locking.
    fn store_anchor_info_in_batch(&self, anchor_info: &Option<AnchorInfo>) -> KeyValueStoreOp {
        if let Some(ref anchor_info) = anchor_info {
            anchor_info.as_kv_store_op(ANCHOR_INFO_KEY)
        } else {
            KeyValueStoreOp::DeleteKey(get_key_for_col(
                DBColumn::BeaconMeta.into(),
                ANCHOR_INFO_KEY.as_bytes(),
            ))
        }
    }

    /// If an anchor exists, return its `anchor_slot` field.
    pub fn get_anchor_slot(&self) -> Option<Slot> {
        self.anchor_info
            .read_recursive()
            .as_ref()
            .map(|a| a.anchor_slot)
    }

    /// Initialize the `BlobInfo` when starting from genesis or a checkpoint.
    pub fn init_blob_info(&self, anchor_slot: Slot) -> Result<KeyValueStoreOp, Error> {
        let oldest_blob_slot = self.spec.deneb_fork_epoch.map(|fork_epoch| {
            std::cmp::max(anchor_slot, fork_epoch.start_slot(E::slots_per_epoch()))
        });
        let blob_info = BlobInfo {
            oldest_blob_slot,
            blobs_db: true,
        };
        self.compare_and_set_blob_info(self.get_blob_info(), blob_info)
    }

    /// Get a clone of the store's blob info.
    ///
    /// To do mutations, use `compare_and_set_blob_info`.
    pub fn get_blob_info(&self) -> BlobInfo {
        self.blob_info.read_recursive().clone()
    }

    /// Atomically update the blob info from `prev_value` to `new_value`.
    ///
    /// Return a `KeyValueStoreOp` which should be written to disk, possibly atomically with other
    /// values.
    ///
    /// Return an `BlobInfoConcurrentMutation` error if the `prev_value` provided
    /// is not correct.
    pub fn compare_and_set_blob_info(
        &self,
        prev_value: BlobInfo,
        new_value: BlobInfo,
    ) -> Result<KeyValueStoreOp, Error> {
        let mut blob_info = self.blob_info.write();
        if *blob_info == prev_value {
            let kv_op = self.store_blob_info_in_batch(&new_value);
            *blob_info = new_value;
            Ok(kv_op)
        } else {
            Err(Error::BlobInfoConcurrentMutation)
        }
    }

    /// As for `compare_and_set_blob_info`, but also writes the blob info to disk immediately.
    pub fn compare_and_set_blob_info_with_write(
        &self,
        prev_value: BlobInfo,
        new_value: BlobInfo,
    ) -> Result<(), Error> {
        let kv_store_op = self.compare_and_set_blob_info(prev_value, new_value)?;
        self.hot_db.do_atomically(vec![kv_store_op])
    }

    /// Load the blob info from disk, but do not set `self.blob_info`.
    fn load_blob_info(&self) -> Result<Option<BlobInfo>, Error> {
        self.hot_db.get(&BLOB_INFO_KEY)
    }

    /// Store the given `blob_info` to disk.
    ///
    /// The argument is intended to be `self.blob_info`, but is passed manually to avoid issues
    /// with recursive locking.
    fn store_blob_info_in_batch(&self, blob_info: &BlobInfo) -> KeyValueStoreOp {
        blob_info.as_kv_store_op(BLOB_INFO_KEY)
    }

    /// Return the slot-window describing the available historic states.
    ///
    /// Returns `(lower_limit, upper_limit)`.
    ///
    /// The lower limit is the maximum slot such that frozen states are available for all
    /// previous slots (<=).
    ///
    /// The upper limit is the minimum slot such that frozen states are available for all
    /// subsequent slots (>=).
    ///
    /// If `lower_limit >= upper_limit` then all states are available. This will be true
    /// if the database is completely filled in, as we'll return `(split_slot, 0)` in this
    /// instance.
    pub fn get_historic_state_limits(&self) -> (Slot, Slot) {
        // If checkpoint sync is used then states in the hot DB will always be available, but may
        // become unavailable as finalisation advances due to the lack of a restore point in the
        // database. For this reason we take the minimum of the split slot and the
        // restore-point-aligned `state_upper_limit`, which should be set _ahead_ of the checkpoint
        // slot during initialisation.
        //
        // E.g. if we start from a checkpoint at slot 2048+1024=3072 with SPRP=2048, then states
        // with slots 3072-4095 will be available only while they are in the hot database, and this
        // function will return the current split slot as the upper limit. Once slot 4096 is reached
        // a new restore point will be created at that slot, making all states from 4096 onwards
        // permanently available.
        let split_slot = self.get_split_slot();
        self.anchor_info
            .read_recursive()
            .as_ref()
            .map_or((split_slot, self.spec.genesis_slot), |a| {
                (a.state_lower_limit, min(a.state_upper_limit, split_slot))
            })
    }

    /// Return the minimum slot such that blocks are available for all subsequent slots.
    pub fn get_oldest_block_slot(&self) -> Slot {
        self.anchor_info
            .read_recursive()
            .as_ref()
            .map_or(self.spec.genesis_slot, |anchor| anchor.oldest_block_slot)
    }

    /// Return the in-memory configuration used by the database.
    pub fn get_config(&self) -> &StoreConfig {
        &self.config
    }

    /// Load previously-stored config from disk.
    fn load_config(&self) -> Result<Option<OnDiskStoreConfig>, Error> {
        self.hot_db.get(&CONFIG_KEY)
    }

    /// Write the config to disk.
    fn store_config(&self) -> Result<(), Error> {
        self.hot_db.put(&CONFIG_KEY, &self.config.as_disk_config())
    }

    /// Load the split point from disk, sans block root.
    fn load_split_partial(&self) -> Result<Option<Split>, Error> {
        self.hot_db.get(&SPLIT_KEY)
    }

    /// Load the split point from disk, including block root.
    fn load_split(&self) -> Result<Option<Split>, Error> {
        match self.load_split_partial()? {
            Some(mut split) => {
                // Load the hot state summary to get the block root.
                split.block_root = self
                    .load_hot_state_summary_latest_block_root_any_version(&split.state_root)?
                    .ok_or(HotColdDBError::MissingSplitState(
                        split.state_root,
                        split.slot,
                    ))?;
                Ok(Some(split))
            }
            None => Ok(None),
        }
    }

    /// Stage the split for storage to disk.
    pub fn store_split_in_batch(&self) -> KeyValueStoreOp {
        self.split.read_recursive().as_kv_store_op(SPLIT_KEY)
    }

    /// Load the state root of a restore point.
    #[allow(unused)]
    fn load_restore_point_hash(&self, restore_point_index: u64) -> Result<Hash256, Error> {
        let key = Self::restore_point_key(restore_point_index);
        self.cold_db
            .get(&key)?
            .map(|r: RestorePointHash| r.state_root)
            .ok_or_else(|| HotColdDBError::MissingRestorePointHash(restore_point_index).into())
    }

    /// Store the state root of a restore point.
    #[allow(unused)]
    fn store_restore_point_hash(
        &self,
        restore_point_index: u64,
        state_root: Hash256,
        ops: &mut Vec<KeyValueStoreOp>,
    ) -> Result<(), Error> {
        let value = &RestorePointHash { state_root };
        let op = value.as_kv_store_op(Self::restore_point_key(restore_point_index));
        ops.push(op);
        Ok(())
    }

    /// Convert a `restore_point_index` into a database key.
    #[allow(unused)]
    fn restore_point_key(restore_point_index: u64) -> Hash256 {
        Hash256::from_low_u64_be(restore_point_index)
    }

    /// Load a frozen state's slot, given its root.
    pub fn load_cold_state_slot(&self, state_root: &Hash256) -> Result<Option<Slot>, Error> {
        Ok(self
            .cold_db
            .get(state_root)?
            .map(|s: ColdStateSummary| s.slot))
    }

    /// Load a hot state's summary, given its root.
    pub fn load_hot_state_summary(
        &self,
        state_root: &Hash256,
    ) -> Result<Option<HotStateSummary>, Error> {
        self.hot_db.get(state_root)
    }

    pub fn load_hot_state_summary_latest_block_root_any_version(
        &self,
        state_root: &Hash256,
    ) -> Result<Option<Hash256>, Error> {
        if let Ok(summary_v24) = self.load_hot_state_summary(state_root) {
            return Ok(summary_v24.map(|s| s.latest_block_root));
        }
        let summary_v1 = self.hot_db.get::<HotStateSummaryV1>(state_root)?;
        Ok(summary_v1.map(|s| s.latest_block_root))
    }

    /// Iterate all hot state summaries in the database.
    pub fn iter_hot_state_summaries(
        &self,
    ) -> impl Iterator<Item = Result<(Hash256, HotStateSummary), Error>> + '_ {
        self.hot_db
            .iter_column(DBColumn::BeaconStateSummary)
            .map(|res| {
                let (key, value_bytes) = res?;
                Ok((key, HotStateSummary::from_store_bytes(&value_bytes)?))
            })
    }

    /// Load the temporary flag for a state root, if one exists.
    ///
    /// Returns `Some` if the state is temporary, or `None` if the state is permanent or does not
    /// exist -- you should call `load_hot_state_summary` to find out which.
    pub fn load_state_temporary_flag(
        &self,
        state_root: &Hash256,
    ) -> Result<Option<TemporaryFlag>, Error> {
        self.hot_db.get(state_root)
    }

    /// Run a compaction pass to free up space used by deleted states.
    pub fn compact(&self) -> Result<(), Error> {
        self.hot_db.compact()?;
        Ok(())
    }

    /// Return `true` if compaction on finalization/pruning is enabled.
    pub fn compact_on_prune(&self) -> bool {
        self.config.compact_on_prune
    }

    /// Get the checkpoint to begin pruning from (the "old finalized checkpoint").
    pub fn get_pruning_checkpoint(&self) -> Checkpoint {
        // Since tree-states we infer the pruning checkpoint from the split, as this is simpler &
        // safer in the presence of crashes that occur after pruning but before the split is
        // updated.
        // FIXME(sproul): ensure delete PRUNING_CHECKPOINT_KEY is deleted in DB migration
        let split = self.get_split_info();
        Checkpoint {
            epoch: split.slot.epoch(E::slots_per_epoch()),
            root: split.block_root,
        }
    }

    /// Load the timestamp of the last compaction as a `Duration` since the UNIX epoch.
    pub fn load_compaction_timestamp(&self) -> Result<Option<Duration>, Error> {
        Ok(self
            .hot_db
            .get(&COMPACTION_TIMESTAMP_KEY)?
            .map(|c: CompactionTimestamp| Duration::from_secs(c.0)))
    }

    /// Store the timestamp of the last compaction as a `Duration` since the UNIX epoch.
    pub fn store_compaction_timestamp(&self, compaction_timestamp: Duration) -> Result<(), Error> {
        self.hot_db.put(
            &COMPACTION_TIMESTAMP_KEY,
            &CompactionTimestamp(compaction_timestamp.as_secs()),
        )
    }

    /// Update the linear array of frozen block roots with the block root for several skipped slots.
    ///
    /// Write the block root at all slots from `start_slot` (inclusive) to `end_slot` (exclusive).
    pub fn store_frozen_block_root_at_skip_slots(
        &self,
        start_slot: Slot,
        end_slot: Slot,
        block_root: Hash256,
    ) -> Result<Vec<KeyValueStoreOp>, Error> {
        let mut ops = vec![];
        for slot in start_slot.as_u64()..end_slot.as_u64() {
            ops.push(KeyValueStoreOp::PutKeyValue(
                get_key_for_col(DBColumn::BeaconBlockRoots.into(), &slot.to_be_bytes()),
                block_root.as_bytes().to_vec(),
            ));
        }
        Ok(ops)
    }

    /// Try to prune all execution payloads, returning early if there is no need to prune.
    pub fn try_prune_execution_payloads(&self, force: bool) -> Result<(), Error> {
        let split = self.get_split_info();

        if split.slot == 0 {
            return Ok(());
        }

        let bellatrix_fork_slot = if let Some(epoch) = self.spec.bellatrix_fork_epoch {
            epoch.start_slot(E::slots_per_epoch())
        } else {
            return Ok(());
        };

        // Load the split state so we can backtrack to find execution payloads.
        let split_state = self.get_state(&split.state_root, Some(split.slot))?.ok_or(
            HotColdDBError::MissingSplitState(split.state_root, split.slot),
        )?;

        // The finalized block may or may not have its execution payload stored, depending on
        // whether it was at a skipped slot. However for a fully pruned database its parent
        // should *always* have been pruned. In case of a long split (no parent found) we
        // continue as if the payloads are pruned, as the node probably has other things to worry
        // about.
        let split_block_root = split_state.get_latest_block_root(split.state_root);

        let already_pruned =
            process_results(split_state.rev_iter_block_roots(&self.spec), |mut iter| {
                iter.find(|(_, block_root)| *block_root != split_block_root)
                    .map_or(Ok(true), |(_, split_parent_root)| {
                        self.execution_payload_exists(&split_parent_root)
                            .map(|exists| !exists)
                    })
            })??;

        if already_pruned && !force {
            info!(self.log, "Execution payloads are pruned");
            return Ok(());
        }

        // Iterate block roots backwards to the Bellatrix fork or the anchor slot, whichever comes
        // first.
        warn!(
            self.log,
            "Pruning finalized payloads";
            "info" => "you may notice degraded I/O performance while this runs"
        );
        let anchor_slot = self.get_anchor_info().map(|info| info.anchor_slot);

        let mut ops = vec![];
        let mut last_pruned_block_root = None;

        for res in std::iter::once(Ok((split_block_root, split.slot)))
            .chain(BlockRootsIterator::new(self, &split_state))
        {
            let (block_root, slot) = match res {
                Ok(tuple) => tuple,
                Err(e) => {
                    warn!(
                        self.log,
                        "Stopping payload pruning early";
                        "error" => ?e,
                    );
                    break;
                }
            };

            if slot < bellatrix_fork_slot {
                info!(
                    self.log,
                    "Payload pruning reached Bellatrix boundary";
                );
                break;
            }

            if Some(block_root) != last_pruned_block_root
                && self.execution_payload_exists(&block_root)?
            {
                debug!(
                    self.log,
                    "Pruning execution payload";
                    "slot" => slot,
                    "block_root" => ?block_root,
                );
                last_pruned_block_root = Some(block_root);
                ops.push(StoreOp::DeleteExecutionPayload(block_root));
            }

            if Some(slot) == anchor_slot {
                info!(
                    self.log,
                    "Payload pruning reached anchor state";
                    "slot" => slot
                );
                break;
            }
        }
        let payloads_pruned = ops.len();
        self.do_atomically_with_block_and_blobs_cache(ops)?;
        info!(
            self.log,
            "Execution payload pruning complete";
            "payloads_pruned" => payloads_pruned,
        );
        Ok(())
    }

    /// Try to prune blobs, approximating the current epoch from the split slot.
    pub fn try_prune_most_blobs(&self, force: bool) -> Result<(), Error> {
        let Some(deneb_fork_epoch) = self.spec.deneb_fork_epoch else {
            debug!(self.log, "Deneb fork is disabled");
            return Ok(());
        };
        // The current epoch is >= split_epoch + 2. It could be greater if the database is
        // configured to delay updating the split or finalization has ceased. In this instance we
        // choose to also delay the pruning of blobs (we never prune without finalization anyway).
        let min_current_epoch = self.get_split_slot().epoch(E::slots_per_epoch()) + 2;
        let min_data_availability_boundary = std::cmp::max(
            deneb_fork_epoch,
            min_current_epoch.saturating_sub(self.spec.min_epochs_for_blob_sidecars_requests),
        );

        self.try_prune_blobs(force, min_data_availability_boundary)
    }

    /// Try to prune blobs older than the data availability boundary.
    ///
    /// Blobs from the epoch `data_availability_boundary - blob_prune_margin_epochs` are retained.
    /// This epoch is an _exclusive_ endpoint for the pruning process.
    ///
    /// This function only supports pruning blobs older than the split point, which is older than
    /// (or equal to) finalization. Pruning blobs newer than finalization is not supported.
    ///
    /// This function also assumes that the split is stationary while it runs. It should only be
    /// run from the migrator thread (where `migrate_database` runs) or the database manager.
    pub fn try_prune_blobs(
        &self,
        force: bool,
        data_availability_boundary: Epoch,
    ) -> Result<(), Error> {
        if self.spec.deneb_fork_epoch.is_none() {
            debug!(self.log, "Deneb fork is disabled");
            return Ok(());
        }

        let pruning_enabled = self.get_config().prune_blobs;
        let margin_epochs = self.get_config().blob_prune_margin_epochs;
        let epochs_per_blob_prune = self.get_config().epochs_per_blob_prune;

        if !force && !pruning_enabled {
            debug!(
                self.log,
                "Blob pruning is disabled";
                "prune_blobs" => pruning_enabled
            );
            return Ok(());
        }

        let blob_info = self.get_blob_info();
        let Some(oldest_blob_slot) = blob_info.oldest_blob_slot else {
            error!(self.log, "Slot of oldest blob is not known");
            return Err(HotColdDBError::BlobPruneLogicError.into());
        };

        // Start pruning from the epoch of the oldest blob stored.
        // The start epoch is inclusive (blobs in this epoch will be pruned).
        let start_epoch = oldest_blob_slot.epoch(E::slots_per_epoch());

        // Prune blobs up until the `data_availability_boundary - margin` or the split
        // slot's epoch, whichever is older. We can't prune blobs newer than the split.
        // The end epoch is also inclusive (blobs in this epoch will be pruned).
        let split = self.get_split_info();
        let end_epoch = std::cmp::min(
            data_availability_boundary - margin_epochs - 1,
            split.slot.epoch(E::slots_per_epoch()) - 1,
        );
        let end_slot = end_epoch.end_slot(E::slots_per_epoch());

        let can_prune = end_epoch != 0 && start_epoch <= end_epoch;
        let should_prune = start_epoch + epochs_per_blob_prune <= end_epoch + 1;

        if !force && !should_prune || !can_prune {
            debug!(
                self.log,
                "Blobs are pruned";
                "oldest_blob_slot" => oldest_blob_slot,
                "data_availability_boundary" => data_availability_boundary,
                "split_slot" => split.slot,
                "end_epoch" => end_epoch,
                "start_epoch" => start_epoch,
            );
            return Ok(());
        }

        // Sanity checks.
        if let Some(anchor) = self.get_anchor_info() {
            if oldest_blob_slot < anchor.oldest_block_slot {
                error!(
                    self.log,
                    "Oldest blob is older than oldest block";
                    "oldest_blob_slot" => oldest_blob_slot,
                    "oldest_block_slot" => anchor.oldest_block_slot
                );
                return Err(HotColdDBError::BlobPruneLogicError.into());
            }
        }

        // Iterate block roots forwards from the oldest blob slot.
        debug!(
            self.log,
            "Pruning blobs";
            "start_epoch" => start_epoch,
            "end_epoch" => end_epoch,
            "data_availability_boundary" => data_availability_boundary,
        );

        let mut ops = vec![];
        let mut last_pruned_block_root = None;

        for res in self.forwards_block_roots_iterator_until(oldest_blob_slot, end_slot, || {
            let (_, split_state) = self
                .get_advanced_hot_state(split.block_root, split.slot, split.state_root)?
                .ok_or(HotColdDBError::MissingSplitState(
                    split.state_root,
                    split.slot,
                ))?;

            Ok((split_state, split.block_root))
        })? {
            let (block_root, slot) = match res {
                Ok(tuple) => tuple,
                Err(e) => {
                    warn!(
                        self.log,
                        "Stopping blob pruning early";
                        "error" => ?e,
                    );
                    break;
                }
            };

            if Some(block_root) != last_pruned_block_root && self.blobs_exist(&block_root)? {
                trace!(
                    self.log,
                    "Pruning blobs of block";
                    "slot" => slot,
                    "block_root" => ?block_root,
                );
                last_pruned_block_root = Some(block_root);
                ops.push(StoreOp::DeleteBlobs(block_root));
            }

            if slot >= end_slot {
                break;
            }
        }
        let blob_lists_pruned = ops.len();
        let new_blob_info = BlobInfo {
            oldest_blob_slot: Some(end_slot + 1),
            blobs_db: blob_info.blobs_db,
        };
        let update_blob_info = self.compare_and_set_blob_info(blob_info, new_blob_info)?;
        ops.push(StoreOp::KeyValueOp(update_blob_info));

        self.do_atomically_with_block_and_blobs_cache(ops)?;
        debug!(
            self.log,
            "Blob pruning complete";
            "blob_lists_pruned" => blob_lists_pruned,
        );

        Ok(())
    }

    /// Delete *all* states from the freezer database and update the anchor accordingly.
    ///
    /// WARNING: this method deletes the genesis state and replaces it with the provided
    /// `genesis_state`. This is to support its use in schema migrations where the storage scheme of
    /// the genesis state may be modified. It is the responsibility of the caller to ensure that the
    /// genesis state is correct, else a corrupt database will be created.
    ///
    /// Although DB ops for the cold DB are returned, this function WILL write a new anchor
    /// immediately to the hot database. It is safe to re-run on failure.
    pub fn prune_historic_states(
        &self,
        genesis_state_root: Hash256,
        genesis_state: &BeaconState<E>,
        cold_ops: &mut Vec<KeyValueStoreOp>,
    ) -> Result<(), Error> {
        // Update the anchor to use the dummy state upper limit and disable historic state storage.
        let old_anchor = self.get_anchor_info();
        let new_anchor = if let Some(old_anchor) = old_anchor.clone() {
            AnchorInfo {
                state_upper_limit: STATE_UPPER_LIMIT_NO_RETAIN,
                state_lower_limit: Slot::new(0),
                ..old_anchor.clone()
            }
        } else {
            AnchorInfo {
                anchor_slot: Slot::new(0),
                oldest_block_slot: Slot::new(0),
                oldest_block_parent: Hash256::zero(),
                state_upper_limit: STATE_UPPER_LIMIT_NO_RETAIN,
                state_lower_limit: Slot::new(0),
            }
        };

        // Commit the anchor change immediately: if the cold database ops fail they can always be
        // retried, and we can't do them atomically with this change anyway.
        self.compare_and_set_anchor_info_with_write(old_anchor, Some(new_anchor))?;

        // Stage freezer data for deletion. Do not bother loading and deserializing values as this
        // wastes time and is less schema-agnostic. My hope is that this method will be useful for
        // migrating to the tree-states schema (delete everything in the freezer then start afresh).
        let columns = [
            DBColumn::BeaconState,
            DBColumn::BeaconStateSummary,
            DBColumn::BeaconStateDiff,
            DBColumn::BeaconRestorePoint,
            DBColumn::BeaconStateRoots,
            DBColumn::BeaconHistoricalRoots,
            DBColumn::BeaconRandaoMixes,
            DBColumn::BeaconHistoricalSummaries,
        ];

        for column in columns {
            for res in self.cold_db.iter_column_keys::<Vec<u8>>(column) {
                let key = res?;
                cold_ops.push(KeyValueStoreOp::DeleteKey(get_key_for_col(
                    column.as_str(),
                    &key,
                )));
            }
        }

        info!(
            self.log,
            "Deleting historic states";
            "num_kv" => cold_ops.len(),
        );

        // If we just deleted the the genesis state, re-store it using the *current* schema, which
        // may be different from the schema of the genesis state we just deleted.
        if self.get_split_slot() > 0 {
            info!(
                self.log,
                "Re-storing genesis state";
                "state_root" => ?genesis_state_root,
            );
            self.store_cold_state(&genesis_state_root, genesis_state, cold_ops)?;
        }

        // In order to reclaim space, we need to compact the freezer DB as well.
        self.cold_db.compact()?;

        Ok(())
    }

    /// Same as `prune_historic_states` but also writing to the cold DB.
    pub fn prune_historic_states_with_cold_write(
        &self,
        genesis_state_root: Hash256,
        genesis_state: &BeaconState<E>,
    ) -> Result<(), Error> {
        let mut cold_db_ops = vec![];
        self.prune_historic_states(genesis_state_root, genesis_state, &mut cold_db_ops)?;
        self.cold_db.do_atomically(cold_db_ops)?;
        Ok(())
    }
}

/// Advance the split point of the store, moving new finalized states to the freezer.
pub fn migrate_database<E: EthSpec, Hot: ItemStore<E>, Cold: ItemStore<E>>(
    store: Arc<HotColdDB<E, Hot, Cold>>,
    finalized_state_root: Hash256,
    finalized_block_root: Hash256,
    finalized_state: &BeaconState<E>,
) -> Result<(), Error> {
    debug!(
        store.log,
        "Freezer migration started";
        "slot" => finalized_state.slot()
    );

    // 0. Check that the migration is sensible.
    // The new finalized state must increase the current split slot, and lie on an epoch
    // boundary (in order for the hot state summary scheme to work).
    let current_split_slot = store.split.read_recursive().slot;
    let anchor_info = store.anchor_info.read_recursive().clone();
    let anchor_slot = anchor_info.as_ref().map(|a| a.anchor_slot);

    if finalized_state.slot() < current_split_slot {
        return Err(HotColdDBError::FreezeSlotError {
            current_split_slot,
            proposed_split_slot: finalized_state.slot(),
        }
        .into());
    }

    if finalized_state.slot() % E::slots_per_epoch() != 0 {
        return Err(HotColdDBError::FreezeSlotUnaligned(finalized_state.slot()).into());
    }

    // Store the new finalized state as a full state in the database. It would likely previously
    // have been stored in memory, or maybe as a diff.
    store.store_full_state(&finalized_state_root, finalized_state)?;

    // Copy all of the states between the new finalized state and the split slot, from the hot DB to
    // the cold DB.
    let mut hot_db_ops: Vec<StoreOp<E>> = Vec::new();
    let mut cold_db_block_ops: Vec<KeyValueStoreOp> = vec![];

    let state_roots = RootsIterator::new(&store, finalized_state)
        .take_while(|result| match result {
            Ok((_, _, slot)) => {
                slot >= &current_split_slot
                    && anchor_slot.map_or(true, |anchor_slot| slot >= &anchor_slot)
            }
            Err(_) => true,
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Iterate states in slot ascending order, as they are stored wrt previous states.
    for (block_root, state_root, slot) in state_roots.into_iter().rev() {
        // Delete the execution payload if payload pruning is enabled. At a skipped slot we may
        // delete the payload for the finalized block itself, but that's OK as we only guarantee
        // that payloads are present for slots >= the split slot. The payload fetching code is also
        // forgiving of missing payloads.
        if store.config.prune_payloads {
            hot_db_ops.push(StoreOp::DeleteExecutionPayload(block_root));
        }

        // Move the blinded block from the hot database to the freezer.
        if store.config.linear_blocks {
            // FIXME(sproul): make this load lazy
            let blinded_block = store
                .get_blinded_block(&block_root, None)?
                .ok_or(Error::BlockNotFound(block_root))?;
            if blinded_block.slot() == slot || slot == current_split_slot {
                store.blinded_block_as_cold_kv_store_ops(
                    &block_root,
                    &blinded_block,
                    &mut cold_db_block_ops,
                )?;
                hot_db_ops.push(StoreOp::DeleteBlock(block_root));
            }
        }

        // Store the slot to block root mapping.
        cold_db_block_ops.push(KeyValueStoreOp::PutKeyValue(
            get_key_for_col(
                DBColumn::BeaconBlockRoots.into(),
                &slot.as_u64().to_be_bytes(),
            ),
            block_root.as_bytes().to_vec(),
        ));

        // Delete the old summary, and the full state if we lie on an epoch boundary.
        hot_db_ops.push(StoreOp::DeleteState(state_root, Some(slot)));

        // Do not try to store states if a restore point is yet to be stored, or will never be
        // stored (see `STATE_UPPER_LIMIT_NO_RETAIN`). Make an exception for the genesis state
        // which always needs to be copied from the hot DB to the freezer and should not be deleted.
        if slot != 0
            && anchor_info
                .as_ref()
                .map_or(false, |anchor| slot < anchor.state_upper_limit)
        {
            debug!(store.log, "Pruning finalized state"; "slot" => slot);
            continue;
        }

        let mut cold_db_ops: Vec<KeyValueStoreOp> = Vec::new();

        // Only store the cold state if it's on a diff boundary
        if matches!(
            store.hierarchy.storage_strategy(slot)?,
            StorageStrategy::ReplayFrom(..)
        ) {
            // Store slot -> state_root and state_root -> slot mappings.
            debug!(
                store.log,
                "Storing cold state summary";
                "slot" => slot,
            );
            store.store_cold_state_summary(&state_root, slot, &mut cold_db_ops)?;
        } else {
            let state: BeaconState<E> = store
                .get_hot_state(&state_root)?
                .ok_or(HotColdDBError::MissingStateToFreeze(state_root))?;
            assert_eq!(state.slot(), slot);
            store.store_cold_state(&state_root, &state, &mut cold_db_ops)?;
        }

        // Cold states are diffed with respect to each other, so we need to finish writing previous
        // states before storing new ones.
        store.cold_db.do_atomically(cold_db_ops)?;
    }

    // Warning: Critical section.  We have to take care not to put any of the two databases in an
    //          inconsistent state if the OS process dies at any point during the freezing
    //          procedure.
    //
    // Since it is pretty much impossible to be atomic across more than one database, we trade
    // temporarily losing track of blocks to delete, for consistency. In other words: We should be
    // safe to die at any point below but it may happen that some blocks won't be deleted from the
    // hot database and will remain there forever. We may also temporarily abandon states, but
    // they will get picked up by the state pruning that iterates over the whole column.

    // Flush to disk all the states that have just been migrated to the cold store.
    store.cold_db.do_atomically(cold_db_block_ops)?;
    store.cold_db.sync()?;

    // Update the split.
    //
    // NOTE(sproul): We do this in its own fsync'd transaction mostly for historical reasons, but
    // I'm scared to change it, because doing an fsync with *more data* while holding the split
    // write lock might have terrible performance implications (jamming the split for 100-500ms+).
    {
        let mut split_guard = store.split.write();
        let latest_split_slot = split_guard.slot;

        // Detect a situation where the split point is (erroneously) changed from more than one
        // place in code.
        if latest_split_slot != current_split_slot {
            error!(
                store.log,
                "Race condition detected: Split point changed while moving states to the freezer";
                "previous split slot" => current_split_slot,
                "current split slot" => latest_split_slot,
            );

            // Assume the freezing procedure will be retried in case this happens.
            return Err(Error::SplitPointModified(
                current_split_slot,
                latest_split_slot,
            ));
        }

        // Before updating the in-memory split value, we flush it to disk first, so that should the
        // OS process die at this point, we pick up from the right place after a restart.
        let split = Split {
            slot: finalized_state.slot(),
            state_root: finalized_state_root,
            block_root: finalized_block_root,
        };
        store.hot_db.put_sync(&SPLIT_KEY, &split)?;

        // Split point is now persisted in the hot database on disk. The in-memory split point
        // hasn't been modified elsewhere since we keep a write lock on it. It's safe to update
        // the in-memory split point now.
        *split_guard = split;
    }

    // Delete the blocks and states from the hot database if we got this far.
    store.do_atomically_with_block_and_blobs_cache(hot_db_ops)?;

    // Update the cache's view of the finalized state.
    store.update_finalized_state(
        finalized_state_root,
        finalized_block_root,
        finalized_state.clone(),
    )?;

    debug!(
        store.log,
        "Freezer migration complete";
        "slot" => finalized_state.slot()
    );

    Ok(())
}

/// Struct for storing the split slot and state root in the database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Encode, Decode, Deserialize, Serialize)]
pub struct Split {
    pub slot: Slot,
    pub state_root: Hash256,
    /// The block root of the split state.
    ///
    /// This is used to provide special handling for the split state in the case where there are
    /// skipped slots. The split state will *always* be the advanced state, so callers
    /// who only have the finalized block root should use `get_advanced_hot_state` to get this state,
    /// rather than fetching `block.state_root()` (the unaligned state) which will have been pruned.
    #[ssz(skip_serializing, skip_deserializing)]
    pub block_root: Hash256,
}

impl StoreItem for Split {
    fn db_column() -> DBColumn {
        DBColumn::BeaconMeta
    }

    fn as_store_bytes(&self) -> Vec<u8> {
        self.as_ssz_bytes()
    }

    fn from_store_bytes(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Self::from_ssz_bytes(bytes)?)
    }
}

/// Struct for summarising a state in the hot database.
///
/// Allows full reconstruction by replaying blocks.
#[superstruct(
    variants(V1, V24),
    variant_attributes(derive(Debug, Clone, Copy, Default, Encode, Decode)),
    no_enum
)]
pub struct HotStateSummary {
    pub slot: Slot,
    pub latest_block_root: Hash256,
    /// The state root of a state prior to this state with respect to which this state's diff is
    /// stored.
    ///
    /// Set to 0 if this state *is not* stored as a diff.
    ///
    /// Formerly known as the `epoch_boundary_state_root`.
    pub diff_base_state_root: Hash256,
    /// The slot of the state with `diff_base_state_root`, or 0 if no diff is stored.
    #[superstruct(only(V24))]
    pub diff_base_slot: Slot,
    /// The state root of the state at the prior slot.
    #[superstruct(only(V24))]
    pub prev_state_root: Hash256,
}

pub type HotStateSummary = HotStateSummaryV24;

macro_rules! impl_store_item_summary {
    ($t:ty) => {
        impl StoreItem for $t {
            fn db_column() -> DBColumn {
                DBColumn::BeaconStateSummary
            }

            fn as_store_bytes(&self) -> Vec<u8> {
                self.as_ssz_bytes()
            }

            fn from_store_bytes(bytes: &[u8]) -> Result<Self, Error> {
                Ok(Self::from_ssz_bytes(bytes)?)
            }
        }
    };
}
impl_store_item_summary!(HotStateSummaryV1);
impl_store_item_summary!(HotStateSummaryV24);

impl HotStateSummary {
    /// Construct a new summary of the given state.
    pub fn new<E: EthSpec>(
        state_root: &Hash256,
        state: &BeaconState<E>,
        diff_base_slot: Option<Slot>,
    ) -> Result<Self, Error> {
        // Fill in the state root on the latest block header if necessary (this happens on all
        // slots where there isn't a skip).
        let slot = state.slot();
        let latest_block_root = state.get_latest_block_root(*state_root);

        // Set the diff state root as appropriate.
        let diff_base_state_root = if let Some(base_slot) = diff_base_slot {
            *state
                .get_state_root(base_slot)
                .map_err(HotColdDBError::HotStateSummaryError)?
        } else {
            Hash256::zero()
        };

        let prev_state_root = if let Ok(prev_slot) = slot.safe_sub(1) {
            *state
                .get_state_root(prev_slot)
                .map_err(HotColdDBError::HotStateSummaryError)?
        } else {
            Hash256::zero()
        };

        Ok(HotStateSummary {
            slot,
            latest_block_root,
            diff_base_state_root,
            diff_base_slot: diff_base_slot.unwrap_or(Slot::new(0)),
            prev_state_root,
        })
    }
}

/// Struct for summarising a state in the freezer database.
#[derive(Debug, Clone, Copy, Default, Encode, Decode)]
pub(crate) struct ColdStateSummary {
    pub slot: Slot,
}

impl StoreItem for ColdStateSummary {
    fn db_column() -> DBColumn {
        DBColumn::BeaconStateSummary
    }

    fn as_store_bytes(&self) -> Vec<u8> {
        self.as_ssz_bytes()
    }

    fn from_store_bytes(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Self::from_ssz_bytes(bytes)?)
    }
}

/// Struct for storing the state root of a restore point in the database.
#[derive(Debug, Clone, Copy, Default, Encode, Decode)]
struct RestorePointHash {
    state_root: Hash256,
}

impl StoreItem for RestorePointHash {
    fn db_column() -> DBColumn {
        DBColumn::BeaconRestorePoint
    }

    fn as_store_bytes(&self) -> Vec<u8> {
        self.as_ssz_bytes()
    }

    fn from_store_bytes(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Self::from_ssz_bytes(bytes)?)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct TemporaryFlag;

impl StoreItem for TemporaryFlag {
    fn db_column() -> DBColumn {
        DBColumn::BeaconStateTemporary
    }

    fn as_store_bytes(&self) -> Vec<u8> {
        vec![]
    }

    fn from_store_bytes(_: &[u8]) -> Result<Self, Error> {
        Ok(TemporaryFlag)
    }
}
