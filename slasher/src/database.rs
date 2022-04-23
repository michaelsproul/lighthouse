use crate::{
    metrics, AttesterRecord, AttesterSlashingStatus, CompactAttesterRecord, Config, Error,
    ProposerSlashingStatus,
};
use byteorder::{BigEndian, ByteOrder};
use lru::LruCache;
use parking_lot::Mutex;
use serde::de::DeserializeOwned;
use sled::{
    transaction::{ConflictableTransactionResult, TransactionalTree},
    Db, IVec, Transactional, Tree,
};
use slog::{info, Logger};
use ssz::{Decode, Encode};
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;
use tree_hash::TreeHash;
use types::{
    Epoch, EthSpec, Hash256, IndexedAttestation, ProposerSlashing, SignedBeaconBlockHeader, Slot,
};

/// Current database schema version, to check compatibility of on-disk DB with software.
pub const CURRENT_SCHEMA_VERSION: u64 = 3;

/// Metadata about the slashing database itself.
const METADATA_DB: &str = "metadata";
/// Map from `(target_epoch, validator_index)` to `CompactAttesterRecord`.
const ATTESTERS_DB: &str = "attesters";
/// Companion database for the attesters DB mapping `validator_index` to largest `target_epoch`
/// stored for that validator in the attesters DB.
///
/// Used to implement wrap-around semantics for target epochs modulo the history length.
const ATTESTERS_MAX_TARGETS_DB: &str = "attesters_max_targets";
/// Map from `indexed_attestation_id` to `IndexedAttestation`.
const INDEXED_ATTESTATION_DB: &str = "indexed_attestations";
/// Map from `(target_epoch, indexed_attestation_hash)` to `indexed_attestation_id`.
const INDEXED_ATTESTATION_ID_DB: &str = "indexed_attestation_ids";
/// Table of minimum targets for every source epoch within range.
const MIN_TARGETS_DB: &str = "min_targets";
/// Table of maximum targets for every source epoch within range.
const MAX_TARGETS_DB: &str = "max_targets";
/// Map from `validator_index` to the `current_epoch` for that validator.
///
/// Used to implement wrap-around semantics for the min and max target arrays.
const CURRENT_EPOCHS_DB: &str = "current_epochs";
/// Map from `(slot, validator_index)` to `SignedBeaconBlockHeader`.
const PROPOSERS_DB: &str = "proposers";

/// Filename for the legacy (LMDB) database file, so that it may be deleted.
const LEGACY_DB_FILENAME: &str = "data.mdb";
const LEGACY_DB_LOCK_FILENAME: &str = "lock.mdb";

/// Constant key under which the schema version is stored in the `metadata_db`.
const METADATA_VERSION_KEY: &[u8] = &[0];
/// Constant key under which the slasher configuration is stored in the `metadata_db`.
const METADATA_CONFIG_KEY: &[u8] = &[1];

const ATTESTER_KEY_SIZE: usize = 7;
const PROPOSER_KEY_SIZE: usize = 16;
const CURRENT_EPOCH_KEY_SIZE: usize = 8;
const INDEXED_ATTESTATION_ID_SIZE: usize = 6;
const INDEXED_ATTESTATION_ID_KEY_SIZE: usize = 40;

#[derive(Debug)]
pub struct SlasherDB<E: EthSpec> {
    pub(crate) _db: Db,
    indexed_attestation: Tree,
    indexed_attestation_id: Tree,
    attesters: Tree,
    attesters_max_targets: Tree,
    min_targets: Tree,
    max_targets: Tree,
    current_epochs: Tree,
    proposers: Tree,
    metadata: Tree,
    /// LRU cache mapping indexed attestation IDs to their attestation data roots.
    attestation_root_cache: Mutex<LruCache<IndexedAttestationId, Hash256>>,
    pub(crate) config: Arc<Config>,
    _phantom: PhantomData<E>,
}

/// Transaction over all the trees of the database.
pub struct Transaction<'a> {
    pub indexed_attestation: &'a TransactionalTree,
    pub indexed_attestation_id: &'a TransactionalTree,
    pub attesters: &'a TransactionalTree,
    pub attesters_max_targets: &'a TransactionalTree,
    pub min_targets: &'a TransactionalTree,
    pub max_targets: &'a TransactionalTree,
    pub current_epochs: &'a TransactionalTree,
    pub proposers: &'a TransactionalTree,
    pub metadata: &'a TransactionalTree,
}

/// Database key for the `attesters` database.
///
/// Stored as big-endian `(target_epoch, validator_index)` to enable efficient iteration
/// while pruning.
///
/// The target epoch is stored in 2 bytes modulo the `history_length`.
///
/// The validator index is stored in 5 bytes (validator registry limit is 2^40).
#[derive(Debug)]
pub struct AttesterKey {
    data: [u8; ATTESTER_KEY_SIZE],
}

impl AttesterKey {
    pub fn new(validator_index: u64, target_epoch: Epoch, config: &Config) -> Self {
        let mut data = [0; ATTESTER_KEY_SIZE];

        BigEndian::write_uint(
            &mut data[..2],
            target_epoch.as_u64() % config.history_length as u64,
            2,
        );
        BigEndian::write_uint(&mut data[2..], validator_index, 5);

        AttesterKey { data }
    }
}

impl AsRef<[u8]> for AttesterKey {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

/// Database key for the `proposers` database.
///
/// Stored as big-endian `(slot, validator_index)` to enable efficient iteration
/// while pruning.
#[derive(Debug)]
pub struct ProposerKey {
    data: [u8; PROPOSER_KEY_SIZE],
}

impl ProposerKey {
    pub fn new(validator_index: u64, slot: Slot) -> Self {
        let mut data = [0; PROPOSER_KEY_SIZE];
        data[0..8].copy_from_slice(&slot.as_u64().to_be_bytes());
        data[8..PROPOSER_KEY_SIZE].copy_from_slice(&validator_index.to_be_bytes());
        ProposerKey { data }
    }

    pub fn parse(data: IVec) -> Result<(Slot, u64), Error> {
        if data.len() == PROPOSER_KEY_SIZE {
            let slot = Slot::new(BigEndian::read_u64(&data[..8]));
            let validator_index = BigEndian::read_u64(&data[8..]);
            Ok((slot, validator_index))
        } else {
            Err(Error::ProposerKeyCorrupt { length: data.len() })
        }
    }
}

impl AsRef<[u8]> for ProposerKey {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

/// Key containing a validator index
pub struct CurrentEpochKey {
    validator_index: [u8; CURRENT_EPOCH_KEY_SIZE],
}

impl CurrentEpochKey {
    pub fn new(validator_index: u64) -> Self {
        Self {
            validator_index: validator_index.to_be_bytes(),
        }
    }
}

impl AsRef<[u8]> for CurrentEpochKey {
    fn as_ref(&self) -> &[u8] {
        &self.validator_index
    }
}

/// Key containing an epoch and an indexed attestation hash.
pub struct IndexedAttestationIdKey {
    target_and_root: [u8; INDEXED_ATTESTATION_ID_KEY_SIZE],
}

impl IndexedAttestationIdKey {
    pub fn new(target_epoch: Epoch, indexed_attestation_root: Hash256) -> Self {
        let mut data = [0; INDEXED_ATTESTATION_ID_KEY_SIZE];
        data[0..8].copy_from_slice(&target_epoch.as_u64().to_be_bytes());
        data[8..INDEXED_ATTESTATION_ID_KEY_SIZE]
            .copy_from_slice(indexed_attestation_root.as_bytes());
        Self {
            target_and_root: data,
        }
    }

    pub fn parse(data: IVec) -> Result<(Epoch, Hash256), Error> {
        if data.len() == INDEXED_ATTESTATION_ID_KEY_SIZE {
            let target_epoch = Epoch::new(BigEndian::read_u64(&data[..8]));
            let indexed_attestation_root = Hash256::from_slice(&data[8..]);
            Ok((target_epoch, indexed_attestation_root))
        } else {
            Err(Error::IndexedAttestationIdKeyCorrupt { length: data.len() })
        }
    }
}

impl AsRef<[u8]> for IndexedAttestationIdKey {
    fn as_ref(&self) -> &[u8] {
        &self.target_and_root
    }
}

/// Key containing a 6-byte indexed attestation ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IndexedAttestationId {
    id: [u8; INDEXED_ATTESTATION_ID_SIZE],
}

impl IndexedAttestationId {
    pub fn new(id: u64) -> Self {
        let mut data = [0; INDEXED_ATTESTATION_ID_SIZE];
        BigEndian::write_uint(&mut data, id, INDEXED_ATTESTATION_ID_SIZE);
        Self { id: data }
    }

    pub fn parse(data: IVec) -> Result<u64, Error> {
        if data.len() == INDEXED_ATTESTATION_ID_SIZE {
            Ok(BigEndian::read_uint(
                data.as_ref(),
                INDEXED_ATTESTATION_ID_SIZE,
            ))
        } else {
            Err(Error::IndexedAttestationIdCorrupt { length: data.len() })
        }
    }

    pub fn null() -> Self {
        Self::new(0)
    }

    pub fn is_null(&self) -> bool {
        self.id == [0, 0, 0, 0, 0, 0]
    }

    pub fn as_u64(&self) -> u64 {
        BigEndian::read_uint(&self.id, INDEXED_ATTESTATION_ID_SIZE)
    }
}

impl AsRef<[u8]> for IndexedAttestationId {
    fn as_ref(&self) -> &[u8] {
        &self.id
    }
}

/// Bincode deserialization specialised to `IVec`.
fn bincode_deserialize<T: DeserializeOwned>(bytes: IVec) -> Result<T, Error> {
    Ok(bincode::deserialize(&bytes)?)
}

fn ssz_decode<T: Decode>(bytes: IVec) -> Result<T, Error> {
    Ok(T::from_ssz_bytes(&bytes)?)
}

impl<E: EthSpec> SlasherDB<E> {
    pub fn open(config: Arc<Config>, log: Logger) -> Result<Self, Error> {
        // Delete any legacy LMDB database.
        Self::delete_legacy_file(&config.database_path, LEGACY_DB_FILENAME, &log)?;
        Self::delete_legacy_file(&config.database_path, LEGACY_DB_LOCK_FILENAME, &log)?;

        std::fs::create_dir_all(&config.database_path)?;

        // FIXME(sproul): enable compression
        let db = sled::Config::new()
            .use_compression(false)
            .mode(sled::Mode::HighThroughput)
            .cache_capacity(6 * 1024 * 1024 * 1024)
            .path(&config.database_path)
            .print_profile_on_drop(true)
            .open()?;

        let indexed_attestation = db.open_tree(INDEXED_ATTESTATION_DB)?;
        let indexed_attestation_id = db.open_tree(INDEXED_ATTESTATION_ID_DB)?;
        let attesters = db.open_tree(ATTESTERS_DB)?;
        let attesters_max_targets = db.open_tree(ATTESTERS_MAX_TARGETS_DB)?;
        let min_targets = db.open_tree(MIN_TARGETS_DB)?;
        let max_targets = db.open_tree(MAX_TARGETS_DB)?;
        let current_epochs = db.open_tree(CURRENT_EPOCHS_DB)?;
        let proposers = db.open_tree(PROPOSERS_DB)?;
        let metadata = db.open_tree(METADATA_DB)?;

        // FIXME(sproul): permissions
        #[cfg(windows)]
        {
            use filesystem::restrict_file_permissions;
            let data = config.database_path.join("mdbx.dat");
            let lock = config.database_path.join("mdbx.lck");
            restrict_file_permissions(data).map_err(Error::DatabasePermissionsError)?;
            restrict_file_permissions(lock).map_err(Error::DatabasePermissionsError)?;
        }

        let attestation_root_cache = Mutex::new(LruCache::new(config.attestation_root_cache_size));

        let mut db = Self {
            _db: db,
            indexed_attestation,
            indexed_attestation_id,
            attesters,
            attesters_max_targets,
            min_targets,
            max_targets,
            current_epochs,
            proposers,
            metadata,
            attestation_root_cache,
            config,
            _phantom: PhantomData,
        };

        db = db.migrate()?;

        db.transaction(|txn| {
            if let Some(on_disk_config) = db.load_config(&txn)? {
                let current_disk_config = db.config.disk_config();
                if current_disk_config != on_disk_config {
                    return Err(Error::ConfigIncompatible {
                        on_disk_config,
                        config: current_disk_config,
                    }
                    .into());
                }
            }
            Ok(())
        })?;

        Ok(db)
    }

    pub fn transaction<T, F>(&self, f: F) -> Result<T, Error>
    where
        F: Fn(&Transaction) -> ConflictableTransactionResult<T, Error>,
    {
        (
            &self.indexed_attestation,
            &self.indexed_attestation_id,
            &self.attesters,
            &self.attesters_max_targets,
            &self.min_targets,
            &self.max_targets,
            &self.current_epochs,
            &self.proposers,
            &self.metadata,
        )
            .transaction(
                |(
                    indexed_attestation,
                    indexed_attestation_id,
                    attesters,
                    attesters_max_targets,
                    min_targets,
                    max_targets,
                    current_epochs,
                    proposers,
                    metadata,
                )| {
                    let txn = Transaction {
                        indexed_attestation,
                        indexed_attestation_id,
                        attesters,
                        attesters_max_targets,
                        min_targets,
                        max_targets,
                        current_epochs,
                        proposers,
                        metadata,
                    };
                    f(&txn)
                },
            )
            .map_err(Into::into)
    }

    fn delete_legacy_file(slasher_dir: &Path, filename: &str, log: &Logger) -> Result<(), Error> {
        let path = slasher_dir.join(filename);

        if path.is_file() {
            info!(
                log,
                "Deleting legacy slasher DB";
                "file" => ?path.display(),
            );
            std::fs::remove_file(&path)?;
        }
        Ok(())
    }

    pub fn load_schema_version(&self, txn: &Transaction) -> Result<Option<u64>, Error> {
        txn.metadata
            .get(&METADATA_VERSION_KEY)?
            .map(bincode_deserialize)
            .transpose()
    }

    pub fn store_schema_version(&self, txn: &Transaction) -> Result<(), Error> {
        txn.metadata.insert(
            METADATA_VERSION_KEY,
            bincode::serialize(&CURRENT_SCHEMA_VERSION)?,
        )?;
        Ok(())
    }

    /// Load a config from disk.
    ///
    /// This is generic in order to allow loading of configs for different schema versions.
    /// Care should be taken to ensure it is only called for `Config`-like `T`.
    pub fn load_config<T: DeserializeOwned>(
        &self,
        txn: &Transaction<'_>,
    ) -> Result<Option<T>, Error> {
        txn.metadata
            .get(METADATA_CONFIG_KEY)?
            .map(bincode_deserialize)
            .transpose()
    }

    pub fn store_config(&self, config: &Config, txn: &Transaction<'_>) -> Result<(), Error> {
        txn.metadata
            .insert(METADATA_CONFIG_KEY, bincode::serialize(config)?)?;
        Ok(())
    }

    pub fn get_attester_max_target(
        &self,
        validator_index: u64,
        txn: &Transaction<'_>,
    ) -> Result<Option<Epoch>, Error> {
        txn.attesters_max_targets
            .get(CurrentEpochKey::new(validator_index))?
            .map(ssz_decode)
            .transpose()
    }

    pub fn update_attester_max_target(
        &self,
        validator_index: u64,
        previous_max_target: Option<Epoch>,
        max_target: Epoch,
        txn: &Transaction<'_>,
    ) -> Result<(), Error> {
        // Don't update maximum if new target is less than or equal to previous. In the case of
        // no previous we *do* want to update.
        if previous_max_target.map_or(false, |prev_max| max_target <= prev_max) {
            return Ok(());
        }

        // Zero out attester DB entries which are now older than the history length.
        // Avoid writing the whole array on initialization (`previous_max_target == None`), and
        // avoid overwriting the entire attesters array more than once.
        if let Some(previous_max_target) = previous_max_target {
            let start_epoch = std::cmp::max(
                previous_max_target.as_u64() + 1,
                (max_target.as_u64() + 1).saturating_sub(self.config.history_length as u64),
            );
            for target_epoch in (start_epoch..max_target.as_u64()).map(Epoch::new) {
                txn.attesters.insert(
                    AttesterKey::new(validator_index, target_epoch, &self.config).as_ref(),
                    CompactAttesterRecord::null().as_bytes(),
                )?;
            }
        }

        txn.attesters_max_targets.insert(
            CurrentEpochKey::new(validator_index).as_ref(),
            max_target.as_ssz_bytes(),
        )?;
        Ok(())
    }

    pub fn get_current_epoch_for_validator(
        &self,
        validator_index: u64,
        txn: &Transaction,
    ) -> Result<Option<Epoch>, Error> {
        txn.current_epochs
            .get(CurrentEpochKey::new(validator_index))?
            .map(ssz_decode)
            .transpose()
    }

    pub fn update_current_epoch_for_validator(
        &self,
        validator_index: u64,
        current_epoch: Epoch,
        txn: &Transaction,
    ) -> Result<(), Error> {
        txn.current_epochs.insert(
            CurrentEpochKey::new(validator_index).as_ref(),
            current_epoch.as_ssz_bytes(),
        )?;
        Ok(())
    }

    fn get_indexed_attestation_id(
        &self,
        txn: &Transaction<'_>,
        key: &IndexedAttestationIdKey,
    ) -> Result<Option<u64>, Error> {
        txn.indexed_attestation_id
            .get(key)?
            .map(IndexedAttestationId::parse)
            .transpose()
    }

    fn put_indexed_attestation_id(
        &self,
        txn: &Transaction<'_>,
        key: &IndexedAttestationIdKey,
        value: IndexedAttestationId,
    ) -> Result<(), Error> {
        txn.indexed_attestation_id
            .insert(key.as_ref(), value.as_ref())?;
        Ok(())
    }

    /// Store an indexed attestation and return its ID.
    ///
    /// If the attestation is already stored then the existing ID will be returned without a write.
    pub fn store_indexed_attestation(
        &self,
        txn: &Transaction<'_>,
        indexed_attestation_hash: Hash256,
        indexed_attestation: &IndexedAttestation<E>,
    ) -> Result<u64, Error> {
        // Look-up ID by hash.
        let id_key = IndexedAttestationIdKey::new(
            indexed_attestation.data.target.epoch,
            indexed_attestation_hash,
        );

        if let Some(indexed_att_id) = self.get_indexed_attestation_id(txn, &id_key)? {
            return Ok(indexed_att_id);
        }

        // Store the new indexed attestation at the end of the current table.
        // Ensure ID is non-zero by adding 1.
        let indexed_att_id = txn.indexed_attestation.generate_id()? + 1;

        let attestation_key = IndexedAttestationId::new(indexed_att_id);
        let data = indexed_attestation.as_ssz_bytes();

        txn.indexed_attestation
            .insert(attestation_key.as_ref(), data)?;

        // Update the (epoch, hash) to ID mapping.
        self.put_indexed_attestation_id(txn, &id_key, attestation_key)?;

        Ok(indexed_att_id)
    }

    pub fn get_indexed_attestation(
        &self,
        txn: &Transaction<'_>,
        indexed_attestation_id: IndexedAttestationId,
    ) -> Result<IndexedAttestation<E>, Error> {
        let bytes = txn
            .indexed_attestation
            .get(indexed_attestation_id.as_ref())?
            .ok_or(Error::MissingIndexedAttestation {
                id: indexed_attestation_id.as_u64(),
            })?;
        ssz_decode(bytes)
    }

    fn get_attestation_data_root(
        &self,
        txn: &Transaction<'_>,
        indexed_id: IndexedAttestationId,
    ) -> Result<(Hash256, Option<IndexedAttestation<E>>), Error> {
        metrics::inc_counter(&metrics::SLASHER_NUM_ATTESTATION_ROOT_QUERIES);

        // If the value already exists in the cache, return it.
        let mut cache = self.attestation_root_cache.lock();
        if let Some(attestation_data_root) = cache.get(&indexed_id) {
            metrics::inc_counter(&metrics::SLASHER_NUM_ATTESTATION_ROOT_HITS);
            return Ok((*attestation_data_root, None));
        }

        // Otherwise, load the indexed attestation, compute the root and cache it.
        let indexed_attestation = self.get_indexed_attestation(txn, indexed_id)?;
        let attestation_data_root = indexed_attestation.data.tree_hash_root();

        cache.put(indexed_id, attestation_data_root);

        Ok((attestation_data_root, Some(indexed_attestation)))
    }

    pub fn cache_attestation_data_root(
        &self,
        indexed_attestation_id: IndexedAttestationId,
        attestation_data_root: Hash256,
    ) {
        let mut cache = self.attestation_root_cache.lock();
        cache.put(indexed_attestation_id, attestation_data_root);
    }

    fn delete_attestation_data_roots(&self, ids: impl IntoIterator<Item = IndexedAttestationId>) {
        let mut cache = self.attestation_root_cache.lock();
        for indexed_id in ids {
            cache.pop(&indexed_id);
        }
    }

    pub fn attestation_root_cache_size(&self) -> usize {
        self.attestation_root_cache.lock().len()
    }

    pub fn check_and_update_attester_record(
        &self,
        txn: &Transaction<'_>,
        validator_index: u64,
        attestation: &IndexedAttestation<E>,
        record: &AttesterRecord,
        indexed_attestation_id: IndexedAttestationId,
    ) -> Result<AttesterSlashingStatus<E>, Error> {
        // See if there's an existing attestation for this attester.
        let target_epoch = attestation.data.target.epoch;

        let prev_max_target = self.get_attester_max_target(validator_index, txn)?;

        if let Some(existing_record) =
            self.get_attester_record(txn, validator_index, target_epoch, prev_max_target)?
        {
            // If the existing indexed attestation is identical, then this attestation is not
            // slashable and no update is required.
            let existing_att_id = existing_record.indexed_attestation_id;
            if existing_att_id == indexed_attestation_id {
                return Ok(AttesterSlashingStatus::NotSlashable);
            }

            // Otherwise, load the attestation data root and check slashability via a hash root
            // comparison.
            let (existing_data_root, opt_existing_att) =
                self.get_attestation_data_root(txn, existing_att_id)?;

            if existing_data_root == record.attestation_data_hash {
                return Ok(AttesterSlashingStatus::NotSlashable);
            }

            // If we made it this far, then the attestation is slashable. Ensure that it's
            // loaded, double-check the slashing condition and return.
            let existing_attestation = opt_existing_att
                .map_or_else(|| self.get_indexed_attestation(txn, existing_att_id), Ok)?;

            if attestation.is_double_vote(&existing_attestation) {
                Ok(AttesterSlashingStatus::DoubleVote(Box::new(
                    existing_attestation,
                )))
            } else {
                Err(Error::InconsistentAttestationDataRoot)
            }
        }
        // If no attestation exists, insert a record for this validator.
        else {
            self.update_attester_max_target(validator_index, prev_max_target, target_epoch, txn)?;

            txn.attesters.insert(
                AttesterKey::new(validator_index, target_epoch, &self.config).as_ref(),
                indexed_attestation_id.as_ref(),
            )?;

            Ok(AttesterSlashingStatus::NotSlashable)
        }
    }

    pub fn get_attestation_for_validator(
        &self,
        txn: &Transaction,
        validator_index: u64,
        target_epoch: Epoch,
    ) -> Result<IndexedAttestation<E>, Error> {
        let max_target = self.get_attester_max_target(validator_index, txn)?;

        let record = self
            .get_attester_record(txn, validator_index, target_epoch, max_target)?
            .ok_or(Error::MissingAttesterRecord {
                validator_index,
                target_epoch,
            })?;
        self.get_indexed_attestation(txn, record.indexed_attestation_id)
    }

    pub fn get_attester_record(
        &self,
        txn: &Transaction,
        validator_index: u64,
        target: Epoch,
        prev_max_target: Option<Epoch>,
    ) -> Result<Option<CompactAttesterRecord>, Error> {
        if prev_max_target.map_or(true, |prev_max| target > prev_max) {
            return Ok(None);
        }

        let attester_key = AttesterKey::new(validator_index, target, &self.config);
        Ok(txn
            .attesters
            .get(attester_key)?
            .map(CompactAttesterRecord::parse)
            .transpose()?
            .filter(|record| !record.is_null()))
    }

    pub fn get_block_proposal(
        &self,
        txn: &Transaction<'_>,
        proposer_index: u64,
        slot: Slot,
    ) -> Result<Option<SignedBeaconBlockHeader>, Error> {
        let proposer_key = ProposerKey::new(proposer_index, slot);
        txn.proposers.get(proposer_key)?.map(ssz_decode).transpose()
    }

    pub fn check_or_insert_block_proposal(
        &self,
        txn: &Transaction<'_>,
        block_header: SignedBeaconBlockHeader,
    ) -> Result<ProposerSlashingStatus, Error> {
        let proposer_index = block_header.message.proposer_index;
        let slot = block_header.message.slot;

        if let Some(existing_block) = self.get_block_proposal(txn, proposer_index, slot)? {
            if existing_block == block_header {
                Ok(ProposerSlashingStatus::NotSlashable)
            } else {
                Ok(ProposerSlashingStatus::DoubleVote(Box::new(
                    ProposerSlashing {
                        signed_header_1: existing_block,
                        signed_header_2: block_header,
                    },
                )))
            }
        } else {
            txn.proposers.insert(
                ProposerKey::new(proposer_index, slot).as_ref(),
                block_header.as_ssz_bytes(),
            )?;
            Ok(ProposerSlashingStatus::NotSlashable)
        }
    }

    /// Attempt to prune the database, deleting old blocks and attestations.
    pub fn prune(&self, current_epoch: Epoch) -> Result<(), Error> {
        self.prune_proposers(current_epoch)?;
        self.prune_indexed_attestations(current_epoch)?;
        Ok(())
    }

    fn prune_proposers(&self, current_epoch: Epoch) -> Result<(), Error> {
        let min_slot = current_epoch
            .saturating_add(1u64)
            .saturating_sub(self.config.history_length)
            .start_slot(E::slots_per_epoch());

        // Collect keys to delete.
        let mut to_prune = vec![];

        for res in self.proposers.range::<IVec, _>(..).keys() {
            let key_bytes = res?;
            let (slot, _) = ProposerKey::parse(key_bytes.clone())?;
            if slot < min_slot {
                to_prune.push(key_bytes);
            } else {
                // End the loop if we've reached a slot that doesn't need pruning.
                break;
            }
        }

        self.transaction(|txn| {
            for key in &to_prune {
                txn.proposers
                    .remove(key)?
                    .ok_or(Error::MissingProposerValue)?;
            }
            Ok(())
        })?;

        Ok(())
    }

    fn prune_indexed_attestations(&self, current_epoch: Epoch) -> Result<(), Error> {
        let min_epoch = current_epoch
            .saturating_add(1u64)
            .saturating_sub(self.config.history_length as u64);

        // Collect indexed attestation IDs to delete.
        let mut indexed_attestation_id_keys = vec![];
        let mut indexed_attestation_ids = vec![];

        for res in self.indexed_attestation_id.range::<IVec, _>(..) {
            let (key_bytes, value) = res?;
            // FIXME(sproul): remove clone
            let (target_epoch, _) = IndexedAttestationIdKey::parse(key_bytes.clone())?;

            if target_epoch < min_epoch {
                indexed_attestation_id_keys.push(key_bytes);
                indexed_attestation_ids.push(IndexedAttestationId::new(
                    IndexedAttestationId::parse(value)?,
                ));
            } else {
                // End the loop if we've reached an epoch that doesn't need pruning.
                break;
            }
        }

        // Delete the IDs themselves _and_ the indexed attestations they point to.
        self.transaction(|txn| {
            for attestation_id_key in &indexed_attestation_id_keys {
                txn.indexed_attestation_id.remove(attestation_id_key)?;
            }
            for attestation_id in &indexed_attestation_ids {
                txn.indexed_attestation.remove(attestation_id.as_ref())?;
            }
            Ok(())
        })?;
        self.delete_attestation_data_roots(indexed_attestation_ids);

        Ok(())
    }
}
