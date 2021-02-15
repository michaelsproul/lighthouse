//! Utilities for managing database schema changes.
use crate::config::OnDiskStoreConfig;
use crate::hot_cold_store::{HotColdDB, HotColdDBError};
use crate::metadata::{SchemaVersion, CONFIG_KEY, CURRENT_SCHEMA_VERSION};
use crate::{DBColumn, Error, ItemStore, StoreItem};
use ssz::{Decode, Encode};
use ssz_derive::{Decode, Encode};
use types::EthSpec;

impl<E, Hot, Cold> HotColdDB<E, Hot, Cold>
where
    E: EthSpec,
    Hot: ItemStore<E>,
    Cold: ItemStore<E>,
{
    /// Migrate the database from one schema version to another, applying all requisite mutations.
    pub fn migrate_schema(&self, from: SchemaVersion, to: SchemaVersion) -> Result<(), Error> {
        match (from, to) {
            // Migration from v0.3.0 to v0.3.x, adding the temporary states column.
            // Nothing actually needs to be done, but once a DB uses v2 it shouldn't go back.
            (SchemaVersion(1), SchemaVersion(2)) => {
                self.store_schema_version(to)?;
                Ok(())
            }
            // Migration from schema v2 (v1.1.0 and before) to schema v3.
            // Update `OnDiskStoreConfig` representation.
            (SchemaVersion(2), SchemaVersion(3)) => {
                if let Some(OnDiskStoreConfigV2 {
                    slots_per_restore_point,
                    ..
                }) = self.hot_db.get(&CONFIG_KEY)?
                {
                    let new_config = OnDiskStoreConfig {
                        slots_per_restore_point: Some(slots_per_restore_point),
                    };
                    self.hot_db.put(&CONFIG_KEY, &new_config)?;
                }

                self.store_schema_version(to)?;
                Ok(())
            }
            // Migrating from the current schema version to iself is always OK, a no-op.
            (_, _) if from == to && to == CURRENT_SCHEMA_VERSION => Ok(()),
            // Anything else is an error.
            (_, _) => Err(HotColdDBError::UnsupportedSchemaVersion {
                target_version: to,
                current_version: from,
            }
            .into()),
        }
    }
}

// Store config used in v2 schema and earlier.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OnDiskStoreConfigV2 {
    pub slots_per_restore_point: u64,
    pub _block_cache_size: usize,
}

impl StoreItem for OnDiskStoreConfigV2 {
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
