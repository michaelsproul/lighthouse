#![cfg(feature = "butter_db")]

use crate::{
    database::{
        interface::{Key, OpenDatabases, Value},
        *,
    },
    Config, Error,
};
use butter_db::{Table, TableId, Transaction};
use std::marker::PhantomData;

pub use butter_db::Cursor;

#[derive(Debug)]
pub struct Environment {
    db: butter_db::Database,
}

#[derive(Debug)]
pub struct RwTransaction<'env> {
    txn: Transaction<'env>,
}

#[derive(Debug)]
pub struct Database<'env> {
    table_id: TableId,
    _phantom: PhantomData<&'env ()>,
}

impl Environment {
    pub fn new(config: &Config) -> Result<Environment, Error> {
        let db = butter_db::Database::open_or_create(config.database_path.clone())?;
        Ok(Self { db })
    }

    pub fn create_databases(&self) -> Result<OpenDatabases, Error> {
        let mut txn = self.db.begin_transaction()?;

        let indexed_attestation_db = txn.create_table(INDEXED_ATTESTATION_DB)?;
        let indexed_attestation_id_db = txn.create_table(INDEXED_ATTESTATION_ID_DB)?;
        let attesters_db = txn.create_table(ATTESTERS_DB)?;
        let attesters_max_targets_db = txn.create_table(ATTESTERS_MAX_TARGETS_DB)?;
        let min_targets_db = txn.create_table(MIN_TARGETS_DB)?;
        let max_targets_db = txn.create_table(MAX_TARGETS_DB)?;
        let current_epochs_db = txn.create_table(CURRENT_EPOCHS_DB)?;
        let proposers_db = txn.create_table(PROPOSERS_DB)?;
        let metadata_db = txn.create_table(METADATA_DB)?;

        txn.commit()?;

        let wrap = |table_id| {
            crate::Database::Butter(Database {
                table_id,
                _phantom: PhantomData,
            })
        };

        Ok(OpenDatabases {
            indexed_attestation_db: wrap(indexed_attestation_db),
            indexed_attestation_id_db: wrap(indexed_attestation_id_db),
            attesters_db: wrap(attesters_db),
            attesters_max_targets_db: wrap(attesters_max_targets_db),
            min_targets_db: wrap(min_targets_db),
            max_targets_db: wrap(max_targets_db),
            current_epochs_db: wrap(current_epochs_db),
            proposers_db: wrap(proposers_db),
            metadata_db: wrap(metadata_db),
        })
    }

    pub fn begin_rw_txn(&self) -> Result<RwTransaction, Error> {
        let mut txn = self.db.begin_transaction()?;

        // Open all the tables in table ID order.
        txn.open_table(INDEXED_ATTESTATION_DB)?;
        txn.open_table(INDEXED_ATTESTATION_ID_DB)?;
        txn.open_table(ATTESTERS_DB)?;
        txn.open_table(ATTESTERS_MAX_TARGETS_DB)?;
        txn.open_table(MIN_TARGETS_DB)?;
        txn.open_table(MAX_TARGETS_DB)?;
        txn.open_table(CURRENT_EPOCHS_DB)?;
        txn.open_table(PROPOSERS_DB)?;
        txn.open_table(METADATA_DB)?;

        Ok(RwTransaction { txn })
    }
}

impl<'env> RwTransaction<'env> {
    pub fn get<K: AsRef<[u8]> + ?Sized>(
        &'env self,
        db: &Database<'env>,
        key: &K,
    ) -> Result<Option<Cow<'env, [u8]>>, Error> {
        let table = self.txn.get_table(db.table_id)?;
        Ok(self.txn.get(table, key.as_ref())?.map(Cow::Owned))
    }

    pub fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &mut self,
        db: &Database,
        key: K,
        value: V,
    ) -> Result<(), Error> {
        let table = self.txn.get_table(db.table_id)?;
        self.txn
            .put(table, key.as_ref(), value.as_ref())
            .map_err(Into::into)
    }

    pub fn del<K: AsRef<[u8]>>(&mut self, db: &Database, key: K) -> Result<(), Error> {
        let table = self.txn.get_table(db.table_id)?;
        self.txn.delete(table, key.as_ref()).map_err(Into::into)
    }

    pub fn cursor<'a>(&'a mut self, db: &Database) -> Result<Cursor<'a>, Error> {
        let table = self.txn.get_table(db.table_id)?;
        self.txn.cursor(table).map_err(Into::into)
    }

    pub fn commit(self) -> Result<(), Error> {
        self.txn.commit().map_err(Into::into)
    }
}
