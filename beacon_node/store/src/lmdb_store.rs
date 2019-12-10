use super::*;
use crate::forwards_iter::SimpleForwardsBlockRootsIterator;
use crate::impls::beacon_state::{get_full_state, store_full_state};
use crate::{metrics, Error, Store};
use lmdb::{Database, DatabaseFlags, Environment, Transaction, WriteFlags};
use std::fs;
use std::marker::PhantomData;
use std::path::Path;
use types::{BeaconState, EthSpec, Hash256};

pub struct LMDB<E: EthSpec> {
    env: Environment,
    db: Database,
    _phantom: PhantomData<E>,
}

impl<E: EthSpec> LMDB<E> {
    pub fn open(path: &Path, map_size: usize) -> Result<Self, Error> {
        // LMDB requires the directory to already exist
        fs::create_dir_all(path).map_err(|e| Error::DBError {
            message: format!("IO error creating LMDB database directory: {}", e),
        })?;

        let env = Environment::new()
            .set_map_size(map_size)
            .open(path)
            .map_err(|e| {
                println!("env error {:?}", e);
                e
            })?;
        let db = env.create_db(None, DatabaseFlags::REVERSE_KEY)?;
        Ok(Self {
            env,
            db,
            _phantom: PhantomData,
        })
    }

    fn get_key_for_col(col: &str, key: &[u8]) -> Vec<u8> {
        let mut col = col.as_bytes().to_vec();
        col.extend_from_slice(key);
        col
    }
}

impl<E: EthSpec> Store<E> for LMDB<E> {
    type ForwardsBlockRootsIterator = SimpleForwardsBlockRootsIterator;

    /// Retrieve some bytes in `column` with `key`.
    fn get_bytes(&self, col: &str, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let column_key = Self::get_key_for_col(col, key);

        metrics::inc_counter(&metrics::DISK_DB_READ_COUNT);
        let timer = metrics::start_timer(&metrics::DISK_DB_READ_TIMES);

        match self.env.begin_ro_txn()?.get(self.db, &column_key) {
            Ok(bytes) => {
                metrics::inc_counter_by(&metrics::DISK_DB_READ_BYTES, bytes.len() as i64);
                metrics::stop_timer(timer);
                Ok(Some(bytes.to_vec()))
            }
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Store some `value` in `column`, indexed with `key`.
    fn put_bytes(&self, col: &str, key: &[u8], val: &[u8]) -> Result<(), Error> {
        let column_key = Self::get_key_for_col(col, key);

        metrics::inc_counter(&metrics::DISK_DB_WRITE_COUNT);
        metrics::inc_counter_by(&metrics::DISK_DB_WRITE_BYTES, val.len() as i64);
        let timer = metrics::start_timer(&metrics::DISK_DB_WRITE_TIMES);

        let mut txn = self.env.begin_rw_txn()?;
        txn.put(self.db, &column_key, &val, WriteFlags::empty())?;
        txn.commit()?;
        metrics::stop_timer(timer);
        Ok(())
    }

    /// Return `true` if `key` exists in `column`.
    fn key_exists(&self, col: &str, key: &[u8]) -> Result<bool, Error> {
        // FIXME(sproul): could avoid a clone by doing !bytes.is_empty()
        self.get_bytes(col, key).map(|val| val.is_some())
    }

    /// Removes `key` from `column`.
    fn key_delete(&self, col: &str, key: &[u8]) -> Result<(), Error> {
        let column_key = Self::get_key_for_col(col, key);

        metrics::inc_counter(&metrics::DISK_DB_DELETE_COUNT);

        let mut txn = self.env.begin_rw_txn()?;
        txn.del(self.db, &column_key, None)?;
        txn.commit()?;
        Ok(())
    }

    /// Store a state in the store.
    fn put_state(&self, state_root: &Hash256, state: &BeaconState<E>) -> Result<(), Error> {
        store_full_state(self, state_root, state)
    }

    /// Fetch a state from the store.
    fn get_state(
        &self,
        state_root: &Hash256,
        _: Option<Slot>,
    ) -> Result<Option<BeaconState<E>>, Error> {
        get_full_state(self, state_root)
    }

    fn forwards_block_roots_iterator(
        store: Arc<Self>,
        start_slot: Slot,
        end_state: BeaconState<E>,
        end_block_root: Hash256,
        _: &ChainSpec,
    ) -> Self::ForwardsBlockRootsIterator {
        SimpleForwardsBlockRootsIterator::new(store, start_slot, end_state, end_block_root)
    }
}

impl From<lmdb::Error> for Error {
    fn from(e: lmdb::Error) -> Error {
        Error::LMDBError(e)
    }
}
