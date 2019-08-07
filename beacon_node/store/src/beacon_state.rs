use crate::chunked_vector::{store_updated_vector_entry, ActiveIndexRoots};
use crate::errors::*;
use crate::*;

pub fn store_state<E: EthSpec, S: Store>(
    store: &S,
    state_root: &Hash256,
    state: &BeaconState<E>,
    spec: &ChainSpec,
) -> Result<(), Error> {
    // 1. Convert to PartialBeaconState and store that in the DB.
    let partial_state = PartialBeaconState::from_state_forgetful(state);
    partial_state.db_put(store, state_root)?;

    // 2. Store updated vector entries as required.
    /*
    store_updated_vector_entry(FieldName::BlockRoots, store, state_root, state, spec)?;
    store_updated_vector_entry(FieldName::StateRoots, store, state_root, state, spec)?;
    store_updated_vector_entry(FieldName::RandaoMixes, store, state_root, state, spec)?;
    */
    store_updated_vector_entry::<ActiveIndexRoots<E>, _, _>(store, state_root, state, spec)?;
    /*
    store_updated_vector_entry(
        FieldName::CompactCommitteesRoots,
        store,
        state_root,
        state,
        spec,
    )?;
    */

    Ok(())
}

pub fn load_partial_state<T: EthSpec>(
    store: &impl Store,
    state_root: &Hash256,
) -> Result<PartialBeaconState<T>, Error> {
    match store.get(state_root)? {
        Some(state) => Ok(state),
        None => Err(Error::from(DBError::new(format!(
            "State not found: {}",
            state_root
        )))),
    }
}

pub fn load_full_state<T: EthSpec>(
    store: &impl Store,
    state_root: &Hash256,
) -> Result<BeaconState<T>, Error> {
    // let partial_state = load_partial_state(store, state_root)?;
    panic!()
}

/*
impl<T: EthSpec> StoreItem for (BeaconState<T>, ChainSpec) {
    /// Store `self`.
    fn db_put(&self, store: &impl Store, state_root: &Hash256) -> Result<(), Error> {

        Ok(())
    }

    /// Retrieve an instance of `Self`.
    fn db_get(store: &impl Store, key: &Hash256) -> Result<Option<Self>, Error> {
        drop((store, key));
        Ok(None)
    }

    /// Return `true` if an instance of `Self` exists in `Store`.
    fn db_exists(store: &impl Store, key: &Hash256) -> Result<bool, Error> {
        drop((store, key));
        Ok(false)
    }

    /// Delete `self` from the `Store`.
    fn db_delete(store: &impl Store, key: &Hash256) -> Result<(), Error> {
        drop((store, key));
        Ok(())
    }
}
*/
