use crate::{Error, SimpleStoreItem, Store};
use types::EthSpec;

pub trait ReadTransaction<S: Store<E>, E: EthSpec> {
    fn begin(store: Arc<S>) -> Result<Self, Error>;

    fn get<T: SimpleStoreItem>(&self, key: &[u8]) -> Result<Self, Error>;

    // TODO
    // fn get_state

    fn commit(self) -> Result<(), Error>;
}

/*
pub trait WriteTransaction<'a, S: Store<E>, E: EthSpec>: ReadTransaction<'a, S, E> {
    fn put<T: SimpleStoreItem>(&self, val: T) -> Result<(), Error>;

    // TODO
    // fn put_state
}
*/

// FIXME(delete):
pub struct NullTransaction;

impl<S: Store<E>, E: EthSpec> ReadTransaction<'_, S, E> for NullTransaction {
    fn begin(store: Arc<S>) -> Result<Self, Error> {
        panic!()
    }

    fn get<T: SimpleStoreItem>(&self) -> Result<Self, Error> {
        unimplemented!();
    }

    // TODO
    // fn get_state
    fn commit(self) -> Result<(), Error> {
        unimplemented!();
    }
}

/*
impl<S: Store> WriteTransaction<'_, S> for NullTransaction {
    fn put<T: SimpleStoreItem>(&self, val: T) -> Result<(), Error> {
        unimplemented!();
    }
}
*/
