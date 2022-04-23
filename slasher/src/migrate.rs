use crate::{Error, SlasherDB};
use types::EthSpec;

impl<E: EthSpec> SlasherDB<E> {
    /// If the database exists, and has a schema, attempt to migrate it to the current version.
    pub fn migrate(self) -> Result<Self, Error> {
        Ok(self)
    }
}
