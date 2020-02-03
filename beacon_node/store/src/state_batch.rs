use types::EthSpec;

/// A collection of states to be stored in the database.
///
/// Consumes minimal space in memory by not storing states between epoch boundaries.
pub struct StateBatch<E: EthSpec> {
    items: Vec<BatchItem<E>>,
}

enum BatchItem<E: EthSpec> {
    State()
}

impl <E: EthSpec> StateBatch<E> {
    pub fn new() -> Self {
        Self {
            items: vec![]
        }
    }

    pub fn add_state(&mut self, state_root: Hash256, state: &BeaconState<E>) {

    }

    pub fn
}
