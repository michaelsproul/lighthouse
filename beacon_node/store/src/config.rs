use serde_derive::{Deserialize, Serialize};
use types::{EthSpec, MinimalEthSpec};

pub const DEFAULT_SLOTS_PER_RESTORE_POINT: u64 = 2048;

/// Database configuration parameters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreConfig {
    /// Number of slots to wait between storing restore points in the freezer database.
    pub slots_per_restore_point: u64,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            // Safe default for tests, shouldn't ever be read by a CLI node.
            slots_per_restore_point: MinimalEthSpec::slots_per_historical_root() as u64,
        }
    }
}
