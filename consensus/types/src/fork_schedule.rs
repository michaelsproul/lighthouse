use crate::Slot;
use lazy_static::lazy_static;
use parking_lot::RwLock;

lazy_static! {
    pub static ref FORK_SCHEDULE: RwLock<Option<ForkSchedule>> = RwLock::new(None);
}

/// Constants related to hard-fork upgrades.
#[derive(Debug)]
pub struct ForkSchedule {
    pub altair_fork_slot: Slot,
    pub altair_fork_version: [u8; 4],
}
