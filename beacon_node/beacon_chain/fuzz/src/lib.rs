pub mod config;
pub mod hydra;
pub mod message_queue;
pub mod node;
pub mod runner;

pub use config::Config;
pub use hydra::Hydra;
pub use message_queue::Message;
pub use node::Node;
pub use runner::Runner;

use beacon_chain::test_utils::{BeaconChainHarness, EphemeralHarnessType};

pub type TestHarness<E> = BeaconChainHarness<EphemeralHarnessType<E>>;
