//! A map of operations keyed by validator index.
//!
//! `insert` and `retain` are the only functions that write the map, and `insert` computes the key
//! from the value. The Lean proofs in `verification/operation_pool` extract `new` and `insert`,
//! and prove that the operations of a map are about pairwise distinct validators.
//!
//! Nothing in this module depends on `EthSpec`, which keeps the extracted code small.

use std::collections::{HashMap, hash_map};

/// An operation that is about one validator.
pub trait ValidatorIndexed {
    /// The index of the validator that the operation is about.
    fn validator_index(&self) -> u64;
}

/// At most one operation per validator, stored under the index of the validator.
///
/// The map is private, so every write goes through `insert` or `retain`.
//
// The struct has no trait bound on `V`: Aeneas does not translate a struct that has one.
#[derive(Debug, PartialEq)]
pub struct ValidatorMap<V> {
    map: HashMap<u64, V>,
}

impl<V> ValidatorMap<V> {
    /// The empty map.
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// Keep the operations that `keep` accepts.
    ///
    /// `keep` reads the validator index and the operation. It cannot change the operation.
    pub fn retain<F>(&mut self, mut keep: F)
    where
        F: FnMut(u64, &V) -> bool,
    {
        self.map.retain(|index, value| keep(*index, value));
    }

    /// The operations, in arbitrary order.
    pub fn values(&self) -> hash_map::Values<'_, u64, V> {
        self.map.values()
    }

    /// The number of operations.
    pub fn len(&self) -> usize {
        self.map.len()
    }
}

impl<V: ValidatorIndexed> ValidatorMap<V> {
    /// Store `value` under the index of its validator.
    ///
    /// An operation that the map already holds for that validator is replaced and returned.
    pub fn insert(&mut self, value: V) -> Option<V> {
        let index = value.validator_index();
        self.map.insert(index, value)
    }
}

impl<V> Default for ValidatorMap<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V: ValidatorIndexed> FromIterator<V> for ValidatorMap<V> {
    /// A later operation replaces an earlier one for the same validator.
    fn from_iter<I: IntoIterator<Item = V>>(iter: I) -> Self {
        let mut map = Self::new();
        for value in iter {
            map.insert(value);
        }
        map
    }
}
