use crate::cases::LoadCase;
use crate::decode::yaml_decode_file;
use crate::error::Error;
use crate::testing_spec;
use serde_derive::Deserialize;
use ssz::{Decode, Encode};
use ssz_derive::{Decode, Encode};
use std::convert::TryFrom;
use std::fmt::Debug;
use std::path::Path;
use tree_hash::TreeHash;
use types::{BeaconState, EthSpec, ForkName, SignedBeaconBlock};

/// Trait for all BLS cases to eliminate some boilerplate.
pub trait BlsCase: serde::de::DeserializeOwned {}

impl<T: BlsCase> LoadCase for T {
    fn load_from_dir(path: &Path, _fork_name: ForkName) -> Result<Self, Error> {
        yaml_decode_file(&path.join("data.yaml"))
    }
}

/// Macro to wrap U128 and U256 so they deserialize correctly.
macro_rules! uint_wrapper {
    ($wrapper_name:ident, $wrapped_type:ty) => {
        #[derive(Debug, Clone, Copy, Default, PartialEq, Decode, Encode, Deserialize)]
        #[serde(try_from = "String")]
        pub struct $wrapper_name {
            pub x: $wrapped_type,
        }

        impl TryFrom<String> for $wrapper_name {
            type Error = String;

            fn try_from(s: String) -> Result<Self, Self::Error> {
                <$wrapped_type>::from_dec_str(&s)
                    .map(|x| Self { x })
                    .map_err(|e| format!("{:?}", e))
            }
        }

        impl tree_hash::TreeHash for $wrapper_name {
            fn tree_hash_type() -> tree_hash::TreeHashType {
                <$wrapped_type>::tree_hash_type()
            }

            fn tree_hash_packed_encoding(&self) -> Vec<u8> {
                self.x.tree_hash_packed_encoding()
            }

            fn tree_hash_packing_factor() -> usize {
                <$wrapped_type>::tree_hash_packing_factor()
            }

            fn tree_hash_root(&self) -> tree_hash::Hash256 {
                self.x.tree_hash_root()
            }
        }
    };
}

uint_wrapper!(TestU128, ethereum_types::U128);
uint_wrapper!(TestU256, ethereum_types::U256);

/// Trait for types that can be used in SSZ static tests.
pub trait SszStaticType:
    serde::de::DeserializeOwned + Encode + TreeHash + Clone + PartialEq + Debug + Sync
{
    // fn decode(bytes: &[u8], fork_name: ForkName) -> Result<Self, ssz::DecodeError>;
}

impl<T> SszStaticType for T where
    T: serde::de::DeserializeOwned + Encode + TreeHash + Clone + PartialEq + Debug + Sync
{
}
/*
{
    fn decode(bytes: &[u8], _: ForkName) -> Result<Self, ssz::DecodeError> {
        Self::from_ssz_bytes(bytes)
    }
}

impl<E: EthSpec> SszStaticType for BeaconState<E> {
    fn decode(bytes: &[u8], fork_name: ForkName) -> Result<Self, ssz::DecodeError> {
        Self::from_ssz_bytes(bytes, &testing_spec::<E>(fork_name))
    }
}

impl<E: EthSpec> SszStaticType for SignedBeaconBlock<E> {
    fn decode(bytes: &[u8], fork_name: ForkName) -> Result<Self, ssz::DecodeError> {
        Self::from_ssz_bytes(bytes, &testing_spec::<E>(fork_name))
    }
}
*/
