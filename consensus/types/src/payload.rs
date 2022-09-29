use crate::{test_utils::TestRandom, *};
use derivative::Derivative;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use ssz::{Decode, Encode};
use ssz_derive::{Decode, Encode};
use std::convert::TryFrom;
use std::fmt::Debug;
use std::hash::Hash;
use test_random_derive::TestRandom;
use tree_hash::TreeHash;
use tree_hash_derive::TreeHash;

#[derive(Debug)]
pub enum BlockType {
    Blinded,
    Full,
}

//  + TryFrom<ExecutionPayloadHeader<T>>
pub trait ExecPayload<T: EthSpec>: Debug + Clone + PartialEq + Hash + TreeHash + Send {
    fn block_type() -> BlockType;

    /// Convert the payload into a payload header.
    fn to_execution_payload_header(&self) -> ExecutionPayloadHeader<T>;

    // We provide a subset of field accessors, for the fields used in `consensus`.
    //
    // More fields can be added here if you wish.
    fn parent_hash(&self) -> ExecutionBlockHash;
    fn prev_randao(&self) -> Hash256;
    fn block_number(&self) -> u64;
    fn timestamp(&self) -> u64;
    fn block_hash(&self) -> ExecutionBlockHash;
    fn fee_recipient(&self) -> Address;
    fn gas_limit(&self) -> u64;
}

pub trait OwnedExecPayload<T: EthSpec>:
    ExecPayload<T> + Default + Serialize + DeserializeOwned + Encode + Decode + TestRandom + 'static
{
}

impl<T: EthSpec, P> OwnedExecPayload<T> for P where
    P: ExecPayload<T>
        + Default
        + Serialize
        + DeserializeOwned
        + Encode
        + Decode
        + TestRandom
        + 'static
{
}

// FIXME(sproul): this ToRef trait and implementations could live in/be generated by superstruct
pub trait ToRef<'a, Ref> {
    fn to_ref(&'a self) -> Ref;
}
impl<'a, T: EthSpec> ToRef<'a, FullPayloadRef<'a, T>> for FullPayloadMerge<T> {
    fn to_ref(&'a self) -> FullPayloadRef<'a, T> {
        FullPayloadRef::Merge(self)
    }
}
impl<'a, T: EthSpec> ToRef<'a, FullPayloadRef<'a, T>> for FullPayloadCapella<T> {
    fn to_ref(&'a self) -> FullPayloadRef<'a, T> {
        FullPayloadRef::Capella(self)
    }
}
impl<'a, T: EthSpec> ToRef<'a, BlindedPayloadRef<'a, T>> for BlindedPayloadMerge<T> {
    fn to_ref(&'a self) -> BlindedPayloadRef<'a, T> {
        BlindedPayloadRef::Merge(self)
    }
}
impl<'a, T: EthSpec> ToRef<'a, BlindedPayloadRef<'a, T>> for BlindedPayloadCapella<T> {
    fn to_ref(&'a self) -> BlindedPayloadRef<'a, T> {
        BlindedPayloadRef::Capella(self)
    }
}

pub trait AbstractExecPayload<T: EthSpec>:
    ExecPayload<T> + Sized + From<ExecutionPayload<T>> + TryFrom<ExecutionPayloadHeader<T>>
{
    type Ref<'a>: ExecPayload<T>;

    type Merge: OwnedExecPayload<T>
        + Into<Self>
        + for<'a> ToRef<'a, Self::Ref<'a>>
        + From<ExecutionPayloadMerge<T>>
        + TryFrom<ExecutionPayloadHeaderMerge<T>>;
    type Capella: OwnedExecPayload<T>
        + Into<Self>
        + for<'a> ToRef<'a, Self::Ref<'a>>
        + From<ExecutionPayloadCapella<T>>
        + TryFrom<ExecutionPayloadHeaderCapella<T>>;
}

#[superstruct(
    variants(Merge, Capella),
    variant_attributes(
        derive(
            Debug,
            Clone,
            Serialize,
            Deserialize,
            Encode,
            Decode,
            TestRandom,
            TreeHash,
            Derivative,
        ),
        derivative(PartialEq, Hash(bound = "T: EthSpec")),
        serde(bound = "T: EthSpec", deny_unknown_fields),
        cfg_attr(feature = "arbitrary-fuzz", derive(arbitrary::Arbitrary))
    ),
    ref_attributes(
        derive(Debug, Derivative, TreeHash),
        derivative(PartialEq, Hash(bound = "T: EthSpec")),
        tree_hash(enum_behaviour = "transparent"),
    ),
    cast_error(ty = "Error", expr = "BeaconStateError::IncorrectStateVariant"),
    partial_getter_error(ty = "Error", expr = "BeaconStateError::IncorrectStateVariant")
)]
#[derive(Debug, Clone, Serialize, Deserialize, TreeHash, Derivative)]
#[derivative(PartialEq, Hash(bound = "T: EthSpec"))]
#[serde(bound = "T: EthSpec")]
#[tree_hash(enum_behaviour = "transparent")]
pub struct FullPayload<T: EthSpec> {
    #[superstruct(only(Merge), partial_getter(rename = "execution_payload_merge"))]
    pub execution_payload: ExecutionPayloadMerge<T>,
    #[superstruct(only(Capella), partial_getter(rename = "execution_payload_capella"))]
    pub execution_payload: ExecutionPayloadCapella<T>,
}

impl<T: EthSpec> ExecPayload<T> for FullPayloadMerge<T> {
    fn block_type() -> BlockType {
        BlockType::Full
    }

    fn to_execution_payload_header(&self) -> ExecutionPayloadHeader<T> {
        ExecutionPayloadHeader::Merge(ExecutionPayloadHeaderMerge::from(&self.execution_payload))
    }

    fn parent_hash(&self) -> ExecutionBlockHash {
        self.execution_payload.parent_hash
    }

    fn prev_randao(&self) -> Hash256 {
        self.execution_payload.prev_randao
    }

    fn block_number(&self) -> u64 {
        self.execution_payload.block_number
    }

    fn timestamp(&self) -> u64 {
        self.execution_payload.timestamp
    }

    fn block_hash(&self) -> ExecutionBlockHash {
        self.execution_payload.block_hash
    }

    fn fee_recipient(&self) -> Address {
        self.execution_payload.fee_recipient
    }

    fn gas_limit(&self) -> u64 {
        self.execution_payload.gas_limit
    }
}
impl<T: EthSpec> ExecPayload<T> for FullPayloadCapella<T> {
    fn block_type() -> BlockType {
        BlockType::Full
    }

    fn to_execution_payload_header(&self) -> ExecutionPayloadHeader<T> {
        ExecutionPayloadHeader::Capella(ExecutionPayloadHeaderCapella::from(
            &self.execution_payload,
        ))
    }

    fn parent_hash(&self) -> ExecutionBlockHash {
        self.execution_payload.parent_hash
    }

    fn prev_randao(&self) -> Hash256 {
        self.execution_payload.prev_randao
    }

    fn block_number(&self) -> u64 {
        self.execution_payload.block_number
    }

    fn timestamp(&self) -> u64 {
        self.execution_payload.timestamp
    }

    fn block_hash(&self) -> ExecutionBlockHash {
        self.execution_payload.block_hash
    }

    fn fee_recipient(&self) -> Address {
        self.execution_payload.fee_recipient
    }

    fn gas_limit(&self) -> u64 {
        self.execution_payload.gas_limit
    }
}

impl<T: EthSpec> ExecPayload<T> for FullPayload<T> {
    fn block_type() -> BlockType {
        BlockType::Full
    }

    fn to_execution_payload_header(&self) -> ExecutionPayloadHeader<T> {
        match self {
            Self::Merge(payload) => payload.to_execution_payload_header(),
            Self::Capella(payload) => payload.to_execution_payload_header(),
        }
    }

    fn parent_hash<'a>(&'a self) -> ExecutionBlockHash {
        map_full_payload_ref!(&'a _, self.to_ref(), move |payload, cons| {
            cons(payload);
            payload.execution_payload.parent_hash
        })
    }

    fn prev_randao<'a>(&'a self) -> Hash256 {
        map_full_payload_ref!(&'a _, self.to_ref(), move |payload, cons| {
            cons(payload);
            payload.execution_payload.prev_randao
        })
    }

    fn block_number<'a>(&'a self) -> u64 {
        map_full_payload_ref!(&'a _, self.to_ref(), move |payload, cons| {
            cons(payload);
            payload.execution_payload.block_number
        })
    }

    fn timestamp<'a>(&'a self) -> u64 {
        map_full_payload_ref!(&'a _, self.to_ref(), move |payload, cons| {
            cons(payload);
            payload.execution_payload.timestamp
        })
    }

    fn block_hash<'a>(&'a self) -> ExecutionBlockHash {
        map_full_payload_ref!(&'a _, self.to_ref(), move |payload, cons| {
            cons(payload);
            payload.execution_payload.block_hash
        })
    }

    fn fee_recipient<'a>(&'a self) -> Address {
        map_full_payload_ref!(&'a _, self.to_ref(), move |payload, cons| {
            cons(payload);
            payload.execution_payload.fee_recipient
        })
    }

    fn gas_limit<'a>(&'a self) -> u64 {
        map_full_payload_ref!(&'a _, self.to_ref(), move |payload, cons| {
            cons(payload);
            payload.execution_payload.gas_limit
        })
    }
}

impl<'b, T: EthSpec> ExecPayload<T> for FullPayloadRef<'b, T> {
    fn block_type() -> BlockType {
        BlockType::Full
    }

    fn to_execution_payload_header(&self) -> ExecutionPayloadHeader<T> {
        match self {
            Self::Merge(payload) => payload.to_execution_payload_header(),
            Self::Capella(payload) => payload.to_execution_payload_header(),
        }
    }

    fn parent_hash<'a>(&'a self) -> ExecutionBlockHash {
        map_full_payload_ref!(&'a _, self, move |payload, cons| {
            cons(payload);
            payload.execution_payload.parent_hash
        })
    }

    fn prev_randao<'a>(&'a self) -> Hash256 {
        map_full_payload_ref!(&'a _, self, move |payload, cons| {
            cons(payload);
            payload.execution_payload.prev_randao
        })
    }

    fn block_number<'a>(&'a self) -> u64 {
        map_full_payload_ref!(&'a _, self, move |payload, cons| {
            cons(payload);
            payload.execution_payload.block_number
        })
    }

    fn timestamp<'a>(&'a self) -> u64 {
        map_full_payload_ref!(&'a _, self, move |payload, cons| {
            cons(payload);
            payload.execution_payload.timestamp
        })
    }

    fn block_hash<'a>(&'a self) -> ExecutionBlockHash {
        map_full_payload_ref!(&'a _, self, move |payload, cons| {
            cons(payload);
            payload.execution_payload.block_hash
        })
    }

    fn fee_recipient<'a>(&'a self) -> Address {
        map_full_payload_ref!(&'a _, self, move |payload, cons| {
            cons(payload);
            payload.execution_payload.fee_recipient
        })
    }

    fn gas_limit<'a>(&'a self) -> u64 {
        map_full_payload_ref!(&'a _, self, move |payload, cons| {
            cons(payload);
            payload.execution_payload.gas_limit
        })
    }
}

impl<T: EthSpec> AbstractExecPayload<T> for FullPayload<T> {
    type Ref<'a> = FullPayloadRef<'a, T>;
    type Merge = FullPayloadMerge<T>;
    type Capella = FullPayloadCapella<T>;
}

/*
// original implementation
impl<T: EthSpec> From<ExecutionPayload<T>> for FullPayload<T> {
    fn from(execution_payload: ExecutionPayload<T>) -> Self {
        Self { execution_payload }
    }
}

impl<T: EthSpec> TryFrom<ExecutionPayloadHeader<T>> for FullPayload<T> {
    type Error = ();

    fn try_from(_: ExecutionPayloadHeader<T>) -> Result<Self, Self::Error> {
        Err(())
    }
}
 */

impl<T: EthSpec> From<ExecutionPayload<T>> for FullPayload<T> {
    fn from(execution_payload: ExecutionPayload<T>) -> Self {
        match execution_payload {
            ExecutionPayload::Merge(execution_payload) => {
                Self::Merge(FullPayloadMerge { execution_payload })
            }
            ExecutionPayload::Capella(execution_payload) => {
                Self::Capella(FullPayloadCapella { execution_payload })
            }
        }
    }
}

impl<T: EthSpec> TryFrom<ExecutionPayloadHeader<T>> for FullPayload<T> {
    type Error = ();
    fn try_from(_: ExecutionPayloadHeader<T>) -> Result<Self, Self::Error> {
        Err(())
    }
}

impl<T: EthSpec> From<ExecutionPayloadMerge<T>> for FullPayloadMerge<T> {
    fn from(execution_payload: ExecutionPayloadMerge<T>) -> Self {
        Self { execution_payload }
    }
}
impl<T: EthSpec> From<ExecutionPayloadCapella<T>> for FullPayloadCapella<T> {
    fn from(execution_payload: ExecutionPayloadCapella<T>) -> Self {
        Self { execution_payload }
    }
}

impl<T: EthSpec> TryFrom<ExecutionPayloadHeader<T>> for FullPayloadMerge<T> {
    type Error = ();
    fn try_from(_: ExecutionPayloadHeader<T>) -> Result<Self, Self::Error> {
        Err(())
    }
}
impl<T: EthSpec> TryFrom<ExecutionPayloadHeader<T>> for FullPayloadCapella<T> {
    type Error = ();
    fn try_from(_: ExecutionPayloadHeader<T>) -> Result<Self, Self::Error> {
        Err(())
    }
}

impl<T: EthSpec> TryFrom<ExecutionPayloadHeaderMerge<T>> for FullPayloadMerge<T> {
    type Error = ();
    fn try_from(_: ExecutionPayloadHeaderMerge<T>) -> Result<Self, Self::Error> {
        Err(())
    }
}
impl<T: EthSpec> TryFrom<ExecutionPayloadHeaderCapella<T>> for FullPayloadCapella<T> {
    type Error = ();
    fn try_from(_: ExecutionPayloadHeaderCapella<T>) -> Result<Self, Self::Error> {
        Err(())
    }
}

#[superstruct(
    variants(Merge, Capella),
    variant_attributes(
        derive(
            Debug,
            Clone,
            Serialize,
            Deserialize,
            Encode,
            Decode,
            TestRandom,
            TreeHash,
            Derivative,
        ),
        derivative(PartialEq, Hash(bound = "T: EthSpec")),
        serde(bound = "T: EthSpec", deny_unknown_fields),
        cfg_attr(feature = "arbitrary-fuzz", derive(arbitrary::Arbitrary))
    ),
    ref_attributes(
        derive(Debug, Derivative, TreeHash),
        derivative(PartialEq, Hash(bound = "T: EthSpec")),
        tree_hash(enum_behaviour = "transparent"),
    ),
    cast_error(ty = "Error", expr = "BeaconStateError::IncorrectStateVariant"),
    partial_getter_error(ty = "Error", expr = "BeaconStateError::IncorrectStateVariant")
)]
#[derive(Debug, Clone, Serialize, Deserialize, TreeHash, Derivative)]
#[derivative(PartialEq, Hash(bound = "T: EthSpec"))]
#[serde(bound = "T: EthSpec")]
#[tree_hash(enum_behaviour = "transparent")]
pub struct BlindedPayload<T: EthSpec> {
    #[superstruct(only(Merge), partial_getter(rename = "execution_payload_merge"))]
    pub execution_payload_header: ExecutionPayloadHeaderMerge<T>,
    #[superstruct(only(Capella), partial_getter(rename = "execution_payload_capella"))]
    pub execution_payload_header: ExecutionPayloadHeaderCapella<T>,
}

impl<T: EthSpec> ExecPayload<T> for BlindedPayload<T> {
    fn block_type() -> BlockType {
        BlockType::Blinded
    }

    fn to_execution_payload_header(&self) -> ExecutionPayloadHeader<T> {
        match self {
            Self::Merge(payload) => {
                ExecutionPayloadHeader::Merge(payload.execution_payload_header.clone())
            }
            Self::Capella(payload) => {
                ExecutionPayloadHeader::Capella(payload.execution_payload_header.clone())
            }
        }
    }

    fn parent_hash(&self) -> ExecutionBlockHash {
        match self {
            Self::Merge(payload) => payload.execution_payload_header.parent_hash,
            Self::Capella(payload) => payload.execution_payload_header.parent_hash,
        }
    }

    fn prev_randao(&self) -> Hash256 {
        match self {
            Self::Merge(payload) => payload.execution_payload_header.prev_randao,
            Self::Capella(payload) => payload.execution_payload_header.prev_randao,
        }
    }

    fn block_number(&self) -> u64 {
        match self {
            Self::Merge(payload) => payload.execution_payload_header.block_number,
            Self::Capella(payload) => payload.execution_payload_header.block_number,
        }
    }

    fn timestamp(&self) -> u64 {
        match self {
            Self::Merge(payload) => payload.execution_payload_header.timestamp,
            Self::Capella(payload) => payload.execution_payload_header.timestamp,
        }
    }

    fn block_hash(&self) -> ExecutionBlockHash {
        match self {
            Self::Merge(payload) => payload.execution_payload_header.block_hash,
            Self::Capella(payload) => payload.execution_payload_header.block_hash,
        }
    }

    fn fee_recipient(&self) -> Address {
        match self {
            Self::Merge(payload) => payload.execution_payload_header.fee_recipient,
            Self::Capella(payload) => payload.execution_payload_header.fee_recipient,
        }
    }

    fn gas_limit(&self) -> u64 {
        match self {
            Self::Merge(payload) => payload.execution_payload_header.gas_limit,
            Self::Capella(payload) => payload.execution_payload_header.gas_limit,
        }
    }
}

// FIXME(sproul): deduplicate this
impl<'b, T: EthSpec> ExecPayload<T> for BlindedPayloadRef<'b, T> {
    fn block_type() -> BlockType {
        BlockType::Blinded
    }

    fn to_execution_payload_header(&self) -> ExecutionPayloadHeader<T> {
        match self {
            Self::Merge(payload) => {
                ExecutionPayloadHeader::Merge(payload.execution_payload_header.clone())
            }
            Self::Capella(payload) => {
                ExecutionPayloadHeader::Capella(payload.execution_payload_header.clone())
            }
        }
    }

    fn parent_hash(&self) -> ExecutionBlockHash {
        match self {
            Self::Merge(payload) => payload.execution_payload_header.parent_hash,
            Self::Capella(payload) => payload.execution_payload_header.parent_hash,
        }
    }

    fn prev_randao(&self) -> Hash256 {
        match self {
            Self::Merge(payload) => payload.execution_payload_header.prev_randao,
            Self::Capella(payload) => payload.execution_payload_header.prev_randao,
        }
    }

    fn block_number(&self) -> u64 {
        match self {
            Self::Merge(payload) => payload.execution_payload_header.block_number,
            Self::Capella(payload) => payload.execution_payload_header.block_number,
        }
    }

    fn timestamp(&self) -> u64 {
        match self {
            Self::Merge(payload) => payload.execution_payload_header.timestamp,
            Self::Capella(payload) => payload.execution_payload_header.timestamp,
        }
    }

    fn block_hash(&self) -> ExecutionBlockHash {
        match self {
            Self::Merge(payload) => payload.execution_payload_header.block_hash,
            Self::Capella(payload) => payload.execution_payload_header.block_hash,
        }
    }

    fn fee_recipient(&self) -> Address {
        match self {
            Self::Merge(payload) => payload.execution_payload_header.fee_recipient,
            Self::Capella(payload) => payload.execution_payload_header.fee_recipient,
        }
    }

    fn gas_limit(&self) -> u64 {
        match self {
            Self::Merge(payload) => payload.execution_payload_header.gas_limit,
            Self::Capella(payload) => payload.execution_payload_header.gas_limit,
        }
    }
}

impl<T: EthSpec> ExecPayload<T> for BlindedPayloadMerge<T> {
    fn block_type() -> BlockType {
        BlockType::Full
    }

    fn to_execution_payload_header(&self) -> ExecutionPayloadHeader<T> {
        ExecutionPayloadHeader::Merge(ExecutionPayloadHeaderMerge::from(
            self.execution_payload_header.clone(),
        ))
    }

    fn parent_hash(&self) -> ExecutionBlockHash {
        self.execution_payload_header.parent_hash
    }

    fn prev_randao(&self) -> Hash256 {
        self.execution_payload_header.prev_randao
    }

    fn block_number(&self) -> u64 {
        self.execution_payload_header.block_number
    }

    fn timestamp(&self) -> u64 {
        self.execution_payload_header.timestamp
    }

    fn block_hash(&self) -> ExecutionBlockHash {
        self.execution_payload_header.block_hash
    }

    fn fee_recipient(&self) -> Address {
        self.execution_payload_header.fee_recipient
    }

    fn gas_limit(&self) -> u64 {
        self.execution_payload_header.gas_limit
    }
}
impl<T: EthSpec> ExecPayload<T> for BlindedPayloadCapella<T> {
    fn block_type() -> BlockType {
        BlockType::Full
    }

    fn to_execution_payload_header(&self) -> ExecutionPayloadHeader<T> {
        ExecutionPayloadHeader::Capella(ExecutionPayloadHeaderCapella::from(
            self.execution_payload_header.clone(),
        ))
    }

    fn parent_hash(&self) -> ExecutionBlockHash {
        self.execution_payload_header.parent_hash
    }

    fn prev_randao(&self) -> Hash256 {
        self.execution_payload_header.prev_randao
    }

    fn block_number(&self) -> u64 {
        self.execution_payload_header.block_number
    }

    fn timestamp(&self) -> u64 {
        self.execution_payload_header.timestamp
    }

    fn block_hash(&self) -> ExecutionBlockHash {
        self.execution_payload_header.block_hash
    }

    fn fee_recipient(&self) -> Address {
        self.execution_payload_header.fee_recipient
    }

    fn gas_limit(&self) -> u64 {
        self.execution_payload_header.gas_limit
    }
}

impl<T: EthSpec> AbstractExecPayload<T> for BlindedPayload<T> {
    type Ref<'a> = BlindedPayloadRef<'a, T>;
    type Merge = BlindedPayloadMerge<T>;
    type Capella = BlindedPayloadCapella<T>;
}

impl<T: EthSpec> Default for FullPayloadMerge<T> {
    fn default() -> Self {
        Self {
            execution_payload: ExecutionPayloadMerge::default(),
        }
    }
}
impl<T: EthSpec> Default for FullPayloadCapella<T> {
    fn default() -> Self {
        Self {
            execution_payload: ExecutionPayloadCapella::default(),
        }
    }
}

// NOTE: the `Default` implementation for `BlindedPayload` needs to be different from the `Default`
// implementation for `ExecutionPayloadHeader` because payloads are checked for equality against the
// default payload in `is_merge_transition_block` to determine whether the merge has occurred.
//
// The default `BlindedPayload` is therefore the payload header that results from blinding the
// default `ExecutionPayload`, which differs from the default `ExecutionPayloadHeader` in that
// its `transactions_root` is the hash of the empty list rather than 0x0.
/*
impl<T: EthSpec> Default for BlindedPayload<T> {
    fn default() -> Self {
        Self {
            execution_payload_header: ExecutionPayloadHeader::from(&ExecutionPayload::default()),
        }
    }
}
*/

impl<T: EthSpec> Default for BlindedPayloadMerge<T> {
    fn default() -> Self {
        Self {
            execution_payload_header: ExecutionPayloadHeaderMerge::from(
                &ExecutionPayloadMerge::default(),
            ),
        }
    }
}

impl<T: EthSpec> Default for BlindedPayloadCapella<T> {
    fn default() -> Self {
        Self {
            execution_payload_header: ExecutionPayloadHeaderCapella::from(
                &ExecutionPayloadCapella::default(),
            ),
        }
    }
}

impl<T: EthSpec> From<ExecutionPayload<T>> for BlindedPayload<T> {
    fn from(payload: ExecutionPayload<T>) -> Self {
        match payload {
            ExecutionPayload::Merge(payload) => BlindedPayload::Merge(payload.into()),
            ExecutionPayload::Capella(payload) => BlindedPayload::Capella(payload.into()),
        }
    }
}

impl<T: EthSpec> From<ExecutionPayloadHeader<T>> for BlindedPayload<T> {
    fn from(execution_payload_header: ExecutionPayloadHeader<T>) -> Self {
        match execution_payload_header {
            ExecutionPayloadHeader::Merge(execution_payload_header) => {
                Self::Merge(BlindedPayloadMerge {
                    execution_payload_header,
                })
            }
            ExecutionPayloadHeader::Capella(execution_payload_header) => {
                Self::Capella(BlindedPayloadCapella {
                    execution_payload_header,
                })
            }
        }
    }
}

impl<T: EthSpec> From<ExecutionPayloadHeaderMerge<T>> for BlindedPayloadMerge<T> {
    fn from(execution_payload_header: ExecutionPayloadHeaderMerge<T>) -> Self {
        Self {
            execution_payload_header,
        }
    }
}
impl<T: EthSpec> From<ExecutionPayloadHeaderCapella<T>> for BlindedPayloadCapella<T> {
    fn from(execution_payload_header: ExecutionPayloadHeaderCapella<T>) -> Self {
        Self {
            execution_payload_header,
        }
    }
}

impl<T: EthSpec> From<BlindedPayload<T>> for ExecutionPayloadHeader<T> {
    fn from(blinded: BlindedPayload<T>) -> Self {
        match blinded {
            BlindedPayload::Merge(blinded_payload) => {
                ExecutionPayloadHeader::Merge(blinded_payload.execution_payload_header)
            }
            BlindedPayload::Capella(blinded_payload) => {
                ExecutionPayloadHeader::Capella(blinded_payload.execution_payload_header)
            }
        }
    }
}

// FIXME(sproul): consider adding references to these From impls
impl<T: EthSpec> From<ExecutionPayloadMerge<T>> for BlindedPayloadMerge<T> {
    fn from(execution_payload: ExecutionPayloadMerge<T>) -> Self {
        Self {
            execution_payload_header: ExecutionPayloadHeaderMerge::from(&execution_payload),
        }
    }
}
impl<T: EthSpec> From<ExecutionPayloadCapella<T>> for BlindedPayloadCapella<T> {
    fn from(execution_payload: ExecutionPayloadCapella<T>) -> Self {
        Self {
            execution_payload_header: ExecutionPayloadHeaderCapella::from(&execution_payload),
        }
    }
}

impl<T: EthSpec> TryFrom<ExecutionPayloadHeader<T>> for BlindedPayloadMerge<T> {
    type Error = ();
    fn try_from(header: ExecutionPayloadHeader<T>) -> Result<Self, Self::Error> {
        match header {
            ExecutionPayloadHeader::Merge(execution_payload_header) => {
                Ok(execution_payload_header.into())
            }
            _ => Err(()),
        }
    }
}
impl<T: EthSpec> TryFrom<ExecutionPayloadHeader<T>> for BlindedPayloadCapella<T> {
    type Error = ();
    fn try_from(header: ExecutionPayloadHeader<T>) -> Result<Self, Self::Error> {
        match header {
            ExecutionPayloadHeader::Capella(execution_payload_header) => {
                Ok(execution_payload_header.into())
            }
            _ => Err(()),
        }
    }
}

/*
impl<T: EthSpec> Decode for BlindedPayload<T> {
    fn is_ssz_fixed_len() -> bool {
        <ExecutionPayloadHeader<T> as Decode>::is_ssz_fixed_len()
    }

    fn ssz_fixed_len() -> usize {
        <ExecutionPayloadHeader<T> as Decode>::ssz_fixed_len()
    }

    fn from_ssz_bytes(bytes: &[u8]) -> Result<Self, DecodeError> {
        Ok(Self {
            execution_payload_header: ExecutionPayloadHeader::from_ssz_bytes(bytes)?,
        })
    }
}
 */

/*
impl<T: EthSpec> Encode for BlindedPayload<T> {
    fn is_ssz_fixed_len() -> bool {
        <ExecutionPayloadHeader<T> as Encode>::is_ssz_fixed_len()
    }

    fn ssz_append(&self, buf: &mut Vec<u8>) {
        self.execution_payload_header.ssz_append(buf)
    }

    fn ssz_bytes_len(&self) -> usize {
        self.execution_payload_header.ssz_bytes_len()
    }
}
*/

// FIXME(sproul): generate these using superstruct
impl<T: EthSpec> From<FullPayloadMerge<T>> for FullPayload<T> {
    fn from(payload: FullPayloadMerge<T>) -> Self {
        Self::Merge(payload)
    }
}

impl<T: EthSpec> From<FullPayloadCapella<T>> for FullPayload<T> {
    fn from(payload: FullPayloadCapella<T>) -> Self {
        Self::Capella(payload)
    }
}

impl<T: EthSpec> From<BlindedPayloadMerge<T>> for BlindedPayload<T> {
    fn from(payload: BlindedPayloadMerge<T>) -> Self {
        Self::Merge(payload)
    }
}
impl<T: EthSpec> From<BlindedPayloadCapella<T>> for BlindedPayload<T> {
    fn from(payload: BlindedPayloadCapella<T>) -> Self {
        Self::Capella(payload)
    }
}

/*
impl<T: EthSpec> TreeHash for FullPayload<T> {
    fn tree_hash_type() -> tree_hash::TreeHashType {
        <ExecutionPayload<T>>::tree_hash_type()
    }

    fn tree_hash_packed_encoding(&self) -> tree_hash::PackedEncoding {
        self.execution_payload.tree_hash_packed_encoding()
    }

    fn tree_hash_packing_factor() -> usize {
        <ExecutionPayload<T>>::tree_hash_packing_factor()
    }

    fn tree_hash_root(&self) -> tree_hash::Hash256 {
        self.execution_payload.tree_hash_root()
    }
}
*/

/*
impl<T: EthSpec> Decode for FullPayload<T> {
    fn is_ssz_fixed_len() -> bool {
        <ExecutionPayload<T> as Decode>::is_ssz_fixed_len()
    }

    fn from_ssz_bytes(bytes: &[u8]) -> Result<Self, DecodeError> {
        Ok(FullPayload {
            execution_payload: Decode::from_ssz_bytes(bytes)?,
        })
    }
}

impl<T: EthSpec> Encode for FullPayload<T> {
    fn is_ssz_fixed_len() -> bool {
        <ExecutionPayload<T> as Encode>::is_ssz_fixed_len()
    }

    fn ssz_append(&self, buf: &mut Vec<u8>) {
        self.execution_payload.ssz_append(buf)
    }

    fn ssz_bytes_len(&self) -> usize {
        self.execution_payload.ssz_bytes_len()
    }
}
*/
