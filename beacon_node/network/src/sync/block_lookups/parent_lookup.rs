use super::DownlodedBlocks;
use crate::sync::block_lookups::single_block_lookup::{RequestableThing, SingleBlobsRequest};
use crate::sync::block_lookups::RootBlockTuple;
use crate::sync::{
    manager::{Id, SLOT_IMPORT_TOLERANCE},
    network_context::SyncNetworkContext,
};
use beacon_chain::blob_verification::BlockWrapper;
use beacon_chain::blob_verification::{AsBlock, MaybeAvailableBlock};
use beacon_chain::BeaconChainTypes;
use lighthouse_network::PeerId;
use std::iter;
use std::sync::Arc;
use store::Hash256;
use strum::IntoStaticStr;
use types::blob_sidecar::BlobIdentifier;
use types::{BlobSidecar, EthSpec, SignedBeaconBlock};

use super::single_block_lookup::{self, SingleBlockRequest};

/// How many attempts we try to find a parent of a block before we give up trying.
pub(crate) const PARENT_FAIL_TOLERANCE: u8 = 5;
/// The maximum depth we will search for a parent block. In principle we should have sync'd any
/// canonical chain to its head once the peer connects. A chain should not appear where it's depth
/// is further back than the most recent head slot.
pub(crate) const PARENT_DEPTH_TOLERANCE: usize = SLOT_IMPORT_TOLERANCE * 2;

/// Maintains a sequential list of parents to lookup and the lookup's current state.
pub(crate) struct ParentLookup<T: BeaconChainTypes> {
    /// The root of the block triggering this parent request.
    chain_hash: Hash256,
    /// The blocks that have currently been downloaded.
    downloaded_blocks: Vec<DownlodedBlocks<T::EthSpec>>,
    /// Request of the last parent.
    pub current_parent_request: SingleBlockRequest<PARENT_FAIL_TOLERANCE, T::EthSpec>,
    /// Id of the last parent request.
    current_parent_request_id: Option<Id>,
    pub current_parent_blob_request: Option<SingleBlobsRequest<PARENT_FAIL_TOLERANCE, T::EthSpec>>,
    current_parent_blob_request_id: Option<Id>,
}

#[derive(Debug, PartialEq, Eq, IntoStaticStr)]
pub enum VerifyError {
    RootMismatch,
    NoBlockReturned,
    ExtraBlocksReturned,
    PreviousFailure { parent_root: Hash256 },
}

#[derive(Debug, PartialEq, Eq)]
pub enum RequestError {
    SendFailed(&'static str),
    ChainTooLong,
    /// We witnessed too many failures trying to complete this parent lookup.
    TooManyAttempts {
        /// We received more failures trying to process the blocks than downloading them
        /// from peers.
        cannot_process: bool,
    },
    NoPeers,
}

impl<T: BeaconChainTypes> ParentLookup<T> {
    pub fn contains_block(&self, block_root: &Hash256) -> bool {
        self.downloaded_blocks
            .iter()
            .any(|(root, _d_block)| root == block_root)
    }

    pub fn contains_blob(&self, blob_id: &BlobIdentifier) -> bool {
        self.downloaded_blocks
            .iter()
            .any(|(_root, block)| match block {
                MaybeAvailableBlock::Available(_) => false,
                MaybeAvailableBlock::AvailabilityPending(pending) => pending.has_blob(&blob_id),
            })
    }

    pub fn new(
        block_root: Hash256,
        block: MaybeAvailableBlock<T::EthSpec>,
        peer_id: PeerId,
    ) -> Self {
        // if available, just add to downloaded blocks,

        // if maybe available, treat it as a single blob lookup that will be requested after
        // this parent chain segment is processed

        let current_parent_request = SingleBlockRequest::new(block.parent_root(), peer_id);

        let (current_parent_blob_request, current_blobs_request) = match block.as_ref() {
            MaybeAvailableBlock::Available(available) => {
                let current_parent_blob_request = if available.da_check_required() {
                    Some(SingleBlobsRequest::new_with_all_ids(
                        block.parent_root(),
                        peer_id,
                    ))
                } else {
                    None
                };
                (current_parent_blob_request, None)
            }
            MaybeAvailableBlock::AvailabilityPending(pending) => {
                let parent_req = SingleBlobsRequest::new_with_all_ids(block.parent_root(), peer_id);
                let current_req =
                    SingleBlobsRequest::new(pending.get_missing_blob_ids().clone(), peer_id);
                (Some(parent_req), Some(current_req))
            }
        };

        Self {
            chain_hash: block_root,
            downloaded_blocks: vec![(block_root, block)],
            current_parent_request,
            current_parent_request_id: None,
            current_parent_blob_request,
            current_parent_blob_request_id: None,
        }
    }

    pub fn new_with_blobs_request(
        block_root: Hash256,
        block_wrapper: MaybeAvailableBlock<T::EthSpec>,
        peer_id: PeerId,
    ) -> Self {
    }

    /// Attempts to request the next unknown parent. If the request fails, it should be removed.
    pub fn request_parent_block(
        &mut self,
        cx: &mut SyncNetworkContext<T>,
    ) -> Result<(), RequestError> {
        // check to make sure this request hasn't failed
        if self.downloaded_blocks.len() >= PARENT_DEPTH_TOLERANCE {
            return Err(RequestError::ChainTooLong);
        }

        let (peer_id, request) = self.current_parent_request.make_request()?;
        match cx.parent_lookup_block_request(peer_id, request) {
            Ok(request_id) => {
                self.current_parent_request_id = Some(request_id);
                Ok(())
            }
            Err(reason) => {
                self.current_parent_request_id = None;
                Err(RequestError::SendFailed(reason))
            }
        }
    }

    pub fn request_parent_blobs(
        &mut self,
        cx: &mut SyncNetworkContext<T>,
    ) -> Result<(), RequestError> {
        if let Some(blob_req) = self.current_parent_blob_request.as_mut() {
            // check to make sure this request hasn't failed
            if self.downloaded_blocks.len() >= PARENT_DEPTH_TOLERANCE {
                return Err(RequestError::ChainTooLong);
            }

            let (peer_id, request) = blob_req.request_blobs()?;
            match cx.parent_lookup_blobs_request(peer_id, request) {
                Ok(request_id) => {
                    self.current_parent_blob_request_id = Some(request_id);
                    Ok(())
                }
                Err(reason) => {
                    self.current_parent_blob_request_id = None;
                    Err(RequestError::SendFailed(reason))
                }
            }
        }
        Ok(())
    }

    pub fn check_block_peer_disconnected(&mut self, peer_id: &PeerId) -> Result<(), ()> {
        self.current_parent_request.check_peer_disconnected(peer_id)
    }

    pub fn check_blob_peer_disconnected(&mut self, peer_id: &PeerId) -> Result<(), ()> {
        self.current_parent_blob_request
            .map(|mut req| req.check_peer_disconnected(peer_id))
            .unwrap_or_default()
    }

    pub fn add_block(&mut self, block: MaybeAvailableBlock<T::EthSpec>) {
        let next_parent = block.parent_root();
        let current_root = self.current_parent_request.requested_thing;

        self.downloaded_blocks.push((current_root, block));

        // Block request updates
        self.current_parent_request.requested_block_root = next_parent;
        self.current_parent_request.request_state.state =
            single_block_lookup::State::AwaitingDownload;
        self.current_parent_request_id = None;

        // Blob request updates
        if let Some(blob_req) = self.current_parent_blob_request.as_mut() {
            let mut all_ids = Vec::with_capacity(T::EthSpec::max_blobs_per_block());
            for i in 0..T::EthSpec::max_blobs_per_block() {
                all_ids.push(BlobIdentifier {
                    block_root: next_parent,
                    index: i as u64,
                });
            }
            blob_req.requested_ids = all_ids;
            blob_req.request_state.state = single_block_lookup::State::AwaitingDownload;
        }
        self.current_parent_blob_request_id = None;
    }

    pub fn add_blobs(&mut self, blobs: Vec<BlobIdentifier>) {
        self.current_parent_blob_request.map_or_else(
            SingleBlobsRequest::new(blobs, peer_id),
            |mut req| {
                req.requested_thing = next_parent;
                req.state = single_block_lookup::State::AwaitingDownload;
            },
        );
        self.current_parent_blob_request_id = None;
    }

    pub fn pending_block_response(&self, req_id: Id) -> bool {
        self.current_parent_request_id == Some(req_id)
    }

    pub fn pending_blob_response(&self, req_id: Id) -> bool {
        self.current_parent_blob_request_id == Some(req_id)
    }

    /// Consumes the parent request and destructures it into it's parts.
    #[allow(clippy::type_complexity)]
    pub fn parts_for_processing(
        self,
    ) -> (
        Hash256,
        Vec<MaybeAvailableBlock<T::EthSpec>>,
        Vec<Hash256>,
        SingleBlockRequest<PARENT_FAIL_TOLERANCE>,
        Option<SingleBlobsRequest<PARENT_FAIL_TOLERANCE, T::EthSpec>>,
    ) {
        let ParentLookup {
            chain_hash,
            downloaded_blocks,
            current_parent_request,
            current_parent_blob_request,
            current_parent_request_id: _,
            current_parent_blob_request_id: _,
        } = self;
        let block_count = downloaded_blocks.len();
        let mut blocks = Vec::with_capacity(block_count);
        let mut hashes = Vec::with_capacity(block_count);
        for (hash, block) in downloaded_blocks.into_iter() {
            blocks.push(block);
            hashes.push(hash);
        }
        (
            chain_hash,
            blocks,
            hashes,
            current_parent_request,
            current_parent_blob_request,
        )
    }

    /// Get the parent lookup's chain hash.
    pub fn chain_hash(&self) -> Hash256 {
        self.chain_hash
    }

    pub fn block_download_failed(&mut self) {
        self.current_parent_request.register_failure_downloading();
        self.current_parent_request_id = None;
    }

    pub fn block_processing_failed(&mut self) {
        self.current_parent_request.register_failure_processing();
        self.current_parent_request_id = None;
    }

    pub fn blob_download_failed(&mut self) {
        self.current_parent_blob_request
            .map(|mut req| req.register_failure_downloading());
        self.current_parent_blob_request_id = None;
    }

    pub fn blob_processing_failed(&mut self) {
        self.current_parent_blob_request
            .map(|mut req| req.register_failure_processing());
        self.current_parent_blob_request_id = None;
    }

    /// Verifies that the received block is what we requested. If so, parent lookup now waits for
    /// the processing result of the block.
    pub fn verify_block(
        &mut self,
        block: Option<Arc<SignedBeaconBlock<T::EthSpec>>>,
        failed_chains: &mut lru_cache::LRUTimeCache<Hash256>,
    ) -> Result<Option<RootBlockTuple<T::EthSpec>>, VerifyError> {
        let root_and_block = self.current_parent_request.verify_block(block)?;

        // check if the parent of this block isn't in the failed cache. If it is, this chain should
        // be dropped and the peer downscored.
        if let Some(parent_root) = root_and_block
            .as_ref()
            .map(|(_, block)| block.parent_root())
        {
            if failed_chains.contains(&parent_root) {
                self.current_parent_request.register_failure_downloading();
                self.current_parent_request_id = None;
                return Err(VerifyError::PreviousFailure { parent_root });
            }
        }

        Ok(root_and_block)
    }

    pub fn verify_blob(
        &mut self,
        blob: Option<Arc<BlobSidecar<T::EthSpec>>>,
        failed_chains: &mut lru_cache::LRUTimeCache<Hash256>,
    ) -> Result<Option<Vec<Arc<BlobSidecar<T::EthSpec>>>>, VerifyError> {
        let blobs = self
            .current_parent_blob_request
            .map(|mut req| req.verify_blob(blob))
            .transpose()?
            .flatten();

        // check if the parent of this block isn't in the failed cache. If it is, this chain should
        // be dropped and the peer downscored.
        if let Some(parent_root) = blobs
            .as_ref()
            .and_then(|blobs| blobs.first())
            .map(|blob| blob.block_parent_root)
        {
            if failed_chains.contains(&parent_root) {
                self.current_parent_blob_request
                    .register_failure_downloading();
                self.current_parent_blob_request_id = None;
                return Err(VerifyError::PreviousFailure { parent_root });
            }
        }

        Ok(blobs)
    }

    pub fn get_block_processing_peer(&self, chain_hash: Hash256) -> Option<PeerId> {
        if self.chain_hash == chain_hash {
            return self.current_parent_request.processing_peer().ok();
        }
        None
    }

    #[cfg(test)]
    pub fn failed_block_attempts(&self) -> u8 {
        self.current_parent_request.failed_attempts()
    }

    pub fn add_block_peer(&mut self, block_root: &Hash256, peer_id: &PeerId) -> bool {
        self.current_parent_request.add_peer(block_root, peer_id)
    }

    pub fn used_block_peers(&self) -> impl Iterator<Item = &PeerId> + '_ {
        self.current_parent_request.used_peers.iter()
    }

    pub fn get_blob_processing_peer(&self, chain_hash: Hash256) -> Option<PeerId> {
        if self.chain_hash == chain_hash {
            return self
                .current_parent_blob_request
                .and_then(|req| req.processing_peer().ok());
        }
        None
    }

    #[cfg(test)]
    pub fn failed_blob_attempts(&self) -> u8 {
        self.current_parent_blob_request
            .map_or(0, |req| req.failed_attempts())
    }

    pub fn add_blobs_peer(&mut self, blobs: &[BlobIdentifier], peer_id: &PeerId) -> bool {
        self.current_parent_blob_request
            .map_or(false, |mut req| req.add_peer(blobs, peer_id))
    }

    pub fn used_blob_peers(&self) -> impl Iterator<Item = &PeerId> + '_ {
        self.current_parent_blob_request
            .map_or(iter::empty(), |req| req.used_peers.iter())
    }
}

impl From<super::single_block_lookup::VerifyError> for VerifyError {
    fn from(e: super::single_block_lookup::VerifyError) -> Self {
        use super::single_block_lookup::VerifyError as E;
        match e {
            E::RootMismatch => VerifyError::RootMismatch,
            E::NoBlockReturned => VerifyError::NoBlockReturned,
            E::ExtraBlocksReturned => VerifyError::ExtraBlocksReturned,
        }
    }
}

impl From<super::single_block_lookup::LookupRequestError> for RequestError {
    fn from(e: super::single_block_lookup::LookupRequestError) -> Self {
        use super::single_block_lookup::LookupRequestError as E;
        match e {
            E::TooManyAttempts { cannot_process } => {
                RequestError::TooManyAttempts { cannot_process }
            }
            E::NoPeers => RequestError::NoPeers,
        }
    }
}

impl<T: BeaconChainTypes> slog::KV for ParentLookup<T> {
    fn serialize(
        &self,
        record: &slog::Record,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        serializer.emit_arguments("chain_hash", &format_args!("{}", self.chain_hash))?;
        slog::Value::serialize(&self.current_parent_request, record, "parent", serializer)?;
        serializer.emit_usize("downloaded_blocks", self.downloaded_blocks.len())?;
        slog::Result::Ok(())
    }
}

impl RequestError {
    pub fn as_static(&self) -> &'static str {
        match self {
            RequestError::SendFailed(e) => e,
            RequestError::ChainTooLong => "chain_too_long",
            RequestError::TooManyAttempts { cannot_process } if *cannot_process => {
                "too_many_processing_attempts"
            }
            RequestError::TooManyAttempts { cannot_process: _ } => "too_many_downloading_attempts",
            RequestError::NoPeers => "no_peers",
        }
    }
}
