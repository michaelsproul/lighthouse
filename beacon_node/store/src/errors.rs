use crate::chunked_vector::ChunkError;
use crate::config::StoreConfigError;
use crate::hot_cold_store::HotColdDBError;
use crate::updated_once::UpdatedOnceError;
use ssz::DecodeError;
use state_processing::BlockReplayError;
use types::{milhouse, BeaconStateError, Hash256, InconsistentFork, Slot};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    SszDecodeError(DecodeError),
    VectorChunkError(ChunkError),
    BeaconStateError(BeaconStateError),
    PartialBeaconStateError,
    HotColdDBError(HotColdDBError),
    DBError {
        message: String,
    },
    RlpError(String),
    BlockNotFound(Hash256),
    NoContinuationData,
    SplitPointModified(Slot, Slot),
    ConfigError(StoreConfigError),
    SchemaMigrationError(String),
    /// The store's `anchor_info` was mutated concurrently, the latest modification wasn't applied.
    AnchorInfoConcurrentMutation,
    /// The block or state is unavailable due to weak subjectivity sync.
    HistoryUnavailable,
    /// State reconstruction cannot commence because not all historic blocks are known.
    MissingHistoricBlocks {
        oldest_block_slot: Slot,
    },
    /// State reconstruction failed because it didn't reach the upper limit slot.
    ///
    /// This should never happen (it's a logic error).
    StateReconstructionDidNotComplete,
    StateReconstructionRootMismatch {
        slot: Slot,
        expected: Hash256,
        computed: Hash256,
    },
    MissingStateRoot(Slot),
    MissingState(Hash256),
    NoBaseStateFound(Hash256),
    BlockReplayError(BlockReplayError),
    MilhouseError(milhouse::Error),
    Compression(std::io::Error),
    MissingPersistedBeaconChain,
    SlotIsBeforeSplit {
        slot: Slot,
    },
    FinalizedStateDecreasingSlot,
    FinalizedStateUnaligned,
    StateForCacheHasPendingUpdates {
        state_root: Hash256,
        slot: Slot,
    },
    AddPayloadLogicError,
    SlotClockUnavailableForMigration,
    MissingImmutableValidator(usize),
    MissingValidator(usize),
    V9MigrationFailure(Hash256),
    ValidatorPubkeyCacheError(String),
    DuplicateValidatorPublicKey,
    InvalidValidatorPubkeyBytes(bls::Error),
    ValidatorPubkeyCacheUninitialized,
    InvalidKey,
    UpdatedOnce(UpdatedOnceError),
    UnableToDowngrade,
    InconsistentFork(InconsistentFork),
}

pub trait HandleUnavailable<T> {
    fn handle_unavailable(self) -> std::result::Result<Option<T>, Error>;
}

impl<T> HandleUnavailable<T> for Result<T> {
    fn handle_unavailable(self) -> std::result::Result<Option<T>, Error> {
        match self {
            Ok(x) => Ok(Some(x)),
            Err(Error::HistoryUnavailable) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

impl From<DecodeError> for Error {
    fn from(e: DecodeError) -> Error {
        Error::SszDecodeError(e)
    }
}

impl From<ChunkError> for Error {
    fn from(e: ChunkError) -> Error {
        Error::VectorChunkError(e)
    }
}

impl From<HotColdDBError> for Error {
    fn from(e: HotColdDBError) -> Error {
        Error::HotColdDBError(e)
    }
}

impl From<BeaconStateError> for Error {
    fn from(e: BeaconStateError) -> Error {
        Error::BeaconStateError(e)
    }
}

impl From<DBError> for Error {
    fn from(e: DBError) -> Error {
        Error::DBError { message: e.message }
    }
}

impl From<StoreConfigError> for Error {
    fn from(e: StoreConfigError) -> Error {
        Error::ConfigError(e)
    }
}

impl From<milhouse::Error> for Error {
    fn from(e: milhouse::Error) -> Self {
        Self::MilhouseError(e)
    }
}

impl From<BlockReplayError> for Error {
    fn from(e: BlockReplayError) -> Error {
        Error::BlockReplayError(e)
    }
}

impl From<UpdatedOnceError> for Error {
    fn from(e: UpdatedOnceError) -> Error {
        Error::UpdatedOnce(e)
    }
}

impl From<InconsistentFork> for Error {
    fn from(e: InconsistentFork) -> Error {
        Error::InconsistentFork(e)
    }
}

#[derive(Debug)]
pub struct DBError {
    pub message: String,
}

impl DBError {
    pub fn new(message: String) -> Self {
        Self { message }
    }
}
