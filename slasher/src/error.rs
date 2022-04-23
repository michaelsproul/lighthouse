use crate::config::{Config, DiskConfig};
use sled::transaction::{
    ConflictableTransactionError, TransactionError, UnabortableTransactionError,
};
use std::io;
use types::Epoch;

#[derive(Debug)]
pub enum Error {
    DatabaseError(sled::Error),
    DatabaseConflict,
    DatabaseIOError(io::Error),
    DatabasePermissionsError(filesystem::Error),
    SszDecodeError(ssz::DecodeError),
    BincodeError(bincode::Error),
    ArithError(safe_arith::ArithError),
    ChunkIndexOutOfBounds(usize),
    IncompatibleSchemaVersion {
        database_schema_version: u64,
        software_schema_version: u64,
    },
    ConfigInvalidChunkSize {
        chunk_size: usize,
        history_length: usize,
    },
    ConfigInvalidHistoryLength {
        history_length: usize,
        max_history_length: usize,
    },
    ConfigInvalidZeroParameter {
        config: Config,
    },
    ConfigIncompatible {
        on_disk_config: DiskConfig,
        config: DiskConfig,
    },
    ConfigMissing,
    DistanceTooLarge,
    DistanceCalculationOverflow,
    /// Missing an attester record that we expected to exist.
    MissingAttesterRecord {
        validator_index: u64,
        target_epoch: Epoch,
    },
    AttesterRecordCorrupt {
        length: usize,
    },
    AttesterKeyCorrupt {
        length: usize,
    },
    ProposerKeyCorrupt {
        length: usize,
    },
    IndexedAttestationIdKeyCorrupt {
        length: usize,
    },
    IndexedAttestationIdCorrupt {
        length: usize,
    },
    MissingIndexedAttestation {
        id: u64,
    },
    MissingAttesterKey,
    MissingProposerValue,
    MissingIndexedAttestationId,
    MissingIndexedAttestationIdKey,
    InconsistentAttestationDataRoot,
}

impl From<sled::Error> for Error {
    fn from(e: sled::Error) -> Self {
        Error::DatabaseError(e)
    }
}

impl From<UnabortableTransactionError> for Error {
    fn from(e: UnabortableTransactionError) -> Self {
        match e {
            UnabortableTransactionError::Conflict => Self::DatabaseConflict,
            UnabortableTransactionError::Storage(e) => Self::DatabaseError(e),
        }
    }
}

impl From<ConflictableTransactionError<Error>> for Error {
    fn from(e: ConflictableTransactionError<Error>) -> Self {
        match e {
            ConflictableTransactionError::Abort(error) => error,
            ConflictableTransactionError::Storage(e) => Self::DatabaseError(e),
            _ => panic!("unexpected error"),
        }
    }
}

impl From<TransactionError<Error>> for Error {
    fn from(e: TransactionError<Error>) -> Self {
        match e {
            TransactionError::Abort(error) => error,
            TransactionError::Storage(e) => Self::DatabaseError(e),
        }
    }
}

impl From<Error> for ConflictableTransactionError<Error> {
    fn from(e: Error) -> Self {
        ConflictableTransactionError::Abort(e)
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::DatabaseIOError(e)
    }
}

impl From<ssz::DecodeError> for Error {
    fn from(e: ssz::DecodeError) -> Self {
        Error::SszDecodeError(e)
    }
}

impl From<bincode::Error> for Error {
    fn from(e: bincode::Error) -> Self {
        Error::BincodeError(e)
    }
}

impl From<safe_arith::ArithError> for Error {
    fn from(e: safe_arith::ArithError) -> Self {
        Error::ArithError(e)
    }
}
