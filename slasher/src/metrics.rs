pub use metrics::*;
use std::sync::LazyLock;

pub static SLASHER_DATABASE_SIZE: LazyLock<Result<IntGauge>> = LazyLock::new(|| {
    try_create_int_gauge(
        "slasher_database_size",
        "Size of the database backing the slasher, in bytes",
    )
});
pub static SLASHER_RUN_TIME: LazyLock<Result<Histogram>> = LazyLock::new(|| {
    try_create_histogram(
        "slasher_process_batch_time",
        "Time taken to process a batch of blocks and attestations",
    )
});
pub static SLASHER_NUM_ATTESTATIONS_DROPPED: LazyLock<Result<IntGauge>> = LazyLock::new(|| {
    try_create_int_gauge(
        "slasher_num_attestations_dropped",
        "Number of attestations dropped per batch",
    )
});
pub static SLASHER_NUM_ATTESTATIONS_DEFERRED: LazyLock<Result<IntGauge>> = LazyLock::new(|| {
    try_create_int_gauge(
        "slasher_num_attestations_deferred",
        "Number of attestations deferred per batch",
    )
});
pub static SLASHER_NUM_ATTESTATIONS_VALID: LazyLock<Result<IntGauge>> = LazyLock::new(|| {
    try_create_int_gauge(
        "slasher_num_attestations_valid",
        "Number of valid attestations per batch",
    )
});
pub static SLASHER_NUM_ATTESTATIONS_STORED_PER_BATCH: LazyLock<Result<IntGauge>> =
    LazyLock::new(|| {
        try_create_int_gauge(
            "slasher_num_attestations_stored_per_batch",
            "Number of attestations stored per batch",
        )
    });
pub static SLASHER_NUM_BLOCKS_PROCESSED: LazyLock<Result<IntGauge>> = LazyLock::new(|| {
    try_create_int_gauge(
        "slasher_num_blocks_processed",
        "Number of blocks processed per batch",
    )
});
pub static SLASHER_NUM_CHUNKS_UPDATED: LazyLock<Result<IntCounterVec>> = LazyLock::new(|| {
    try_create_int_counter_vec(
        "slasher_num_chunks_updated",
        "Number of min or max target chunks updated on disk",
        &["array"],
    )
});
pub static SLASHER_COMPRESSION_RATIO: LazyLock<Result<Gauge>> = LazyLock::new(|| {
    try_create_float_gauge(
        "slasher_compression_ratio",
        "Compression ratio for min-max array chunks (higher is better)",
    )
});
pub static SLASHER_NUM_ATTESTATION_ROOT_QUERIES: LazyLock<Result<IntCounter>> =
    LazyLock::new(|| {
        try_create_int_counter(
            "slasher_num_attestation_root_queries",
            "Number of requests for an attestation data root",
        )
    });
pub static SLASHER_NUM_ATTESTATION_ROOT_HITS: LazyLock<Result<IntCounter>> = LazyLock::new(|| {
    try_create_int_counter(
        "slasher_num_attestation_root_hits",
        "Number of requests for an attestation data root that hit the LRU cache",
    )
});
pub static SLASHER_ATTESTATION_ROOT_CACHE_SIZE: LazyLock<Result<IntGauge>> = LazyLock::new(|| {
    try_create_int_gauge(
        "slasher_attestation_root_cache_size",
        "Number of attestation data roots cached in memory",
    )
});

/*
 * Phase 1 fine-grained timing metrics for slasher performance debugging.
 */

// Service-level breakdown (service.rs)
pub static SLASHER_PRUNE_TIME: LazyLock<Result<Histogram>> = LazyLock::new(|| {
    try_create_histogram(
        "slasher_prune_time",
        "Time taken to prune the slasher database",
    )
});
pub static SLASHER_DB_SIZE_TIME: LazyLock<Result<Histogram>> = LazyLock::new(|| {
    try_create_histogram(
        "slasher_db_size_time",
        "Time taken to compute the slasher database size on disk",
    )
});

// Attestation processing breakdown (slasher.rs)
pub static SLASHER_ATTESTATION_STORE_TIME: LazyLock<Result<Histogram>> = LazyLock::new(|| {
    try_create_histogram(
        "slasher_attestation_store_time",
        "Time taken to store indexed attestations",
    )
});
pub static SLASHER_ATTESTATION_PROCESSING_TIME: LazyLock<Result<Histogram>> = LazyLock::new(|| {
    try_create_histogram(
        "slasher_attestation_processing_time",
        "Time taken to process attestation subqueues (double-vote and surround checks)",
    )
});
pub static SLASHER_NUM_SUBQUEUES: LazyLock<Result<IntGauge>> = LazyLock::new(|| {
    try_create_int_gauge(
        "slasher_num_subqueues",
        "Number of non-empty validator chunk subqueues per batch",
    )
});

// Per-subqueue breakdown (slasher.rs)
pub static SLASHER_DOUBLE_VOTE_CHECK_TIME: LazyLock<Result<Histogram>> = LazyLock::new(|| {
    try_create_histogram(
        "slasher_double_vote_check_time",
        "Time taken to check for double votes across all subqueues",
    )
});
pub static SLASHER_SURROUND_VOTE_CHECK_TIME: LazyLock<Result<Histogram>> = LazyLock::new(|| {
    try_create_histogram(
        "slasher_surround_vote_check_time",
        "Time taken to check for surround votes (array::update) across all subqueues",
    )
});

// Transaction commit timing (slasher.rs)
pub static SLASHER_TXN_COMMIT_TIME: LazyLock<Result<Histogram>> = LazyLock::new(|| {
    try_create_histogram(
        "slasher_txn_commit_time",
        "Time taken to commit the slasher RW transaction",
    )
});

// Array update breakdown (array.rs)
pub static SLASHER_EPOCH_UPDATE_TIME: LazyLock<Result<Histogram>> = LazyLock::new(|| {
    try_create_histogram(
        "slasher_epoch_update_time",
        "Time taken in epoch_update_for_validator loop per update_array call",
    )
});
pub static SLASHER_EPOCH_UPDATE_VALIDATORS_COUNT: LazyLock<Result<IntCounter>> =
    LazyLock::new(|| {
        try_create_int_counter(
            "slasher_epoch_update_validators_count",
            "Total number of validators epoch-updated",
        )
    });
pub static SLASHER_ATTESTATION_APPLY_TIME: LazyLock<Result<Histogram>> = LazyLock::new(|| {
    try_create_histogram(
        "slasher_attestation_apply_time",
        "Time taken applying attestations to min-max arrays per update_array call",
    )
});
pub static SLASHER_CHUNK_STORE_TIME: LazyLock<Result<Histogram>> = LazyLock::new(|| {
    try_create_histogram(
        "slasher_chunk_store_time",
        "Time taken writing updated chunks back to disk per update_array call",
    )
});
pub static SLASHER_CHUNK_LOAD_COUNT: LazyLock<Result<IntCounter>> = LazyLock::new(|| {
    try_create_int_counter(
        "slasher_chunk_load_count",
        "Total number of chunks loaded from disk",
    )
});
pub static SLASHER_CHUNK_STORE_COUNT: LazyLock<Result<IntCounter>> = LazyLock::new(|| {
    try_create_int_counter(
        "slasher_chunk_store_count",
        "Total number of chunks stored to disk",
    )
});

// Compression timing (array.rs)
pub static SLASHER_CHUNK_DECOMPRESS_TIME: LazyLock<Result<Histogram>> = LazyLock::new(|| {
    try_create_histogram(
        "slasher_chunk_decompress_time",
        "Total time spent decompressing chunks per batch",
    )
});
pub static SLASHER_CHUNK_COMPRESS_TIME: LazyLock<Result<Histogram>> = LazyLock::new(|| {
    try_create_histogram(
        "slasher_chunk_compress_time",
        "Total time spent compressing chunks per batch",
    )
});

// Double-vote check DB access (database.rs)
pub static SLASHER_ATTESTER_RECORD_LOOKUPS: LazyLock<Result<IntCounter>> = LazyLock::new(|| {
    try_create_int_counter(
        "slasher_attester_record_lookups",
        "Number of check_and_update_attester_record calls",
    )
});
pub static SLASHER_ATTESTER_MAX_TARGET_GAPS: LazyLock<Result<IntCounter>> = LazyLock::new(|| {
    try_create_int_counter(
        "slasher_attester_max_target_gaps",
        "Total null records written in update_attester_max_target gap-fill",
    )
});
