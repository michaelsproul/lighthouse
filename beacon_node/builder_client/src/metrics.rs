pub use metrics::*;
use std::sync::LazyLock;

pub static SIGNED_BUILDER_BID_DECODE_TIME: LazyLock<Result<Histogram>> = LazyLock::new(|| {
    try_create_histogram(
        "builder_client_signed_bid_decode",
        "Decoding time for SignedBuilderBid",
    )
});

pub static SIGNED_BUILDER_BID_OVERHEAD_TIME: LazyLock<Result<Histogram>> = LazyLock::new(|| {
    try_create_histogram(
        "builder_client_signed_bid_overhead",
        "Time for SignedBuilderBid processing after receiving a response",
    )
});
