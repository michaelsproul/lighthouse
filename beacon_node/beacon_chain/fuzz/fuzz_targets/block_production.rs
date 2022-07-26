#![no_main]

use beacon_chain::test_utils::test_spec;
use beacon_chain_fuzz::{Config, Runner, TestHarness};
use libfuzzer_sys::fuzz_target;
use tokio::runtime::Runtime;
use types::{Keypair, MinimalEthSpec};

type E = MinimalEthSpec;

fn get_harness(keypairs: &[Keypair]) -> TestHarness<E> {
    let spec = test_spec::<E>();

    let harness = TestHarness::builder(MinimalEthSpec)
        .spec(spec)
        .keypairs(keypairs.to_vec())
        .fresh_ephemeral_store()
        .mock_execution_layer()
        .build();
    harness
}

fuzz_target!(|data: &[u8]| {
    let config = Config::default();
    let rt = Runtime::new().unwrap();

    let mut runner = Runner::new(data, config, get_harness);

    if let Err(arbitrary::Error::EmptyChoose | arbitrary::Error::IncorrectFormat) =
        rt.block_on(async move { runner.run().await })
    {
        panic!("bad arbitrary usage");
    }
});
