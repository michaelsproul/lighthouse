#![no_main]

use beacon_chain::test_utils::test_spec;
use beacon_chain_fuzz::{Config, LogConfig, LogInterceptor, Runner, TestHarness};
use libfuzzer_sys::fuzz_target;
use tokio::runtime::Runtime;
use types::{Keypair, MinimalEthSpec};

type E = MinimalEthSpec;

fn get_harness(id: String, log_config: LogConfig, keypairs: &[Keypair]) -> TestHarness<E> {
    let spec = test_spec::<E>();

    let log = LogInterceptor::new(id, log_config).into_logger();

    let harness = TestHarness::builder(MinimalEthSpec)
        .spec(spec)
        .logger(log)
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

    match rt.block_on(async move { runner.run().await }) {
        Ok(()) => (),
        Err(arbitrary::Error::NotEnoughData) => {
            println!("aborted run due to lack of entropy");
        }
        Err(_) => {
            panic!("bad arbitrary usage");
        }
    }
});
