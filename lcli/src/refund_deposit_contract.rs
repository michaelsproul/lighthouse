use clap::ArgMatches;
use environment::Environment;
use types::EthSpec;
use web3::{
    transports::Http,
    types::{Address, TransactionRequest, U256},
    Web3,
};

/// `keccak("steal()")[0..4]`
pub const STEAL_FN_SIGNATURE: &[u8] = &[0xcf, 0x7a, 0x89, 0x65];

pub fn run<T: EthSpec>(mut env: Environment<T>, matches: &ArgMatches<'_>) -> Result<(), String> {
    let eth1_http_path: String = clap_utils::parse_required(matches, "eth1-http")?;
    let from: Address = clap_utils::parse_required(matches, "from-address")?;
    let contract_address: Address = clap_utils::parse_required(matches, "contract-address")?;

    let transport = Http::new(&eth1_http_path)
        .map_err(|e| format!("Unable to connect to eth1 HTTP RPC: {:?}", e))?;
    let web3 = Web3::new(transport);

    env.runtime().block_on(async {
        let _ = web3
            .eth()
            .send_transaction(TransactionRequest {
                from,
                to: Some(contract_address),
                gas: Some(U256::from(400_000)),
                gas_price: None,
                value: Some(U256::zero()),
                data: Some(STEAL_FN_SIGNATURE.into()),
                nonce: None,
                condition: None,
            })
            .await
            .map_err(|e| format!("Failed to call steal fn: {:?}", e))?;

        Ok(())
    })
}
