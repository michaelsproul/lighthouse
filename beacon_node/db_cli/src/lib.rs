mod cli;

pub use cli::cli_app;

use clap::ArgMatches;
use directory::{get_data_dir, DEFAULT_BEACON_NODE_DIR};
use slog::Logger;
use std::path::PathBuf;
use store::{HotColdDB, StoreConfig};
use types::{ChainSpec, EthSpec, Hash256};

pub fn run<E: EthSpec>(cli_args: &ArgMatches, spec: &ChainSpec, log: Logger) -> Result<(), String> {
    let datadir = get_data_dir(cli_args);

    let mut config = StoreConfig::default();

    let chain_db_path = datadir.join(DEFAULT_BEACON_NODE_DIR).join("chain_db");

    let freezer_db_path = if let Some(freezer_dir) = cli_args.value_of("freezer-dir") {
        PathBuf::from(freezer_dir)
    } else {
        datadir.join(DEFAULT_BEACON_NODE_DIR).join("freezer_db")
    };

    config.slots_per_restore_point =
        if let Some(slots_per_restore_point) = cli_args.value_of("slots-per-restore-point") {
            slots_per_restore_point
                .parse()
                .map_err(|_| "slots-per-restore-point is not a valid integer".to_string())?
        } else {
            std::cmp::min(
                E::slots_per_historical_root() as u64,
                store::config::DEFAULT_SLOTS_PER_RESTORE_POINT,
            )
        };

    let store =
        HotColdDB::<E, _, _>::open(&chain_db_path, &freezer_db_path, config, spec.clone(), log)
            .map_err(|e| format!("Unable to open database: {:?}", e))?;

    match matches.subcommand() {
        ("delete-from-fork-choice", Some(matches)) => {
            let block_root: Hash256 = clap_utils::parse_required(&matches, "block-root")
                .map_err(|_| "Block root argument is required")?;
            println!("Deleting this block root: {:?}", block_root);
        }
        _ => return Err("No subcommand supplied".to_string()),
    }

    Ok(())
}
