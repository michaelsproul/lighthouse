#![no_main]
use libfuzzer_sys::fuzz_target;
use slashing_protection::{
    interchange_test::{check_minification_invariants, MultiTestCase},
    SigningRoot, SlashingDatabase, SUPPORTED_INTERCHANGE_FORMAT_VERSION,
};
use tempfile::tempdir;
use types::{Epoch, Slot};

/// Test runner that ignores the details of whether attestations should be signed and just asserts
/// that no unexpected errors occur.
trait RunNoErrors {
    fn run_without_errors(&self, minify: bool);
}

impl RunNoErrors for MultiTestCase {
    fn run_without_errors(&self, minify: bool) {
        let dir = tempdir().unwrap();
        let slashing_db_file = dir.path().join("slashing_protection.sqlite");
        let slashing_db = SlashingDatabase::create(&slashing_db_file).unwrap();

        for test_case in &self.steps {
            let interchange = if minify {
                let minified = test_case.interchange.minify().unwrap();
                check_minification_invariants(&test_case.interchange, &minified);
                minified
            } else {
                test_case.interchange.clone()
            };

            match slashing_db.import_interchange_info(interchange, self.genesis_validators_root) {
                Ok(import_outcomes) => {
                    let none_failed = import_outcomes.iter().all(|o| !o.failed());
                    assert!(
                        none_failed,
                        "test `{}` failed to import some records: {:#?}",
                        self.name, import_outcomes
                    );
                }
                Err(e) => {
                    panic!("import should never error: {:?}", e);
                }
            }

            for block in &test_case.blocks {
                let _ = slashing_db.check_and_insert_block_signing_root(
                    &block.pubkey,
                    block.slot,
                    SigningRoot::from(block.signing_root),
                );
            }

            for att in &test_case.attestations {
                let _ = slashing_db.check_and_insert_attestation_signing_root(
                    &att.pubkey,
                    att.source_epoch,
                    att.target_epoch,
                    SigningRoot::from(att.signing_root),
                );
            }
        }
    }
}

fuzz_target!(|test_case: MultiTestCase| {
    // Skip test cases with excessively high epochs and slots.
    let max_slot = Slot::new(i64::MAX as u64);
    let max_epoch = Epoch::new(i64::MAX as u64);
    for step in &test_case.steps {
        for validator_data in &step.interchange.data {
            for block in &validator_data.signed_blocks {
                if block.slot > max_slot {
                    return;
                }
            }
            for attestation in &validator_data.signed_attestations {
                if attestation.source_epoch > max_epoch || attestation.target_epoch > max_epoch {
                    return;
                }
            }
        }
    }

    // Munge.
    let mut test_case = test_case;
    let gvr = test_case.genesis_validators_root;
    for step in &mut test_case.steps {
        step.interchange.metadata.interchange_format_version = SUPPORTED_INTERCHANGE_FORMAT_VERSION;
        step.interchange.metadata.genesis_validators_root = gvr;
    }

    let minify = false;
    test_case.run_without_errors(minify);
});
