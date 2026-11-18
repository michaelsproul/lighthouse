import OperationPool
import StateProcessing

/-!
# Axiom audit

`./lake.sh build Audit` prints the axioms each main theorem depends on. Expected:

* Lean's `propext`, `Classical.choice` and `Quot.sound`;
* opaque declarations of `ExternalTypes.lean`, `TypesExternal.lean` and `FunsExternal.lean`.
  These are external Rust types, and hashing functions that the extracted code never calls.
  They appear because the extracted signatures mention them. Each is an uninterpreted symbol:
  it asserts that a type or a function exists, and nothing about it.

`sorryAx` must not appear.
-/

#print axioms OperationPool.packed_slashings_and_exits_valid
#print axioms operation_pool.yields_slice
#print axioms operation_pool.AttesterCover.model
#print axioms operation_pool.exclusionSet_ok
#print axioms EthCLSpecs.Proofs.Fulu.OperationsRoom.exits_of_timeBound
#print axioms EthCLSpecs.Proofs.Fulu.OperationsRoom.rewards_of_supplyCap
#print axioms EthCLSpecs.Proofs.Fulu.configValidMainnet
#print axioms EthCLSpecs.Proofs.Fulu.ReachableState.simpleStateInvariants
#print axioms OperationPool.packed_attester_slashings_valid_uniform
#print axioms OperationPool.packed_attester_slashings_valid
#print axioms operation_pool.maximum_cover_spec
#print axioms OperationPool.packed_proposer_slashings_valid_uniform
#print axioms OperationPool.packed_proposer_slashings_valid
#print axioms OperationPool.packed_exits_valid_after_slashings_uniform
#print axioms OperationPool.packed_exits_valid_after_slashings
#print axioms OperationPool.packed_exits_valid
#print axioms OperationPool.packed_exits_valid_direct
#print axioms operation_pool.exclusionSet_covers
#print axioms OperationPool.Bridge.exitGates_of_coresOk
#print axioms OperationPool.Bridge.proposerSlashingGates_of_coreOk
#print axioms OperationPool.Bridge.isSlashableValidator_of_slashableOn
#print axioms EthCLLib.Proofs.mem_qsort
#print axioms EthCLSpecs.Proofs.Fulu.attesterSlashings_succeed
#print axioms EthCLSpecs.Proofs.Fulu.processAttesterSlashing_succeeds
#print axioms EthCLSpecs.Proofs.Fulu.exitView_of_slashings
#print axioms EthCLSpecs.Proofs.Fulu.processVoluntaryExit_succeeds
#print axioms EthCLSpecs.Proofs.Fulu.proposerSlashings_succeed
#print axioms EthCLSpecs.Proofs.Fulu.processProposerSlashing_succeeds
#print axioms EthCLSpecs.Proofs.Fulu.slashValidator_succeeds
#print axioms operation_pool.filter_limit_operations_eq
#print axioms operation_pool.insert_proposer_slashing_indices_eq
#print axioms operation_pool.insert_common_attesting_indices_eq
#print axioms state_processing.verify_exit_validator_checks_ok_iff
#print axioms state_processing.verify_exit_pending_balance_ok_iff
#print axioms state_processing.verify_proposer_slashing_validator_checks_ok_iff
#print axioms EthCLSpecs.Proofs.Fulu.voluntaryExits_succeed_of_exitView
