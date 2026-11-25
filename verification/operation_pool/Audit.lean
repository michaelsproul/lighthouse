import OperationPool
import StateProcessing

/-!
# Axiom audit

`lake build Audit` prints the axioms each main theorem depends on. Expected:

* Lean's `propext`, `Classical.choice` and `Quot.sound`;
* opaque declarations of `ExternalTypes.lean`, `TypesExternal.lean` and `FunsExternal.lean`.
  These are external Rust types, and hashing functions that the extracted code never calls.
  They appear because the extracted signatures mention them. Each is an uninterpreted symbol:
  it asserts that a type or a function exists, and nothing about it.

`sorryAx` must not appear.
-/

#print axioms OperationPool.packed_slashings_and_exits_valid
#print axioms OperationPool.packed_operations_valid
#print axioms OperationPool.packed_slashings_and_exits_valid_of_history
#print axioms OperationPool.poolsValid_of_inv
#print axioms OperationPool.PoolInv.empty
#print axioms OperationPool.PoolInv.step
#print axioms OperationPool.PoolInv.steps
#print axioms OperationPool.PoolStep.advance_of_run
#print axioms operation_pool.validator_map.keyed_new
#print axioms operation_pool.validator_map.insert_ok
#print axioms operation_pool.validator_map.Keyed.insert
#print axioms operation_pool.validator_map.Keyed.retain
#print axioms operation_pool.validator_map.Keyed.values_pairwise
#print axioms EthLibOps.extends_of_run
#print axioms EthLibOps.ProposerSlashingSigned.of_extends
#print axioms EthLibOps.ExitSigned.of_extends
#print axioms EthLibOps.AttesterSlashingChecks.of_extends
#print axioms EthLibOps.processOperations_of_holdsOnly
#print axioms OperationPool.Bridge.proposerSlashingWellFormed_of_headersOk
#print axioms OperationPool.yields_list
#print axioms OperationPool.proposerSlashingFilter_spec
#print axioms OperationPool.voluntaryExitFilter_spec
#print axioms OperationPool.attesterCovers_spec
#print axioms operation_pool.yields_slice
#print axioms operation_pool.AttesterCover.model
#print axioms operation_pool.exclusionSet_ok
#print axioms EthLibOps.StateRoom.of_bounds
#print axioms EthLibOps.OperationsRoom.exits_of_timeBound
#print axioms EthLibOps.OperationsRoom.rewards_of_supplyCap
#print axioms EthLibOps.operationsRoom_mainnet
#print axioms EthLibOps.operationsRoom_minimal
#print axioms EthLibOps.balancesMatch_of_reachable
#print axioms OperationPool.packed_attester_slashings_valid
#print axioms operation_pool.maximum_cover_spec
#print axioms OperationPool.packed_proposer_slashings_valid
#print axioms OperationPool.packed_exits_valid_after_slashings
#print axioms OperationPool.packed_exits_valid
#print axioms OperationPool.packed_exits_valid_direct
#print axioms operation_pool.exclusionSet_covers
#print axioms OperationPool.Bridge.exitGates_of_coresOk
#print axioms OperationPool.Bridge.proposerSlashingGates_of_coreOk
#print axioms OperationPool.Bridge.isSlashableValidator_of_slashableOn
#print axioms EthLibOps.attesterSlashings_foldlM_ok
#print axioms EthLibOps.processAttesterSlashing_ok_of_frame
#print axioms EthLibOps.slashCandidates_ok
#print axioms EthLibOps.voluntaryExits_foldlM_ok
#print axioms EthLibOps.processVoluntaryExit_ok_of_frame
#print axioms EthLibOps.proposerSlashings_foldlM_ok
#print axioms EthLibOps.processProposerSlashing_ok_of_frame
#print axioms EthLibOps.slashValidator_ok_frame
#print axioms EthLibOps.initiateValidatorExit_ok_of_room
#print axioms operation_pool.filter_limit_operations_eq
#print axioms operation_pool.insert_proposer_slashing_indices_eq
#print axioms operation_pool.insert_common_attesting_indices_eq
#print axioms state_processing.verify_exit_validator_checks_ok_iff
#print axioms state_processing.verify_exit_pending_balance_ok_iff
#print axioms state_processing.verify_proposer_slashing_validator_checks_ok_iff
