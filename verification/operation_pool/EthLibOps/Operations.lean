module

public import EthLibOps.ProposerSlashings
public import EthLib.Consensus.Proofs.Transition.Operations

/-!
# The slashings and the exits of a block, as one step

`process_operations` runs each list of a block body in turn. `processSlashingsAndExits` is the
part of it that this project is about: the proposer slashings, then the attester slashings, then
the voluntary exits.

`processOperations_of_holdsOnly` says that the definition is not a new one. At Fulu, EthLib's
`processOperations` on a block body that holds those three lists and no other operation is
`processSlashingsAndExits` on them.
-/

@[expose] public section

namespace EthLibOps

open EthLib EthLib.Base EthLib.Consensus

variable {preset : Preset} {fork : Base.Fork} {config : Config} {checks : SignatureChecks}

/-- The slashings and the exits of a block body, in the spec's form. -/
structure SlashingsAndExits (preset : Preset) (fork : Base.Fork) where
  /-- `body.proposer_slashings`. -/
  proposerSlashings : List ProposerSlashing
  /-- `body.attester_slashings`. -/
  attesterSlashings : List (AttesterSlashing preset fork)
  /-- `body.voluntary_exits`. -/
  voluntaryExits : List SignedVoluntaryExit

/-- The part of `process_operations` that runs the slashings and the exits of a block body: each
proposer slashing through `process_proposer_slashing`, then each attester slashing through
`process_attester_slashing`, then each exit through `process_voluntary_exit`. -/
def processSlashingsAndExits (config : Config) (checks : SignatureChecks)
    (s : BeaconState preset fork) (ops : SlashingsAndExits preset fork) :
    Except TransitionError (BeaconState preset fork) := do
  let s ← ops.proposerSlashings.foldlM (processProposerSlashing config checks) s
  let s ← ops.attesterSlashings.foldlM (processAttesterSlashing config checks) s
  ops.voluntaryExits.foldlM (processVoluntaryExit config checks) s

/-- The block body holds the slashings and the exits `ops`, and no other operation: no
attestation, no deposit, no change of BLS credentials and no execution request. -/
structure HoldsOnly (body : BeaconBlockBody preset fork) (ops : SlashingsAndExits preset fork) :
    Prop where
  proposerSlashings : body.proposerSlashings.data.toList = ops.proposerSlashings
  attesterSlashings : body.attesterSlashings.data.toList = ops.attesterSlashings
  voluntaryExits : body.voluntaryExits.data.toList = ops.voluntaryExits
  attestations : body.attestations.data = #[]
  deposits : ∀ h, (body.deposits h).data = #[]
  blsToExecutionChanges : ∀ h, (body.blsToExecutionChanges h).data = #[]
  executionRequests : ∀ h, (body.executionRequests h).deposits.data = #[] ∧
    (body.executionRequests h).withdrawals.data = #[] ∧
    (body.executionRequests h).consolidations.data = #[]

/-- At Fulu, `process_operations` on a block body that holds the slashings and the exits `ops`
and no other operation is `processSlashingsAndExits` on `ops`. -/
theorem processOperations_of_holdsOnly (hfork : IsFulu fork) {body : BeaconBlockBody preset fork}
    {ops : SlashingsAndExits preset fork} (holds : HoldsOnly body ops)
    (s : BeaconState preset fork) (parentSlot : Slot) :
    processOperations config checks s body parentSlot =
      processSlashingsAndExits config checks s ops := by
  have gloas : ¬ .glamsterdam ≤ fork := Nat.not_le.mpr hfork.before
  have electra : .pectra ≤ fork := hfork.electra
  have capella : .shapella ≤ fork := Nat.le_trans (by decide) hfork.fulu
  have before : fork < .hegota := Nat.lt_trans hfork.before (by decide)
  obtain ⟨noDeposits, noWithdrawals, noConsolidations⟩ :=
    holds.executionRequests ⟨electra, hfork.before⟩
  simp only [processOperations, gloas, ↓reduceDIte, expectedDepositCount_of_fusaka hfork.fulu,
    holds.deposits before, holds.attestations, holds.blsToExecutionChanges capella,
    processBlsToExecutionChanges, capella, processExecutionRequests, electra, noDeposits,
    noWithdrawals, noConsolidations, processSlashingsAndExits, ← Array.foldlM_toList,
    holds.proposerSlashings, holds.attesterSlashings, holds.voluntaryExits, List.size_toArray,
    List.length_nil]
  -- The folds over the empty lists return the state, and the deposit count is the expected zero.
  simp only [Array.foldlM_empty, ok_bind, ensure_of trivial, pure_bind, bind_pure]

end EthLibOps
