module

public import EthLibOps.Bounds
public import EthLib.Consensus.Proofs.Transition.ValidatorKeys
public import EthLib.Consensus.Proofs.Transition.ForkVersions
public import EthLib.Consensus.Proofs.DirectSafety.Signatures

/-!
# The signature checks of an operation hold on later states

An operation of the pool is checked on one state and packed on a later one. The checks of this
file read three things of the state: the public keys of the validators the operation names, the
fork versions, and the genesis validators root.

`Extends s s'` says that `s'` keeps those three things of `s`: the public keys of `s` are a
prefix of the public keys of `s'`, and the two states have the same fork versions and the same
genesis validators root.

* `extends_of_run`: a run of slots and blocks from `s` to `s'` gives `Extends s s'`, when the
  two states are at one fork and a run from a genesis state reaches `s`.
* `ProposerSlashingSigned.of_extends`, `ExitSigned.of_extends` and
  `AttesterSlashingChecks.of_extends`: a check that passes on `s` passes on a state that extends
  `s`.

`Extends` holds in one direction only. A state with a shorter registry than `s` does not extend
it, and an operation that names a validator past the end of that registry fails there.
-/

@[expose] public section

namespace EthLibOps

open EthLib EthLib.Base EthLib.Consensus

variable {preset : Preset} {fork : Base.Fork} {config : Config} {checks : SignatureChecks}

/-- The state `s'` keeps what the signature checks read of `s`. -/
structure Extends (s s' : BeaconState preset fork) : Prop where
  /-- The public keys of `s` are a prefix of the public keys of `s'`. -/
  keys : validatorKeys s <+: validatorKeys s'
  /-- The two states have the same fork versions. -/
  forkVersions : s'.forkVersions = s.forkVersions
  /-- The two states have the same genesis validators root. -/
  genesisValidatorsRoot : s'.genesisValidatorsRoot = s.genesisValidatorsRoot

variable {s s' s'' : BeaconState preset fork}

/-- A state extends itself. -/
theorem Extends.refl (s : BeaconState preset fork) : Extends s s :=
  ⟨List.prefix_refl _, rfl, rfl⟩

/-- `Extends` composes. -/
theorem Extends.trans (first : Extends s s') (second : Extends s' s'') : Extends s s'' :=
  ⟨first.keys.trans second.keys, second.forkVersions.trans first.forkVersions,
    second.genesisValidatorsRoot.trans first.genesisValidatorsRoot⟩

/-- A run of slots and blocks from `s` reaches only states that extend `s`, among the states of
the fork of `s`. A run from a genesis state reaches `s`.

A step of a run never overwrites a public key, and never writes the genesis validators root. Two
states of one fork that runs from one genesis state reach have the same fork versions. -/
theorem extends_of_run {g : ForkedBeaconState preset} (genesis : Genesis config checks g)
    (reached : Run config checks g ⟨fork, s⟩) (run : Run config checks ⟨fork, s⟩ ⟨fork, s'⟩) :
    Extends s s' := by
  obtain ⟨hash, time, deposits, initialized⟩ := genesis
  refine ⟨(validatorKeys_prefix_and_distinct_of_chainSteps run.chainSteps).1, ?_, ?_⟩
  · exact forkVersions_eq_of_fork initialized (reached.append run).chainSteps reached.chainSteps
      rfl
  · exact run.genesisValidatorsRoot

/-- A validator of `s` is at the same index of a state that extends `s`, with the same public
key. -/
theorem Extends.getValidator (h : Extends s s') {i : ValidatorIndex} {v : Validator}
    (found : getValidator s i = .ok v) :
    ∃ v', getValidator s' i = .ok v' ∧ v'.pubkey = v.pubkey := by
  obtain ⟨v', found', key⟩ :=
    validators_getElem?_of_validatorKeys_prefix h.keys (getValidator_eq_ok_iff.mp found)
  exact ⟨v', getValidator_eq_ok_iff.mpr found', key⟩

/-- The registry of a state that extends `s` is at least as long as the registry of `s`. -/
theorem Extends.size_le (h : Extends s s') :
    s.validators.data.size ≤ s'.validators.data.size := by
  simpa [validatorKeys] using h.keys.length_le

/-- A proposer slashing that is signed on `s` is signed on a state that extends `s`. -/
theorem ProposerSlashingSigned.of_extends (h : Extends s s') {ps : ProposerSlashing}
    (signed : ProposerSlashingSigned config checks s ps) :
    ProposerSlashingSigned config checks s' ps := by
  obtain ⟨v, found, first, second⟩ := signed
  obtain ⟨v', found', key⟩ := h.getValidator found
  refine ⟨v', found', ?_, ?_⟩
  · rw [key, checkHeaderSignature_congr h.forkVersions h.genesisValidatorsRoot]
    exact first
  · rw [key, checkHeaderSignature_congr h.forkVersions h.genesisValidatorsRoot]
    exact second

/-- A voluntary exit that is signed on `s` is signed on a state that extends `s`. -/
theorem ExitSigned.of_extends (h : Extends s s') {sve : SignedVoluntaryExit}
    (signed : ExitSigned config checks s sve) : ExitSigned config checks s' sve := by
  obtain ⟨v, domain, root, found, domainOk, rootOk, verified⟩ := signed
  obtain ⟨v', found', key⟩ := h.getValidator found
  refine ⟨v', domain, root, found', ?_, rootOk, ?_⟩
  · rw [voluntaryExitDomain_congr h.forkVersions h.genesisValidatorsRoot]
    exact domainOk
  · rw [key]
    exact verified

/-- An attester slashing that passes the checks before the walk on `s` passes them on a state
that extends `s`.

Each attestation names validators of the registry of `s`, and the registry of the later state
holds them at the same indices with the same public keys. -/
theorem AttesterSlashingChecks.of_extends (h : Extends s s')
    {a : AttesterSlashing preset fork} (passes : AttesterSlashingChecks config checks s a) :
    AttesterSlashingChecks config checks s' a := by
  have size := h.size_le
  refine ⟨passes.slashableData, ?_, ?_⟩
  · exact isValidIndexedAttestation_of_keys passes.valid1 h.keys (List.prefix_refl _)
      h.forkVersions h.genesisValidatorsRoot fun index member =>
        Nat.lt_of_lt_of_le (isValidIndexedAttestation_lt_size passes.valid1 index member) size
  · exact isValidIndexedAttestation_of_keys passes.valid2 h.keys (List.prefix_refl _)
      h.forkVersions h.genesisValidatorsRoot fun index member =>
        Nat.lt_of_lt_of_le (isValidIndexedAttestation_lt_size passes.valid2 index member) size

end EthLibOps
