module

public import EthLibOps.AttesterSlashings
public import EthLibOps.VoluntaryExits
public import EthLib.Consensus.Proofs.Transition.ChainRoots
public import EthLib.Consensus.Proofs.Transition.AssignedProposers

/-!
# Bounds a real state meets, and the room they give

The theorems on the slashings and the exits of a block take the room of the packing state as
hypotheses. This file reduces that room to three facts.

* `TimeBound electra s`: the slot and the earliest exit epoch of `s` are below `2 ^ 63`.
* `SupplyCap cap s`: the balances, the effective balances and the slashed balance of the current
  epoch are each at most `cap` Gwei.
* `OperationsRoom preset config fork cap nP nA nE`: an arithmetic fact about the constants. A
  block with `nP` proposer slashings, `nA` attester slashings and `nE` voluntary exits fits, on a
  state within the two bounds above.

`StateRoom.of_bounds`, `OperationsRoom.exits_of_timeBound` and
`OperationsRoom.rewards_of_supplyCap` are the room. `operationsRoom_mainnet` and
`operationsRoom_minimal` check the constants of the two presets at Fulu, for the largest block
and a cap of `2 ^ 58` Gwei.

`balancesMatch_of_reachable` restates EthLib's `reachable_balancesMatch`: a state that a run of
slots and blocks reaches from a genesis state has one balance for each validator.

The file also imports EthLib's `Run.getBeaconProposerIndex_ok` for the main theorem: from Fulu,
such a state has a proposer for its slot, and the proposer is a validator of the registry. Its
premise on the preset is the field `slots` of `OperationsRoom`.
-/

@[expose] public section

namespace EthLibOps

open EthLib EthLib.Base EthLib.Base.Uint64 EthLib.Consensus

variable {preset : Preset} {fork : Base.Fork} {config : Config} {checks : SignatureChecks}

/-- The two clocks of the state are below `2 ^ 63`: the slot, and the earliest epoch the next
exit can take. -/
structure TimeBound (electra : .pectra ≤ fork) (s : BeaconState preset fork) : Prop where
  /-- The slot is below `2 ^ 63`. -/
  slot : s.slot.toNat < 2 ^ 63
  /-- The exit queue has not reached epoch `2 ^ 63`. -/
  earliestExitEpoch : (s.earliestExitEpoch electra).toNat < 2 ^ 63

/-- The state holds `cap` Gwei at most. -/
structure SupplyCap (cap : Nat) (s : BeaconState preset fork) : Prop where
  /-- The balances add up to `cap` Gwei at most. -/
  balances : (s.balances.data.toList.map UInt64.toNat).sum ≤ cap
  /-- The effective balances add up to `cap` Gwei at most. -/
  effectiveBalances : effectiveBalanceSum s ≤ cap
  /-- The slashed balance recorded for the current epoch is `cap` Gwei at most. -/
  slashed : slashedNow s ≤ cap

/-- The constants leave room for a block of `nP` proposer slashings, `nA` attester slashings and
`nE` voluntary exits, on a state that holds `cap` Gwei at most and whose clocks are below
`2 ^ 63`. -/
structure OperationsRoom (preset : Preset) (config : Config) (fork : Base.Fork)
    (cap nP nA nE : Nat) : Prop where
  /-- The constants of the exit churn. -/
  churn : ChurnOk preset config
  /-- An epoch has at least one slot. -/
  slots : preset.SLOTS_PER_EPOCH ≠ 0
  /-- The slashings vector has a nonzero length. -/
  vector : preset.EPOCHS_PER_SLASHINGS_VECTOR ≠ 0
  /-- The length of the slashings vector is at most `2 ^ 63`. -/
  window : preset.EPOCHS_PER_SLASHINGS_VECTOR.toNat ≤ 2 ^ 63
  /-- Twice the cap fits in 64 bits. -/
  doubled : 2 * cap < 2 ^ 64
  /-- The cap plus the largest churn of an epoch fits in 64 bits. -/
  churnRoom : cap + config.MAX_PER_EPOCH_ACTIVATION_EXIT_CHURN_LIMIT.toNat ≤ 2 ^ 64
  /-- The largest whistleblower reward times the proposer weight fits in 64 bits. -/
  weight : rewardMax preset fork cap * PROPOSER_WEIGHT.toNat < 2 ^ 64
  /-- The exit queue has room: one exit for each proposer slashing, for each validator an attester
  slashing can name, and for each voluntary exit. -/
  exits : 2 ^ 63 + preset.MAX_SEED_LOOKAHEAD.toNat +
    (nP + nA * maxAttesters preset fork + nE) * exitStep preset cap +
    config.MIN_VALIDATOR_WITHDRAWABILITY_DELAY.toNat < 2 ^ 64
  /-- A balance has room for one whistleblower reward for each slashed validator, and for one
  more effective balance. -/
  rewards : cap + (nP + nA * maxAttesters preset fork) * rewardMax preset fork cap + cap < 2 ^ 64

/-- Room for a block is room for a smaller block. -/
theorem OperationsRoom.mono {cap nP nA nE nP' nA' nE' : Nat}
    (h : OperationsRoom preset config fork cap nP nA nE) (hP : nP' ≤ nP) (hA : nA' ≤ nA)
    (hE : nE' ≤ nE) : OperationsRoom preset config fork cap nP' nA' nE' := by
  have count : nP' + nA' * maxAttesters preset fork + nE' ≤
      nP + nA * maxAttesters preset fork + nE := by
    have := Nat.mul_le_mul_right (maxAttesters preset fork) hA
    omega
  have slashed : nP' + nA' * maxAttesters preset fork ≤ nP + nA * maxAttesters preset fork := by
    have := Nat.mul_le_mul_right (maxAttesters preset fork) hA
    omega
  have exits := Nat.mul_le_mul_right (exitStep preset cap) count
  have rewards := Nat.mul_le_mul_right (rewardMax preset fork cap) slashed
  exact ⟨h.churn, h.slots, h.vector, h.window, h.doubled, h.churnRoom, h.weight,
    Nat.lt_of_le_of_lt (by omega) h.exits, Nat.lt_of_le_of_lt (by omega) h.rewards⟩

variable {electra : .pectra ≤ fork} {cap nP nA nE : Nat} {s : BeaconState preset fork}
  {p : ValidatorIndex}

/-- The current epoch is at most the slot. -/
theorem getCurrentEpoch_toNat_le (s : BeaconState preset fork) :
    (getCurrentEpoch s).toNat ≤ s.slot.toNat := by
  simp only [getCurrentEpoch, computeEpochAtSlot, UInt64.toNat_div]
  exact Nat.div_le_self _ _

/-- Within the time bound, the next exit starts from an epoch below `2 ^ 63` plus the seed
lookahead. -/
theorem TimeBound.exitStart_le (h : TimeBound electra s) :
    exitStart electra s ≤ 2 ^ 63 + preset.MAX_SEED_LOOKAHEAD.toNat := by
  have := h.slot
  have := h.earliestExitEpoch
  have := getCurrentEpoch_toNat_le s
  unfold exitStart
  omega

/-- Under the supply cap, each balance is at most the cap. -/
theorem SupplyCap.balance_le (h : SupplyCap cap s) {i : ValidatorIndex} {b : Gwei}
    (found : getBalance s i = .ok b) : b.toNat ≤ cap := by
  have entry : s.balances.data.toList[i.toNat]? = some b := by
    simpa using getBalance_eq_ok_iff.mp found
  exact Nat.le_trans (le_sum_map_of_getElem? UInt64.toNat entry) h.balances

/-- Under the supply cap, each effective balance is at most the cap. -/
theorem SupplyCap.effectiveBalance_le (h : SupplyCap cap s) :
    ∀ v ∈ s.validators.data, v.effectiveBalance.toNat ≤ cap := by
  intro v hv
  obtain ⟨i, inside, rfl⟩ := Array.mem_iff_getElem.mp hv
  have entry : s.validators.data.toList[i]? = some s.validators.data[i] := by simp [inside]
  exact Nat.le_trans
    (le_sum_map_of_getElem? (fun v : Validator => v.effectiveBalance.toNat) entry)
    h.effectiveBalances

/-- A state within the two bounds, with matching balances and a proposer in the registry, has
the room a slashing reads. -/
theorem StateRoom.of_bounds (room : OperationsRoom preset config fork cap nP nA nE)
    (sizes : BalancesMatch s) (proposer : getBeaconProposerIndex s = .ok p)
    (proposerIn : p.toNat < s.validators.data.size) (time : TimeBound electra s)
    (supply : SupplyCap cap s) : StateRoom config cap p s where
  summed := Nat.lt_of_le_of_lt supply.effectiveBalances (by have := room.doubled; omega)
  capped := supply.effectiveBalance_le
  churnRoom := room.churnRoom
  sizes := sizes
  proposer := proposer
  proposerIn := proposerIn
  vector := room.vector
  window := by
    have := getCurrentEpoch_toNat_le s
    have := time.slot
    have := room.window
    omega
  slashedRoom := by
    have := unslashedSum_le s
    have := supply.effectiveBalances
    have := supply.slashed
    have := room.doubled
    omega
  weight := room.weight

/-- Within the time bound, the exit queue has room for the exits of the block. -/
theorem OperationsRoom.exits_of_timeBound (room : OperationsRoom preset config fork cap nP nA nE)
    (time : TimeBound electra s) :
    exitStart electra s + (nP + nA * maxAttesters preset fork + nE) * exitStep preset cap +
      config.MIN_VALIDATOR_WITHDRAWABILITY_DELAY.toNat < 2 ^ 64 := by
  have := room.exits
  have := time.exitStart_le
  omega

/-- Under the supply cap, a balance has room for the whistleblower rewards of the block and for
one more effective balance. -/
theorem OperationsRoom.rewards_of_supplyCap
    (room : OperationsRoom preset config fork cap nP nA nE) (supply : SupplyCap cap s)
    {i : ValidatorIndex} {b : Gwei} (found : getBalance s i = .ok b) :
    b.toNat + (nP + nA * maxAttesters preset fork) * rewardMax preset fork cap + cap < 2 ^ 64 := by
  have := room.rewards
  have := supply.balance_le found
  omega

/-- A state that a run of slots and blocks reaches from a genesis state has one balance for each
validator. This is EthLib's `reachable_balancesMatch`. -/
theorem balancesMatch_of_reachable {g : ForkedBeaconState preset}
    (run : Run config checks g ⟨fork, s⟩) (genesis : Genesis config checks g) :
    BalancesMatch s :=
  reachable_balancesMatch run genesis

/-- The mainnet constants at Fulu leave room for the largest block, on a state that holds
`2 ^ 58` Gwei at most. -/
theorem operationsRoom_mainnet :
    OperationsRoom Preset.mainnet Config.mainnet .fusaka (2 ^ 58)
      Preset.mainnet.MAX_PROPOSER_SLASHINGS.toNat
      (parameters Preset.mainnet .fusaka).MAX_ATTESTER_SLASHINGS.toNat
      Preset.mainnet.MAX_VOLUNTARY_EXITS.toNat := by
  refine ⟨⟨?_, ?_, ?_⟩, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩ <;> decide

/-- The minimal constants at Fulu leave room for the largest block, on a state that holds
`2 ^ 58` Gwei at most. -/
theorem operationsRoom_minimal :
    OperationsRoom Preset.minimal Config.minimal .fusaka (2 ^ 58)
      Preset.minimal.MAX_PROPOSER_SLASHINGS.toNat
      (parameters Preset.minimal .fusaka).MAX_ATTESTER_SLASHINGS.toNat
      Preset.minimal.MAX_VOLUNTARY_EXITS.toNat := by
  refine ⟨⟨?_, ?_, ?_⟩, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩ <;> decide

end EthLibOps
