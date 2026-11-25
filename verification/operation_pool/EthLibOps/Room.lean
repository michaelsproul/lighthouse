module

public import EthLibOps.SlashValidator

/-!
# The room a block's slashings and exits need

The operations of a block run one after the other. Each one is checked on the state the block is
packed on, here `s`, and runs on a later state `w`. `SlashFrame` relates the two.

Two structures name what the operations read on `s` that no operation changes:

* `QueueRoom config B s`: every effective balance is at most `B`, and the exit churn can be
  computed. This is what an exit needs.
* `StateRoom config B p s`: on top of that, `p` is the proposer of the slot, and the slashings
  vector and the proposer weight have room. This is what a slashing needs.

Two quantities move with each operation: the exit queue and the proposer's balance. The theorems
take their room as separate hypotheses, with `exitStep preset B` epochs and
`rewardMax preset fork B` Gwei for each validator that exits or is slashed.

`slashValidator_ok_of_frame` and `initiateValidatorExit_ok_of_frame` are the single steps on a
state `w` that has the frame of `s`.
-/

@[expose] public section

namespace EthLibOps

open EthLib EthLib.Base EthLib.Base.Uint64 EthLib.Consensus

variable {preset : Preset} {fork : Base.Fork} {config : Config}

/-- The largest whistleblower reward for a validator of effective balance `B` at most. -/
def rewardMax (preset : Preset) (fork : Base.Fork) (B : Nat) : Nat :=
  B / (parameters preset fork).WHISTLEBLOWER_REWARD_QUOTIENT.toNat

/-- What an exit reads on `s` besides the exit queue: the effective balances sum below `2 ^ 64`,
each is at most `B`, and `B` plus the largest churn of an epoch fits in 64 bits. -/
structure QueueRoom (config : Config) (B : Nat) (s : BeaconState preset fork) : Prop where
  /-- The effective balances sum below `2 ^ 64`, so the total active balance can be computed. -/
  summed : effectiveBalanceSum s < 2 ^ 64
  /-- Every effective balance is at most `B`. -/
  capped : ∀ v ∈ s.validators.data, v.effectiveBalance.toNat ≤ B
  /-- `B` plus the largest churn of an epoch fits in 64 bits. -/
  churnRoom : B + config.MAX_PER_EPOCH_ACTIVATION_EXIT_CHURN_LIMIT.toNat ≤ 2 ^ 64

/-- What a slashing reads on `s` besides the exit queue and the proposer's balance. -/
structure StateRoom (config : Config) (B : Nat) (p : ValidatorIndex)
    (s : BeaconState preset fork) : Prop extends QueueRoom config B s where
  /-- The balances have the registry's length. -/
  sizes : BalancesMatch s
  /-- `p` is the proposer of the slot. -/
  proposer : getBeaconProposerIndex s = .ok p
  /-- The proposer is in the registry. -/
  proposerIn : p.toNat < s.validators.data.size
  /-- The slashings vector has a nonzero length. -/
  vector : preset.EPOCHS_PER_SLASHINGS_VECTOR ≠ 0
  /-- The current epoch plus the length of the slashings vector fits in 64 bits. -/
  window : (getCurrentEpoch s).toNat + preset.EPOCHS_PER_SLASHINGS_VECTOR.toNat < 2 ^ 64
  /-- The slashed balance of the epoch plus the balances not yet slashed fits in 64 bits. -/
  slashedRoom : slashedNow s + unslashedSum s < 2 ^ 64
  /-- The largest whistleblower reward times the proposer weight fits in 64 bits. -/
  weight : rewardMax preset fork B * PROPOSER_WEIGHT.toNat < 2 ^ 64

variable {electra : .pectra ≤ fork} {S : ValidatorIndex → Prop} {E R B : Nat}
  {s w : BeaconState preset fork} {p : ValidatorIndex}

/-- A validator read on a state with the frame of `s` has an effective balance of `B` at most. -/
theorem QueueRoom.effectiveBalance_le (room : QueueRoom config B s)
    (frame : SlashFrame electra S E R s w) {i : ValidatorIndex} {y : Validator}
    (found : getValidator w i = .ok y) : y.effectiveBalance.toNat ≤ B := by
  have entry := getValidator_eq_ok_iff.mp found
  have inside : i.toNat < s.validators.data.size :=
    frame.size ▸ (Array.getElem?_eq_some_iff.mp entry).1
  have old : s.validators.data[i.toNat]? = some s.validators.data[i.toNat] :=
    Array.getElem?_eq_getElem inside
  rw [(frame.fixed _ _ _ old entry).2]
  exact room.capped _ (Array.getElem_mem inside)

/-- An exit succeeds on a state `w` that has the frame of `s`, when the exit queue of `s` has room
for the `E` epochs of the frame and one more exit. The state after it has the frame of `s`, with
the validator added to the set and one exit step added to the bound. -/
theorem initiateValidatorExit_ok_of_frame (ok : ChurnOk preset config)
    (before : fork < .glamsterdam) (room : QueueRoom config B s)
    (frame : SlashFrame electra S E R s w) {i : ValidatorIndex} {y : Validator}
    (found : getValidator w i = .ok y)
    (exits : exitStart electra s + E + exitStep preset B +
      config.MIN_VALIDATOR_WITHDRAWABILITY_DELAY.toNat < 2 ^ 64) :
    ∃ w', initiateValidatorExit config w i = .ok w' ∧
      SlashFrame electra (fun j => S j ∨ j = i) (E + exitStep preset B) R s w' := by
  have capped := room.effectiveBalance_le frame found
  have step := exitStep_mono (preset := preset) capped
  have start := frame.start
  have churn := room.churnRoom
  obtain ⟨w', computed, effect⟩ := initiateValidatorExit_ok_of_room ok electra before found
    (by rw [frame.effectiveBalanceSum_eq]; exact room.summed) (by omega) (by omega)
  exact ⟨w', computed, (frame.trans (effect.slashFrame found)).mono (fun _ h => h) (by omega)
    (by omega)⟩

/-- `slash_validator` succeeds on a state `w` that has the frame of `s`, for a validator that is
not yet slashed, when `s` has room for the bounds of the frame and one more slashing. The state
after it has the frame of `s`, with the validator added to the set, one exit step added to the
first bound and one reward added to the second.

`rewards`: the proposer's balance on `s` has room for the `R` Gwei of the frame and for `B` more.
EthLib's success theorem asks for room for the whole effective balance of the validator. -/
theorem slashValidator_ok_of_frame (ok : ChurnOk preset config) (fulu : .fusaka ≤ fork)
    (before : fork < .glamsterdam) (room : StateRoom config B p s)
    (frame : SlashFrame electra S E R s w) {i : ValidatorIndex} {y : Validator}
    (found : getValidator w i = .ok y) (unslashed : y.slashed = false)
    (exits : exitStart electra s + E + exitStep preset B +
      config.MIN_VALIDATOR_WITHDRAWABILITY_DELAY.toNat < 2 ^ 64)
    (rewards : ∀ b, getBalance s p = .ok b → b.toNat + R + B < 2 ^ 64) :
    ∃ w', slashValidator config w i = .ok w' ∧
      SlashFrame electra (fun j => S j ∨ j = i) (E + exitStep preset B)
        (R + rewardMax preset fork B) s w' := by
  have capped := room.toQueueRoom.effectiveBalance_le frame found
  have step := exitStep_mono (preset := preset) capped
  have start := frame.start
  have churn := room.churnRoom
  have reward : (whistleblowerReward preset fork y.effectiveBalance).toNat ≤
      rewardMax preset fork B := whistleblowerReward_le capped
  have weight := Nat.mul_le_mul_right PROPOSER_WEIGHT.toNat reward
  have weightRoom := room.weight
  have slashed := frame.slashed
  have slashedRoom := room.slashedRoom
  -- The proposer's balance on `w` is at most its balance on `s` plus the bound of the frame.
  have proposerBalance : p.toNat < s.balances.data.size := by
    rw [show s.balances.data.size = s.validators.data.size from room.sizes]
    exact room.proposerIn
  have old : s.balances.data[p.toNat]? = some s.balances.data[p.toNat] :=
    Array.getElem?_eq_getElem proposerBalance
  obtain ⟨w', computed, single⟩ := slashValidator_ok_frame ok electra before found unslashed
    (frame.balancesMatch room.sizes) (by rw [frame.proposer_eq fulu]; exact room.proposer)
    (by rw [frame.size]; exact room.proposerIn)
    (by rw [frame.effectiveBalanceSum_eq]; exact room.summed) (by omega) (by omega) room.vector
    (by rw [frame.currentEpoch]; exact room.window) (by omega) (by omega)
    (fun b' read => by
      have := frame.balance _ _ _ old (getBalance_eq_ok_iff.mp read)
      have := rewards _ (getBalance_eq_ok_iff.mpr old)
      omega)
  exact ⟨w', computed, (frame.trans single).mono (fun _ h => h) (by omega) (by omega)⟩

end EthLibOps
