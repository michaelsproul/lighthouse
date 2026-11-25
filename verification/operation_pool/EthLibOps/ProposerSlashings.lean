module

public import EthLibOps.Room
public import EthLib.Consensus.Proofs.Transition.Block

/-!
# The proposer slashings of a block succeed

`process_proposer_slashing` checks that two signed headers are for one slot and one proposer and
differ, that the proposer can be slashed, and that it signed both headers. It then slashes the
proposer.

Three predicates name the checks, for a slashing `ps` on a state `s`:

* `ProposerSlashingWellFormed ps`: the checks that read the slashing alone.
* `ProposerSlashingGates s ps`: the proposer is in the registry and can be slashed.
* `ProposerSlashingSigned config checks s ps`: both signatures verify.

`processProposerSlashing_ok_of_frame` is one slashing, on a state that has the `SlashFrame` of
`s`. `proposerSlashings_ok` is the fold `process_operations` runs over the proposer slashings of a
block body.
-/

@[expose] public section

namespace EthLibOps

open EthLib EthLib.Base EthLib.Base.Uint64 EthLib.Consensus

variable {preset : Preset} {fork : Base.Fork} {config : Config} {checks : SignatureChecks}

/-- An assertion whose condition holds succeeds. -/
theorem ensure_of {ε : Type} {condition : Prop} [Decidable condition] (h : condition)
    (error : ε) : ensure condition error = .ok () := by
  simp [ensure, h]

/-- The proposer index a slashing names. -/
abbrev slashedIndex (ps : ProposerSlashing) : ValidatorIndex :=
  ps.signedHeader1.message.proposerIndex

/-- The checks of `process_proposer_slashing` that read the slashing alone: the two headers have
the same slot and the same proposer, and they differ. -/
structure ProposerSlashingWellFormed (ps : ProposerSlashing) : Prop where
  /-- The two headers are for one slot. -/
  sameSlot : ps.signedHeader1.message.slot = ps.signedHeader2.message.slot
  /-- The two headers name one proposer. -/
  sameProposer :
    ps.signedHeader1.message.proposerIndex = ps.signedHeader2.message.proposerIndex
  /-- The two headers differ. -/
  differ : ps.signedHeader1.message ≠ ps.signedHeader2.message

/-- The checks of `process_proposer_slashing` that read the validator's record, for the slashing
`ps` on `s`: the proposer is in the registry and can be slashed at the current epoch. -/
def ProposerSlashingGates (s : BeaconState preset fork) (ps : ProposerSlashing) : Prop :=
  ∃ v, getValidator s (slashedIndex ps) = .ok v ∧
    isSlashableValidator v (getCurrentEpoch s) = true

/-- The two signature checks of `process_proposer_slashing`: each header's signature verifies
against the proposer's public key, under the proposer domain at the epoch of the header's slot. -/
def ProposerSlashingSigned (config : Config) (checks : SignatureChecks)
    (s : BeaconState preset fork) (ps : ProposerSlashing) : Prop :=
  ∃ v, getValidator s (slashedIndex ps) = .ok v ∧
    checkHeaderSignature config checks s v.pubkey ps.signedHeader1 = .ok () ∧
    checkHeaderSignature config checks s v.pubkey ps.signedHeader2 = .ok ()

/-- A validator that can be slashed is not slashed. -/
theorem not_slashed_of_isSlashableValidator {v : Validator} {e : Epoch}
    (h : isSlashableValidator v e = true) : v.slashed = false := by
  simp only [isSlashableValidator, Bool.and_eq_true, Bool.not_eq_true'] at h
  exact h.1.1

/-- The signature check of a header reads the fork versions and the genesis validators root of
the state, and nothing else. -/
theorem checkHeaderSignature_congr {s w : BeaconState preset fork}
    (forkVersions : w.forkVersions = s.forkVersions)
    (genesisValidatorsRoot : w.genesisValidatorsRoot = s.genesisValidatorsRoot)
    (pubkey : Cryptography.Bls.BLSPubkey) (signed : SignedBeaconBlockHeader) :
    checkHeaderSignature config checks w pubkey signed =
      checkHeaderSignature config checks s pubkey signed := by
  simp only [checkHeaderSignature, getDomain, forkVersions, genesisValidatorsRoot,
    Option.getD_some]

variable {electra : .pectra ≤ fork} {S : ValidatorIndex → Prop} {E R B : Nat}
  {s w : BeaconState preset fork} {p : ValidatorIndex}

/-- `process_proposer_slashing` succeeds on a state `w` that has the frame of `s`, for a slashing
that passes the checks on `s` and names a proposer outside the set of the frame. The state after
it has the frame of `s`, with the proposer added to the set. -/
theorem processProposerSlashing_ok_of_frame (ok : ChurnOk preset config) (fulu : .fusaka ≤ fork)
    (before : fork < .glamsterdam) (room : StateRoom config B p s)
    (frame : SlashFrame electra S E R s w) {ps : ProposerSlashing}
    (wellFormed : ProposerSlashingWellFormed ps) (gates : ProposerSlashingGates s ps)
    (signed : ProposerSlashingSigned config checks s ps) (fresh : ¬ S (slashedIndex ps))
    (exits : exitStart electra s + E + exitStep preset B +
      config.MIN_VALIDATOR_WITHDRAWABILITY_DELAY.toNat < 2 ^ 64)
    (rewards : ∀ b, getBalance s p = .ok b → b.toNat + R + B < 2 ^ 64) :
    ∃ w', processProposerSlashing config checks w ps = .ok w' ∧
      SlashFrame electra (fun j => S j ∨ j = slashedIndex ps) (E + exitStep preset B)
        (R + rewardMax preset fork B) s w' := by
  obtain ⟨v, found, slashable⟩ := gates
  obtain ⟨v', found', signed1, signed2⟩ := signed
  cases found.symm.trans found'
  -- The checks read the same things on `w`.
  have foundW : getValidator w (slashedIndex ps) = .ok v := by
    rw [frame.getValidator_eq fresh]
    exact found
  have slashableW : isSlashableValidator v (getCurrentEpoch w) = true := by
    rw [frame.currentEpoch]
    exact slashable
  have signed1W : checkHeaderSignature config checks w v.pubkey ps.signedHeader1 = .ok () := by
    rw [checkHeaderSignature_congr frame.forkVersions frame.genesisValidatorsRoot]
    exact signed1
  have signed2W : checkHeaderSignature config checks w v.pubkey ps.signedHeader2 = .ok () := by
    rw [checkHeaderSignature_congr frame.forkVersions frame.genesisValidatorsRoot]
    exact signed2
  obtain ⟨w', computed, frame'⟩ := slashValidator_ok_of_frame ok fulu before room frame foundW
    (not_slashed_of_isSlashableValidator slashable) exits rewards
  refine ⟨w', ?_, frame'⟩
  have foundW' : getValidator w ps.signedHeader1.message.proposerIndex = .ok v := foundW
  have computed' : slashValidator config w ps.signedHeader1.message.proposerIndex = .ok w' :=
    computed
  simp only [processProposerSlashing, ensure_of wellFormed.sameSlot,
    ensure_of wellFormed.sameProposer, ensure_of wellFormed.differ, foundW', liftAccessor,
    Except.mapError, ensure_of slashableW, signed1W, signed2W, computed', bind, Except.bind]

/-- The proposer slashings of a list succeed, one after the other, from a state `w` that has the
frame of `s`. The checks are made on `s`. The slashings name pairwise distinct proposers outside
the set of the frame. The exit queue and the proposer's balance of `s` have room for the bounds of
the frame and for one exit step and one reward for each slashing.

The end state has the frame of `s`, for the set of the frame and every slashed proposer. -/
theorem proposerSlashings_foldlM_ok (ok : ChurnOk preset config) (fulu : .fusaka ≤ fork)
    (before : fork < .glamsterdam) (room : StateRoom config B p s) :
    ∀ (pss : List ProposerSlashing) (S : ValidatorIndex → Prop) (E R : Nat)
      (w : BeaconState preset fork),
      SlashFrame electra S E R s w →
      pss.Pairwise (fun a b => slashedIndex a ≠ slashedIndex b) →
      (∀ ps ∈ pss, ¬ S (slashedIndex ps)) →
      (∀ ps ∈ pss, ProposerSlashingWellFormed ps ∧ ProposerSlashingGates s ps ∧
        ProposerSlashingSigned config checks s ps) →
      exitStart electra s + E + pss.length * exitStep preset B +
        config.MIN_VALIDATOR_WITHDRAWABILITY_DELAY.toNat < 2 ^ 64 →
      (∀ b, getBalance s p = .ok b →
        b.toNat + R + pss.length * rewardMax preset fork B + B < 2 ^ 64) →
      ∃ w', pss.foldlM (processProposerSlashing config checks) w = .ok w' ∧
        SlashFrame electra (fun j => S j ∨ ∃ ps ∈ pss, slashedIndex ps = j)
          (E + pss.length * exitStep preset B) (R + pss.length * rewardMax preset fork B) s w'
  | [], S, E, R, w, frame, _, _, _, _, _ =>
    ⟨w, rfl, frame.mono (fun _ h => Or.inl h) (by simp) (by simp)⟩
  | ps :: rest, S, E, R, w, frame, distinct, fresh, passes, exits, rewards => by
    generalize hstep : exitStep preset B = step at exits ⊢
    generalize hreward : rewardMax preset fork B = reward at rewards ⊢
    simp only [List.length_cons, Nat.succ_mul] at exits rewards ⊢
    obtain ⟨wellFormed, gates, signed⟩ := passes ps (List.mem_cons_self ..)
    obtain ⟨w₁, computed, frame₁⟩ := processProposerSlashing_ok_of_frame ok fulu before room
      frame wellFormed gates signed (fresh ps (List.mem_cons_self ..))
      (by rw [hstep]; omega) (fun b hb => by have := rewards b hb; omega)
    rw [hstep, hreward] at frame₁
    obtain ⟨w₂, rest_ok, frame₂⟩ := proposerSlashings_foldlM_ok ok fulu before room rest
      (fun j => S j ∨ j = slashedIndex ps) (E + step) (R + reward) w₁ frame₁
      (List.pairwise_cons.mp distinct).2
      (fun q hq hS => by
        rcases hS with hS | same
        · exact fresh q (List.mem_cons_of_mem _ hq) hS
        · exact (List.pairwise_cons.mp distinct).1 q hq same.symm)
      (fun q hq => passes q (List.mem_cons_of_mem _ hq))
      (by rw [hstep]; omega)
      (fun b hb => by rw [hreward]; have := rewards b hb; omega)
    rw [hstep, hreward] at frame₂
    refine ⟨w₂, ?_, frame₂.mono (fun j hj => ?_) (by omega) (by omega)⟩
    · simp only [List.foldlM_cons, computed, bind, Except.bind]
      exact rest_ok
    · rcases hj with (hj | rfl) | ⟨q, hq, rfl⟩
      · exact Or.inl hj
      · exact Or.inr ⟨ps, List.mem_cons_self .., rfl⟩
      · exact Or.inr ⟨q, List.mem_cons_of_mem _ hq, rfl⟩

end EthLibOps
