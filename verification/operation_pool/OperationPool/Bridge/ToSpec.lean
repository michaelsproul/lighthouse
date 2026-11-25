import OperationPool.Closures

/-!
# The spec's form of a packed operation

A packed proposer slashing or voluntary exit is a Rust value. The spec processes a value of
EthLib's type. This file defines the conversion, field by field, so that the main theorem does
not have to assume what the spec's form of an operation holds.

* A `u64` goes through `u64ToSpec`, and a 32-byte root through `rootToSpec`. Both are injective.
* A signature is an opaque Rust type. `signatureBytes` stands for its 96 bytes, and nothing is
  assumed about it.

`headersOk` is the three checks that `verify_proposer_slashing` makes on the two headers.
`proposerSlashingWellFormed_of_headersOk` proves that they are the spec's.

Everything here is a model. `RSignedVoluntaryExit` is written by hand from the Rust struct, which
no extracted function mentions. That these definitions describe the Rust values is TRUSTED.
-/

set_option autoImplicit false

namespace OperationPool.Bridge

open EthLib EthLib.Consensus
open EthLibOps

/-- The Rust `Signature`. -/
abbrev RSignature : Type :=
  bls.generic_signature.GenericSignature blst.min_pk.PublicKey blst.min_pk.Signature

/-- The Rust `BeaconBlockHeader`, as the extraction of the `operation_pool` crate names it. -/
abbrev RBeaconBlockHeader : Type := operation_pool.types.block.beacon_block_header.BeaconBlockHeader

/-- The Rust `SignedVoluntaryExit`:

```rust
pub struct SignedVoluntaryExit {
    pub message: VoluntaryExit,
    pub signature: Signature,
}
```
-/
structure RSignedVoluntaryExit where
  message : RVoluntaryExit
  signature : RSignature

/-- The 96 bytes of a Rust signature. The type is opaque, so this is an uninterpreted function. -/
opaque signatureBytes : RSignature → Cryptography.Bls.BLSSignature :=
  fun _ => Vector.replicate 96 0

/-- The spec's `UInt8` for an extracted Rust `u8`. -/
def u8ToSpec (x : Aeneas.Std.U8) : UInt8 := UInt8.ofBitVec x.bv

theorem u8ToSpec_inj {x y : Aeneas.Std.U8} (h : u8ToSpec x = u8ToSpec y) : x = y := by
  have hbv : x.bv = y.bv := by
    have := congrArg UInt8.toBitVec h
    simpa [u8ToSpec] using this
  cases x; cases y; simp_all

open Aeneas.Std in
/-- The spec's `Root` for a Rust `Hash256`: the same 32 bytes. -/
def rootToSpec (a : Aeneas.Std.Array U8 32#usize) : Root :=
  ⟨(a.val.map u8ToSpec).toArray, by
    have := a.property
    simp only [List.size_toArray, List.length_map]
    scalar_tac⟩

open Aeneas.Std in
theorem rootToSpec_inj {a b : Aeneas.Std.Array U8 32#usize} (h : rootToSpec a = rootToSpec b) :
    a = b := by
  have lists : a.val.map u8ToSpec = b.val.map u8ToSpec := by
    have := congrArg (fun v : Root => v.toArray.toList) h
    simpa [rootToSpec] using this
  have injective : Function.Injective u8ToSpec := fun _ _ => u8ToSpec_inj
  exact Aeneas.Std.Array.ext _ _ (List.map_injective_iff.mpr injective lists)

/-- The spec's form of a block header. -/
def toSpecHeader (h : RBeaconBlockHeader) : BeaconBlockHeader where
  slot := u64ToSpec h.slot
  proposerIndex := u64ToSpec h.proposer_index
  parentRoot := rootToSpec h.parent_root
  stateRoot := rootToSpec h.state_root
  bodyRoot := rootToSpec h.body_root

theorem toSpecHeader_inj {a b : RBeaconBlockHeader} (h : toSpecHeader a = toSpecHeader b) :
    a = b := by
  cases a
  cases b
  simp only [toSpecHeader, BeaconBlockHeader.mk.injEq] at h
  obtain ⟨slot, proposer, parent, state, body⟩ := h
  rw [u64ToSpec_inj slot, u64ToSpec_inj proposer, rootToSpec_inj parent, rootToSpec_inj state,
    rootToSpec_inj body]

/-- The spec's form of a proposer slashing. -/
def toSpecProposerSlashing (r : RProposerSlashing) : ProposerSlashing where
  signedHeader1 :=
    { message := toSpecHeader r.signed_header_1.message
      signature := signatureBytes r.signed_header_1.signature }
  signedHeader2 :=
    { message := toSpecHeader r.signed_header_2.message
      signature := signatureBytes r.signed_header_2.signature }

/-- The spec's form of a voluntary exit. -/
def toSpecVoluntaryExit (x : RSignedVoluntaryExit) : SignedVoluntaryExit where
  message :=
    { epoch := u64ToSpec x.message.epoch
      validatorIndex := u64ToSpec x.message.validator_index }
  signature := signatureBytes x.signature

/-- The spec's form of a proposer slashing names the Rust proposer index. -/
theorem slashedIndex_toSpecProposerSlashing (r : RProposerSlashing) :
    slashedIndex (toSpecProposerSlashing r) =
      u64ToSpec r.signed_header_1.message.proposer_index := rfl

open Classical in
/-- The three checks that `verify_proposer_slashing` makes on the two headers:

```rust
verify!(header_1.slot == header_2.slot, ..);
verify!(header_1.proposer_index == header_2.proposer_index, ..);
verify!(header_1 != header_2, ..);
```

`BeaconBlockHeader` derives `PartialEq`, so the last check compares the headers field by field. -/
noncomputable def headersOk (r : RProposerSlashing) : Bool :=
  decide (r.signed_header_1.message.slot = r.signed_header_2.message.slot) &&
    decide (r.signed_header_1.message.proposer_index =
      r.signed_header_2.message.proposer_index) &&
    decide (r.signed_header_1.message ≠ r.signed_header_2.message)

/-- A proposer slashing whose headers pass the three checks of `verify_proposer_slashing` is well
formed for the spec. -/
theorem proposerSlashingWellFormed_of_headersOk {r : RProposerSlashing}
    (h : headersOk r = true) : ProposerSlashingWellFormed (toSpecProposerSlashing r) := by
  simp only [headersOk, Bool.and_eq_true, decide_eq_true_eq] at h
  obtain ⟨⟨slot, proposer⟩, differ⟩ := h
  exact ⟨congrArg u64ToSpec slot, congrArg u64ToSpec proposer,
    fun same => differ (toSpecHeader_inj same)⟩

end OperationPool.Bridge
