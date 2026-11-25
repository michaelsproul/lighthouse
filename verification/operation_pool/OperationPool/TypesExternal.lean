-- Hand-written models of the external types listed in `TypesExternal_Template.lean`.
--
-- Everything in this file is TRUSTED: it states what an external Rust type is, and is not checked
-- against the Rust implementation.
import Aeneas
import ExternalTypes
open Aeneas Aeneas.Std Result ControlFlow Error
set_option linter.dupNamespace false
set_option linter.hashCommand false
set_option linter.unusedVariables false

/-- [std::collections::hash::set::HashSet]

    Model: the distinct values inserted so far, as a list. The hasher `S` and allocator `A` carry no
    information. This assumes the element type's `Hash` and `Eq` impls agree with equality of
    values, which is the contract `HashSet` requires. -/
@[rust_type "std::collections::hash::set::HashSet"]
def std.collections.hash.set.HashSet (T : Type) (S : Type) (A : Type) : Type := List T

/-- [std::collections::hash::map::HashMap]

    Model: the entries of the map, as a list of key and value, in some order. The hasher `S` and
    allocator `A` carry no information. The functions that write a map keep one entry per key
    (`OperationPool/FunsExternal.lean`). This assumes the key type's `Hash` and `Eq` impls agree
    with equality of values, which is the contract `HashMap` requires. -/
@[rust_type "std::collections::hash::map::HashMap"]
def std.collections.hash.map.HashMap (K : Type) (V : Type) (S : Type) (A : Type) : Type :=
  List (K × V)

/-! The types below are left opaque: nothing is assumed about them beyond their existence.
`blst::min_pk::PublicKey` is in `ExternalTypes.lean`, shared with the other extracted crate. -/

/-- [std::hash::random::RandomState] -/
@[rust_type "std::hash::random::RandomState"]
axiom std.hash.random.RandomState : Type

/-- [std::hash::random::DefaultHasher] -/
@[rust_type "std::hash::random::DefaultHasher"]
axiom std.hash.random.DefaultHasher : Type

/-- [bls::generic_signature::GenericSignature] -/
@[rust_type "bls::generic_signature::GenericSignature"]
axiom bls.generic_signature.GenericSignature (Pub : Type) (Sig : Type) : Type

/-- [blst::min_pk::Signature] -/
@[rust_type "blst::min_pk::Signature"]
axiom blst.min_pk.Signature : Type

/-- [types::core::slot_epoch::Slot]

    Model: the `u64` the type wraps (`pub struct Slot(u64)`). The extraction of `state_processing`
    generates the same definition; this crate leaves the module opaque, so it is written here.
    The extracted code of this crate never reads a slot. -/
@[reducible, rust_type "types::core::slot_epoch::Slot"]
def types.core.slot_epoch.Slot : Type := Std.U64
