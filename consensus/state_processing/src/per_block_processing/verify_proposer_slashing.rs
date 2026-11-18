use super::errors::{BlockOperationError, ProposerSlashingInvalid as Invalid};
use super::signature_sets::{get_pubkey_from_state, proposer_slashing_signature_set};
use crate::VerifySignatures;
use types::*;

type Result<T> = std::result::Result<T, BlockOperationError<Invalid>>;

fn error(reason: Invalid) -> BlockOperationError<Invalid> {
    BlockOperationError::invalid(reason)
}

/// Indicates if a `ProposerSlashing` is valid to be included in a block in the current epoch of the given
/// state.
///
/// Returns `Ok(())` if the `ProposerSlashing` is valid, otherwise indicates the reason for invalidity.
///
/// Spec v0.12.1
pub fn verify_proposer_slashing<E: EthSpec>(
    proposer_slashing: &ProposerSlashing,
    state: &BeaconState<E>,
    verify_signatures: VerifySignatures,
    spec: &ChainSpec,
) -> Result<()> {
    let header_1 = &proposer_slashing.signed_header_1.message;
    let header_2 = &proposer_slashing.signed_header_2.message;

    // Verify slots match
    verify!(
        header_1.slot == header_2.slot,
        Invalid::ProposalSlotMismatch(header_1.slot, header_2.slot)
    );

    // Verify header proposer indices match
    verify!(
        header_1.proposer_index == header_2.proposer_index,
        Invalid::ProposerIndexMismatch(header_1.proposer_index, header_2.proposer_index)
    );

    // But the headers are different
    verify!(header_1 != header_2, Invalid::ProposalsIdentical);

    // Check proposer is slashable
    verify_proposer_slashing_validator_checks(
        state.validators().get(header_1.proposer_index as usize),
        header_1.proposer_index,
        state.current_epoch(),
    )?;

    if verify_signatures.is_true() {
        let (signature_set_1, signature_set_2) = proposer_slashing_signature_set(
            state,
            |i| get_pubkey_from_state(state, i),
            proposer_slashing,
            spec,
        )?;
        verify!(signature_set_1.verify(), Invalid::BadProposal1Signature);
        verify!(signature_set_2.verify(), Invalid::BadProposal2Signature);
    }

    Ok(())
}

/// The check of `verify_proposer_slashing` that reads the proposer's record.
///
/// Free of `EthSpec` so that it can be extracted with Aeneas and proved correct in Lean (see
/// `verification/operation_pool`).
pub fn verify_proposer_slashing_validator_checks(
    proposer: Option<&Validator>,
    proposer_index: u64,
    current_epoch: Epoch,
) -> Result<()> {
    let Some(proposer) = proposer else {
        return Err(error(Invalid::ProposerUnknown(proposer_index)));
    };

    verify!(
        proposer.is_slashable_at(current_epoch),
        Invalid::ProposerNotSlashable(proposer_index)
    );

    Ok(())
}
