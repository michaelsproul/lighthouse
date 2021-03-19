use crate::per_epoch_processing::base::TotalBalances;
use crate::per_epoch_processing::Error;
use safe_arith::SafeArith;
use types::{BeaconState, Checkpoint, EthSpec};

/// Update the justified and finalized checkpoints for matching target attestations.
/// FIXME(altair): abstract over target indices, etc
#[allow(clippy::if_same_then_else)] // For readability and consistency with spec.
pub fn process_justification_and_finalization<T: EthSpec>(
    state: &mut BeaconState<T>,
    total_balances: &TotalBalances,
) -> Result<(), Error> {
    if state.current_epoch() <= T::genesis_epoch().safe_add(1)? {
        return Ok(());
    }

    let previous_epoch = state.previous_epoch();
    let current_epoch = state.current_epoch();

    let old_previous_justified_checkpoint = state.previous_justified_checkpoint();
    let old_current_justified_checkpoint = state.current_justified_checkpoint();

    // Process justifications
    *state.previous_justified_checkpoint_mut() = state.current_justified_checkpoint();
    state.justification_bits_mut().shift_up(1)?;

    if total_balances
        .previous_epoch_target_attesters()
        .safe_mul(3)?
        >= total_balances.current_epoch().safe_mul(2)?
    {
        *state.current_justified_checkpoint_mut() = Checkpoint {
            epoch: previous_epoch,
            root: *state.get_block_root_at_epoch(previous_epoch)?,
        };
        state.justification_bits_mut().set(1, true)?;
    }
    // If the current epoch gets justified, fill the last bit.
    if total_balances
        .current_epoch_target_attesters()
        .safe_mul(3)?
        >= total_balances.current_epoch().safe_mul(2)?
    {
        *state.current_justified_checkpoint_mut() = Checkpoint {
            epoch: current_epoch,
            root: *state.get_block_root_at_epoch(current_epoch)?,
        };
        state.justification_bits_mut().set(0, true)?;
    }

    let bits = state.justification_bits().clone();

    // The 2nd/3rd/4th most recent epochs are all justified, the 2nd using the 4th as source.
    if (1..4).all(|i| bits.get(i).unwrap_or(false))
        && old_previous_justified_checkpoint.epoch.safe_add(3)? == current_epoch
    {
        *state.finalized_checkpoint_mut() = old_previous_justified_checkpoint;
    }
    // The 2nd/3rd most recent epochs are both justified, the 2nd using the 3rd as source.
    else if (1..3).all(|i| bits.get(i).unwrap_or(false))
        && old_previous_justified_checkpoint.epoch.safe_add(2)? == current_epoch
    {
        *state.finalized_checkpoint_mut() = old_previous_justified_checkpoint;
    }
    // The 1st/2nd/3rd most recent epochs are all justified, the 1st using the 3nd as source.
    if (0..3).all(|i| bits.get(i).unwrap_or(false))
        && old_current_justified_checkpoint.epoch.safe_add(2)? == current_epoch
    {
        *state.finalized_checkpoint_mut() = old_current_justified_checkpoint;
    }
    // The 1st/2nd most recent epochs are both justified, the 1st using the 2nd as source.
    else if (0..2).all(|i| bits.get(i).unwrap_or(false))
        && old_current_justified_checkpoint.epoch.safe_add(1)? == current_epoch
    {
        *state.finalized_checkpoint_mut() = old_current_justified_checkpoint;
    }

    Ok(())
}
