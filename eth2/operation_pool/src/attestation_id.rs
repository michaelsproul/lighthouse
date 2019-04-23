use int_to_bytes::int_to_bytes8;
use ssz::ssz_encode;
use std::fmt;
use types::{AttestationData, BeaconState, ChainSpec, Domain, Epoch};

/// Serialized `AttestationData` augmented with a domain to encode the fork info.
#[derive(PartialEq, Eq, Clone, Hash)]
pub struct AttestationId(Vec<u8>);

impl fmt::Debug for AttestationId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AttestationId(..)")
    }
}

/// Number of domain bytes that the end of an attestation ID is padded with.
const DOMAIN_BYTES_LEN: usize = 8;

impl AttestationId {
    pub fn from_data(attestation: &AttestationData, state: &BeaconState, spec: &ChainSpec) -> Self {
        let mut bytes = ssz_encode(attestation);
        let epoch = attestation.slot.epoch(spec.slots_per_epoch);
        bytes.extend_from_slice(&AttestationId::compute_domain_bytes(epoch, state, spec));
        AttestationId(bytes)
    }

    pub fn compute_domain_bytes(epoch: Epoch, state: &BeaconState, spec: &ChainSpec) -> Vec<u8> {
        int_to_bytes8(spec.get_domain(epoch, Domain::Attestation, &state.fork))
    }

    pub fn domain_bytes_match(&self, domain_bytes: &[u8]) -> bool {
        &self.0[self.0.len() - DOMAIN_BYTES_LEN..] == domain_bytes
    }
}
