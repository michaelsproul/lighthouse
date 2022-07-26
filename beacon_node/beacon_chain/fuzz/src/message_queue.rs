use types::{Attestation, EthSpec, SignedBeaconBlock};

#[derive(Clone)]
pub enum Message<E: EthSpec> {
    Attestation(Attestation<E>),
    Block(SignedBeaconBlock<E>),
}
