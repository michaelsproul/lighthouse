use types::{Attestation, EthSpec, Hash256, SignedBeaconBlock};

#[derive(Clone)]
pub enum Message<E: EthSpec> {
    Attestation(Attestation<E>),
    Block(SignedBeaconBlock<E>),
}

impl<E: EthSpec> Message<E> {
    pub fn block_root(&self) -> Option<Hash256> {
        match self {
            Self::Attestation(_) => None,
            Self::Block(block) => Some(block.canonical_root()),
        }
    }
}
