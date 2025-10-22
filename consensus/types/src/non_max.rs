use crate::Epoch;
use nonmax::NonMaxU64;
use std::cmp::Ordering;
use std::convert::TryFrom;

/// Epoch value which cannot be equal to `u64::MAX` aka `FAR_FUTURE_EPOCH`.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct NonMaxEpoch {
    value: NonMaxU64,
}

impl NonMaxEpoch {
    pub fn new(epoch: Epoch) -> Option<Self> {
        let value = NonMaxU64::new(epoch)?;
        Some(NonMaxEpoch { value })
    }

    pub fn get(&self) -> Epoch {
        Epoch::new(self.value.get())
    }
}

impl From<NonMaxEpoch> for u64 {
    fn from(non_max: NonMaxEpoch) -> u64 {
        non_max.value.get()
    }
}

impl TryFrom<u64> for NonMaxEpoch {
    type Error = &'static str;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value).ok_or("max u64 cannot be converted to NonMaxEpoch")
    }
}

impl PartialEq<Epoch> for NonMaxEpoch {
    fn eq(&self, other: &Epoch) -> bool {
        self.get() == other
    }
}

impl PartialEq<NonMaxEpoch> for Epoch {
    fn eq(&self, other: &NonMaxEpoch) -> bool {
        self == other.get()
    }
}

impl PartialOrd<Epoch> for NonMaxEpoch {
    fn partial_cmp(&self, other: &Epoch) -> Option<Ordering> {
        self.get().cmp(other)
    }
}

impl PartialOrd<NonMaxEpoch> for Epoch {
    fn partial_cmp(&self, other: &NonMaxEpoch) -> Option<Ordering> {
        self.cmp(other.get())
    }
}
