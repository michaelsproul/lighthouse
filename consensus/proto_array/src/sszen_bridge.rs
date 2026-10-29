/// Implement the `sszen` codec traits for a type by forwarding to its `ethereum_ssz` codec.
///
/// This lets a container derive its codec with `sszen` while holding fields, such as
/// `ProtoNode`, that only implement `ethereum_ssz`. The encoding doesn't change.
///
/// `$fixed_size` must be the type's `ethereum_ssz` fixed length, or `None` if it has a variable
/// length. Debug builds check this when encoding.
#[macro_export]
macro_rules! impl_sszen_via_ethereum_ssz {
    ($type:ty, $fixed_size:expr) => {
        impl ::sszen::SszSize for $type {
            const FIXED_SIZE: Option<usize> = $fixed_size;
        }

        impl ::sszen::Encode for $type {
            fn ssz_bytes_len(&self) -> usize {
                <Self as ::ssz::Encode>::ssz_bytes_len(self)
            }

            fn ssz_append(&self, buf: &mut Vec<u8>) {
                debug_assert_eq!(
                    <Self as ::sszen::SszSize>::FIXED_SIZE,
                    <Self as ::ssz::Encode>::is_ssz_fixed_len()
                        .then(<Self as ::ssz::Encode>::ssz_fixed_len),
                    "sszen and ethereum_ssz disagree on the fixed size of {}",
                    stringify!($type),
                );
                <Self as ::ssz::Encode>::ssz_append(self, buf)
            }
        }

        impl ::sszen::Decode for $type {
            fn from_ssz_bytes(bytes: &[u8]) -> Result<Self, ::sszen::DecodeError> {
                <Self as ::ssz::Decode>::from_ssz_bytes(bytes)
                    .map_err(|e| ::sszen::DecodeErrorKind::BytesInvalid(format!("{e:?}")).into())
            }
        }
    };
}
