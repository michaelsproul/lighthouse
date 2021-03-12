use proc_macro::TokenStream;
use quote::quote;
use std::iter;
use syn::{parse_macro_input, DataEnum, DataStruct, DeriveInput};

/// Returns true if some field has an attribute declaring it should be generated from default (not
/// randomized).
///
/// The field attribute is: `#[test_random(default)]`
fn should_use_default(field: &syn::Field) -> bool {
    field.attrs.iter().any(|attr| {
        attr.path.is_ident("test_random") && attr.tokens.to_string().replace(" ", "") == "(default)"
    })
}

#[proc_macro_derive(TestRandom, attributes(test_random))]
pub fn test_random_derive(input: TokenStream) -> TokenStream {
    let derive_input = parse_macro_input!(input as DeriveInput);

    match &derive_input.data {
        syn::Data::Struct(s) => derive_struct(&derive_input, s),
        syn::Data::Enum(e) => derive_enum(&derive_input, e),
        _ => panic!("test_random_derive only supports structs and enums"),
    }
}

fn derive_struct(derive_input: &DeriveInput, struct_data: &DataStruct) -> TokenStream {
    let name = &derive_input.ident;
    let (impl_generics, ty_generics, where_clause) = &derive_input.generics.split_for_impl();

    // Build quotes for fields that should be generated and those that should be built from
    // `Default`.
    let mut quotes = vec![];
    for field in &struct_data.fields {
        match &field.ident {
            Some(ref ident) => {
                if should_use_default(field) {
                    quotes.push(quote! {
                        #ident: <_>::default(),
                    });
                } else {
                    quotes.push(quote! {
                        #ident: <_>::random_for_test(rng),
                    });
                }
            }
            _ => panic!("test_random_derive only supports named struct fields."),
        };
    }

    let output = quote! {
        impl #impl_generics TestRandom for #name #ty_generics #where_clause {
            fn random_for_test(rng: &mut impl rand::RngCore) -> Self {
               Self {
                    #(
                        #quotes
                    )*
               }
            }
        }
    };

    output.into()
}

fn derive_enum(derive_input: &DeriveInput, enum_data: &DataEnum) -> TokenStream {
    let name = &derive_input.ident;
    let (impl_generics, ty_generics, where_clause) = &derive_input.generics.split_for_impl();

    let n = enum_data.variants.len();

    let indices = 0..n;
    let variant_exprs = enum_data.variants.iter().map(|variant| {
        let variant_name = &variant.ident;

        match &variant.fields {
            syn::Fields::Unnamed(f) => {
                let field_exprs = iter::repeat(quote! {
                        <_>::random_for_test(rng)
                })
                .take(f.unnamed.len());
                quote! {
                    #name::#variant_name(
                        #(
                            #field_exprs
                        )*
                    )
                }
            }
            _ => unimplemented!("enums with no fields or named fields not supported"),
        }
    });

    let output = quote! {
        impl #impl_generics TestRandom for #name #ty_generics #where_clause {
            fn random_for_test(rng: &mut impl rand::RngCore) -> Self {
               let variant = rng.next_u32() as usize % #n;
               match variant {
                    #(
                        #indices => #variant_exprs,
                    )*
                    _ => panic!("variant index out of bounds"),
               }
            }
        }
    };
    output.into()
}
