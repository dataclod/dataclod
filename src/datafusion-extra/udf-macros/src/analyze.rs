//! User function analysis utilities.
//!
//! This module extracts information from the user's Rust function signature
//! to determine how to generate the UDF implementation.

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{FnArg, Ident, ItemFn, Pat, PatIdent, PatType, Result, Type};

/// Information extracted from the user's Rust function.
pub struct UserFunctionAttr {
    /// Function name
    pub name: Ident,
    /// Argument names and types
    pub args: Vec<(Ident, Type)>,
    /// Whether each argument is Option<T>
    pub args_option: Vec<bool>,
    /// Return type kind
    pub return_type_kind: ReturnTypeKind,
}

/// Classification of function return types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReturnTypeKind {
    /// Returns `T` directly
    T,
    /// Returns `Option<T>`
    Option,
}

impl ReturnTypeKind {
    /// Wrap a function call to produce `Option<T>` output.
    ///
    /// - `T` → `Some(call)`
    /// - `Option<T>` → `call`
    pub fn wrap_call(&self, call: TokenStream2) -> TokenStream2 {
        match self {
            ReturnTypeKind::T => quote!(Some(#call)),
            ReturnTypeKind::Option => call,
        }
    }
}

/// Parse a user's Rust function to extract relevant information.
pub fn parse_user_function(input_fn: &ItemFn) -> Result<UserFunctionAttr> {
    let mut args = Vec::new();
    let mut args_option = Vec::new();

    for arg in &input_fn.sig.inputs {
        match arg {
            FnArg::Typed(PatType { pat, ty, .. }) => {
                if let Pat::Ident(PatIdent { ident, .. }) = pat.as_ref() {
                    args.push((ident.clone(), ty.as_ref().clone()));
                    args_option.push(is_option_type(ty));
                } else {
                    return Err(syn::Error::new_spanned(pat, "expected identifier pattern"));
                }
            }
            FnArg::Receiver(_) => {
                return Err(syn::Error::new_spanned(arg, "self parameter not supported"));
            }
        }
    }

    let return_type_kind = analyze_return_type(&input_fn.sig.output);

    Ok(UserFunctionAttr {
        name: input_fn.sig.ident.clone(),
        args,
        args_option,
        return_type_kind,
    })
}

/// Check if a type is `Option<T>`.
pub fn is_option_type(ty: &Type) -> bool {
    if let Type::Path(path) = ty
        && let Some(seg) = path.path.segments.last()
    {
        return seg.ident == "Option";
    }
    false
}

/// Analyze the return type to determine its kind.
fn analyze_return_type(output: &syn::ReturnType) -> ReturnTypeKind {
    match output {
        syn::ReturnType::Default => ReturnTypeKind::T,
        syn::ReturnType::Type(_, ty) => {
            if is_option_type(ty) {
                ReturnTypeKind::Option
            } else {
                ReturnTypeKind::T
            }
        }
    }
}
