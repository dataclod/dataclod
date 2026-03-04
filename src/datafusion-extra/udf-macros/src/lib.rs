//! Procedural macros for creating `DataFusion` UDFs with minimal boilerplate.
//!
//! This crate provides the `#[function]` attribute macro that generates all
//! the boilerplate code required for `DataFusion` scalar UDFs, inspired by
//! [arrow-udf](https://github.com/risingwavelabs/arrow-udf).
//!
//! # Quick Start
//!
//! ```ignore
//! use datafusion_udf_macros::function;
//!
//! #[function("st_area(Geometry) -> Float64")]
//! fn st_area(wkb: &[u8]) -> Option<f64> {
//!     Geometry::new_from_wkb(wkb).ok()?.area().ok()
//! }
//!
//! // Register with DataFusion
//! ctx.register_udf(StAreaGeometryUdf::udf());
//! ```
//!
//! # Design Philosophy
//!
//! The macro infers behavior from both the SQL signature and Rust function:
//!
//! | Pattern | Behavior |
//! |---------|----------|
//! | `fn f(x: T)` | Non-nullable input, returns NULL on NULL input |
//! | `fn f(x: Option<T>)` | Nullable input, receives NULL as None |
//! | `fn f() -> T` | Always returns a value |
//! | `fn f() -> Option<T>` | May return NULL |
//!
//! # Supported Types
//!
//! | SQL Type | Rust Input | Rust Output | Arrow Type |
//! |----------|------------|-------------|------------|
//! | `Geometry` | `&[u8]` | `Vec<u8>` | `BinaryView` |
//! | `Float64` | `f64` | `f64` | `Float64` |
//! | `Float32` | `f32` | `f32` | `Float32` |
//! | `Int64` | `i64` | `i64` | `Int64` |
//! | `Int32` | `i32` | `i32` | `Int32` |
//! | `Int16` | `i16` | `i16` | `Int16` |
//! | `Int8` | `i8` | `i8` | `Int8` |
//! | `UInt64` | `u64` | `u64` | `UInt64` |
//! | `UInt32` | `u32` | `u32` | `UInt32` |
//! | `UInt16` | `u16` | `u16` | `UInt16` |
//! | `UInt8` | `u8` | `u8` | `UInt8` |
//! | `Boolean` | `bool` | `bool` | `Boolean` |
//! | `Utf8` | `&str` | `String` | `Utf8View` |
//! | `Binary` | `&[u8]` | `Vec<u8>` | `BinaryView` |
//! | `Date32` | `i32` | `i32` | `Date32` |
//! | `Date64` | `i64` | `i64` | `Date64` |
//! | `Timestamp` | `i64` | `i64` | `Timestamp(μs)` |
//! | `TimestampMs` | `i64` | `i64` | `Timestamp(ms)` |
//! | `TimestampS` | `i64` | `i64` | `Timestamp(s)` |
//! | `TimestampNs` | `i64` | `i64` | `Timestamp(ns)` |
//! | `Json` | `&str` | `String` | `Utf8View` |
//! | `Struct Name` | struct | struct | `Struct` |
//! | `List<T>` | - | - | `List` |
//!
//! # Features
//!
//! ## Multiple Signatures
//!
//! Create overloaded functions with multiple `#[function]` attributes:
//!
//! ```ignore
//! #[function("abs(Int32) -> Int32")]
//! #[function("abs(Int64) -> Int64")]
//! #[function("abs(Float64) -> Float64")]
//! fn abs<T: Signed>(x: T) -> T { x.abs() }
//!
//! // Generates a single AbsUdf with Signature::one_of
//! ctx.register_udf(AbsUdf::udf());
//! ```
//!
//! ## Struct Types
//!
//! Return complex types with `#[derive(StructType)]`:
//!
//! ```ignore
//! #[derive(StructType)]
//! struct BBox { xmin: f64, ymin: f64, xmax: f64, ymax: f64 }
//!
//! #[function("ST_Extent(Geometry) -> Struct BBox")]
//! fn st_extent(wkb: Option<&[u8]>) -> Option<BBox> { ... }
//! ```
//!
//! # Generated Code
//!
//! For `#[function("st_area(Geometry) -> Float64")]`, the macro generates:
//!
//! - `StAreaGeometryUdf` struct implementing `ScalarUDFImpl`
//! - `StAreaGeometryUdf::udf()` returning `ScalarUDF`

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::parse::Parser;
use syn::{
    DeriveInput, Error, Expr, ExprLit, Fields, ItemFn, ItemStruct, Lit, LitStr, Meta,
    MetaNameValue, Result, parse_macro_input,
};

mod analyze;
mod codegen;
mod parser;
mod struct_type;
mod types;
mod utils;

use analyze::parse_user_function;
use codegen::{generate_combined_invoke_impl, generate_invoke_impl};
use parser::FunctionSignature;
use types::{ArgType, GEOMETRY_EXTENSION_NAME, ReturnTypeInfo};
use utils::to_struct_name;

/// Attribute macro for defining `DataFusion` scalar UDFs.
#[proc_macro_attribute]
pub fn function(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr_str = parse_macro_input!(attr as Expr);
    let mut input_fn = parse_macro_input!(item as ItemFn);

    let mut signatures = vec![attr_str];

    let mut remaining_attrs = Vec::new();
    for attr in input_fn.attrs.drain(..) {
        if attr.path().is_ident("function") {
            match attr.parse_args::<Expr>() {
                Ok(expr) => signatures.push(expr),
                Err(e) => return e.into_compile_error().into(),
            }
        } else {
            remaining_attrs.push(attr);
        }
    }
    input_fn.attrs = remaining_attrs;

    if signatures.len() == 1 {
        match generate_udf(signatures.remove(0), input_fn) {
            Ok(tokens) => tokens.into(),
            Err(e) => e.into_compile_error().into(),
        }
    } else {
        match generate_multi_udf(signatures, input_fn) {
            Ok(tokens) => tokens.into(),
            Err(e) => e.into_compile_error().into(),
        }
    }
}

/// Attribute macro for defining Arrow extension types.
#[proc_macro_attribute]
pub fn extension_type(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attrs = match parse_extension_type_attr(attr) {
        Ok(attrs) => attrs,
        Err(err) => return err.into_compile_error().into(),
    };

    let input = parse_macro_input!(item as ItemStruct);
    if !matches!(input.fields, Fields::Unit) {
        return Error::new_spanned(&input, "extension_type only supports unit structs")
            .into_compile_error()
            .into();
    }

    let ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let name = attrs.name;
    let data_type = attrs.data_type;

    let expanded = quote! {
        #input

        impl #impl_generics ::arrow_schema::extension::ExtensionType for #ident #ty_generics #where_clause {
            const NAME: &'static str = #name;

            type Metadata = ();

            fn metadata(&self) -> &Self::Metadata {
                &()
            }

            fn serialize_metadata(&self) -> Option<String> {
                None
            }

            fn deserialize_metadata(
                metadata: Option<&str>,
            ) -> Result<Self::Metadata, ::arrow_schema::ArrowError> {
                metadata.map_or_else(
                    || Ok(()),
                    |_| {
                        Err(::arrow_schema::ArrowError::InvalidArgumentError(format!(
                            "{} extension type expects no metadata",
                            Self::NAME
                        )))
                    },
                )
            }

            fn supports_data_type(
                &self,
                data_type: &::arrow_schema::DataType,
            ) -> Result<(), ::arrow_schema::ArrowError> {
                if data_type == &#data_type {
                    Ok(())
                } else {
                    Err(::arrow_schema::ArrowError::InvalidArgumentError(format!(
                        "{} data type mismatch, expected {}, found {}",
                        Self::NAME,
                        #data_type,
                        data_type,
                    )))
                }
            }

            fn try_new(
                data_type: &::arrow_schema::DataType,
                _metadata: Self::Metadata,
            ) -> Result<Self, ::arrow_schema::ArrowError> {
                Self.supports_data_type(data_type).map(|_| Self)
            }
        }
    };

    expanded.into()
}

/// Attribute macro for defining multiple UDF variants from a single generic
/// function.
#[proc_macro_attribute]
pub fn functions(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);

    let signatures: syn::punctuated::Punctuated<Expr, syn::Token![,]> =
        match syn::punctuated::Punctuated::parse_terminated.parse(attr) {
            Ok(sigs) => sigs,
            Err(e) => return e.into_compile_error().into(),
        };

    if signatures.is_empty() {
        return Error::new_spanned(&input_fn, "functions macro requires at least one signature")
            .into_compile_error()
            .into();
    }

    match generate_multi_udf(signatures.into_iter().collect(), input_fn) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.into_compile_error().into(),
    }
}

struct ExtensionTypeAttr {
    name: LitStr,
    data_type: Expr,
}

fn parse_extension_type_attr(attr: TokenStream) -> Result<ExtensionTypeAttr> {
    let metas: syn::punctuated::Punctuated<Meta, syn::Token![,]> =
        syn::punctuated::Punctuated::parse_terminated.parse(attr)?;

    let mut name: Option<LitStr> = None;
    let mut data_type: Option<Expr> = None;

    for meta in metas {
        match meta {
            Meta::NameValue(MetaNameValue { path, value, .. }) if path.is_ident("name") => {
                if name.is_some() {
                    return Err(Error::new_spanned(
                        path,
                        "duplicate `name` in extension_type",
                    ));
                }
                match value {
                    Expr::Lit(ExprLit {
                        lit: Lit::Str(lit), ..
                    }) => {
                        name = Some(lit);
                    }
                    other => {
                        return Err(Error::new_spanned(
                            other,
                            "extension_type `name` must be a string literal",
                        ));
                    }
                }
            }
            Meta::NameValue(MetaNameValue { path, value, .. }) if path.is_ident("data_type") => {
                if data_type.is_some() {
                    return Err(Error::new_spanned(
                        path,
                        "duplicate `data_type` in extension_type",
                    ));
                }
                data_type = Some(value);
            }
            other => {
                return Err(Error::new_spanned(
                    other,
                    "expected `name = \"...\"` or `data_type = ...`",
                ));
            }
        }
    }

    let name = name.ok_or_else(|| {
        Error::new(
            proc_macro2::Span::call_site(),
            "extension_type requires `name`",
        )
    })?;
    let data_type = data_type.ok_or_else(|| {
        Error::new(
            proc_macro2::Span::call_site(),
            "extension_type requires `data_type`",
        )
    })?;

    Ok(ExtensionTypeAttr { name, data_type })
}

/// Derive macro for creating Arrow-compatible struct types.
#[proc_macro_derive(StructType)]
pub fn derive_struct_type(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    match struct_type::generate_struct_type(input) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.into_compile_error().into(),
    }
}

/// Parsed signature information for code generation.
struct ParsedSignature {
    signature: FunctionSignature,
    arg_types: Vec<ArgType>,
    return_info: ReturnTypeInfo,
}

/// Generate a single combined UDF from multiple signatures.
fn generate_multi_udf(signatures: Vec<Expr>, input_fn: ItemFn) -> Result<TokenStream2> {
    let user_fn = parse_user_function(&input_fn)?;
    let fn_arg_count = user_fn.args.len();

    let mut impl_fn = input_fn.clone();
    impl_fn.vis = syn::Visibility::Inherited;

    let mut parsed_sigs: Vec<ParsedSignature> = Vec::new();

    for sig_expr in &signatures {
        let sig_str = match sig_expr {
            Expr::Lit(ExprLit {
                lit: Lit::Str(s), ..
            }) => s.value(),
            _ => {
                return Err(Error::new_spanned(
                    sig_expr,
                    "expected a string literal for function signature",
                ));
            }
        };

        let signature = FunctionSignature::parse(&sig_str)?;

        let arg_types: Vec<ArgType> = signature.args.iter().map(|a| a.arg_type.clone()).collect();
        let return_info = ReturnTypeInfo::from_type(&signature.return_type)?;

        parsed_sigs.push(ParsedSignature {
            signature,
            arg_types,
            return_info,
        });
    }

    let sql_name = &parsed_sigs[0].signature.name;
    for (idx, ps) in parsed_sigs.iter().enumerate().skip(1) {
        if &ps.signature.name != sql_name {
            return Err(Error::new_spanned(
                &signatures[idx],
                format!(
                    "All signatures must have the same function name. Expected '{}', found '{}'",
                    sql_name, ps.signature.name
                ),
            ));
        }
    }

    let max_sig_arg_count = parsed_sigs
        .iter()
        .map(|ps| ps.arg_types.len())
        .max()
        .unwrap_or(0);

    if fn_arg_count < max_sig_arg_count {
        return Err(Error::new_spanned(
            &input_fn.sig,
            format!(
                "Function has {fn_arg_count} parameters but signature requires \
                 {max_sig_arg_count} arguments. Add Option parameters for the extra arguments.",
            ),
        ));
    }

    let min_sig_arg_count = parsed_sigs
        .iter()
        .map(|ps| ps.arg_types.len())
        .min()
        .unwrap_or(0);
    for i in min_sig_arg_count..fn_arg_count {
        if !user_fn.args_option[i] {
            return Err(Error::new_spanned(
                &input_fn.sig,
                format!(
                    "Parameter {} must be Option<T> to support signatures with fewer arguments",
                    user_fn.args[i].0
                ),
            ));
        }
    }

    let struct_name = format!("{}UDF", to_struct_name(sql_name));
    let struct_ident = format_ident!("{}", struct_name);

    let return_info = &parsed_sigs[0].return_info;
    let return_data_type = return_info.data_type_tokens();

    let type_signatures: Vec<TokenStream2> = parsed_sigs
        .iter()
        .map(|ps| {
            let exact_types: Vec<TokenStream2> =
                ps.arg_types.iter().map(|t| t.data_type_tokens()).collect();
            quote! {
                datafusion::logical_expr::TypeSignature::Exact(vec![#(#exact_types),*])
            }
        })
        .collect();

    let signature_init = quote! {
        datafusion::logical_expr::Signature::one_of(
            vec![#(#type_signatures),*],
            datafusion::logical_expr::Volatility::Immutable,
        )
    };

    // Group signatures by arity
    let mut sigs_by_arity: std::collections::BTreeMap<usize, Vec<&ParsedSignature>> =
        std::collections::BTreeMap::new();
    for ps in &parsed_sigs {
        sigs_by_arity
            .entry(ps.arg_types.len())
            .or_default()
            .push(ps);
    }

    let invoke_branches: Vec<TokenStream2> = sigs_by_arity
        .iter()
        .map(|(&arity, sigs)| {
            if sigs.len() == 1 {
                // Single signature for this arity - no type dispatch needed
                let invoke_impl = generate_combined_invoke_impl(
                    &user_fn,
                    &sigs[0].arg_types,
                    return_info,
                    fn_arg_count,
                );
                quote! { #arity => { #invoke_impl } }
            } else {
                // Multiple signatures with same arity - need type dispatch
                let type_branches: Vec<TokenStream2> = sigs
                    .iter()
                    .map(|ps| {
                        let type_checks: Vec<TokenStream2> = ps
                            .arg_types
                            .iter()
                            .enumerate()
                            .map(|(i, arg_type)| {
                                let expected_type = arg_type.data_type_tokens();
                                quote! {
                                    args[#i].data_type() == &#expected_type
                                }
                            })
                            .collect();
                        let invoke_impl = generate_combined_invoke_impl(
                            &user_fn,
                            &ps.arg_types,
                            return_info,
                            fn_arg_count,
                        );
                        quote! {
                            if #(#type_checks)&&* {
                                return { #invoke_impl };
                            }
                        }
                    })
                    .collect();

                let expected_types_str = sigs
                    .iter()
                    .map(|ps| {
                        format!(
                            "({})",
                            ps.arg_types
                                .iter()
                                .map(|t| t.display_name())
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(" or ");

                quote! {
                    #arity => {
                        #(#type_branches)*
                        return exec_err!(
                            "no matching signature for {}: got {:?}, expected {}",
                            self.name(),
                            args.iter().map(|a| a.data_type()).collect::<Vec<_>>(),
                            #expected_types_str
                        );
                    }
                }
            }
        })
        .collect();

    let valid_counts: Vec<usize> = sigs_by_arity.keys().copied().collect();
    let valid_counts_str = valid_counts
        .iter()
        .map(|c| c.to_string())
        .collect::<Vec<_>>()
        .join(", ");

    let max_arg_count = parsed_sigs
        .iter()
        .map(|sig| sig.arg_types.len())
        .max()
        .unwrap_or(0);
    let geometry_arg_indices: Vec<usize> = (0..max_arg_count)
        .filter(|idx| {
            parsed_sigs
                .iter()
                .all(|sig| sig.arg_types.get(*idx).is_some_and(|arg| arg.is_geometry()))
        })
        .collect();
    let geometry_arg_checks: Vec<TokenStream2> = geometry_arg_indices
        .iter()
        .map(|idx| {
            let position = idx + 1;
            quote! {
                if _args
                    .arg_fields
                    .get(#idx)
                    .and_then(|field| field.extension_type_name())
                    != Some(#GEOMETRY_EXTENSION_NAME)
                {
                    return datafusion::common::exec_err!(
                        "argument {} to {} must have extension type '{}'",
                        #position,
                        self.name(),
                        #GEOMETRY_EXTENSION_NAME
                    );
                }
            }
        })
        .collect();

    let needs_return_field = return_info.is_geometry() || !geometry_arg_indices.is_empty();
    let return_field_impl = if needs_return_field {
        let field_expr = if return_info.is_geometry() {
            quote! {{
                let field = datafusion::arrow::datatypes::Field::new(
                    self.name(),
                    #return_data_type,
                    true,
                )
                .with_metadata(
                    [("ARROW:extension:name".to_owned(), #GEOMETRY_EXTENSION_NAME.to_owned())]
                        .into_iter()
                        .collect(),
                );
                field
            }}
        } else {
            quote! {
                datafusion::arrow::datatypes::Field::new(self.name(), #return_data_type, true)
            }
        };

        quote! {
            fn return_field_from_args(
                &self,
                _args: datafusion::logical_expr::ReturnFieldArgs,
            ) -> datafusion::common::Result<std::sync::Arc<datafusion::arrow::datatypes::Field>> {
                #(#geometry_arg_checks)*
                Ok(std::sync::Arc::new(#field_expr))
            }
        }
    } else {
        quote! {}
    };

    let lowercase_alias = sql_name.to_lowercase();
    let needs_alias = &lowercase_alias != sql_name;

    let (struct_fields, struct_new, aliases_impl) = if needs_alias {
        (
            quote! {
                signature: datafusion::logical_expr::Signature,
                aliases: Vec<String>,
            },
            quote! {
                Self {
                    signature: #signature_init,
                    aliases: vec![#lowercase_alias.to_owned()],
                }
            },
            quote! {
                fn aliases(&self) -> &[String] {
                    &self.aliases
                }
            },
        )
    } else {
        (
            quote! { signature: datafusion::logical_expr::Signature, },
            quote! { Self { signature: #signature_init, } },
            quote! {},
        )
    };

    let udf_impl = quote! {
        #[doc = concat!("UDF implementation for `", #sql_name, "` with multiple signatures")]
        #[derive(Debug, PartialEq, Eq, Hash)]
        pub struct #struct_ident {
            #struct_fields
        }

        impl Default for #struct_ident {
            fn default() -> Self {
                Self::new()
            }
        }

        impl #struct_ident {
            pub fn new() -> Self {
                #struct_new
            }

            pub fn udf() -> datafusion::logical_expr::ScalarUDF {
                datafusion::logical_expr::ScalarUDF::new_from_impl(Self::new())
            }
        }

        impl datafusion::logical_expr::ScalarUDFImpl for #struct_ident {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn name(&self) -> &str {
                #sql_name
            }

            #aliases_impl

            fn signature(&self) -> &datafusion::logical_expr::Signature {
                &self.signature
            }

            fn return_type(
                &self,
                _arg_types: &[datafusion::arrow::datatypes::DataType],
            ) -> datafusion::common::Result<datafusion::arrow::datatypes::DataType> {
                Ok(#return_data_type)
            }

            #return_field_impl

            fn invoke_with_args(
                &self,
                args: datafusion::logical_expr::ScalarFunctionArgs,
            ) -> datafusion::common::Result<datafusion::logical_expr::ColumnarValue> {
                use datafusion::arrow::array::{Array, AsArray};
                use datafusion::common::{exec_err, ScalarValue};
                use datafusion::logical_expr::ColumnarValue;

                let args = args.args;

                match args.len() {
                    #(#invoke_branches)*
                    n => exec_err!(
                        "invalid number of arguments for {}: got {}, expected one of [{}]",
                        self.name(),
                        n,
                        #valid_counts_str
                    ),
                }
            }
        }
    };

    let fn_name = &input_fn.sig.ident;
    let register_fn_name = format_ident!("register_{}", fn_name);

    let register_fn = quote! {
        /// Register this function with all its overloaded signatures.
        pub fn #register_fn_name(ctx: &::datafusion::prelude::SessionContext) {
            ctx.register_udf(#struct_ident::udf());
        }
    };

    Ok(quote! {
        #impl_fn
        #udf_impl
        #register_fn
    })
}

fn generate_udf(attr: Expr, input_fn: ItemFn) -> Result<TokenStream2> {
    let sig_str = match &attr {
        Expr::Lit(ExprLit {
            lit: Lit::Str(s), ..
        }) => s.value(),
        _ => {
            return Err(Error::new_spanned(
                attr,
                "expected a string literal for function signature",
            ));
        }
    };

    let signature = FunctionSignature::parse(&sig_str)?;
    let user_fn = parse_user_function(&input_fn)?;

    let sig_arg_count = signature.args.len();
    let fn_arg_count = user_fn.args.len();

    if sig_arg_count != fn_arg_count {
        return Err(Error::new_spanned(
            &input_fn.sig,
            format!(
                "function signature has {sig_arg_count} arguments but implementation has {fn_arg_count}"
            ),
        ));
    }

    let struct_name = format!("{}UDF", to_struct_name(&signature.name));
    let struct_ident = format_ident!("{}", struct_name);
    let sql_name = &signature.name;
    let display_name = &signature.name;
    let lowercase_alias = signature.name.to_lowercase();
    let needs_alias = &lowercase_alias != sql_name;

    let return_info = ReturnTypeInfo::from_type(&signature.return_type)?;
    let return_data_type = return_info.data_type_tokens();

    let arg_types: Vec<ArgType> = signature.args.iter().map(|a| a.arg_type.clone()).collect();
    let exact_types: Vec<TokenStream2> = arg_types.iter().map(|t| t.data_type_tokens()).collect();

    let invoke_impl = generate_invoke_impl(&user_fn, &arg_types, &return_info);

    let geometry_arg_indices: Vec<usize> = arg_types
        .iter()
        .enumerate()
        .filter_map(|(idx, arg)| arg.is_geometry().then_some(idx))
        .collect();
    let geometry_arg_checks: Vec<TokenStream2> = geometry_arg_indices
        .iter()
        .map(|idx| {
            let position = idx + 1;
            quote! {
                if _args
                    .arg_fields
                    .get(#idx)
                    .and_then(|field| field.extension_type_name())
                    != Some(#GEOMETRY_EXTENSION_NAME)
                {
                    return datafusion::common::exec_err!(
                        "argument {} to {} must have extension type '{}'",
                        #position,
                        self.name(),
                        #GEOMETRY_EXTENSION_NAME
                    );
                }
            }
        })
        .collect();
    let needs_return_field = return_info.is_geometry() || !geometry_arg_indices.is_empty();
    let return_field_impl = if needs_return_field {
        let field_expr = if return_info.is_geometry() {
            quote! {{
                let field = datafusion::arrow::datatypes::Field::new(
                    self.name(),
                    #return_data_type,
                    true,
                )
                .with_metadata(
                    [("ARROW:extension:name".to_owned(), #GEOMETRY_EXTENSION_NAME.to_owned())]
                        .into_iter()
                        .collect(),
                );
                field
            }}
        } else {
            quote! {
                datafusion::arrow::datatypes::Field::new(self.name(), #return_data_type, true)
            }
        };
        quote! {
            fn return_field_from_args(
                &self,
                _args: datafusion::logical_expr::ReturnFieldArgs,
            ) -> datafusion::common::Result<std::sync::Arc<datafusion::arrow::datatypes::Field>> {
                #(#geometry_arg_checks)*
                Ok(std::sync::Arc::new(#field_expr))
            }
        }
    } else {
        quote! {}
    };

    let signature_init = quote! {
        datafusion::logical_expr::Signature::exact(
            vec![#(#exact_types),*],
            datafusion::logical_expr::Volatility::Immutable,
        )
    };

    let mut impl_fn = input_fn;
    impl_fn.vis = syn::Visibility::Inherited;

    let (struct_fields, struct_new, aliases_impl) = if needs_alias {
        (
            quote! {
                signature: datafusion::logical_expr::Signature,
                aliases: Vec<String>,
            },
            quote! {
                Self {
                    signature: #signature_init,
                    aliases: vec![#lowercase_alias.to_owned()],
                }
            },
            quote! {
                fn aliases(&self) -> &[String] {
                    &self.aliases
                }
            },
        )
    } else {
        (
            quote! { signature: datafusion::logical_expr::Signature, },
            quote! { Self { signature: #signature_init, } },
            quote! {},
        )
    };

    Ok(quote! {
        #impl_fn

        #[doc = concat!("UDF implementation for `", #sql_name, "`")]
        #[derive(Debug, PartialEq, Eq, Hash)]
        pub struct #struct_ident {
            #struct_fields
        }

        impl Default for #struct_ident {
            fn default() -> Self {
                Self::new()
            }
        }

        impl #struct_ident {
            pub fn new() -> Self {
                #struct_new
            }

            pub fn udf() -> datafusion::logical_expr::ScalarUDF {
                datafusion::logical_expr::ScalarUDF::new_from_impl(Self::new())
            }
        }

        impl datafusion::logical_expr::ScalarUDFImpl for #struct_ident {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn name(&self) -> &str {
                #display_name
            }

            #aliases_impl

            fn signature(&self) -> &datafusion::logical_expr::Signature {
                &self.signature
            }

            fn return_type(
                &self,
                _arg_types: &[datafusion::arrow::datatypes::DataType],
            ) -> datafusion::common::Result<datafusion::arrow::datatypes::DataType> {
                Ok(#return_data_type)
            }

            #return_field_impl

            fn invoke_with_args(
                &self,
                args: datafusion::logical_expr::ScalarFunctionArgs,
            ) -> datafusion::common::Result<datafusion::logical_expr::ColumnarValue> {
                use datafusion::arrow::array::{Array, AsArray};
                use datafusion::common::{exec_err, ScalarValue};
                use datafusion::logical_expr::ColumnarValue;

                let args = args.args;
                #invoke_impl
            }
        }
    })
}
