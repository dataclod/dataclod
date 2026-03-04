//! Implementation of the `#[derive(StructType)]` macro.
//!
//! This module generates the glue code needed to use Rust structs as Arrow
//! `StructArray` types in UDF return values and arguments.
//!
//! # Generated Code
//!
//! For a struct like:
//!
//! ```ignore
//! #[derive(StructType)]
//! struct Point { x: f64, y: f64 }
//! ```
//!
//! The macro generates:
//!
//! - `Point::arrow_fields()` - Arrow field definitions for the struct
//! - `Point::arrow_data_type()` - The `DataType::Struct` for this type
//! - `Point::from_struct_array(arr, idx)` - Extract a value from a
//!   `StructArray`
//! - `PointBuilder` - A builder for creating arrays of `Point` values
//!
//! # Supported Field Types
//!
//! - Primitives: `f64`, `f32`, `i64`, `i32`, `i16`, `i8`, `u64`-`u8`, `bool`
//! - Strings: `String`
//! - Binary: `Vec<u8>`

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Error, Fields, Result, Type};

/// Generate all implementations for a `#[derive(StructType)]` struct.
pub fn generate_struct_type(input: DeriveInput) -> Result<TokenStream> {
    let name = &input.ident;
    let builder_name = format_ident!("{}Builder", name);

    let fields = match &input.data {
        Data::Struct(data) => {
            match &data.fields {
                Fields::Named(fields) => &fields.named,
                _ => {
                    return Err(Error::new_spanned(
                        &input,
                        "StructType only supports structs with named fields",
                    ));
                }
            }
        }
        _ => {
            return Err(Error::new_spanned(
                &input,
                "StructType can only be derived for structs",
            ));
        }
    };

    // Collect field information
    let field_names: Vec<_> = fields
        .iter()
        .map(|f| f.ident.as_ref().unwrap().clone())
        .collect();

    // Generate field definitions for Arrow schema
    let field_defs: Vec<TokenStream> = fields
        .iter()
        .map(|f| {
            let field_name = f.ident.as_ref().unwrap();
            let field_name_str = field_name.to_string();
            let (data_type, nullable) = rust_type_to_arrow_type(&f.ty);

            quote! {
                datafusion::arrow::datatypes::Field::new(#field_name_str, #data_type, #nullable)
            }
        })
        .collect();

    // Generate builder fields (including validity bitmap for struct-level nulls)
    let builder_fields: Vec<TokenStream> = fields
        .iter()
        .map(|f| {
            let field_name = f.ident.as_ref().unwrap();
            let builder_type = rust_type_to_builder_type(&f.ty);
            quote! { #field_name: #builder_type }
        })
        .collect();
    let validity_field = quote! { validity: datafusion::arrow::array::NullBufferBuilder };

    // Generate builder field initializers
    let builder_inits: Vec<TokenStream> = fields
        .iter()
        .map(|f| {
            let field_name = f.ident.as_ref().unwrap();
            let init = rust_type_to_builder_init(&f.ty);
            quote! { #field_name: #init }
        })
        .collect();
    let validity_init =
        quote! { validity: datafusion::arrow::array::NullBufferBuilder::new(capacity) };

    // Generate append calls (including validity = true for non-null values)
    let append_calls: Vec<TokenStream> = fields
        .iter()
        .map(|f| {
            let field_name = f.ident.as_ref().unwrap();
            let append = rust_type_to_append(&f.ty, quote!(value.#field_name));
            quote! { self.#field_name.#append; }
        })
        .collect();
    let append_valid = quote! { self.validity.append_non_null(); };

    // Generate append_null calls for each field (including validity = false)
    let append_null_calls: Vec<TokenStream> = fields
        .iter()
        .map(|f| {
            let field_name = f.ident.as_ref().unwrap();
            quote! { self.#field_name.append_null(); }
        })
        .collect();
    let append_null_valid = quote! { self.validity.append_null(); };

    // Generate finish calls
    let finish_calls: Vec<TokenStream> = fields
        .iter()
        .map(|f| {
            let field_name = f.ident.as_ref().unwrap();
            quote! {
                std::sync::Arc::new(self.#field_name.finish()) as datafusion::arrow::array::ArrayRef
            }
        })
        .collect();

    // Generate field extraction for from_struct_array
    let field_extractions: Vec<TokenStream> = fields
        .iter()
        .enumerate()
        .map(|(idx, f)| {
            let field_name = f.ident.as_ref().unwrap();
            let extraction = rust_type_to_extraction(&f.ty, idx);
            quote! {
                let #field_name = #extraction;
            }
        })
        .collect();

    let field_count = fields.len();

    Ok(quote! {
        impl #name {
            /// Returns the Arrow field definitions for this struct type.
            pub fn arrow_fields() -> Vec<datafusion::arrow::datatypes::Field> {
                vec![#(#field_defs),*]
            }

            /// Returns the Arrow DataType for this struct type.
            pub fn arrow_data_type() -> datafusion::arrow::datatypes::DataType {
                datafusion::arrow::datatypes::DataType::Struct(
                    datafusion::arrow::datatypes::Fields::from(Self::arrow_fields())
                )
            }

            /// Get a value from a StructArray at the specified index.
            ///
            /// Returns `None` if the index is out of bounds or the value is null.
            pub fn from_struct_array(
                arr: &datafusion::arrow::array::StructArray,
                idx: usize,
            ) -> Option<Self> {
                use datafusion::arrow::array::Array;

                if idx >= arr.len() || arr.is_null(idx) {
                    return None;
                }

                #(#field_extractions)*

                Some(Self {
                    #(#field_names,)*
                })
            }

            /// Get the number of fields in this struct.
            pub const fn num_fields() -> usize {
                #field_count
            }
        }

        /// Builder for creating arrays of #name structs.
        pub struct #builder_name {
            #validity_field,
            #(#builder_fields,)*
        }

        impl #builder_name {
            /// Create a new builder with default capacity.
            pub fn new() -> Self {
                Self::with_capacity(1024)
            }

            /// Create a new builder with the specified capacity.
            pub fn with_capacity(capacity: usize) -> Self {
                Self {
                    #validity_init,
                    #(#builder_inits,)*
                }
            }

            /// Append a value to the builder.
            pub fn append(&mut self, value: #name) {
                #append_valid
                #(#append_calls)*
            }

            /// Append an optional value to the builder.
            pub fn append_option(&mut self, value: Option<#name>) {
                match value {
                    Some(v) => self.append(v),
                    None => self.append_null(),
                }
            }

            /// Append a null value to the builder.
            pub fn append_null(&mut self) {
                #append_null_valid
                #(#append_null_calls)*
            }

            /// Finish building and return a StructArray.
            pub fn finish(&mut self) -> datafusion::arrow::array::StructArray {
                let arrays: Vec<datafusion::arrow::array::ArrayRef> = vec![
                    #(#finish_calls,)*
                ];
                datafusion::arrow::array::StructArray::new(
                    datafusion::arrow::datatypes::Fields::from(#name::arrow_fields()),
                    arrays,
                    self.validity.finish(),
                )
            }

            /// Finish building and return an ArrayRef.
            pub fn finish_as_array(&mut self) -> datafusion::arrow::array::ArrayRef {
                std::sync::Arc::new(self.finish())
            }
        }

        impl Default for #builder_name {
            fn default() -> Self {
                Self::new()
            }
        }
    })
}

/// Convert a Rust type to Arrow `DataType` and nullability.
fn rust_type_to_arrow_type(ty: &Type) -> (TokenStream, bool) {
    let ty_str = quote!(#ty).to_string().replace(' ', "");

    // Check for Option<T>
    if ty_str.starts_with("Option<") {
        let inner = ty_str
            .strip_prefix("Option<")
            .and_then(|s| s.strip_suffix('>'))
            .unwrap_or(&ty_str);
        let (inner_type, _) = rust_type_str_to_arrow(inner);
        return (inner_type, true);
    }

    let (data_type, _) = rust_type_str_to_arrow(&ty_str);
    (data_type, false)
}

fn rust_type_str_to_arrow(ty_str: &str) -> (TokenStream, bool) {
    match ty_str {
        "f64" => {
            (
                quote!(datafusion::arrow::datatypes::DataType::Float64),
                false,
            )
        }
        "f32" => {
            (
                quote!(datafusion::arrow::datatypes::DataType::Float32),
                false,
            )
        }
        "i64" => (quote!(datafusion::arrow::datatypes::DataType::Int64), false),
        "i32" => (quote!(datafusion::arrow::datatypes::DataType::Int32), false),
        "i16" => (quote!(datafusion::arrow::datatypes::DataType::Int16), false),
        "i8" => (quote!(datafusion::arrow::datatypes::DataType::Int8), false),
        "u64" => {
            (
                quote!(datafusion::arrow::datatypes::DataType::UInt64),
                false,
            )
        }
        "u32" => {
            (
                quote!(datafusion::arrow::datatypes::DataType::UInt32),
                false,
            )
        }
        "u16" => {
            (
                quote!(datafusion::arrow::datatypes::DataType::UInt16),
                false,
            )
        }
        "u8" => (quote!(datafusion::arrow::datatypes::DataType::UInt8), false),
        "bool" => {
            (
                quote!(datafusion::arrow::datatypes::DataType::Boolean),
                false,
            )
        }
        "String" | "&str" => {
            (
                quote!(datafusion::arrow::datatypes::DataType::Utf8View),
                false,
            )
        }
        "Vec<u8>" | "&[u8]" => {
            (
                quote!(datafusion::arrow::datatypes::DataType::BinaryView),
                false,
            )
        }
        _ => {
            (
                quote!(compile_error!(
                    "Unsupported field type in StructType derive"
                )),
                false,
            )
        }
    }
}

/// Convert a Rust type to the appropriate Arrow builder type.
fn rust_type_to_builder_type(ty: &Type) -> TokenStream {
    let ty_str = quote!(#ty).to_string().replace(' ', "");

    // Handle Option<T> - use the same builder, just with nullable support
    let inner_type = if ty_str.starts_with("Option<") {
        ty_str
            .strip_prefix("Option<")
            .and_then(|s| s.strip_suffix('>'))
            .unwrap_or(&ty_str)
    } else {
        &ty_str
    };

    match inner_type {
        "f64" => quote!(datafusion::arrow::array::Float64Builder),
        "f32" => quote!(datafusion::arrow::array::Float32Builder),
        "i64" => quote!(datafusion::arrow::array::Int64Builder),
        "i32" => quote!(datafusion::arrow::array::Int32Builder),
        "i16" => quote!(datafusion::arrow::array::Int16Builder),
        "i8" => quote!(datafusion::arrow::array::Int8Builder),
        "u64" => quote!(datafusion::arrow::array::UInt64Builder),
        "u32" => quote!(datafusion::arrow::array::UInt32Builder),
        "u16" => quote!(datafusion::arrow::array::UInt16Builder),
        "u8" => quote!(datafusion::arrow::array::UInt8Builder),
        "bool" => quote!(datafusion::arrow::array::BooleanBuilder),
        "String" | "&str" => quote!(datafusion::arrow::array::StringViewBuilder),
        "Vec<u8>" | "&[u8]" => quote!(datafusion::arrow::array::BinaryViewBuilder),
        _ => {
            quote!(compile_error!(
                "Unsupported field type in StructType derive"
            ))
        }
    }
}

/// Generate builder initialization code.
fn rust_type_to_builder_init(ty: &Type) -> TokenStream {
    let ty_str = quote!(#ty).to_string().replace(' ', "");

    let inner_type = if ty_str.starts_with("Option<") {
        ty_str
            .strip_prefix("Option<")
            .and_then(|s| s.strip_suffix('>'))
            .unwrap_or(&ty_str)
    } else {
        &ty_str
    };

    match inner_type {
        "f64" => {
            quote!(datafusion::arrow::array::Float64Builder::with_capacity(
                capacity
            ))
        }
        "f32" => {
            quote!(datafusion::arrow::array::Float32Builder::with_capacity(
                capacity
            ))
        }
        "i64" => {
            quote!(datafusion::arrow::array::Int64Builder::with_capacity(
                capacity
            ))
        }
        "i32" => {
            quote!(datafusion::arrow::array::Int32Builder::with_capacity(
                capacity
            ))
        }
        "i16" => {
            quote!(datafusion::arrow::array::Int16Builder::with_capacity(
                capacity
            ))
        }
        "i8" => {
            quote!(datafusion::arrow::array::Int8Builder::with_capacity(
                capacity
            ))
        }
        "u64" => {
            quote!(datafusion::arrow::array::UInt64Builder::with_capacity(
                capacity
            ))
        }
        "u32" => {
            quote!(datafusion::arrow::array::UInt32Builder::with_capacity(
                capacity
            ))
        }
        "u16" => {
            quote!(datafusion::arrow::array::UInt16Builder::with_capacity(
                capacity
            ))
        }
        "u8" => {
            quote!(datafusion::arrow::array::UInt8Builder::with_capacity(
                capacity
            ))
        }
        "bool" => {
            quote!(datafusion::arrow::array::BooleanBuilder::with_capacity(
                capacity
            ))
        }
        "String" | "&str" => {
            quote!(datafusion::arrow::array::StringViewBuilder::with_capacity(
                capacity
            ))
        }
        "Vec<u8>" | "&[u8]" => {
            quote!(datafusion::arrow::array::BinaryViewBuilder::with_capacity(
                capacity
            ))
        }
        _ => {
            quote!(compile_error!(
                "Unsupported field type in StructType derive"
            ))
        }
    }
}

/// Generate append method call for a field.
fn rust_type_to_append(ty: &Type, value_expr: TokenStream) -> TokenStream {
    let ty_str = quote!(#ty).to_string().replace(' ', "");

    if ty_str.starts_with("Option<") {
        // For Option types, use append_option
        quote!(append_option(#value_expr))
    } else {
        // For non-option types, use append_value
        match ty_str.as_str() {
            "String" => quote!(append_value(&#value_expr)),
            "&str" => quote!(append_value(#value_expr)),
            "Vec<u8>" => quote!(append_value(&#value_expr)),
            "&[u8]" => quote!(append_value(#value_expr)),
            _ => quote!(append_value(#value_expr)),
        }
    }
}

/// Generate code to extract a field value from a `StructArray` at a given
/// index.
fn rust_type_to_extraction(ty: &Type, field_idx: usize) -> TokenStream {
    let ty_str = quote!(#ty).to_string().replace(' ', "");

    let is_option = ty_str.starts_with("Option<");
    let inner_type = if is_option {
        ty_str
            .strip_prefix("Option<")
            .and_then(|s| s.strip_suffix('>'))
            .unwrap_or(&ty_str)
    } else {
        &ty_str
    };

    let array_downcast = match inner_type {
        "f64" => quote!(as_primitive::<datafusion::arrow::datatypes::Float64Type>()),
        "f32" => quote!(as_primitive::<datafusion::arrow::datatypes::Float32Type>()),
        "i64" => quote!(as_primitive::<datafusion::arrow::datatypes::Int64Type>()),
        "i32" => quote!(as_primitive::<datafusion::arrow::datatypes::Int32Type>()),
        "i16" => quote!(as_primitive::<datafusion::arrow::datatypes::Int16Type>()),
        "i8" => quote!(as_primitive::<datafusion::arrow::datatypes::Int8Type>()),
        "u64" => quote!(as_primitive::<datafusion::arrow::datatypes::UInt64Type>()),
        "u32" => quote!(as_primitive::<datafusion::arrow::datatypes::UInt32Type>()),
        "u16" => quote!(as_primitive::<datafusion::arrow::datatypes::UInt16Type>()),
        "u8" => quote!(as_primitive::<datafusion::arrow::datatypes::UInt8Type>()),
        "bool" => quote!(as_boolean()),
        "String" | "&str" => quote!(as_string_view()),
        "Vec<u8>" | "&[u8]" => quote!(as_binary_view()),
        _ => {
            quote!(compile_error!(
                "Unsupported field type in StructType derive"
            ))
        }
    };

    let value_conversion = match inner_type {
        "String" => quote!(.to_owned()),
        "Vec<u8>" => quote!(.to_vec()),
        _ => quote!(),
    };

    if is_option {
        quote! {{
            use datafusion::arrow::array::{Array, AsArray};
            let col = arr.column(#field_idx);
            let typed = col.#array_downcast;
            if typed.is_null(idx) {
                None
            } else {
                Some(typed.value(idx)#value_conversion)
            }
        }}
    } else {
        quote! {{
            use datafusion::arrow::array::{Array, AsArray};
            let col = arr.column(#field_idx);
            let typed = col.#array_downcast;
            if typed.is_null(idx) {
                return None; // Required field is null
            }
            typed.value(idx)#value_conversion
        }}
    }
}
