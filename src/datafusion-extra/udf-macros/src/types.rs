//! Type definitions and conversions for the UDF macro.
//!
//! This module defines the internal type representations used during macro
//! expansion to map SQL types to Arrow types and generate appropriate code.
//!
//! # Supported Types
//!
//! Only Arrow/DataFusion `DataType` names are accepted (case-insensitive):
//!
//! | Type Name | Arrow DataType | Rust Input | Rust Output |
//! |-----------|----------------|------------|-------------|
//! | `Float64` | Float64 | `f64` | `f64` |
//! | `Float32` | Float32 | `f32` | `f32` |
//! | `Int64` | Int64 | `i64` | `i64` |
//! | `Int32` | Int32 | `i32` | `i32` |
//! | `Int16` | Int16 | `i16` | `i16` |
//! | `Int8` | Int8 | `i8` | `i8` |
//! | `UInt64` | UInt64 | `u64` | `u64` |
//! | `UInt32` | UInt32 | `u32` | `u32` |
//! | `UInt16` | UInt16 | `u16` | `u16` |
//! | `UInt8` | UInt8 | `u8` | `u8` |
//! | `Boolean` | Boolean | `bool` | `bool` |
//! | `Utf8` / `Utf8View` | Utf8View | `&str` | `String` |
//! | `Binary` / `BinaryView` | BinaryView | `&[u8]` | `Vec<u8>` |
//! | `Date32` | Date32 | `i32` | `i32` |
//! | `Date64` | Date64 | `i64` | `i64` |
//! | `Timestamp` | Timestamp(μs) | `i64` | `i64` |
//! | `TimestampMs` | Timestamp(ms) | `i64` | `i64` |
//! | `TimestampS` | Timestamp(s) | `i64` | `i64` |
//! | `TimestampNs` | Timestamp(ns) | `i64` | `i64` |
//! | `Geometry` | BinaryView+meta | `&[u8]` | `Vec<u8>` |
//! | `Json` | Utf8View | `&str` | `String` |
//! | `Struct Name` | Struct | struct | struct |
//! | `List<T>` | List | - | - |

use proc_macro2::TokenStream;
use quote::{format_ident, quote};

/// A function argument with its type.
#[derive(Debug, Clone)]
pub struct FunctionArg {
    pub arg_type: ArgType,
}

pub const GEOMETRY_EXTENSION_NAME: &str = "Geometry";

impl FunctionArg {
    pub fn parse(s: &str) -> syn::Result<Self> {
        Ok(FunctionArg {
            arg_type: ArgType::parse(s.trim())?,
        })
    }
}

/// SQL argument types supported in function signatures.
#[derive(Debug, Clone, PartialEq)]
pub enum ArgType {
    Float64,
    Float32,
    Int64,
    Int32,
    Int16,
    Int8,
    UInt64,
    UInt32,
    UInt16,
    UInt8,
    Boolean,
    Utf8,
    Binary,
    Date32,
    Date64,
    Timestamp,
    TimestampMs,
    TimestampS,
    TimestampNs,
    Geometry,
    Json,
    Struct(String),
    List(Box<ArgType>),
}

impl ArgType {
    pub fn parse(s: &str) -> syn::Result<Self> {
        let s_trimmed = s.trim();

        // Check for List type
        if let Some(rest) = s_trimmed
            .strip_prefix("List<")
            .or_else(|| s_trimmed.strip_prefix("list<"))
        {
            let inner = rest.strip_suffix('>').ok_or_else(|| {
                syn::Error::new(
                    proc_macro2::Span::call_site(),
                    "List type must end with '>' (e.g., 'List<Int32>')",
                )
            })?;
            return Ok(ArgType::List(Box::new(ArgType::parse(inner)?)));
        }

        // Check for Struct type
        if let Some(struct_name) = s_trimmed
            .strip_prefix("Struct ")
            .or_else(|| s_trimmed.strip_prefix("struct "))
        {
            let struct_name = struct_name.trim();
            if struct_name.is_empty() {
                return Err(syn::Error::new(
                    proc_macro2::Span::call_site(),
                    "Struct type requires a name (e.g., 'Struct BBox')",
                ));
            }
            return Ok(ArgType::Struct(struct_name.to_owned()));
        }

        match s_trimmed.to_lowercase().as_str() {
            "float64" => Ok(ArgType::Float64),
            "float32" => Ok(ArgType::Float32),
            "int64" => Ok(ArgType::Int64),
            "int32" => Ok(ArgType::Int32),
            "int16" => Ok(ArgType::Int16),
            "int8" => Ok(ArgType::Int8),
            "uint64" => Ok(ArgType::UInt64),
            "uint32" => Ok(ArgType::UInt32),
            "uint16" => Ok(ArgType::UInt16),
            "uint8" => Ok(ArgType::UInt8),
            "boolean" => Ok(ArgType::Boolean),
            "utf8" | "utf8view" => Ok(ArgType::Utf8),
            "binary" | "binaryview" => Ok(ArgType::Binary),
            "date32" => Ok(ArgType::Date32),
            "date64" => Ok(ArgType::Date64),
            "timestamp" => Ok(ArgType::Timestamp),
            "timestampms" => Ok(ArgType::TimestampMs),
            "timestamps" => Ok(ArgType::TimestampS),
            "timestampns" => Ok(ArgType::TimestampNs),
            "geometry" => Ok(ArgType::Geometry),
            "json" => Ok(ArgType::Json),
            _ => {
                Err(syn::Error::new(
                    proc_macro2::Span::call_site(),
                    format!(
                        "unknown type: '{s_trimmed}'. Supported types: \
                     Float64, Float32, Int64, Int32, Int16, Int8, \
                     UInt64, UInt32, UInt16, UInt8, Boolean, Utf8, Utf8View, Binary, BinaryView, \
                     Date32, Date64, Timestamp, TimestampMs, TimestampS, TimestampNs, \
                     Geometry, Json, Struct <Name>, List<Type>"
                    ),
                ))
            }
        }
    }

    pub fn is_primitive(&self) -> bool {
        matches!(
            self,
            ArgType::Float64
                | ArgType::Float32
                | ArgType::Int64
                | ArgType::Int32
                | ArgType::Int16
                | ArgType::Int8
                | ArgType::UInt64
                | ArgType::UInt32
                | ArgType::UInt16
                | ArgType::UInt8
        )
    }

    pub fn display_name(&self) -> String {
        match self {
            ArgType::Float64 => "Float64".to_owned(),
            ArgType::Float32 => "Float32".to_owned(),
            ArgType::Int64 => "Int64".to_owned(),
            ArgType::Int32 => "Int32".to_owned(),
            ArgType::Int16 => "Int16".to_owned(),
            ArgType::Int8 => "Int8".to_owned(),
            ArgType::UInt64 => "UInt64".to_owned(),
            ArgType::UInt32 => "UInt32".to_owned(),
            ArgType::UInt16 => "UInt16".to_owned(),
            ArgType::UInt8 => "UInt8".to_owned(),
            ArgType::Boolean => "Boolean".to_owned(),
            ArgType::Utf8 => "Utf8".to_owned(),
            ArgType::Binary => "Binary".to_owned(),
            ArgType::Date32 => "Date32".to_owned(),
            ArgType::Date64 => "Date64".to_owned(),
            ArgType::Timestamp => "Timestamp".to_owned(),
            ArgType::TimestampMs => "TimestampMs".to_owned(),
            ArgType::TimestampS => "TimestampS".to_owned(),
            ArgType::TimestampNs => "TimestampNs".to_owned(),
            ArgType::Geometry => "Geometry".to_owned(),
            ArgType::Json => "Json".to_owned(),
            ArgType::Struct(name) => format!("Struct {name}"),
            ArgType::List(inner) => format!("List<{}>", inner.display_name()),
        }
    }

    pub fn data_type_tokens(&self) -> TokenStream {
        match self {
            ArgType::Float64 => quote!(datafusion::arrow::datatypes::DataType::Float64),
            ArgType::Float32 => quote!(datafusion::arrow::datatypes::DataType::Float32),
            ArgType::Int64 => quote!(datafusion::arrow::datatypes::DataType::Int64),
            ArgType::Int32 => quote!(datafusion::arrow::datatypes::DataType::Int32),
            ArgType::Int16 => quote!(datafusion::arrow::datatypes::DataType::Int16),
            ArgType::Int8 => quote!(datafusion::arrow::datatypes::DataType::Int8),
            ArgType::UInt64 => quote!(datafusion::arrow::datatypes::DataType::UInt64),
            ArgType::UInt32 => quote!(datafusion::arrow::datatypes::DataType::UInt32),
            ArgType::UInt16 => quote!(datafusion::arrow::datatypes::DataType::UInt16),
            ArgType::UInt8 => quote!(datafusion::arrow::datatypes::DataType::UInt8),
            ArgType::Boolean => quote!(datafusion::arrow::datatypes::DataType::Boolean),
            ArgType::Utf8 | ArgType::Json => {
                quote!(datafusion::arrow::datatypes::DataType::Utf8View)
            }
            ArgType::Binary | ArgType::Geometry => {
                quote!(datafusion::arrow::datatypes::DataType::BinaryView)
            }
            ArgType::Date32 => quote!(datafusion::arrow::datatypes::DataType::Date32),
            ArgType::Date64 => quote!(datafusion::arrow::datatypes::DataType::Date64),
            ArgType::Timestamp => {
                quote!(datafusion::arrow::datatypes::DataType::Timestamp(
                    datafusion::arrow::datatypes::TimeUnit::Microsecond,
                    None
                ))
            }
            ArgType::TimestampMs => {
                quote!(datafusion::arrow::datatypes::DataType::Timestamp(
                    datafusion::arrow::datatypes::TimeUnit::Millisecond,
                    None
                ))
            }
            ArgType::TimestampS => {
                quote!(datafusion::arrow::datatypes::DataType::Timestamp(
                    datafusion::arrow::datatypes::TimeUnit::Second,
                    None
                ))
            }
            ArgType::TimestampNs => {
                quote!(datafusion::arrow::datatypes::DataType::Timestamp(
                    datafusion::arrow::datatypes::TimeUnit::Nanosecond,
                    None
                ))
            }
            ArgType::Struct(name) => {
                let struct_ident = format_ident!("{}", name);
                quote!(#struct_ident::arrow_data_type())
            }
            ArgType::List(inner) => {
                let inner_type = inner.data_type_tokens();
                quote!(datafusion::arrow::datatypes::DataType::List(
                    std::sync::Arc::new(datafusion::arrow::datatypes::Field::new("item", #inner_type, true))
                ))
            }
        }
    }

    pub fn array_type_tokens(&self) -> TokenStream {
        match self {
            ArgType::Float64 => quote!(datafusion::arrow::array::Float64Array),
            ArgType::Float32 => quote!(datafusion::arrow::array::Float32Array),
            ArgType::Int64 => quote!(datafusion::arrow::array::Int64Array),
            ArgType::Int32 => quote!(datafusion::arrow::array::Int32Array),
            ArgType::Int16 => quote!(datafusion::arrow::array::Int16Array),
            ArgType::Int8 => quote!(datafusion::arrow::array::Int8Array),
            ArgType::UInt64 => quote!(datafusion::arrow::array::UInt64Array),
            ArgType::UInt32 => quote!(datafusion::arrow::array::UInt32Array),
            ArgType::UInt16 => quote!(datafusion::arrow::array::UInt16Array),
            ArgType::UInt8 => quote!(datafusion::arrow::array::UInt8Array),
            ArgType::Boolean => quote!(datafusion::arrow::array::BooleanArray),
            ArgType::Utf8 | ArgType::Json => quote!(datafusion::arrow::array::StringViewArray),
            ArgType::Binary | ArgType::Geometry => {
                quote!(datafusion::arrow::array::BinaryViewArray)
            }
            ArgType::Date32 => quote!(datafusion::arrow::array::Date32Array),
            ArgType::Date64 => quote!(datafusion::arrow::array::Date64Array),
            ArgType::Timestamp => quote!(datafusion::arrow::array::TimestampMicrosecondArray),
            ArgType::TimestampMs => quote!(datafusion::arrow::array::TimestampMillisecondArray),
            ArgType::TimestampS => quote!(datafusion::arrow::array::TimestampSecondArray),
            ArgType::TimestampNs => quote!(datafusion::arrow::array::TimestampNanosecondArray),
            ArgType::Struct(_) => quote!(datafusion::arrow::array::StructArray),
            ArgType::List(_) => quote!(datafusion::arrow::array::ListArray),
        }
    }

    pub fn scalar_value_tokens(&self) -> TokenStream {
        match self {
            ArgType::Float64 => quote!(ScalarValue::Float64),
            ArgType::Float32 => quote!(ScalarValue::Float32),
            ArgType::Int64 => quote!(ScalarValue::Int64),
            ArgType::Int32 => quote!(ScalarValue::Int32),
            ArgType::Int16 => quote!(ScalarValue::Int16),
            ArgType::Int8 => quote!(ScalarValue::Int8),
            ArgType::UInt64 => quote!(ScalarValue::UInt64),
            ArgType::UInt32 => quote!(ScalarValue::UInt32),
            ArgType::UInt16 => quote!(ScalarValue::UInt16),
            ArgType::UInt8 => quote!(ScalarValue::UInt8),
            ArgType::Boolean => quote!(ScalarValue::Boolean),
            ArgType::Utf8 | ArgType::Json => quote!(ScalarValue::Utf8View),
            ArgType::Binary | ArgType::Geometry => quote!(ScalarValue::BinaryView),
            ArgType::Date32 => quote!(ScalarValue::Date32),
            ArgType::Date64 => quote!(ScalarValue::Date64),
            ArgType::Timestamp => quote!(|v| ScalarValue::TimestampMicrosecond(v, None)),
            ArgType::TimestampMs => quote!(|v| ScalarValue::TimestampMillisecond(v, None)),
            ArgType::TimestampS => quote!(|v| ScalarValue::TimestampSecond(v, None)),
            ArgType::TimestampNs => quote!(|v| ScalarValue::TimestampNanosecond(v, None)),
            ArgType::Struct(_) => {
                // For struct, we need special handling - return a closure placeholder
                // Actual implementation is in to_scalar_tokens
                quote!(|v| panic!("Use to_scalar_tokens for Struct types"))
            }
            ArgType::List(inner) => {
                let inner_dt = inner.data_type_tokens();
                quote!(|v: Option<datafusion::arrow::array::ArrayRef>| {
                    match v {
                        Some(arr) => ScalarValue::List(std::sync::Arc::new(
                            datafusion::arrow::array::ListArray::new(
                                std::sync::Arc::new(datafusion::arrow::datatypes::Field::new("item", #inner_dt, true)),
                                datafusion::arrow::buffer::OffsetBuffer::from_lengths([arr.len()]),
                                arr,
                                None,
                            )
                        )),
                        None => ScalarValue::List(std::sync::Arc::new(
                            datafusion::arrow::array::ListArray::new_null(
                                std::sync::Arc::new(datafusion::arrow::datatypes::Field::new("item", #inner_dt, true)),
                                1,
                            )
                        )),
                    }
                })
            }
        }
    }

    pub fn supports_scalar_value(&self) -> bool {
        !matches!(self, ArgType::Struct(_) | ArgType::List(_))
    }

    pub fn is_geometry(&self) -> bool {
        matches!(self, ArgType::Geometry)
    }

    pub fn is_list(&self) -> bool {
        matches!(self, ArgType::List(_))
    }

    pub fn is_struct(&self) -> bool {
        matches!(self, ArgType::Struct(_))
    }

    pub fn inner_type(&self) -> Option<&ArgType> {
        match self {
            ArgType::List(inner) => Some(inner),
            _ => None,
        }
    }

    pub fn is_timestamp(&self) -> bool {
        matches!(
            self,
            ArgType::Timestamp | ArgType::TimestampMs | ArgType::TimestampS | ArgType::TimestampNs
        )
    }

    pub fn to_scalar_tokens(&self, result_expr: TokenStream) -> TokenStream {
        match self {
            ArgType::Timestamp
            | ArgType::TimestampMs
            | ArgType::TimestampS
            | ArgType::TimestampNs => {
                let scalar_type = self.scalar_value_tokens();
                quote!((#scalar_type)(#result_expr))
            }
            ArgType::List(inner) => {
                let inner_dt = inner.data_type_tokens();
                quote! {{
                    let result_opt: Option<datafusion::arrow::array::ArrayRef> = #result_expr;
                    match result_opt {
                        Some(arr) => ScalarValue::List(std::sync::Arc::new(
                            datafusion::arrow::array::ListArray::new(
                                std::sync::Arc::new(datafusion::arrow::datatypes::Field::new("item", #inner_dt, true)),
                                datafusion::arrow::buffer::OffsetBuffer::from_lengths([arr.len()]),
                                arr,
                                None,
                            )
                        )),
                        None => ScalarValue::List(std::sync::Arc::new(
                            datafusion::arrow::array::ListArray::new_null(
                                std::sync::Arc::new(datafusion::arrow::datatypes::Field::new("item", #inner_dt, true)),
                                1,
                            )
                        )),
                    }
                }}
            }
            _ => {
                let scalar_type = self.scalar_value_tokens();
                quote!(#scalar_type(#result_expr))
            }
        }
    }

    /// Generate code to extract a value at a given index.
    /// Assumes `arr_var` is already downcast to the correct array type.
    pub fn value_at_index_tokens(&self, arr_var: &str, idx_var: &str) -> TokenStream {
        let arr = format_ident!("{}", arr_var);
        let idx = format_ident!("{}", idx_var);

        match self {
            ArgType::Struct(name) => {
                let struct_ident = format_ident!("{}", name);
                quote! {
                    #struct_ident::from_struct_array(#arr, #idx)
                }
            }
            ArgType::List(_) => {
                // For List, value() returns an ArrayRef, we wrap it in Option
                quote! {
                    if #arr.is_null(#idx) {
                        None
                    } else {
                        Some(#arr.value(#idx))
                    }
                }
            }
            _ => {
                quote! {
                    if #arr.is_null(#idx) { None } else { Some(#arr.value(#idx)) }
                }
            }
        }
    }

    pub fn scalar_pattern_tokens(&self) -> TokenStream {
        match self {
            ArgType::Float64 => quote!(ScalarValue::Float64(v)),
            ArgType::Float32 => quote!(ScalarValue::Float32(v)),
            ArgType::Int64 => quote!(ScalarValue::Int64(v)),
            ArgType::Int32 => quote!(ScalarValue::Int32(v)),
            ArgType::Int16 => quote!(ScalarValue::Int16(v)),
            ArgType::Int8 => quote!(ScalarValue::Int8(v)),
            ArgType::UInt64 => quote!(ScalarValue::UInt64(v)),
            ArgType::UInt32 => quote!(ScalarValue::UInt32(v)),
            ArgType::UInt16 => quote!(ScalarValue::UInt16(v)),
            ArgType::UInt8 => quote!(ScalarValue::UInt8(v)),
            ArgType::Boolean => quote!(ScalarValue::Boolean(v)),
            ArgType::Utf8 | ArgType::Json => quote!(ScalarValue::Utf8View(v)),
            ArgType::Binary | ArgType::Geometry => quote!(ScalarValue::BinaryView(v)),
            ArgType::Date32 => quote!(ScalarValue::Date32(v)),
            ArgType::Date64 => quote!(ScalarValue::Date64(v)),
            ArgType::Timestamp => quote!(ScalarValue::TimestampMicrosecond(v, _)),
            ArgType::TimestampMs => quote!(ScalarValue::TimestampMillisecond(v, _)),
            ArgType::TimestampS => quote!(ScalarValue::TimestampSecond(v, _)),
            ArgType::TimestampNs => quote!(ScalarValue::TimestampNanosecond(v, _)),
            ArgType::Struct(_) => quote!(ScalarValue::Struct(v)),
            ArgType::List(_) => quote!(ScalarValue::List(v)),
        }
    }

    pub fn scalar_pattern_tokens_named(&self, var_name: &str) -> TokenStream {
        let var = format_ident!("{}", var_name);
        match self {
            ArgType::Float64 => quote!(ScalarValue::Float64(#var)),
            ArgType::Float32 => quote!(ScalarValue::Float32(#var)),
            ArgType::Int64 => quote!(ScalarValue::Int64(#var)),
            ArgType::Int32 => quote!(ScalarValue::Int32(#var)),
            ArgType::Int16 => quote!(ScalarValue::Int16(#var)),
            ArgType::Int8 => quote!(ScalarValue::Int8(#var)),
            ArgType::UInt64 => quote!(ScalarValue::UInt64(#var)),
            ArgType::UInt32 => quote!(ScalarValue::UInt32(#var)),
            ArgType::UInt16 => quote!(ScalarValue::UInt16(#var)),
            ArgType::UInt8 => quote!(ScalarValue::UInt8(#var)),
            ArgType::Boolean => quote!(ScalarValue::Boolean(#var)),
            ArgType::Utf8 | ArgType::Json => quote!(ScalarValue::Utf8View(#var)),
            ArgType::Binary | ArgType::Geometry => quote!(ScalarValue::BinaryView(#var)),
            ArgType::Date32 => quote!(ScalarValue::Date32(#var)),
            ArgType::Date64 => quote!(ScalarValue::Date64(#var)),
            ArgType::Timestamp => quote!(ScalarValue::TimestampMicrosecond(#var, _)),
            ArgType::TimestampMs => quote!(ScalarValue::TimestampMillisecond(#var, _)),
            ArgType::TimestampS => quote!(ScalarValue::TimestampSecond(#var, _)),
            ArgType::TimestampNs => quote!(ScalarValue::TimestampNanosecond(#var, _)),
            ArgType::Struct(_) => quote!(ScalarValue::Struct(#var)),
            ArgType::List(_) => quote!(ScalarValue::List(#var)),
        }
    }

    pub fn scalar_convert_expr(&self, var_name: &str) -> TokenStream {
        let var = format_ident!("{}", var_name);
        match self {
            ArgType::Utf8 | ArgType::Json => quote!(#var.as_deref()),
            ArgType::Binary | ArgType::Geometry => quote!(#var.as_deref()),
            // For Struct scalar, extract the StructArray from Arc and get value at index 0
            ArgType::Struct(name) => {
                let struct_ident = format_ident!("{}", name);
                quote! {
                    #var.as_ref().and_then(|arr| #struct_ident::from_struct_array(arr.as_ref(), 0))
                }
            }
            // For List scalar, extract the ListArray from Arc and get value at index 0
            ArgType::List(_) => {
                quote! {
                    #var.as_ref().and_then(|arr| {
                        use datafusion::arrow::array::Array;
                        if arr.is_null(0) {
                            None
                        } else {
                            Some(arr.as_list::<i32>().value(0))
                        }
                    })
                }
            }
            // For primitive types, ScalarValue contains Option<T>, and we get &Option<T>
            // Use * to dereference &Option<T> to Option<T> for Copy types
            _ => quote!(*#var),
        }
    }

    /// Get the downcast method call (without variable prefix).
    /// Used when you have `arr.#method` pattern.
    pub fn array_downcast_method(&self) -> TokenStream {
        match self {
            ArgType::Float64 => {
                quote!(as_primitive::<datafusion::arrow::datatypes::Float64Type>())
            }
            ArgType::Float32 => {
                quote!(as_primitive::<datafusion::arrow::datatypes::Float32Type>())
            }
            ArgType::Int64 => {
                quote!(as_primitive::<datafusion::arrow::datatypes::Int64Type>())
            }
            ArgType::Int32 => {
                quote!(as_primitive::<datafusion::arrow::datatypes::Int32Type>())
            }
            ArgType::Int16 => {
                quote!(as_primitive::<datafusion::arrow::datatypes::Int16Type>())
            }
            ArgType::Int8 => {
                quote!(as_primitive::<datafusion::arrow::datatypes::Int8Type>())
            }
            ArgType::UInt64 => {
                quote!(as_primitive::<datafusion::arrow::datatypes::UInt64Type>())
            }
            ArgType::UInt32 => {
                quote!(as_primitive::<datafusion::arrow::datatypes::UInt32Type>())
            }
            ArgType::UInt16 => {
                quote!(as_primitive::<datafusion::arrow::datatypes::UInt16Type>())
            }
            ArgType::UInt8 => {
                quote!(as_primitive::<datafusion::arrow::datatypes::UInt8Type>())
            }
            ArgType::Boolean => quote!(as_boolean()),
            ArgType::Utf8 | ArgType::Json => quote!(as_string_view()),
            ArgType::Binary | ArgType::Geometry => quote!(as_binary_view()),
            ArgType::Date32 => {
                quote!(as_primitive::<datafusion::arrow::datatypes::Date32Type>())
            }
            ArgType::Date64 => {
                quote!(as_primitive::<datafusion::arrow::datatypes::Date64Type>())
            }
            ArgType::Timestamp => {
                quote!(as_primitive::<
                    datafusion::arrow::datatypes::TimestampMicrosecondType,
                >())
            }
            ArgType::TimestampMs => {
                quote!(as_primitive::<
                    datafusion::arrow::datatypes::TimestampMillisecondType,
                >())
            }
            ArgType::TimestampS => {
                quote!(as_primitive::<
                    datafusion::arrow::datatypes::TimestampSecondType,
                >())
            }
            ArgType::TimestampNs => {
                quote!(as_primitive::<
                    datafusion::arrow::datatypes::TimestampNanosecondType,
                >())
            }
            ArgType::Struct(_) => quote!(as_struct()),
            ArgType::List(_) => quote!(as_list::<i32>()),
        }
    }

    /// Get the downcast expression with a specific variable name.
    /// Returns `var.as_*()` style expression.
    pub fn array_type_tokens_named(&self, var_name: &str) -> TokenStream {
        let var = format_ident!("{}", var_name);
        let method = self.array_downcast_method();
        quote!(#var.#method)
    }
}

/// Information about the return type of a UDF.
#[derive(Debug)]
pub struct ReturnTypeInfo {
    pub arg_type: ArgType,
}

impl ReturnTypeInfo {
    pub fn from_type(type_name: &str) -> syn::Result<Self> {
        Ok(Self {
            arg_type: ArgType::parse(type_name)?,
        })
    }

    pub fn data_type_tokens(&self) -> TokenStream {
        self.arg_type.data_type_tokens()
    }

    pub fn array_type_tokens(&self) -> TokenStream {
        self.arg_type.array_type_tokens()
    }

    pub fn supports_scalar_value(&self) -> bool {
        self.arg_type.supports_scalar_value()
    }

    pub fn is_geometry(&self) -> bool {
        self.arg_type.is_geometry()
    }

    pub fn is_primitive(&self) -> bool {
        self.arg_type.is_primitive()
    }

    pub fn is_struct(&self) -> bool {
        matches!(self.arg_type, ArgType::Struct(_))
    }

    pub fn is_list(&self) -> bool {
        matches!(self.arg_type, ArgType::List(_))
    }

    pub fn struct_name(&self) -> Option<&str> {
        match &self.arg_type {
            ArgType::Struct(name) => Some(name),
            _ => None,
        }
    }

    pub fn struct_builder_ident(&self) -> Option<proc_macro2::Ident> {
        self.struct_name()
            .map(|name| format_ident!("{}Builder", name))
    }

    pub fn inner_type(&self) -> Option<&ArgType> {
        self.arg_type.inner_type()
    }

    pub fn to_scalar_tokens(&self, result_expr: TokenStream) -> TokenStream {
        self.arg_type.to_scalar_tokens(result_expr)
    }
}
