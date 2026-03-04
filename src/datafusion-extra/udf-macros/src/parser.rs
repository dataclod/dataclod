//! Parser for SQL-style function signatures.
//!
//! This module parses function signature strings into structured data that
//! the macro uses to generate UDF implementations.
//!
//! # Signature Format
//!
//! ```text
//! function_name(arg1_type, arg2_type) -> return_type
//! ```
//!
//! - **Args**: `Type` (e.g., `Geometry`, `Float64`)
//!
//! # Examples
//!
//! | Signature | Description |
//! |-----------|-------------|
//! | `st_area(Geometry) -> Float64` | Unary function |
//! | `st_buffer(Geometry, Float64) -> Geometry` | Binary function |

use crate::types::FunctionArg;

/// A parsed function signature.
///
/// Contains all information extracted from a signature string needed to
/// generate the UDF implementation.
#[derive(Debug)]
pub struct FunctionSignature {
    /// SQL function name (e.g., `"st_area"`).
    pub name: String,
    /// List of function arguments.
    pub args: Vec<FunctionArg>,
    /// Return type name (e.g., `"float64"`).
    pub return_type: String,
}

impl FunctionSignature {
    /// Parse a function signature string.
    ///
    /// # Format
    ///
    /// `name(type1, type2?) -> return_type`
    ///
    /// # Errors
    ///
    /// Returns an error if the signature is malformed or contains unknown
    /// types.
    pub fn parse(s: &str) -> syn::Result<Self> {
        let s = s.trim();

        // Split by "->" to get name+args and return type
        let (name_args, return_type) = match s.split_once("->") {
            Some((left, right)) => (left.trim(), right.trim()),
            None => {
                return Err(syn::Error::new(
                    proc_macro2::Span::call_site(),
                    format!(
                        "missing return type in signature '{s}'. Expected format: \
                         'function_name(arg1, arg2) -> ReturnType'"
                    ),
                ));
            }
        };

        // Find opening parenthesis
        let open_paren = name_args.find('(').ok_or_else(|| {
            syn::Error::new(
                proc_macro2::Span::call_site(),
                format!("missing '(' in signature '{s}'"),
            )
        })?;

        // Find matching closing parenthesis using depth counter
        let mut depth = 0;
        let mut close_paren = None;
        for (i, c) in name_args.char_indices() {
            match c {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        close_paren = Some(i);
                        break;
                    }
                }
                _ => {}
            }
        }

        let close_paren = close_paren.ok_or_else(|| {
            syn::Error::new(
                proc_macro2::Span::call_site(),
                format!("missing closing ')' in signature '{s}'"),
            )
        })?;

        // Validate nothing comes after closing paren
        let after_paren = name_args[close_paren + 1..].trim();
        if !after_paren.is_empty() {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                format!("unexpected content after ')' in signature '{s}': '{after_paren}'"),
            ));
        }

        let name = name_args[..open_paren].trim().to_owned();
        if name.is_empty() {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                "function name cannot be empty",
            ));
        }

        let args_str = name_args[open_paren + 1..close_paren].trim();

        // Parse arguments (handle nested generics like List<T>)
        let args = if args_str.is_empty() {
            Vec::new()
        } else {
            split_args(args_str)
                .iter()
                .map(|s| FunctionArg::parse(s.trim()))
                .collect::<syn::Result<Vec<_>>>()?
        };

        Ok(FunctionSignature {
            name,
            args,
            return_type: normalize_type(return_type),
        })
    }
}

/// Split arguments by comma, respecting nested angle brackets.
/// e.g., "List<Int32>, Utf8" -> ["List<Int32>", "Utf8"]
fn split_args(s: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let mut depth = 0;
    let mut start = 0;

    for (i, c) in s.char_indices() {
        match c {
            '<' => depth += 1,
            '>' => depth -= 1,
            ',' if depth == 0 => {
                result.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }

    if start < s.len() {
        result.push(&s[start..]);
    }

    result
}

/// Normalize type names to canonical forms.
///
/// Only accepts Arrow/DataFusion `DataType` names. No aliases are supported.
fn normalize_type(ty: &str) -> String {
    let ty = ty.trim();

    // Handle List types (e.g., "List<Int32>")
    if let Some(inner) = ty
        .strip_prefix("List<")
        .or_else(|| ty.strip_prefix("list<"))
        .and_then(|rest| rest.strip_suffix('>'))
    {
        let inner_normalized = normalize_type(inner);
        return format!("List<{inner_normalized}>");
    }

    // Preserve Struct types as-is (struct name is case-sensitive)
    if let Some(name) = ty
        .strip_prefix("Struct ")
        .or_else(|| ty.strip_prefix("struct "))
    {
        return format!("Struct {name}");
    }

    // Return as-is (case preserved for validation in ArgType::parse)
    ty.to_owned()
}
