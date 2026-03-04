//! Utility functions for the UDF macro.

/// Convert a function name to a valid Rust struct name.
///
/// Preserves existing uppercase letters and removes underscores.
/// - `ST_Area` → `STArea`
/// - `st_area` → `StArea`
/// - `ST_Contains` → `STContains`
pub fn to_struct_name(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut capitalize_next = true;

    for c in s.chars() {
        if c == '_' {
            capitalize_next = true;
        } else if capitalize_next && c.is_ascii_lowercase() {
            result.push(c.to_ascii_uppercase());
            capitalize_next = false;
        } else {
            result.push(c);
            capitalize_next = false;
        }
    }

    result
}
