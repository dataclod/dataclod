use std::str::FromStr;

use bigdecimal::BigDecimal;
use datafusion::arrow::datatypes::{Decimal128Type, Decimal256Type, DecimalType, i256};
use half::f16;

/// Represents a constant for NULL string in your database.
pub const NULL_STR: &str = "NULL";

pub(crate) fn bool_to_str(value: bool) -> String {
    if value {
        "true".to_owned()
    } else {
        "false".to_owned()
    }
}

pub(crate) fn varchar_to_str(value: &str) -> String {
    if value.is_empty() {
        "(empty)".to_owned()
    } else {
        // Escape nulls so that github renders them correctly in the webui
        value.trim_end_matches('\n').replace("\u{0000}", "\\0")
    }
}

pub(crate) fn f16_to_str(value: f16) -> String {
    if value.is_nan() {
        // The sign of NaN can be different depending on platform.
        // So the string representation of NaN ignores the sign.
        "NaN".to_owned()
    } else if value == f16::INFINITY {
        "Infinity".to_owned()
    } else if value == f16::NEG_INFINITY {
        "-Infinity".to_owned()
    } else {
        big_decimal_to_str(BigDecimal::from_str(&value.to_string()).unwrap(), None)
    }
}

pub(crate) fn f32_to_str(value: f32) -> String {
    if value.is_nan() {
        // The sign of NaN can be different depending on platform.
        // So the string representation of NaN ignores the sign.
        "NaN".to_owned()
    } else if value == f32::INFINITY {
        "Infinity".to_owned()
    } else if value == f32::NEG_INFINITY {
        "-Infinity".to_owned()
    } else {
        big_decimal_to_str(BigDecimal::from_str(&value.to_string()).unwrap(), None)
    }
}

pub(crate) fn f64_to_str(value: f64) -> String {
    if value.is_nan() {
        // The sign of NaN can be different depending on platform.
        // So the string representation of NaN ignores the sign.
        "NaN".to_owned()
    } else if value == f64::INFINITY {
        "Infinity".to_owned()
    } else if value == f64::NEG_INFINITY {
        "-Infinity".to_owned()
    } else {
        big_decimal_to_str(BigDecimal::from_str(&value.to_string()).unwrap(), None)
    }
}

pub(crate) fn decimal_128_to_str(value: i128, scale: i8) -> String {
    let precision = u8::MAX; // does not matter
    big_decimal_to_str(
        BigDecimal::from_str(&Decimal128Type::format_decimal(value, precision, scale)).unwrap(),
        None,
    )
}

pub(crate) fn decimal_256_to_str(value: i256, scale: i8) -> String {
    let precision = u8::MAX; // does not matter
    big_decimal_to_str(
        BigDecimal::from_str(&Decimal256Type::format_decimal(value, precision, scale)).unwrap(),
        None,
    )
}

/// Converts a `BigDecimal` to its plain string representation, optionally
/// rounding to a specified number of decimal places.
///
/// If `round_digits` is `None`, the value is rounded to 12 decimal places by
/// default.
#[expect(clippy::needless_pass_by_value)]
pub(crate) fn big_decimal_to_str(value: BigDecimal, round_digits: Option<i64>) -> String {
    // Round the value to limit the number of decimal places
    let value = value.round(round_digits.unwrap_or(12)).normalized();
    // Format the value to a string
    value.to_plain_string()
}
