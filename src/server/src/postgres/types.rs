mod decimal;
mod interval;

use std::sync::Arc;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, IntervalMonthDayNano, IntervalUnit, TimeUnit};
use datafusion::common::DFSchema;
use datafusion::prelude::DataFrame;
use datafusion::scalar::ScalarValue;
use datafusion::sql::parser::Statement;
use decimal::PgDecimal;
use duplicate::duplicate_item;
use futures::{TryStreamExt, stream};
use interval::PgInterval;
use num_traits::NumCast;
use pgwire::api::Type;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::results::{DataRowEncoder, FieldInfo, QueryResponse};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use super::utils::*;

pub fn encode_parameters(portal: &Portal<Statement>) -> PgWireResult<Vec<ScalarValue>> {
    portal
        .statement
        .parameter_types
        .iter()
        .enumerate()
        .map(|(i, parameter_type)| {
            Ok(match parameter_type.as_ref().unwrap_or(&Type::UNKNOWN) {
                &Type::UNKNOWN => ScalarValue::Null,
                &Type::BOOL => ScalarValue::Boolean(portal.parameter(i, &Type::BOOL)?),
                &Type::CHAR => ScalarValue::Int8(portal.parameter(i, &Type::CHAR)?),
                &Type::INT2 => ScalarValue::Int16(portal.parameter(i, &Type::INT2)?),
                &Type::INT4 => ScalarValue::Int32(portal.parameter(i, &Type::INT4)?),
                &Type::INT8 => ScalarValue::Int64(portal.parameter(i, &Type::INT8)?),
                &Type::OID => ScalarValue::UInt32(portal.parameter(i, &Type::OID)?),
                &Type::FLOAT4 => ScalarValue::Float32(portal.parameter(i, &Type::FLOAT4)?),
                &Type::FLOAT8 => ScalarValue::Float64(portal.parameter(i, &Type::FLOAT8)?),
                &Type::TIMESTAMP => {
                    ScalarValue::TimestampMicrosecond(portal.parameter(i, &Type::TIMESTAMP)?, None)
                }
                &Type::DATE => ScalarValue::Date32(portal.parameter(i, &Type::DATE)?),
                &Type::TIME => ScalarValue::Time64Microsecond(portal.parameter(i, &Type::TIME)?),
                &Type::INTERVAL => {
                    ScalarValue::IntervalMonthDayNano(
                        portal
                            .parameter::<PgInterval>(i, &Type::INTERVAL)?
                            .map(|interval| {
                                IntervalMonthDayNano {
                                    months: interval.months,
                                    days: interval.days,
                                    nanoseconds: interval.microseconds * 1000,
                                }
                            }),
                    )
                }
                &Type::BYTEA => ScalarValue::Binary(portal.parameter(i, &Type::BYTEA)?),
                &Type::NAME => ScalarValue::Utf8(portal.parameter(i, &Type::NAME)?),
                &Type::TEXT => ScalarValue::Utf8(portal.parameter(i, &Type::TEXT)?),
                &Type::BPCHAR => ScalarValue::Utf8(portal.parameter(i, &Type::BPCHAR)?),
                &Type::VARCHAR => ScalarValue::Utf8(portal.parameter(i, &Type::VARCHAR)?),
                typ => return Err(PgWireError::InvalidRustTypeForParameter(typ.to_string())),
            })
        })
        .collect()
}

pub async fn encode_dataframe(
    df: DataFrame, format: &Format, _row_limit: usize,
) -> PgWireResult<QueryResponse> {
    let schema = df.schema();
    let fields = Arc::new(encode_schema(schema, format)?);

    let recordbatch_stream = df
        .execute_stream()
        .await
        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

    let fields_ref = fields.clone();
    let pg_row_stream = recordbatch_stream
        .map_err(|e| PgWireError::ApiError(Box::new(e)))
        .map_ok(move |rb| {
            let rows = rb.num_rows();
            let cols = rb.num_columns();

            let fields = fields_ref.clone();

            let row_stream = (0..rows).map(move |row| {
                let mut encoder = DataRowEncoder::new(fields.clone());
                for col in 0..cols {
                    let array = rb.column(col);
                    if array.is_null(row) {
                        encoder.encode_field(&None::<i8>).unwrap();
                    } else {
                        encode_value(&mut encoder, array, row).unwrap();
                    }
                }
                encoder.finish()
            });

            stream::iter(row_stream)
        })
        .try_flatten();

    Ok(QueryResponse::new(fields, pg_row_stream))
}

pub fn encode_schema(schema: &DFSchema, format: &Format) -> PgWireResult<Vec<FieldInfo>> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            let pg_type = into_pg_type(field.data_type())?;
            Ok(FieldInfo::new(
                field.name().to_owned(),
                None,
                None,
                pg_type,
                format.format_for(idx),
            ))
        })
        .collect::<PgWireResult<Vec<FieldInfo>>>()
}

pub fn into_pg_type(df_type: &DataType) -> PgWireResult<Type> {
    Ok(match df_type {
        DataType::Null => Type::UNKNOWN,
        DataType::Boolean => Type::BOOL,
        DataType::Int8 | DataType::UInt8 => Type::CHAR,
        DataType::Int16 | DataType::UInt16 => Type::INT2,
        DataType::Int32 => Type::INT4,
        DataType::UInt32 => Type::OID,
        DataType::Int64 | DataType::UInt64 => Type::INT8,
        DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Timestamp(..) => Type::TIMESTAMP,
        DataType::Date32 | DataType::Date64 => Type::DATE,
        DataType::Time32(_) | DataType::Time64(_) => Type::TIME,
        DataType::Interval(_) => Type::INTERVAL,
        DataType::Binary
        | DataType::FixedSizeBinary(_)
        | DataType::LargeBinary
        | DataType::BinaryView => Type::BYTEA,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Type::VARCHAR,
        DataType::List(field)
        | DataType::ListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::LargeList(field)
        | DataType::LargeListView(field) => {
            match field.data_type() {
                DataType::Boolean => Type::BOOL_ARRAY,
                DataType::Int8 | DataType::UInt8 => Type::CHAR_ARRAY,
                DataType::Int16 | DataType::UInt16 => Type::INT2_ARRAY,
                DataType::Int32 => Type::INT4_ARRAY,
                DataType::UInt32 => Type::OID_ARRAY,
                DataType::Int64 | DataType::UInt64 => Type::INT8_ARRAY,
                DataType::Float32 => Type::FLOAT4_ARRAY,
                DataType::Float64 => Type::FLOAT8_ARRAY,
                DataType::Timestamp(..) => Type::TIMESTAMP_ARRAY,
                DataType::Date32 | DataType::Date64 => Type::DATE_ARRAY,
                DataType::Time32(_) | DataType::Time64(_) => Type::TIME_ARRAY,
                DataType::Interval(_) => Type::INTERVAL_ARRAY,
                DataType::Binary
                | DataType::FixedSizeBinary(_)
                | DataType::LargeBinary
                | DataType::BinaryView => Type::BYTEA_ARRAY,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Type::VARCHAR_ARRAY,
                value_type => {
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "XX000".to_owned(),
                        format!("Unsupported List Datatype {value_type}"),
                    ))));
                }
            }
        }
        DataType::Decimal128(..) | DataType::Decimal256(..) => Type::NUMERIC,
        _ => {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("Unsupported Datatype {df_type}"),
            ))));
        }
    })
}

pub fn encode_value(encoder: &mut DataRowEncoder, arr: &ArrayRef, idx: usize) -> PgWireResult<()> {
    match arr.data_type() {
        DataType::Boolean => encode_bool_value(encoder, arr, idx)?,
        DataType::Int8 => encode_i8_value(encoder, arr, idx)?,
        DataType::Int16 => encode_i16_value(encoder, arr, idx)?,
        DataType::Int32 => encode_i32_value(encoder, arr, idx)?,
        DataType::Int64 => encode_i64_value(encoder, arr, idx)?,
        DataType::UInt8 => encode_u8_value(encoder, arr, idx)?,
        DataType::UInt16 => encode_u16_value(encoder, arr, idx)?,
        DataType::UInt32 => encode_u32_value(encoder, arr, idx)?,
        DataType::UInt64 => encode_u64_value(encoder, arr, idx)?,
        DataType::Float32 => encode_f32_value(encoder, arr, idx)?,
        DataType::Float64 => encode_f64_value(encoder, arr, idx)?,
        DataType::Timestamp(TimeUnit::Second, _) => encode_ts_value(encoder, arr, idx)?,
        DataType::Timestamp(TimeUnit::Millisecond, _) => encode_ts_millis_value(encoder, arr, idx)?,
        DataType::Timestamp(TimeUnit::Microsecond, _) => encode_ts_micros_value(encoder, arr, idx)?,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => encode_ts_nanos_value(encoder, arr, idx)?,
        DataType::Interval(IntervalUnit::YearMonth) => {
            encode_interval_year_month_value(encoder, arr, idx)?
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            encode_interval_day_time_value(encoder, arr, idx)?
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            encode_interval_month_day_nano_value(encoder, arr, idx)?
        }
        DataType::Date32 => encode_date32_value(encoder, arr, idx)?,
        DataType::Date64 => encode_date64_value(encoder, arr, idx)?,
        DataType::Time32(TimeUnit::Second) => encode_time32_value(encoder, arr, idx)?,
        DataType::Time32(TimeUnit::Millisecond) => encode_time32_millis_value(encoder, arr, idx)?,
        DataType::Time64(TimeUnit::Microsecond) => encode_time64_micros_value(encoder, arr, idx)?,
        DataType::Time64(TimeUnit::Nanosecond) => encode_time64_nanos_value(encoder, arr, idx)?,
        DataType::Binary => encode_binary_value(encoder, arr, idx)?,
        DataType::FixedSizeBinary(_) => encode_fixed_size_binary_value(encoder, arr, idx)?,
        DataType::LargeBinary => encode_large_binary_value(encoder, arr, idx)?,
        DataType::BinaryView => encode_binary_view_value(encoder, arr, idx)?,
        DataType::Utf8 => encode_utf8_value(encoder, arr, idx)?,
        DataType::LargeUtf8 => encode_large_utf8_value(encoder, arr, idx)?,
        DataType::Utf8View => encode_utf8_view_value(encoder, arr, idx)?,
        list_type @ (DataType::List(field)
        | DataType::ListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::LargeList(field)
        | DataType::LargeListView(field)) => {
            let list_value = match list_type {
                DataType::List(_) => arr.as_any().downcast_ref::<ListArray>().unwrap().value(idx),
                DataType::ListView(_) => {
                    arr.as_any()
                        .downcast_ref::<ListViewArray>()
                        .unwrap()
                        .value(idx)
                }
                DataType::FixedSizeList(..) => {
                    arr.as_any()
                        .downcast_ref::<FixedSizeListArray>()
                        .unwrap()
                        .value(idx)
                }
                DataType::LargeList(_) => {
                    arr.as_any()
                        .downcast_ref::<LargeListArray>()
                        .unwrap()
                        .value(idx)
                }
                DataType::LargeListView(_) => {
                    arr.as_any()
                        .downcast_ref::<LargeListViewArray>()
                        .unwrap()
                        .value(idx)
                }
                _ => panic!("{list_type} is not a valid list type"),
            };
            assert_eq!(list_value.data_type(), field.data_type());

            match field.data_type() {
                DataType::Boolean => encode_bool_list_value(encoder, &list_value)?,
                DataType::Int8 => encode_i8_list_value(encoder, &list_value)?,
                DataType::Int16 => encode_i16_list_value(encoder, &list_value)?,
                DataType::Int32 => encode_i32_list_value(encoder, &list_value)?,
                DataType::Int64 => encode_i64_list_value(encoder, &list_value)?,
                DataType::UInt8 => encode_u8_list_value(encoder, &list_value)?,
                DataType::UInt16 => encode_u16_list_value(encoder, &list_value)?,
                DataType::UInt32 => encode_u32_list_value(encoder, &list_value)?,
                DataType::UInt64 => encode_u64_list_value(encoder, &list_value)?,
                DataType::Float32 => encode_f32_list_value(encoder, &list_value)?,
                DataType::Float64 => encode_f64_list_value(encoder, &list_value)?,
                DataType::Timestamp(TimeUnit::Second, _) => {
                    encode_ts_list_value(encoder, &list_value)?
                }
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    encode_ts_millis_list_value(encoder, &list_value)?
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    encode_ts_micros_list_value(encoder, &list_value)?
                }
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    encode_ts_nanos_list_value(encoder, &list_value)?
                }
                DataType::Date32 => encode_date32_list_value(encoder, &list_value)?,
                DataType::Date64 => encode_date64_list_value(encoder, &list_value)?,
                DataType::Time32(TimeUnit::Second) => {
                    encode_time32_list_value(encoder, &list_value)?
                }
                DataType::Time32(TimeUnit::Millisecond) => {
                    encode_time32_millis_list_value(encoder, &list_value)?
                }
                DataType::Time64(TimeUnit::Microsecond) => {
                    encode_time64_micros_list_value(encoder, &list_value)?
                }
                DataType::Time64(TimeUnit::Nanosecond) => {
                    encode_time64_nanos_list_value(encoder, &list_value)?
                }
                DataType::Interval(IntervalUnit::YearMonth) => {
                    encode_interval_year_month_list_value(encoder, &list_value)?
                }
                DataType::Interval(IntervalUnit::DayTime) => {
                    encode_interval_day_time_list_value(encoder, &list_value)?
                }
                DataType::Interval(IntervalUnit::MonthDayNano) => {
                    encode_interval_month_day_nano_list_value(encoder, &list_value)?
                }
                DataType::Binary => encode_binary_list_value(encoder, &list_value)?,
                DataType::FixedSizeBinary(_) => {
                    encode_fixed_size_binary_list_value(encoder, &list_value)?
                }
                DataType::LargeBinary => encode_large_binary_list_value(encoder, &list_value)?,
                DataType::BinaryView => encode_binary_view_list_value(encoder, &list_value)?,
                DataType::Utf8 => encode_utf8_list_value(encoder, &list_value)?,
                DataType::LargeUtf8 => encode_large_utf8_list_value(encoder, &list_value)?,
                DataType::Utf8View => encode_utf8_view_list_value(encoder, &list_value)?,
                DataType::Decimal128(precision, scale) => {
                    encoder.encode_field(
                        &list_value
                            .as_any()
                            .downcast_ref::<Decimal128Array>()
                            .unwrap()
                            .iter()
                            .map(|opt| opt.map(|dec| PgDecimal::new(dec, *precision, *scale)))
                            .collect::<Vec<Option<PgDecimal>>>(),
                    )?
                }
                DataType::Decimal256(precision, scale) => {
                    encoder.encode_field(
                        &list_value
                            .as_any()
                            .downcast_ref::<Decimal256Array>()
                            .unwrap()
                            .iter()
                            .map(|opt| {
                                opt.map(|dec| PgDecimal::new(dec.as_i128(), *precision, *scale))
                            })
                            .collect::<Vec<Option<PgDecimal>>>(),
                    )?
                }
                value_type => {
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "XX000".to_owned(),
                        format!(
                            "Unsupported List value Datatype {} and Array {:?}",
                            value_type, &arr
                        ),
                    ))));
                }
            }
        }
        DataType::Decimal128(precision, scale) => {
            encoder.encode_field(&PgDecimal::new(
                arr.as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap()
                    .value(idx),
                *precision,
                *scale,
            ))?
        }
        DataType::Decimal256(precision, scale) => {
            encoder.encode_field(&PgDecimal::new(
                arr.as_any()
                    .downcast_ref::<Decimal256Array>()
                    .unwrap()
                    .value(idx)
                    .as_i128(),
                *precision,
                *scale,
            ))?
        }
        arr_type => {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("Unsupported Datatype {} and Array {:?}", arr_type, &arr),
            ))));
        }
    }
    Ok(())
}

#[duplicate_item(
    fn_name                             arr_ty;
    [encode_bool_value]                 [BooleanArray];
    [encode_i8_value]                   [Int8Array];
    [encode_i16_value]                  [Int16Array];
    [encode_i32_value]                  [Int32Array];
    [encode_i64_value]                  [Int64Array];
    [encode_u32_value]                  [UInt32Array];
    [encode_f32_value]                  [Float32Array];
    [encode_f64_value]                  [Float64Array];
    [encode_binary_value]               [BinaryArray];
    [encode_fixed_size_binary_value]    [FixedSizeBinaryArray];
    [encode_large_binary_value]         [LargeBinaryArray];
    [encode_binary_view_value]          [BinaryViewArray];
    [encode_utf8_value]                 [StringArray];
    [encode_large_utf8_value]           [LargeStringArray];
    [encode_utf8_view_value]            [StringViewArray];
)]
fn fn_name(encoder: &mut DataRowEncoder, arr: &ArrayRef, idx: usize) -> PgWireResult<()> {
    encoder.encode_field(&arr.as_any().downcast_ref::<arr_ty>().unwrap().value(idx))
}

#[duplicate_item(
    fn_name                                 arr_ty                      closure_fn;
    [encode_u8_value]                       [UInt8Array]                [<i8 as NumCast>::from];
    [encode_u16_value]                      [UInt16Array]               [<i16 as NumCast>::from];
    [encode_u64_value]                      [UInt64Array]               [<i64 as NumCast>::from];
    [encode_ts_value]                       [TimestampSecondArray]      [make_ts];
    [encode_ts_millis_value]                [TimestampMillisecondArray] [make_ts_millis];
    [encode_ts_micros_value]                [TimestampMicrosecondArray] [make_ts_micros];
    [encode_ts_nanos_value]                 [TimestampNanosecondArray]  [make_ts_nanos];
    [encode_interval_year_month_value]      [IntervalYearMonthArray]    [PgInterval::from];
    [encode_interval_day_time_value]        [IntervalDayTimeArray]      [PgInterval::from];
    [encode_interval_month_day_nano_value]  [IntervalMonthDayNanoArray] [PgInterval::from];
    [encode_date32_value]                   [Date32Array]               [make_date32];
    [encode_date64_value]                   [Date64Array]               [make_date64];
    [encode_time32_value]                   [Time32SecondArray]         [make_time32];
    [encode_time32_millis_value]            [Time32MillisecondArray]    [make_time32_millis];
    [encode_time64_micros_value]            [Time64MicrosecondArray]    [make_time64_micros];
    [encode_time64_nanos_value]             [Time64NanosecondArray]     [make_time64_nanos];
)]
fn fn_name(encoder: &mut DataRowEncoder, arr: &ArrayRef, idx: usize) -> PgWireResult<()> {
    encoder.encode_field(&closure_fn(
        arr.as_any().downcast_ref::<arr_ty>().unwrap().value(idx),
    ))
}

#[duplicate_item(
    fn_name                                 value_ty                collect_ty;
    [encode_bool_list_value]                [BooleanArray]          [bool];
    [encode_i8_list_value]                  [Int8Array]             [i8];
    [encode_i16_list_value]                 [Int16Array]            [i16];
    [encode_i32_list_value]                 [Int32Array]            [i32];
    [encode_i64_list_value]                 [Int64Array]            [i64];
    [encode_u32_list_value]                 [UInt32Array]           [u32];
    [encode_f32_list_value]                 [Float32Array]          [f32];
    [encode_f64_list_value]                 [Float64Array]          [f64];
    [encode_binary_list_value]              [BinaryArray]           [&[u8]];
    [encode_fixed_size_binary_list_value]   [FixedSizeBinaryArray]  [&[u8]];
    [encode_large_binary_list_value]        [LargeBinaryArray]      [&[u8]];
    [encode_binary_view_list_value]         [BinaryViewArray]       [&[u8]];
    [encode_utf8_list_value]                [StringArray]           [&str];
    [encode_large_utf8_list_value]          [LargeStringArray]      [&str];
    [encode_utf8_view_list_value]           [StringViewArray]       [&str];
)]
fn fn_name(encoder: &mut DataRowEncoder, list_value: &ArrayRef) -> PgWireResult<()> {
    encoder.encode_field(
        &list_value
            .as_any()
            .downcast_ref::<value_ty>()
            .unwrap()
            .iter()
            .collect::<Vec<Option<collect_ty>>>(),
    )
}

#[duplicate_item(
    fn_name                                     value_ty                    closure_fn                      collect_ty;
    [encode_u8_list_value]                      [UInt8Array]                [<i8 as NumCast>::from]         [i8];
    [encode_u16_list_value]                     [UInt16Array]               [<i16 as NumCast>::from]        [i16];
    [encode_u64_list_value]                     [UInt64Array]               [<i64 as NumCast>::from]        [i64];
    [encode_ts_list_value]                      [TimestampSecondArray]      [make_ts]                       [NaiveDateTime];
    [encode_ts_millis_list_value]               [TimestampMillisecondArray] [make_ts_millis]                [NaiveDateTime];
    [encode_ts_micros_list_value]               [TimestampMicrosecondArray] [make_ts_micros]                [NaiveDateTime];
    [encode_ts_nanos_list_value]                [TimestampNanosecondArray]  [make_ts_nanos]                 [NaiveDateTime];
    [encode_interval_year_month_list_value]     [IntervalYearMonthArray]    [|v| Some(PgInterval::from(v))] [PgInterval];
    [encode_interval_day_time_list_value]       [IntervalDayTimeArray]      [|v| Some(PgInterval::from(v))] [PgInterval];
    [encode_interval_month_day_nano_list_value] [IntervalMonthDayNanoArray] [|v| Some(PgInterval::from(v))] [PgInterval];
    [encode_date32_list_value]                  [Date32Array]               [make_date32]                   [NaiveDate];
    [encode_date64_list_value]                  [Date64Array]               [make_date64]                   [NaiveDate];
    [encode_time32_list_value]                  [Time32SecondArray]         [make_time32]                   [NaiveTime];
    [encode_time32_millis_list_value]           [Time32MillisecondArray]    [make_time32_millis]            [NaiveTime];
    [encode_time64_micros_list_value]           [Time64MicrosecondArray]    [make_time64_micros]            [NaiveTime];
    [encode_time64_nanos_list_value]            [Time64NanosecondArray]     [make_time64_nanos]             [NaiveTime];
)]
fn fn_name(encoder: &mut DataRowEncoder, list_value: &ArrayRef) -> PgWireResult<()> {
    encoder.encode_field(
        &list_value
            .as_any()
            .downcast_ref::<value_ty>()
            .unwrap()
            .iter()
            .map(|opt| opt.and_then(closure_fn))
            .collect::<Vec<Option<collect_ty>>>(),
    )
}
