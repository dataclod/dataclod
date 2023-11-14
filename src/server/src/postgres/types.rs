use std::sync::Arc;
use std::time::UNIX_EPOCH;

use chrono::{DateTime, Duration, NaiveDateTime, NaiveTime, Utc};
use datafusion::arrow::array::{Array, BinaryArray, BooleanArray, PrimitiveArray, StringArray};
use datafusion::arrow::datatypes::*;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DFSchema;
use datafusion::prelude::DataFrame;
use futures::{stream, StreamExt};
use pgwire::api::portal::Format;
use pgwire::api::results::{DataRowEncoder, FieldInfo, QueryResponse};
use pgwire::api::Type;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

pub async fn encode_dataframe<'a>(
    df: DataFrame, format: &Format,
) -> PgWireResult<QueryResponse<'a>> {
    let schema = df.schema();
    let fields = Arc::new(encode_schema(schema, format)?);

    let recordbatch_stream = df
        .execute_stream()
        .await
        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

    let fields_ref = fields.clone();
    let pg_row_stream = recordbatch_stream
        .map(move |rb: datafusion::error::Result<RecordBatch>| {
            let rb = rb.unwrap();
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
        .flatten();

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
                field.name().to_string(),
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
        DataType::Int32 | DataType::UInt32 => Type::INT4,
        DataType::Int64 | DataType::UInt64 => Type::INT8,
        DataType::Timestamp(..) => Type::TIMESTAMP,
        DataType::Time32(_) | DataType::Time64(_) => Type::TIME,
        DataType::Date32 | DataType::Date64 => Type::DATE,
        DataType::Binary => Type::BYTEA,
        DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Utf8 => Type::VARCHAR,
        _ => {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("Unsupported Datatype {df_type}"),
            ))));
        }
    })
}

pub fn encode_value(
    encoder: &mut DataRowEncoder, arr: &Arc<dyn Array>, idx: usize,
) -> PgWireResult<()> {
    match arr.data_type() {
        DataType::Boolean => encoder.encode_field(&get_bool_value(arr, idx))?,
        DataType::Int8 => encoder.encode_field(&get_i8_value(arr, idx))?,
        DataType::Int16 => encoder.encode_field(&get_i16_value(arr, idx))?,
        DataType::Int32 => encoder.encode_field(&get_i32_value(arr, idx))?,
        DataType::Int64 => encoder.encode_field(&get_i64_value(arr, idx))?,
        DataType::UInt8 => encoder.encode_field(&(get_u8_value(arr, idx) as i8))?,
        DataType::UInt16 => encoder.encode_field(&(get_u16_value(arr, idx) as i16))?,
        DataType::UInt32 => encoder.encode_field(&get_u32_value(arr, idx))?,
        DataType::UInt64 => encoder.encode_field(&(get_u64_value(arr, idx) as i64))?,
        DataType::Float32 => encoder.encode_field(&get_f32_value(arr, idx))?,
        DataType::Float64 => encoder.encode_field(&get_f64_value(arr, idx))?,
        DataType::Utf8 => encoder.encode_field(&get_utf8_value(arr, idx))?,
        DataType::Binary => encoder.encode_field(&get_binary_value(arr, idx))?,
        DataType::Timestamp(TimeUnit::Second, _) => {
            let ts = NaiveDateTime::from_timestamp_opt(get_ts_value(arr, idx), 0);
            encoder.encode_field(&ts)?
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let ts = NaiveDateTime::from_timestamp_millis(get_ts_millis_value(arr, idx));
            encoder.encode_field(&ts)?
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let ts = NaiveDateTime::from_timestamp_micros(get_ts_micros_value(arr, idx));
            encoder.encode_field(&ts)?
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let nanos = get_ts_nanos_value(arr, idx);
            let secs = nanos.div_euclid(1_000_000_000);
            let nsecs = nanos.rem_euclid(1_000_000_000) as u32;
            let ts = NaiveDateTime::from_timestamp_opt(secs, nsecs);
            encoder.encode_field(&ts)?
        }
        DataType::Date32 => {
            let days = get_date32_value(arr, idx);
            let utc_time: DateTime<Utc> = DateTime::<Utc>::from(UNIX_EPOCH);
            let date = utc_time.checked_add_signed(Duration::days(days as i64));
            encoder.encode_field(&date)?
        }
        DataType::Date64 => {
            let millis = get_date64_value(arr, idx);
            let utc_time: DateTime<Utc> = DateTime::<Utc>::from(UNIX_EPOCH);
            let date = utc_time.checked_add_signed(Duration::milliseconds(millis));
            encoder.encode_field(&date)?
        }
        DataType::Time32(TimeUnit::Second) => {
            let secs = get_time32_value(arr, idx).rem_euclid(86_400) as u32;
            let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, 0);
            encoder.encode_field(&time)?
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            let millis = get_time32_millis_value(arr, idx).rem_euclid(86_400_000);
            let secs = millis.div_euclid(1000) as u32;
            let nano = millis.rem_euclid(1000) as u32 * 1_000_000;
            let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, nano);
            encoder.encode_field(&time)?
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let micros = get_time64_micros_value(arr, idx).rem_euclid(86_400_000_000);
            let secs = micros.div_euclid(1_000_000) as u32;
            let nano = micros.rem_euclid(1_000_000) as u32 * 1000;
            let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, nano);
            encoder.encode_field(&time)?
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let nanos = get_time64_nanos_value(arr, idx).rem_euclid(86_400_000_000_000);
            let secs = nanos.div_euclid(1_000_000_000) as u32;
            let nano = nanos.rem_euclid(1_000_000_000) as u32;
            let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, nano);
            encoder.encode_field(&time)?
        }

        _ => {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!(
                    "Unsupported Datatype {} and array {:?}",
                    arr.data_type(),
                    &arr
                ),
            ))));
        }
    }
    Ok(())
}

fn get_bool_value(arr: &Arc<dyn Array>, idx: usize) -> bool {
    arr.as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .value(idx)
}

macro_rules! get_primitive_value {
    ($name:ident, $t:ty, $pt:ty) => {
        fn $name(arr: &Arc<dyn Array>, idx: usize) -> $pt {
            arr.as_any()
                .downcast_ref::<PrimitiveArray<$t>>()
                .unwrap()
                .value(idx)
        }
    };
}

get_primitive_value!(get_i8_value, Int8Type, i8);
get_primitive_value!(get_i16_value, Int16Type, i16);
get_primitive_value!(get_i32_value, Int32Type, i32);
get_primitive_value!(get_i64_value, Int64Type, i64);
get_primitive_value!(get_u8_value, UInt8Type, u8);
get_primitive_value!(get_u16_value, UInt16Type, u16);
get_primitive_value!(get_u32_value, UInt32Type, u32);
get_primitive_value!(get_u64_value, UInt64Type, u64);
get_primitive_value!(get_f32_value, Float32Type, f32);
get_primitive_value!(get_f64_value, Float64Type, f64);
get_primitive_value!(get_ts_value, TimestampSecondType, i64);
get_primitive_value!(get_ts_millis_value, TimestampMillisecondType, i64);
get_primitive_value!(get_ts_micros_value, TimestampMicrosecondType, i64);
get_primitive_value!(get_ts_nanos_value, TimestampNanosecondType, i64);
get_primitive_value!(get_date32_value, Date32Type, i32);
get_primitive_value!(get_date64_value, Date64Type, i64);
get_primitive_value!(get_time32_value, Time32SecondType, i32);
get_primitive_value!(get_time32_millis_value, Time32MillisecondType, i32);
get_primitive_value!(get_time64_micros_value, Time64MicrosecondType, i64);
get_primitive_value!(get_time64_nanos_value, Time64NanosecondType, i64);

fn get_utf8_value(arr: &Arc<dyn Array>, idx: usize) -> &str {
    arr.as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(idx)
}

fn get_binary_value(arr: &Arc<dyn Array>, idx: usize) -> &[u8] {
    arr.as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap()
        .value(idx)
}
