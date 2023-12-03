use std::sync::Arc;
use std::time::UNIX_EPOCH;

use chrono::{DateTime, Duration, NaiveDateTime, NaiveTime, Utc};
use datafusion::arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, ListArray,
    StringArray, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt32Array,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
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
        DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Timestamp(..) => Type::TIMESTAMP,
        DataType::Date32 | DataType::Date64 => Type::DATE,
        DataType::Time32(_) | DataType::Time64(_) => Type::TIME,
        DataType::Binary | DataType::LargeBinary => Type::BYTEA,
        DataType::Utf8 | DataType::LargeUtf8 => Type::VARCHAR,
        DataType::List(field) | DataType::LargeList(field) => {
            match field.data_type() {
                DataType::Boolean => Type::BOOL_ARRAY,
                DataType::Int8 | DataType::UInt8 => Type::CHAR_ARRAY,
                DataType::Int16 | DataType::UInt16 => Type::INT2_ARRAY,
                DataType::Int32 | DataType::UInt32 => Type::INT4_ARRAY,
                DataType::Int64 | DataType::UInt64 => Type::INT8_ARRAY,
                DataType::Float32 => Type::FLOAT4_ARRAY,
                DataType::Float64 => Type::FLOAT8_ARRAY,
                DataType::Timestamp(..) => Type::TIMESTAMP_ARRAY,
                DataType::Date32 | DataType::Date64 => Type::DATE_ARRAY,
                DataType::Time32(_) | DataType::Time64(_) => Type::TIME_ARRAY,
                DataType::Binary | DataType::LargeBinary => Type::BYTEA_ARRAY,
                DataType::Utf8 | DataType::LargeUtf8 => Type::VARCHAR_ARRAY,
                list_type => {
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "XX000".to_owned(),
                        format!("Unsupported List Datatype {list_type}"),
                    ))));
                }
            }
        }
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
        DataType::Int8 | DataType::UInt8 => encode_i8_value(encoder, arr, idx)?,
        DataType::Int16 | DataType::UInt16 => encode_i16_value(encoder, arr, idx)?,
        DataType::Int32 => encode_i32_value(encoder, arr, idx)?,
        DataType::UInt32 => encode_u32_value(encoder, arr, idx)?,
        DataType::Int64 | DataType::UInt64 => encode_i64_value(encoder, arr, idx)?,
        DataType::Float32 => encode_f32_value(encoder, arr, idx)?,
        DataType::Float64 => encode_f64_value(encoder, arr, idx)?,
        DataType::Timestamp(TimeUnit::Second, _) => encode_ts_value(encoder, arr, idx)?,
        DataType::Timestamp(TimeUnit::Millisecond, _) => encode_ts_millis_value(encoder, arr, idx)?,
        DataType::Timestamp(TimeUnit::Microsecond, _) => encode_ts_micros_value(encoder, arr, idx)?,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => encode_ts_nanos_value(encoder, arr, idx)?,
        DataType::Date32 => encode_date32_value(encoder, arr, idx)?,
        DataType::Date64 => encode_date64_value(encoder, arr, idx)?,
        DataType::Time32(TimeUnit::Second) => encode_time32_value(encoder, arr, idx)?,
        DataType::Time32(TimeUnit::Millisecond) => encode_time32_millis_value(encoder, arr, idx)?,
        DataType::Time64(TimeUnit::Microsecond) => encode_time64_micros_value(encoder, arr, idx)?,
        DataType::Time64(TimeUnit::Nanosecond) => encode_time64_nanos_value(encoder, arr, idx)?,
        DataType::Binary => encode_binary_value(encoder, arr, idx)?,
        DataType::LargeBinary => encode_large_binary_value(encoder, arr, idx)?,
        DataType::Utf8 => encode_utf8_value(encoder, arr, idx)?,
        DataType::LargeUtf8 => encode_large_utf8_value(encoder, arr, idx)?,
        DataType::List(field) => {
            match field.data_type() {
                DataType::Boolean => encode_bool_list_value(encoder, arr, idx)?,
                DataType::Int8 | DataType::UInt8 => encode_i8_list_value(encoder, arr, idx)?,
                DataType::Int16 | DataType::UInt16 => encode_i16_list_value(encoder, arr, idx)?,
                DataType::Int32 => encode_i32_list_value(encoder, arr, idx)?,
                DataType::UInt32 => encode_u32_list_value(encoder, arr, idx)?,
                DataType::Int64 | DataType::UInt64 => encode_i64_list_value(encoder, arr, idx)?,
                DataType::Float32 => encode_f32_list_value(encoder, arr, idx)?,
                DataType::Float64 => encode_f64_list_value(encoder, arr, idx)?,
                DataType::Timestamp(TimeUnit::Second, _) => {
                    encode_ts_list_value(encoder, arr, idx)?
                }
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    encode_ts_millis_list_value(encoder, arr, idx)?
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    encode_ts_micros_list_value(encoder, arr, idx)?
                }
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    encode_ts_nanos_list_value(encoder, arr, idx)?
                }
                DataType::Date32 => encode_date32_list_value(encoder, arr, idx)?,
                DataType::Date64 => encode_date64_list_value(encoder, arr, idx)?,
                DataType::Time32(TimeUnit::Second) => encode_time32_list_value(encoder, arr, idx)?,
                DataType::Time32(TimeUnit::Millisecond) => {
                    encode_time32_millis_list_value(encoder, arr, idx)?
                }
                DataType::Time64(TimeUnit::Microsecond) => {
                    encode_time64_micros_list_value(encoder, arr, idx)?
                }
                DataType::Time64(TimeUnit::Nanosecond) => {
                    encode_time64_nanos_list_value(encoder, arr, idx)?
                }
                DataType::Binary => encode_binary_list_value(encoder, arr, idx)?,
                DataType::LargeBinary => encode_large_binary_list_value(encoder, arr, idx)?,
                DataType::Utf8 => encode_utf8_list_value(encoder, arr, idx)?,
                DataType::LargeUtf8 => encode_large_utf8_list_value(encoder, arr, idx)?,
                list_type => {
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "XX000".to_owned(),
                        format!(
                            "Unsupported List Datatype {} and list array {:?}",
                            list_type, &arr
                        ),
                    ))));
                }
            }
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

macro_rules! encode_value {
    ($fn_name:ident, $arr_ty:ty) => {
        fn $fn_name(encoder: &mut DataRowEncoder, arr: &ArrayRef, idx: usize) -> PgWireResult<()> {
            encoder.encode_field(&arr.as_any().downcast_ref::<$arr_ty>().unwrap().value(idx))
        }
    };

    ($fn_name:ident, $arr_ty:ty, $closure:expr) => {
        fn $fn_name(encoder: &mut DataRowEncoder, arr: &ArrayRef, idx: usize) -> PgWireResult<()> {
            #[allow(clippy::redundant_closure_call)]
            encoder.encode_field(&$closure(
                arr.as_any().downcast_ref::<$arr_ty>().unwrap().value(idx),
            ))
        }
    };
}

encode_value!(encode_bool_value, BooleanArray);
encode_value!(encode_i8_value, Int8Array);
encode_value!(encode_i16_value, Int16Array);
encode_value!(encode_i32_value, Int32Array);
encode_value!(encode_i64_value, Int64Array);
encode_value!(encode_u32_value, UInt32Array);
encode_value!(encode_f32_value, Float32Array);
encode_value!(encode_f64_value, Float64Array);
encode_value!(encode_ts_value, TimestampSecondArray, |val: i64| {
    NaiveDateTime::from_timestamp_opt(val, 0)
});
encode_value!(
    encode_ts_millis_value,
    TimestampMillisecondArray,
    |val: i64| { NaiveDateTime::from_timestamp_millis(val) }
);
encode_value!(
    encode_ts_micros_value,
    TimestampMicrosecondArray,
    |val: i64| { NaiveDateTime::from_timestamp_micros(val) }
);
encode_value!(
    encode_ts_nanos_value,
    TimestampNanosecondArray,
    |val: i64| {
        let secs = val.div_euclid(1_000_000_000);
        let nsecs = val.rem_euclid(1_000_000_000) as u32;
        NaiveDateTime::from_timestamp_opt(secs, nsecs)
    }
);
encode_value!(encode_date32_value, Date32Array, |val: i32| {
    let utc_time: DateTime<Utc> = DateTime::<Utc>::from(UNIX_EPOCH);
    utc_time.checked_add_signed(Duration::days(val as i64))
});
encode_value!(encode_date64_value, Date64Array, |val: i64| {
    let utc_time: DateTime<Utc> = DateTime::<Utc>::from(UNIX_EPOCH);
    utc_time.checked_add_signed(Duration::milliseconds(val))
});
encode_value!(encode_time32_value, Time32SecondArray, |val: i32| {
    let secs = val.rem_euclid(86_400) as u32;
    NaiveTime::from_num_seconds_from_midnight_opt(secs, 0)
});
encode_value!(
    encode_time32_millis_value,
    Time32MillisecondArray,
    |val: i32| {
        let millis = val.rem_euclid(86_400_000);
        let secs = millis.div_euclid(1000) as u32;
        let nano = millis.rem_euclid(1000) as u32 * 1_000_000;
        NaiveTime::from_num_seconds_from_midnight_opt(secs, nano)
    }
);
encode_value!(
    encode_time64_micros_value,
    Time64MicrosecondArray,
    |val: i64| {
        let micros = val.rem_euclid(86_400_000_000);
        let secs = micros.div_euclid(1_000_000) as u32;
        let nano = micros.rem_euclid(1_000_000) as u32 * 1000;
        NaiveTime::from_num_seconds_from_midnight_opt(secs, nano)
    }
);
encode_value!(
    encode_time64_nanos_value,
    Time64NanosecondArray,
    |val: i64| {
        let nanos = val.rem_euclid(86_400_000_000_000);
        let secs = nanos.div_euclid(1_000_000_000) as u32;
        let nano = nanos.rem_euclid(1_000_000_000) as u32;
        NaiveTime::from_num_seconds_from_midnight_opt(secs, nano)
    }
);
encode_value!(encode_binary_value, BinaryArray);
encode_value!(encode_large_binary_value, LargeBinaryArray);
encode_value!(encode_utf8_value, StringArray);
encode_value!(encode_large_utf8_value, LargeStringArray);

macro_rules! encode_list_value {
    ($fn_name:ident, $list_ty:ty, $collect_ty:ty) => {
        fn $fn_name(encoder: &mut DataRowEncoder, arr: &ArrayRef, idx: usize) -> PgWireResult<()> {
            encoder.encode_field(
                &arr.as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap()
                    .value(idx)
                    .as_any()
                    .downcast_ref::<$list_ty>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<Option<$collect_ty>>>(),
            )
        }
    };

    ($fn_name:ident, $list_ty:ty, $collect_ty:ty, $closure:expr) => {
        fn $fn_name(encoder: &mut DataRowEncoder, arr: &ArrayRef, idx: usize) -> PgWireResult<()> {
            encoder.encode_field(
                &arr.as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap()
                    .value(idx)
                    .as_any()
                    .downcast_ref::<$list_ty>()
                    .unwrap()
                    .iter()
                    .map(|opt| opt.and_then($closure))
                    .collect::<Vec<Option<$collect_ty>>>(),
            )
        }
    };
}

encode_list_value!(encode_bool_list_value, BooleanArray, bool);
encode_list_value!(encode_i8_list_value, Int8Array, i8);
encode_list_value!(encode_i16_list_value, Int16Array, i16);
encode_list_value!(encode_i32_list_value, Int32Array, i32);
encode_list_value!(encode_i64_list_value, Int64Array, i64);
encode_list_value!(encode_u32_list_value, UInt32Array, u32);
encode_list_value!(encode_f32_list_value, Float32Array, f32);
encode_list_value!(encode_f64_list_value, Float64Array, f64);
encode_list_value!(
    encode_ts_list_value,
    TimestampSecondArray,
    NaiveDateTime,
    |val: i64| { NaiveDateTime::from_timestamp_opt(val, 0) }
);
encode_list_value!(
    encode_ts_millis_list_value,
    TimestampMillisecondArray,
    NaiveDateTime,
    |val: i64| { NaiveDateTime::from_timestamp_millis(val) }
);
encode_list_value!(
    encode_ts_micros_list_value,
    TimestampMicrosecondArray,
    NaiveDateTime,
    |val: i64| { NaiveDateTime::from_timestamp_micros(val) }
);
encode_list_value!(
    encode_ts_nanos_list_value,
    TimestampNanosecondArray,
    NaiveDateTime,
    |val: i64| {
        let secs = val.div_euclid(1_000_000_000);
        let nsecs = val.rem_euclid(1_000_000_000) as u32;
        NaiveDateTime::from_timestamp_opt(secs, nsecs)
    }
);
encode_list_value!(
    encode_date32_list_value,
    Date32Array,
    DateTime<Utc>,
    |val: i32| {
        let utc_time: DateTime<Utc> = DateTime::<Utc>::from(UNIX_EPOCH);
        utc_time.checked_add_signed(Duration::days(val as i64))
    }
);
encode_list_value!(
    encode_date64_list_value,
    Date64Array,
    DateTime<Utc>,
    |val: i64| {
        let utc_time: DateTime<Utc> = DateTime::<Utc>::from(UNIX_EPOCH);
        utc_time.checked_add_signed(Duration::milliseconds(val))
    }
);
encode_list_value!(
    encode_time32_list_value,
    Time32SecondArray,
    NaiveTime,
    |val: i32| {
        let secs = val.rem_euclid(86_400) as u32;
        NaiveTime::from_num_seconds_from_midnight_opt(secs, 0)
    }
);
encode_list_value!(
    encode_time32_millis_list_value,
    Time32MillisecondArray,
    NaiveTime,
    |val: i32| {
        let millis = val.rem_euclid(86_400_000);
        let secs = millis.div_euclid(1000) as u32;
        let nano = millis.rem_euclid(1000) as u32 * 1_000_000;
        NaiveTime::from_num_seconds_from_midnight_opt(secs, nano)
    }
);
encode_list_value!(
    encode_time64_micros_list_value,
    Time64MicrosecondArray,
    NaiveTime,
    |val: i64| {
        let micros = val.rem_euclid(86_400_000_000);
        let secs = micros.div_euclid(1_000_000) as u32;
        let nano = micros.rem_euclid(1_000_000) as u32 * 1000;
        NaiveTime::from_num_seconds_from_midnight_opt(secs, nano)
    }
);
encode_list_value!(
    encode_time64_nanos_list_value,
    Time64NanosecondArray,
    NaiveTime,
    |val: i64| {
        let nanos = val.rem_euclid(86_400_000_000_000);
        let secs = nanos.div_euclid(1_000_000_000) as u32;
        let nano = nanos.rem_euclid(1_000_000_000) as u32;
        NaiveTime::from_num_seconds_from_midnight_opt(secs, nano)
    }
);
encode_list_value!(encode_binary_list_value, BinaryArray, &[u8]);
encode_list_value!(encode_large_binary_list_value, LargeBinaryArray, &[u8]);
encode_list_value!(encode_utf8_list_value, StringArray, &str);
encode_list_value!(encode_large_utf8_list_value, LargeStringArray, &str);
