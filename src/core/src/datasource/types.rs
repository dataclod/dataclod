use std::sync::Arc;

use anyhow::{bail, Result};
use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use datafusion::arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, Int8Builder, ListBuilder, NullBuilder, StringBuilder,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder, UInt32Builder,
};
use datafusion::arrow::datatypes::{DataType, Field, IntervalUnit, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use tokio_postgres::types::{Kind, Type};
use tokio_postgres::Row;

pub fn postgres_to_arrow(pg_type: &Type) -> Result<DataType> {
    Ok(match pg_type.kind() {
        Kind::Simple => {
            match *pg_type {
                Type::UNKNOWN => DataType::Null,
                Type::BOOL => DataType::Boolean,
                Type::CHAR => DataType::Int8,
                Type::INT2 => DataType::Int16,
                Type::INT4 => DataType::Int32,
                Type::INT8 => DataType::Int64,
                Type::OID => DataType::UInt32,
                Type::FLOAT4 => DataType::Float32,
                Type::FLOAT8 => DataType::Float64,
                Type::TIMESTAMP => DataType::Timestamp(TimeUnit::Microsecond, None),
                Type::DATE => DataType::Date32,
                Type::TIME => DataType::Time64(TimeUnit::Microsecond),
                Type::INTERVAL => DataType::Interval(IntervalUnit::MonthDayNano),
                Type::NUMERIC => DataType::Decimal256(0, 0),
                Type::BYTEA => DataType::Binary,
                Type::NAME | Type::TEXT | Type::BPCHAR | Type::VARCHAR => DataType::Utf8,
                _ => bail!("Unsupported postgres type: {}", pg_type),
            }
        }
        Kind::Array(element_type) => {
            match *element_type {
                Type::BOOL => {
                    DataType::List(Arc::new(Field::new(
                        "array_element",
                        DataType::Boolean,
                        true,
                    )))
                }
                Type::CHAR => {
                    DataType::List(Arc::new(Field::new("array_element", DataType::Int8, true)))
                }
                Type::INT2 => {
                    DataType::List(Arc::new(Field::new("array_element", DataType::Int16, true)))
                }
                Type::INT4 => {
                    DataType::List(Arc::new(Field::new("array_element", DataType::Int32, true)))
                }
                Type::INT8 => {
                    DataType::List(Arc::new(Field::new("array_element", DataType::Int64, true)))
                }
                Type::OID => {
                    DataType::List(Arc::new(Field::new(
                        "array_element",
                        DataType::UInt32,
                        true,
                    )))
                }
                Type::FLOAT4 => {
                    DataType::List(Arc::new(Field::new(
                        "array_element",
                        DataType::Float32,
                        true,
                    )))
                }
                Type::FLOAT8 => {
                    DataType::List(Arc::new(Field::new(
                        "array_element",
                        DataType::Float64,
                        true,
                    )))
                }
                Type::TIMESTAMP => {
                    DataType::List(Arc::new(Field::new(
                        "array_element",
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        true,
                    )))
                }
                Type::DATE => {
                    DataType::List(Arc::new(Field::new(
                        "array_element",
                        DataType::Date32,
                        true,
                    )))
                }
                Type::TIME => {
                    DataType::List(Arc::new(Field::new(
                        "array_element",
                        DataType::Time32(TimeUnit::Microsecond),
                        true,
                    )))
                }
                Type::INTERVAL => {
                    DataType::List(Arc::new(Field::new(
                        "array_element",
                        DataType::Interval(IntervalUnit::MonthDayNano),
                        true,
                    )))
                }
                Type::NUMERIC => {
                    DataType::List(Arc::new(Field::new(
                        "array_element",
                        DataType::Decimal256(0, 0),
                        true,
                    )))
                }
                Type::BYTEA => {
                    DataType::List(Arc::new(Field::new(
                        "array_element",
                        DataType::Binary,
                        true,
                    )))
                }
                Type::NAME | Type::TEXT | Type::BPCHAR | Type::VARCHAR => {
                    DataType::List(Arc::new(Field::new("array_element", DataType::Utf8, true)))
                }
                _ => bail!("Unsupported postgres type: {}", pg_type),
            }
        }
        _ => bail!("Unsupported postgres type: {}", pg_type),
    })
}

pub fn encode_postgres_rows(rows: &[Row], schema: &SchemaRef) -> Result<RecordBatch> {
    let row_len = rows.len();
    let col_len = schema.fields().len();

    let mut columns = Vec::<ArrayRef>::with_capacity(col_len);
    for col in 0..col_len {
        match schema.field(col).data_type() {
            DataType::Null => {
                let mut builder = NullBuilder::with_capacity(row_len);
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(row_len);
                for row in rows {
                    let value: Option<bool> = row.get(col);
                    builder.append_option(value);
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Int8 => {
                let mut builder = Int8Builder::with_capacity(row_len);
                for row in rows {
                    let value: Option<i8> = row.get(col);
                    builder.append_option(value);
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Int16 => {
                let mut builder = Int16Builder::with_capacity(row_len);
                for row in rows {
                    let value: Option<i16> = row.get(col);
                    builder.append_option(value);
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Int32 => {
                let mut builder = Int32Builder::with_capacity(row_len);
                for row in rows {
                    let value: Option<i32> = row.get(col);
                    builder.append_option(value);
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Int64 => {
                let mut builder = Int64Builder::with_capacity(row_len);
                for row in rows {
                    let value: Option<i64> = row.get(col);
                    builder.append_option(value);
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::UInt32 => {
                let mut builder = UInt32Builder::with_capacity(row_len);
                for row in rows {
                    let value: Option<u32> = row.get(col);
                    builder.append_option(value);
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Float32 => {
                let mut builder = Float32Builder::with_capacity(row_len);
                for row in rows {
                    let value: Option<f32> = row.get(col);
                    builder.append_option(value);
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::with_capacity(row_len);
                for row in rows {
                    let value: Option<f64> = row.get(col);
                    builder.append_option(value);
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                let mut builder = TimestampMicrosecondBuilder::with_capacity(row_len);
                for row in rows {
                    let value: Option<NaiveDateTime> = row.get(col);
                    builder.append_option(value.map(|v| v.timestamp_micros()));
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Date32 => {
                let mut builder = Date32Builder::with_capacity(row_len);
                for row in rows {
                    let value: Option<NaiveDate> = row.get(col);
                    builder.append_option(value.map(|v| v.num_days_from_ce()));
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                let mut builder = Time64MicrosecondBuilder::with_capacity(row_len);
                for row in rows {
                    let value: Option<NaiveTime> = row.get(col);
                    builder.append_option(value.map(|v| {
                        let secs = v.num_seconds_from_midnight() as i64;
                        let nano = v.nanosecond() as i64;
                        secs * 1_000_000 + nano.div_euclid(1000)
                    }));
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                todo!()
            }
            DataType::Decimal256(..) => {
                todo!()
            }
            DataType::Binary => {
                let mut builder = BinaryBuilder::with_capacity(row_len, 0);
                for row in rows {
                    let value: Option<&[u8]> = row.get(col);
                    builder.append_option(value);
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Utf8 => {
                let mut builder = StringBuilder::with_capacity(row_len, 0);
                for row in rows {
                    let value: Option<&str> = row.get(col);
                    builder.append_option(value);
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::List(element_field) => {
                match element_field.data_type() {
                    DataType::Null => {
                        let mut builder = ListBuilder::with_capacity(NullBuilder::new(), row_len);
                        columns.push(Arc::new(builder.finish()));
                    }
                    DataType::Boolean => {
                        let mut builder =
                            ListBuilder::with_capacity(BooleanBuilder::new(), row_len);
                        for row in rows {
                            let value: Option<Vec<Option<bool>>> = row.get(col);
                            builder.append_option(value);
                        }
                        columns.push(Arc::new(builder.finish()));
                    }
                    DataType::Int8 => {
                        let mut builder = ListBuilder::with_capacity(Int8Builder::new(), row_len);
                        for row in rows {
                            let value: Option<Vec<Option<i8>>> = row.get(col);
                            builder.append_option(value);
                        }
                        columns.push(Arc::new(builder.finish()));
                    }
                    DataType::Int16 => {
                        let mut builder = ListBuilder::with_capacity(Int16Builder::new(), row_len);
                        for row in rows {
                            let value: Option<Vec<Option<i16>>> = row.get(col);
                            builder.append_option(value);
                        }
                        columns.push(Arc::new(builder.finish()));
                    }
                    DataType::Int32 => {
                        let mut builder = ListBuilder::with_capacity(Int32Builder::new(), row_len);
                        for row in rows {
                            let value: Option<Vec<Option<i32>>> = row.get(col);
                            builder.append_option(value);
                        }
                        columns.push(Arc::new(builder.finish()));
                    }
                    DataType::Int64 => {
                        let mut builder = ListBuilder::with_capacity(Int64Builder::new(), row_len);
                        for row in rows {
                            let value: Option<Vec<Option<i64>>> = row.get(col);
                            builder.append_option(value);
                        }
                        columns.push(Arc::new(builder.finish()));
                    }
                    DataType::UInt32 => {
                        let mut builder = ListBuilder::with_capacity(UInt32Builder::new(), row_len);
                        for row in rows {
                            let value: Option<Vec<Option<u32>>> = row.get(col);
                            builder.append_option(value);
                        }
                        columns.push(Arc::new(builder.finish()));
                    }
                    DataType::Float32 => {
                        let mut builder =
                            ListBuilder::with_capacity(Float32Builder::new(), row_len);
                        for row in rows {
                            let value: Option<Vec<Option<f32>>> = row.get(col);
                            builder.append_option(value);
                        }
                        columns.push(Arc::new(builder.finish()));
                    }
                    DataType::Float64 => {
                        let mut builder =
                            ListBuilder::with_capacity(Float64Builder::new(), row_len);
                        for row in rows {
                            let value: Option<Vec<Option<f64>>> = row.get(col);
                            builder.append_option(value);
                        }
                        columns.push(Arc::new(builder.finish()));
                    }
                    DataType::Timestamp(TimeUnit::Microsecond, None) => {
                        let mut builder =
                            ListBuilder::with_capacity(TimestampMicrosecondBuilder::new(), row_len);
                        for row in rows {
                            let value: Option<Vec<Option<NaiveDateTime>>> = row.get(col);
                            builder.append_option(value.map(|v| {
                                v.into_iter()
                                    .map(|opt| opt.map(|v| v.timestamp_micros()))
                                    .collect::<Vec<_>>()
                            }));
                        }
                        columns.push(Arc::new(builder.finish()));
                    }
                    DataType::Date32 => {
                        let mut builder = ListBuilder::with_capacity(Date32Builder::new(), row_len);
                        for row in rows {
                            let value: Option<Vec<Option<NaiveDate>>> = row.get(col);
                            builder.append_option(value.map(|v| {
                                v.into_iter()
                                    .map(|opt| opt.map(|v| v.num_days_from_ce()))
                                    .collect::<Vec<_>>()
                            }));
                        }
                        columns.push(Arc::new(builder.finish()));
                    }
                    DataType::Time64(TimeUnit::Microsecond) => {
                        let mut builder =
                            ListBuilder::with_capacity(Time64MicrosecondBuilder::new(), row_len);
                        for row in rows {
                            let value: Option<Vec<Option<NaiveTime>>> = row.get(col);
                            builder.append_option(value.map(|v| {
                                v.into_iter()
                                    .map(|opt| {
                                        opt.map(|v| {
                                            let secs = v.num_seconds_from_midnight() as i64;
                                            let nano = v.nanosecond() as i64;
                                            secs * 1_000_000 + nano.div_euclid(1000)
                                        })
                                    })
                                    .collect::<Vec<_>>()
                            }));
                        }
                        columns.push(Arc::new(builder.finish()));
                    }
                    DataType::Interval(_) => todo!(),
                    DataType::Binary => {
                        let mut builder = ListBuilder::with_capacity(BinaryBuilder::new(), row_len);
                        for row in rows {
                            let value: Option<Vec<Option<&[u8]>>> = row.get(col);
                            builder.append_option(value);
                        }
                        columns.push(Arc::new(builder.finish()));
                    }
                    DataType::Utf8 => {
                        let mut builder = ListBuilder::with_capacity(StringBuilder::new(), row_len);
                        for row in rows {
                            let value: Option<Vec<Option<&str>>> = row.get(col);
                            builder.append_option(value);
                        }
                        columns.push(Arc::new(builder.finish()));
                    }
                    DataType::Decimal256(..) => todo!(),
                    data_type => bail!("Unsupported list element data type: {}", data_type),
                }
            }
            data_type => bail!("Unsupported data type: {}", data_type),
        }
    }
    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}
