use std::sync::Arc;

use anyhow::{bail, Result};
use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int16Builder,
    Int32Builder, Int64Builder, Int8Builder, NullArray, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef, TimeUnit};
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
                Type::FLOAT4 => DataType::Float32,
                Type::FLOAT8 => DataType::Float64,
                Type::TIMESTAMP => DataType::Timestamp(TimeUnit::Microsecond, None),
                Type::DATE => DataType::Date32,
                Type::TIME => DataType::Time32(TimeUnit::Millisecond),
                Type::BYTEA => DataType::Binary,
                Type::TEXT | Type::BPCHAR | Type::VARCHAR => DataType::Utf8,
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
                        DataType::Time32(TimeUnit::Millisecond),
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
                Type::VARCHAR | Type::TEXT => {
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
            DataType::Null => columns.push(Arc::new(NullArray::new(row_len))),
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
            DataType::Date32 => {
                let mut builder = Date32Builder::with_capacity(row_len);
                for row in rows {
                    let value: Option<i32> = row.get(col);
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
            data_type => bail!("Unsupported data type: {}", data_type),
        }
    }
    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}
