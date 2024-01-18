use std::sync::Arc;

use anyhow::{bail, Result};
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
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
                Type::VARCHAR | Type::TEXT => DataType::Utf8,
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

pub fn encode_postgres_rows(_rows: &[Row]) -> Result<RecordBatch> {
    todo!()
}
