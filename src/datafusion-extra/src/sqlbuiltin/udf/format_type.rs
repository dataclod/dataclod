use std::any::Any;
use std::sync::Arc;

use common_utils::PgTypeId;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::cast::as_int64_array;
use datafusion::common::{Result as DFResult, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility};

pub fn create_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(FormatType {
        signature: Signature::exact(
            vec![DataType::Int64, DataType::Int64],
            Volatility::Immutable,
        ),
    })
}

#[derive(Debug)]
struct FormatType {
    signature: Signature,
}

impl ScalarUDFImpl for FormatType {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "format_type"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        match (&args[0], &args[1]) {
            (ColumnarValue::Array(_), ColumnarValue::Array(_))
            | (ColumnarValue::Array(_), ColumnarValue::Scalar(ScalarValue::Int64(_)))
            | (ColumnarValue::Scalar(ScalarValue::Int64(_)), ColumnarValue::Array(_)) => {
                let arrays = ColumnarValue::values_to_arrays(args)?;
                let type_arr = as_int64_array(&arrays[0])?;
                let typemod_arr = as_int64_array(&arrays[0])?;

                let result: StringArray = type_arr
                    .iter()
                    .zip(typemod_arr.iter())
                    .map(|opt| {
                        match opt {
                            (Some(oid), typemod) => {
                                Some(match PgTypeId::from_oid(oid as u32) {
                                    Some(type_id) => {
                                        let typemod_str = || {
                                            match type_id {
                                                PgTypeId::VARCHAR
                                                | PgTypeId::ARRAYVARCHAR
                                                | PgTypeId::CHAR
                                                | PgTypeId::ARRAYCHAR => {
                                                    match typemod {
                                                        Some(typemod) if typemod >= 5 => {
                                                            format!("({})", typemod - 4)
                                                        }
                                                        _ => "".to_owned(),
                                                    }
                                                }
                                                _ => {
                                                    match typemod {
                                                        Some(typemod) if typemod >= 0 => {
                                                            format!("({typemod})")
                                                        }
                                                        _ => "".to_owned(),
                                                    }
                                                }
                                            }
                                        };

                                        match type_id {
                                            PgTypeId::BOOL => "boolean".to_owned(),
                                            PgTypeId::BYTEA => format!("bytea{}", typemod_str()),
                                            PgTypeId::INT8 => "bigint".to_owned(),
                                            PgTypeId::INT2 => "smallint".to_owned(),
                                            PgTypeId::INT4 => "integer".to_owned(),
                                            PgTypeId::FLOAT4 => "real".to_owned(),
                                            PgTypeId::FLOAT8 => "double precision".to_owned(),
                                            PgTypeId::ARRAYBOOL => "boolean[]".to_owned(),
                                            PgTypeId::ARRAYBYTEA => {
                                                format!("bytea{}[]", typemod_str())
                                            }
                                            PgTypeId::ARRAYINT2 => "smallint[]".to_owned(),
                                            PgTypeId::ARRAYINT4 => "integer[]".to_owned(),
                                            PgTypeId::ARRAYVARCHAR => {
                                                format!("character varying{}[]", typemod_str())
                                            }
                                            PgTypeId::ARRAYINT8 => "bigint[]".to_owned(),
                                            PgTypeId::ARRAYFLOAT4 => "real[]".to_owned(),
                                            PgTypeId::ARRAYFLOAT8 => {
                                                "double precision[]".to_owned()
                                            }
                                            PgTypeId::VARCHAR => {
                                                format!("character varying{}", typemod_str())
                                            }
                                            PgTypeId::DATE => format!("date{}", typemod_str()),
                                            PgTypeId::TIME => {
                                                format!("time{} without time zone", typemod_str())
                                            }
                                            PgTypeId::TIMESTAMP => {
                                                format!(
                                                    "timestamp{} without time zone",
                                                    typemod_str()
                                                )
                                            }
                                            PgTypeId::ARRAYTIMESTAMP => {
                                                format!(
                                                    "timestamp{} without time zone[]",
                                                    typemod_str()
                                                )
                                            }
                                            PgTypeId::ARRAYDATE => {
                                                format!("date{}[]", typemod_str())
                                            }
                                            PgTypeId::ARRAYTIME => {
                                                format!("time{} without time zone[]", typemod_str())
                                            }
                                            PgTypeId::TIMESTAMPTZ => {
                                                format!("timestamp{} with time zone", typemod_str())
                                            }
                                            PgTypeId::ARRAYTIMESTAMPTZ => {
                                                format!(
                                                    "timestamp{} with time zone[]",
                                                    typemod_str()
                                                )
                                            }
                                            PgTypeId::INTERVAL => {
                                                match typemod {
                                                    Some(typemod) if typemod >= 0 => "-".to_owned(),
                                                    _ => "interval".to_owned(),
                                                }
                                            }
                                            PgTypeId::ARRAYINTERVAL => {
                                                match typemod {
                                                    Some(typemod) if typemod >= 0 => "-".to_owned(),
                                                    _ => "interval[]".to_owned(),
                                                }
                                            }
                                            PgTypeId::CHAR => {
                                                format!("character{}", typemod_str())
                                            }
                                            PgTypeId::ARRAYCHAR => {
                                                format!("character{}[]", typemod_str())
                                            }
                                        }
                                    }
                                    None => "???".to_owned(),
                                })
                            }
                            _ => None,
                        }
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            (
                ColumnarValue::Scalar(ScalarValue::Int64(scalar1)),
                ColumnarValue::Scalar(ScalarValue::Int64(scalar2)),
            ) => todo!(),
            (ColumnarValue::Array(_), ColumnarValue::Scalar(_)) => todo!(),
            (ColumnarValue::Scalar(_), ColumnarValue::Array(_)) => todo!(),
            (ColumnarValue::Scalar(_), ColumnarValue::Scalar(_)) => todo!(),
        }
    }
}
