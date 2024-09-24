use std::any::Any;
use std::sync::Arc;

use common_utils::PgTypeId;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::cast::as_int64_array;
use datafusion::common::{Result as DFResult, ScalarValue, plan_err};
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
        fn format_type(oid: i64, typemod: Option<i64>) -> String {
            if let Some(type_id) = PgTypeId::from_oid(oid as u32) {
                let typemod_str = match type_id {
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
                };

                match type_id {
                    PgTypeId::BOOL => "boolean".to_owned(),
                    PgTypeId::BYTEA => format!("bytea{typemod_str}"),
                    PgTypeId::INT8 => "bigint".to_owned(),
                    PgTypeId::INT2 => "smallint".to_owned(),
                    PgTypeId::INT4 => "integer".to_owned(),
                    PgTypeId::FLOAT4 => "real".to_owned(),
                    PgTypeId::FLOAT8 => "double precision".to_owned(),
                    PgTypeId::ARRAYBOOL => "boolean[]".to_owned(),
                    PgTypeId::ARRAYBYTEA => {
                        format!("bytea{typemod_str}[]")
                    }
                    PgTypeId::ARRAYINT2 => "smallint[]".to_owned(),
                    PgTypeId::ARRAYINT4 => "integer[]".to_owned(),
                    PgTypeId::ARRAYVARCHAR => {
                        format!("character varying{typemod_str}[]")
                    }
                    PgTypeId::ARRAYINT8 => "bigint[]".to_owned(),
                    PgTypeId::ARRAYFLOAT4 => "real[]".to_owned(),
                    PgTypeId::ARRAYFLOAT8 => "double precision[]".to_owned(),
                    PgTypeId::VARCHAR => {
                        format!("character varying{typemod_str}")
                    }
                    PgTypeId::DATE => format!("date{typemod_str}"),
                    PgTypeId::TIME => {
                        format!("time{typemod_str} without time zone")
                    }
                    PgTypeId::TIMESTAMP => {
                        format!("timestamp{typemod_str} without time zone")
                    }
                    PgTypeId::ARRAYTIMESTAMP => {
                        format!("timestamp{typemod_str} without time zone[]")
                    }
                    PgTypeId::ARRAYDATE => {
                        format!("date{typemod_str}[]")
                    }
                    PgTypeId::ARRAYTIME => {
                        format!("time{typemod_str} without time zone[]")
                    }
                    PgTypeId::TIMESTAMPTZ => {
                        format!("timestamp{typemod_str} with time zone")
                    }
                    PgTypeId::ARRAYTIMESTAMPTZ => {
                        format!("timestamp{typemod_str} with time zone[]")
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
                        format!("character{typemod_str}")
                    }
                    PgTypeId::ARRAYCHAR => {
                        format!("character{typemod_str}[]")
                    }
                }
            } else {
                "???".to_owned()
            }
        }

        match (&args[0], &args[1]) {
            (ColumnarValue::Array(array1), ColumnarValue::Array(array2)) => {
                let type_arr = as_int64_array(array1)?;
                let typemod_arr = as_int64_array(array2)?;

                let result: StringArray = type_arr
                    .iter()
                    .zip(typemod_arr.iter())
                    .map(|opt| {
                        match opt {
                            (Some(oid), typemod) => Some(format_type(oid, typemod)),
                            _ => None,
                        }
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            (ColumnarValue::Array(array1), ColumnarValue::Scalar(ScalarValue::Int64(scalar2))) => {
                let type_arr = as_int64_array(array1)?;

                let result: StringArray = type_arr
                    .iter()
                    .map(|opt| opt.map(|oid| format_type(oid, *scalar2)))
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            (ColumnarValue::Scalar(ScalarValue::Int64(scalar1)), ColumnarValue::Array(array2)) => {
                let typemod_arr = as_int64_array(array2)?;

                let result: StringArray = typemod_arr
                    .iter()
                    .map(|typemod| scalar1.map(|typ| format_type(typ, typemod)))
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            (
                ColumnarValue::Scalar(ScalarValue::Int64(scalar1)),
                ColumnarValue::Scalar(ScalarValue::Int64(scalar2)),
            ) => {
                match scalar1 {
                    Some(typ) => {
                        let result = format_type(*typ, *scalar2);
                        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))))
                    }
                    None => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
                }
            }
            (ColumnarValue::Array(_), ColumnarValue::Scalar(scalar)) => {
                plan_err!(
                    "`format_type` expects an integer as second argument, actual: {}",
                    scalar.data_type()
                )
            }
            (ColumnarValue::Scalar(scalar), ColumnarValue::Array(_)) => {
                plan_err!(
                    "`format_type` expects an integer as first argument, actual: {}",
                    scalar.data_type()
                )
            }
            (ColumnarValue::Scalar(scalar1), ColumnarValue::Scalar(scalar2)) => {
                plan_err!(
                    "`format_type` expects two integer as arguments, actual: {}, {}",
                    scalar1.data_type(),
                    scalar2.data_type()
                )
            }
        }
    }
}
