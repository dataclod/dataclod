use std::any::Any;
use std::sync::Arc;

use arrow::array::{AsArray, BinaryViewArray, new_null_array};
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafabric_common_schema::schema_ext::FIELD_TARGET_TYPE;
use datafusion::common::{Result as DFResult, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use geos::{CoordSeq, Geometry};

use crate::utils::GeosExt;

pub fn st_makepoint() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(MakePointUdf {
        signature: Signature::user_defined(Volatility::Immutable),
        aliases: vec!["st_makepoint".to_string()],
    }))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct MakePointUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl ScalarUDFImpl for MakePointUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ST_MakePoint"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::BinaryView)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> DFResult<FieldRef> {
        Ok(Arc::new(
            Field::new(self.name(), DataType::BinaryView, true).with_metadata(
                [(FIELD_TARGET_TYPE.to_string(), "geometry".to_string())]
                    .into_iter()
                    .collect(),
            ),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = args.args;
        if args.len() != 2 {
            return exec_err!("invalid number of arguments for udf {}", self.name());
        }
        if args[0].data_type() != DataType::Float64 {
            return exec_err!(
                "unsupported data type '{}' for udf {}",
                args[0].data_type(),
                self.name()
            );
        }
        if args[1].data_type() != DataType::Float64 {
            return exec_err!(
                "unsupported data type '{}' for udf {}",
                args[1].data_type(),
                self.name()
            );
        }

        match (&args[0], &args[1]) {
            (ColumnarValue::Array(x_arr), ColumnarValue::Array(y_arr)) => {
                let x_arr = x_arr.as_primitive::<Float64Type>();
                let y_arr = y_arr.as_primitive::<Float64Type>();
                if x_arr.len() != y_arr.len() {
                    return exec_err!("array length mismatch for udf {}", self.name());
                }

                let result: BinaryViewArray = x_arr
                    .iter()
                    .zip(y_arr.iter())
                    .map(|opt| {
                        match opt {
                            (Some(x), Some(y)) => {
                                CoordSeq::new_from_vec(&[&[x, y]])
                                    .and_then(Geometry::create_point)
                                    .and_then(|geom| geom.to_ewkb())
                                    .ok()
                            }
                            _ => None,
                        }
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            (ColumnarValue::Array(x_arr), ColumnarValue::Scalar(ScalarValue::Float64(y_opt))) => {
                match y_opt {
                    Some(y) => {
                        let x_arr = x_arr.as_primitive::<Float64Type>();
                        let result: BinaryViewArray = x_arr
                            .iter()
                            .map(|opt| {
                                opt.and_then(|x| {
                                    CoordSeq::new_from_vec(&[&[x, *y]])
                                        .and_then(Geometry::create_point)
                                        .and_then(|geom| geom.to_ewkb())
                                        .ok()
                                })
                            })
                            .collect();
                        Ok(ColumnarValue::Array(Arc::new(result)))
                    }
                    None => {
                        Ok(ColumnarValue::Array(new_null_array(
                            &DataType::BinaryView,
                            x_arr.len(),
                        )))
                    }
                }
            }
            (ColumnarValue::Scalar(ScalarValue::Float64(x_opt)), ColumnarValue::Array(y_arr)) => {
                match x_opt {
                    Some(x) => {
                        let y_arr = y_arr.as_primitive::<Float64Type>();
                        let result: BinaryViewArray = y_arr
                            .iter()
                            .map(|opt| {
                                opt.and_then(|y| {
                                    CoordSeq::new_from_vec(&[&[*x, y]])
                                        .and_then(Geometry::create_point)
                                        .and_then(|geom| geom.to_ewkb())
                                        .ok()
                                })
                            })
                            .collect();
                        Ok(ColumnarValue::Array(Arc::new(result)))
                    }
                    None => {
                        Ok(ColumnarValue::Array(new_null_array(
                            &DataType::BinaryView,
                            y_arr.len(),
                        )))
                    }
                }
            }
            (
                ColumnarValue::Scalar(ScalarValue::Float64(x_opt)),
                ColumnarValue::Scalar(ScalarValue::Float64(y_opt)),
            ) => {
                let result = match (x_opt, y_opt) {
                    (Some(x), Some(y)) => {
                        CoordSeq::new_from_vec(&[&[*x, *y]])
                            .and_then(Geometry::create_point)
                            .and_then(|geom| geom.to_ewkb())
                            .ok()
                    }
                    _ => None,
                };

                Ok(ColumnarValue::Scalar(ScalarValue::BinaryView(result)))
            }
            other => {
                exec_err!("unsupported data type '{other:?}' for udf {}", self.name())
            }
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        let len = arg_types.len();
        if len != 2 {
            return exec_err!("invalid number of arguments for udf {}", self.name());
        }
        if !matches!(
            arg_types[0],
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float16
                | DataType::Float32
                | DataType::Float64
        ) {
            return exec_err!(
                "unsupported data type '{}' for udf {}",
                arg_types[0],
                self.name()
            );
        }
        if !matches!(
            arg_types[1],
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float16
                | DataType::Float32
                | DataType::Float64
        ) {
            return exec_err!(
                "unsupported data type '{}' for udf {}",
                arg_types[1],
                self.name()
            );
        }
        Ok(vec![DataType::Float64, DataType::Float64])
    }
}
