use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, AsArray, BinaryViewArray, new_null_array};
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafabric_common_schema::schema_ext::FIELD_TARGET_TYPE;
use datafusion::common::{Result as DFResult, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use geos::Geometry;

use crate::utils::GeosExt;

pub fn st_simplify() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(SimplifyUdf {
        signature: Signature::user_defined(Volatility::Immutable),
        aliases: vec!["st_simplify".to_owned()],
    }))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SimplifyUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl ScalarUDFImpl for SimplifyUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ST_Simplify"
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
        if args[0].data_type() != DataType::BinaryView {
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
            (ColumnarValue::Array(wkb_arr), ColumnarValue::Array(tolerance_arr)) => {
                let wkb_arr = wkb_arr.as_binary_view();
                let tolerance_arr = tolerance_arr.as_primitive::<Float64Type>();
                if wkb_arr.len() != tolerance_arr.len() {
                    return exec_err!("array length mismatch for udf {}", self.name());
                }

                let result: BinaryViewArray = wkb_arr
                    .iter()
                    .zip(tolerance_arr.iter())
                    .map(|opt| {
                        match opt {
                            (Some(wkb), Some(tolerance)) => {
                                Geometry::new_from_wkb(wkb)
                                    .and_then(|geom| geom.simplify(tolerance))
                                    .and_then(|geom| geom.to_ewkb())
                                    .ok()
                            }
                            _ => None,
                        }
                    })
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            (
                ColumnarValue::Array(wkb_arr),
                ColumnarValue::Scalar(ScalarValue::Float64(tolerance_opt)),
            ) => {
                let result = match tolerance_opt {
                    Some(tolerance) => {
                        let wkb_arr = wkb_arr.as_binary_view();
                        let result: BinaryViewArray = wkb_arr
                            .iter()
                            .map(|opt| {
                                opt.and_then(|wkb| {
                                    Geometry::new_from_wkb(wkb)
                                        .and_then(|geom| geom.simplify(*tolerance))
                                        .and_then(|geom| geom.to_ewkb())
                                        .ok()
                                })
                            })
                            .collect();
                        Arc::new(result)
                    }
                    None => new_null_array(&DataType::BinaryView, wkb_arr.len()),
                };
                Ok(ColumnarValue::Array(result))
            }
            (
                ColumnarValue::Scalar(ScalarValue::BinaryView(wkb_opt)),
                ColumnarValue::Array(tolerance_arr),
            ) => {
                let result = match wkb_opt {
                    Some(wkb) => {
                        match Geometry::new_from_wkb(wkb) {
                            Ok(geom) => {
                                let tolerance_arr = tolerance_arr.as_primitive::<Float64Type>();
                                let result: BinaryViewArray = tolerance_arr
                                    .iter()
                                    .map(|opt| {
                                        opt.and_then(|tolerance| {
                                            geom.simplify(tolerance)
                                                .and_then(|geom| geom.to_ewkb())
                                                .ok()
                                        })
                                    })
                                    .collect();
                                Arc::new(result)
                            }
                            _ => new_null_array(&DataType::BinaryView, tolerance_arr.len()),
                        }
                    }
                    None => new_null_array(&DataType::BinaryView, tolerance_arr.len()),
                };
                Ok(ColumnarValue::Array(result))
            }
            (
                ColumnarValue::Scalar(ScalarValue::BinaryView(wkb_opt)),
                ColumnarValue::Scalar(ScalarValue::Float64(tolerance_opt)),
            ) => {
                let result = match (wkb_opt, tolerance_opt) {
                    (Some(wkb), Some(tolerance)) => {
                        Geometry::new_from_wkb(wkb)
                            .and_then(|geom| geom.simplify(*tolerance))
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
        if arg_types.len() != 2 {
            return exec_err!("invalid number of arguments for udf {}", self.name());
        }
        if !matches!(
            arg_types[0],
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView
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
        Ok(vec![DataType::BinaryView, DataType::Float64])
    }
}
