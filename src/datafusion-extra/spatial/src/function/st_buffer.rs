use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, AsArray, BinaryViewArray, new_null_array};
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafabric_common_schema::schema_ext::FIELD_TARGET_TYPE;
use datafusion::common::{Result as DFResult, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::scalar::ScalarValue;
use geos::{Geom, Geometry};

use crate::utils::GeosExt;

pub fn st_buffer() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(BufferUdf {
        signature: Signature::user_defined(Volatility::Immutable),
        aliases: vec!["st_buffer".to_owned()],
    }))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct BufferUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl ScalarUDFImpl for BufferUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ST_Buffer"
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
        if args.len() < 2 || args.len() > 3 {
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
        if args.len() > 2 && args[2].data_type() != DataType::Int64 {
            return exec_err!(
                "unsupported data type '{}' for udf {}",
                args[2].data_type(),
                self.name()
            );
        }

        let quadsegs =
            if let Some(ColumnarValue::Scalar(ScalarValue::Int64(Some(quadsegs)))) = args.get(2) {
                *quadsegs as i32
            } else {
                8
            };

        match (&args[0], &args[1]) {
            (ColumnarValue::Array(geom_arr), ColumnarValue::Array(radius_arr)) => {
                let geom_arr = geom_arr.as_binary_view();
                let radius_arr = radius_arr.as_primitive::<Float64Type>();
                if geom_arr.len() != radius_arr.len() {
                    return exec_err!("array length mismatch for udf {}", self.name());
                }

                let result: BinaryViewArray = geom_arr
                    .iter()
                    .zip(radius_arr.iter())
                    .map(|opt| {
                        match opt {
                            (Some(wkb), Some(radius)) => {
                                Geometry::new_from_wkb(wkb)
                                    .and_then(|geom| geom.buffer(radius, quadsegs))
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
                ColumnarValue::Array(geom_arr),
                ColumnarValue::Scalar(ScalarValue::Float64(radius_opt)),
            ) => {
                match radius_opt {
                    Some(radius) => {
                        let geom_arr = geom_arr.as_binary_view();
                        let result: BinaryViewArray = geom_arr
                            .iter()
                            .map(|opt| {
                                opt.and_then(|wkb| {
                                    Geometry::new_from_wkb(wkb)
                                        .and_then(|geom| geom.buffer(*radius, quadsegs))
                                        .and_then(|geom| geom.to_ewkb())
                                        .ok()
                                })
                            })
                            .collect();
                        Ok(ColumnarValue::Array(Arc::new(result)))
                    }
                    _ => {
                        Ok(ColumnarValue::Array(new_null_array(
                            &DataType::BinaryView,
                            geom_arr.len(),
                        )))
                    }
                }
            }
            (
                ColumnarValue::Scalar(ScalarValue::BinaryView(geom_opt)),
                ColumnarValue::Array(radius_arr),
            ) => {
                match geom_opt {
                    Some(wkb) => {
                        match Geometry::new_from_wkb(wkb) {
                            Ok(geom) => {
                                let radius_arr = radius_arr.as_primitive::<Float64Type>();
                                let result: BinaryViewArray = radius_arr
                                    .iter()
                                    .map(|opt| {
                                        opt.and_then(|radius| geom.buffer(radius, quadsegs).ok())
                                            .and_then(|geom| geom.to_ewkb().ok())
                                    })
                                    .collect();
                                Ok(ColumnarValue::Array(Arc::new(result)))
                            }
                            _ => {
                                Ok(ColumnarValue::Array(new_null_array(
                                    &DataType::BinaryView,
                                    radius_arr.len(),
                                )))
                            }
                        }
                    }
                    _ => {
                        Ok(ColumnarValue::Array(new_null_array(
                            &DataType::BinaryView,
                            radius_arr.len(),
                        )))
                    }
                }
            }
            (
                ColumnarValue::Scalar(ScalarValue::BinaryView(geom_opt)),
                ColumnarValue::Scalar(ScalarValue::Float64(radius_opt)),
            ) => {
                let result = match (geom_opt, radius_opt) {
                    (Some(wkb), Some(radius)) => {
                        Geometry::new_from_wkb(wkb)
                            .and_then(|geom| geom.buffer(*radius, quadsegs))
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
        if arg_types.len() != 2 && arg_types.len() != 3 {
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
        if arg_types.len() == 2 {
            Ok(vec![DataType::BinaryView, DataType::Float64])
        } else {
            if !matches!(
                arg_types[2],
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
                    arg_types[2],
                    self.name()
                );
            }
            Ok(vec![
                DataType::BinaryView,
                DataType::Float64,
                DataType::Int64,
            ])
        }
    }
}
