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
use geos::{Geom, Geometry};
use itertools::multizip;

use crate::utils::GeosExt;

pub fn st_concavehull() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(ConcaveHullUdf {
        signature: Signature::user_defined(Volatility::Immutable),
        aliases: vec!["st_concavehull".to_owned()],
    }))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ConcaveHullUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl ScalarUDFImpl for ConcaveHullUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ST_ConcaveHull"
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
        if args.len() != 2 && args.len() != 3 {
            return exec_err!("invalid number of arguments for udf {}", self.name());
        }
        if args[0].data_type() != DataType::BinaryView {
            return exec_err!(
                "unsupported data type '{:?}' for udf {}",
                args[0].data_type(),
                self.name()
            );
        }
        if args[1].data_type() != DataType::Float64 {
            return exec_err!(
                "unsupported data type '{:?}' for udf {}",
                args[1].data_type(),
                self.name()
            );
        }
        if args.len() == 3 && args[2].data_type() != DataType::Boolean {
            return exec_err!(
                "unsupported data type '{:?}' for udf {}",
                args[2].data_type(),
                self.name()
            );
        }

        match (&args[0], &args[1]) {
            (ColumnarValue::Array(wkb_arr), ColumnarValue::Array(pctconvex_arr)) => {
                let wkb_arr = wkb_arr.as_binary_view();
                let pctconvex_arr = pctconvex_arr.as_primitive::<Float64Type>();

                let result: BinaryViewArray = if args.len() == 3 {
                    match &args[2] {
                        ColumnarValue::Array(allow_holes_arr) => {
                            let allow_holes_arr = allow_holes_arr.as_boolean();

                            multizip((wkb_arr, pctconvex_arr, allow_holes_arr))
                                .map(|opt| {
                                    match opt {
                                        (Some(wkb), Some(pctconvex), allow_holes) => {
                                            Geometry::new_from_wkb(wkb)
                                                .and_then(|geom| {
                                                    geom.concave_hull(
                                                        pctconvex,
                                                        allow_holes.unwrap_or(false),
                                                    )
                                                })
                                                .and_then(|geom| geom.to_ewkb())
                                                .ok()
                                        }
                                        _ => None,
                                    }
                                })
                                .collect()
                        }
                        ColumnarValue::Scalar(ScalarValue::Boolean(allow_holes)) => {
                            wkb_arr
                                .iter()
                                .zip(pctconvex_arr.iter())
                                .map(|opt| {
                                    match opt {
                                        (Some(wkb), Some(pctconvex)) => {
                                            Geometry::new_from_wkb(wkb)
                                                .and_then(|geom| {
                                                    geom.concave_hull(
                                                        pctconvex,
                                                        allow_holes.unwrap_or(false),
                                                    )
                                                })
                                                .and_then(|geom| geom.to_ewkb())
                                                .ok()
                                        }
                                        _ => None,
                                    }
                                })
                                .collect()
                        }
                        other => {
                            return exec_err!(
                                "unsupported data type '{}' for udf {}",
                                other,
                                self.name()
                            );
                        }
                    }
                } else {
                    wkb_arr
                        .iter()
                        .zip(pctconvex_arr.iter())
                        .map(|opt| {
                            match opt {
                                (Some(wkb), Some(pctconvex)) => {
                                    Geometry::new_from_wkb(wkb)
                                        .and_then(|geom| geom.concave_hull(pctconvex, false))
                                        .and_then(|geom| geom.to_ewkb())
                                        .ok()
                                }
                                _ => None,
                            }
                        })
                        .collect()
                };

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            (
                ColumnarValue::Array(wkb_arr),
                ColumnarValue::Scalar(ScalarValue::Float64(concavity_opt)),
            ) => {
                let allow_holes = if args.len() == 3 {
                    if let ColumnarValue::Scalar(ScalarValue::Boolean(allow_holes)) = &args[2] {
                        allow_holes.unwrap_or(false)
                    } else {
                        return exec_err!(
                            "unsupported data type '{}' for udf {}",
                            args[2].data_type(),
                            self.name()
                        );
                    }
                } else {
                    false
                };

                let result = match concavity_opt {
                    Some(concavity) => {
                        let wkb_arr = wkb_arr.as_binary_view();
                        let result: BinaryViewArray = wkb_arr
                            .iter()
                            .map(|opt| {
                                opt.and_then(|wkb| {
                                    Geometry::new_from_wkb(wkb)
                                        .and_then(|geom| geom.concave_hull(*concavity, allow_holes))
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
                ColumnarValue::Scalar(ScalarValue::Float64(concavity_opt)),
            ) => {
                let allow_holes = if args.len() == 3 {
                    if let ColumnarValue::Scalar(ScalarValue::Boolean(allow_holes)) = &args[2] {
                        allow_holes.unwrap_or(false)
                    } else {
                        return exec_err!(
                            "unsupported data type '{}' for udf {}",
                            args[2].data_type(),
                            self.name()
                        );
                    }
                } else {
                    false
                };

                let result = match (wkb_opt, concavity_opt) {
                    (Some(wkb), Some(concavity)) => {
                        Geometry::new_from_wkb(wkb)
                            .and_then(|geom| geom.concave_hull(*concavity, allow_holes))
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
        if !(2..=3).contains(&len) {
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
        if len == 2 {
            Ok(vec![DataType::BinaryView, DataType::Float64])
        } else {
            if !matches!(arg_types[2], DataType::Boolean) {
                return exec_err!(
                    "unsupported data type '{}' for udf {}",
                    arg_types[2],
                    self.name()
                );
            }
            Ok(vec![
                DataType::BinaryView,
                DataType::Float64,
                DataType::Boolean,
            ])
        }
    }
}
