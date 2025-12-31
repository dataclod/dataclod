use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, AsArray, BooleanArray, new_null_array};
use arrow::datatypes::{DataType, Float64Type};
use datafusion::common::{Result as DFResult, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use geos::{Geom, Geometry};

pub fn st_dwithin() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(DWithinUdf {
        signature: Signature::user_defined(Volatility::Immutable),
        aliases: vec!["st_dwithin".to_owned()],
    }))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct DWithinUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl ScalarUDFImpl for DWithinUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ST_DWithin"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = args.args;
        if args.len() != 3 {
            return exec_err!("invalid number of arguments for udf {}", self.name());
        }
        if args[0].data_type() != DataType::BinaryView {
            return exec_err!(
                "unsupported data type '{}' for udf {}",
                args[0].data_type(),
                self.name()
            );
        }
        if args[1].data_type() != DataType::BinaryView {
            return exec_err!(
                "unsupported data type '{}' for udf {}",
                args[1].data_type(),
                self.name()
            );
        }

        if args[2].data_type() != DataType::Float64 {
            return exec_err!(
                "unsupported data type '{}' for udf {}",
                args[2].data_type(),
                self.name()
            );
        }

        match (&args[0], &args[1], &args[2]) {
            (
                ColumnarValue::Array(wkb_arr1),
                ColumnarValue::Array(wkb_arr2),
                ColumnarValue::Array(distance_arr),
            ) => {
                let wkb_arr1 = wkb_arr1.as_binary_view();
                let wkb_arr2 = wkb_arr2.as_binary_view();
                let distance_arr = distance_arr.as_primitive::<Float64Type>();
                if wkb_arr1.len() != wkb_arr2.len() && wkb_arr1.len() != distance_arr.len() {
                    return exec_err!("array length mismatch for udf {}", self.name());
                }

                let result: BooleanArray = wkb_arr1
                    .iter()
                    .zip(wkb_arr2.iter())
                    .zip(distance_arr.iter())
                    .map(|opt| {
                        match opt {
                            ((Some(wkb1), Some(wkb2)), Some(target_distance)) => {
                                match (Geometry::new_from_wkb(wkb1), Geometry::new_from_wkb(wkb2)) {
                                    (Ok(geom1), Ok(geom2)) => {
                                        match geom1.distance(&geom2) {
                                            Ok(distance) => {
                                                if distance <= target_distance {
                                                    Some(true)
                                                } else {
                                                    Some(false)
                                                }
                                            }
                                            Err(_) => None,
                                        }
                                    }
                                    _ => None,
                                }
                            }
                            _ => None,
                        }
                    })
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            (
                ColumnarValue::Scalar(ScalarValue::BinaryView(wkb1_opt)),
                ColumnarValue::Array(wkb_arr2),
                ColumnarValue::Array(distance_arr),
            ) => {
                let wkb1 = match wkb1_opt {
                    Some(wkb) => wkb,
                    None => {
                        return Ok(ColumnarValue::Array(new_null_array(
                            &DataType::Boolean,
                            wkb_arr2.len(),
                        )));
                    }
                };
                let wkb_arr2 = wkb_arr2.as_binary_view();
                let distance_arr = distance_arr.as_primitive::<Float64Type>();
                if wkb_arr2.len() != distance_arr.len() {
                    return exec_err!("array length mismatch for udf {}", self.name());
                }

                let geom1 = match Geometry::new_from_wkb(wkb1) {
                    Ok(geom1) => geom1,
                    _ => {
                        return Ok(ColumnarValue::Array(new_null_array(
                            &DataType::Boolean,
                            wkb_arr2.len(),
                        )));
                    }
                };

                let result: BooleanArray = wkb_arr2
                    .iter()
                    .zip(distance_arr.iter())
                    .map(|opt| {
                        match opt {
                            (Some(wkb2), Some(target_distance)) => {
                                match Geometry::new_from_wkb(wkb2) {
                                    Ok(geom2) => {
                                        match geom2.distance(&geom1) {
                                            Ok(distance) => {
                                                if distance <= target_distance {
                                                    Some(true)
                                                } else {
                                                    Some(false)
                                                }
                                            }
                                            Err(_) => None,
                                        }
                                    }
                                    _ => None,
                                }
                            }
                            _ => None,
                        }
                    })
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            (
                ColumnarValue::Array(wkb_arr1),
                ColumnarValue::Scalar(ScalarValue::BinaryView(wkb2_opt)),
                ColumnarValue::Array(distance_arr),
            ) => {
                let wkb2 = match wkb2_opt {
                    Some(wkb) => wkb,
                    None => {
                        return Ok(ColumnarValue::Array(new_null_array(
                            &DataType::Boolean,
                            wkb_arr1.len(),
                        )));
                    }
                };
                let wkb_arr1 = wkb_arr1.as_binary_view();
                let distance_arr = distance_arr.as_primitive::<Float64Type>();
                if wkb_arr1.len() != distance_arr.len() {
                    return exec_err!("array length mismatch for udf {}", self.name());
                }

                let geom2 = match Geometry::new_from_wkb(wkb2) {
                    Ok(geom2) => geom2,
                    _ => {
                        return Ok(ColumnarValue::Array(new_null_array(
                            &DataType::Boolean,
                            wkb_arr1.len(),
                        )));
                    }
                };

                let result: BooleanArray = wkb_arr1
                    .iter()
                    .zip(distance_arr.iter())
                    .map(|opt| {
                        match opt {
                            (Some(wkb1), Some(target_distance)) => {
                                match Geometry::new_from_wkb(wkb1) {
                                    Ok(geom1) => {
                                        match geom1.distance(&geom2) {
                                            Ok(distance) => {
                                                if distance <= target_distance {
                                                    Some(true)
                                                } else {
                                                    Some(false)
                                                }
                                            }
                                            Err(_) => None,
                                        }
                                    }
                                    _ => None,
                                }
                            }
                            _ => None,
                        }
                    })
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }

            (
                ColumnarValue::Array(wkb_arr1),
                ColumnarValue::Array(wkb_arr2),
                ColumnarValue::Scalar(ScalarValue::Float64(target_distance_opt)),
            ) => {
                if wkb_arr1.len() != wkb_arr2.len() {
                    return exec_err!("array length mismatch for udf {}", self.name());
                }
                let target_distance = match target_distance_opt {
                    Some(distance) => distance,
                    None => {
                        return Ok(ColumnarValue::Array(new_null_array(
                            &DataType::Boolean,
                            wkb_arr1.len(),
                        )));
                    }
                };
                let wkb_arr1 = wkb_arr1.as_binary_view();
                let wkb_arr2 = wkb_arr2.as_binary_view();

                let result: BooleanArray = wkb_arr1
                    .iter()
                    .zip(wkb_arr2.iter())
                    .map(|opt| {
                        match opt {
                            (Some(wkb1), Some(wkb2)) => {
                                match (Geometry::new_from_wkb(wkb1), Geometry::new_from_wkb(wkb2)) {
                                    (Ok(geom1), Ok(geom2)) => {
                                        match geom1.distance(&geom2) {
                                            Ok(distance) => {
                                                if distance <= *target_distance {
                                                    Some(true)
                                                } else {
                                                    Some(false)
                                                }
                                            }
                                            Err(_) => None,
                                        }
                                    }
                                    _ => None,
                                }
                            }
                            _ => None,
                        }
                    })
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            (
                ColumnarValue::Array(wkb_arr1),
                ColumnarValue::Scalar(ScalarValue::BinaryView(wkb2_opt)),
                ColumnarValue::Scalar(ScalarValue::Float64(target_distance_opt)),
            ) => {
                let (wkb2, target_distance) = match (wkb2_opt, target_distance_opt) {
                    (Some(wkb), Some(distance)) => (wkb, distance),
                    _ => {
                        return Ok(ColumnarValue::Array(new_null_array(
                            &DataType::Boolean,
                            wkb_arr1.len(),
                        )));
                    }
                };
                let result = match Geometry::new_from_wkb(wkb2) {
                    Ok(geom2) => {
                        let wkb_arr1 = wkb_arr1.as_binary_view();
                        let result: BooleanArray = wkb_arr1
                            .iter()
                            .map(|opt| {
                                match opt {
                                    Some(wkb1) => {
                                        match Geometry::new_from_wkb(wkb1) {
                                            Ok(geom1) => {
                                                match geom1.distance(&geom2) {
                                                    Ok(distance) => {
                                                        if distance <= *target_distance {
                                                            Some(true)
                                                        } else {
                                                            Some(false)
                                                        }
                                                    }
                                                    Err(_) => None,
                                                }
                                            }
                                            _ => None,
                                        }
                                    }
                                    _ => None,
                                }
                            })
                            .collect();
                        Arc::new(result)
                    }
                    _ => new_null_array(&DataType::Boolean, wkb_arr1.len()),
                };
                Ok(ColumnarValue::Array(result))
            }
            (
                ColumnarValue::Scalar(ScalarValue::BinaryView(wkb1_opt)),
                ColumnarValue::Array(wkb_arr2),
                ColumnarValue::Scalar(ScalarValue::Float64(target_distance_opt)),
            ) => {
                let (wkb1, target_distance) = match (wkb1_opt, target_distance_opt) {
                    (Some(wkb), Some(distance)) => (wkb, distance),
                    _ => {
                        return Ok(ColumnarValue::Array(new_null_array(
                            &DataType::Boolean,
                            wkb_arr2.len(),
                        )));
                    }
                };
                let result = match Geometry::new_from_wkb(wkb1) {
                    Ok(geom1) => {
                        let wkb_arr2 = wkb_arr2.as_binary_view();
                        let result: BooleanArray = wkb_arr2
                            .iter()
                            .map(|opt| {
                                match opt {
                                    Some(wkb2) => {
                                        match Geometry::new_from_wkb(wkb2) {
                                            Ok(geom2) => {
                                                match geom2.distance(&geom1) {
                                                    Ok(distance) => {
                                                        if distance <= *target_distance {
                                                            Some(true)
                                                        } else {
                                                            Some(false)
                                                        }
                                                    }
                                                    Err(_) => None,
                                                }
                                            }
                                            _ => None,
                                        }
                                    }
                                    _ => None,
                                }
                            })
                            .collect();
                        Arc::new(result)
                    }
                    _ => new_null_array(&DataType::Boolean, wkb_arr2.len()),
                };
                Ok(ColumnarValue::Array(result))
            }
            (
                ColumnarValue::Scalar(ScalarValue::BinaryView(wkb1_opt)),
                ColumnarValue::Scalar(ScalarValue::BinaryView(wkb2_opt)),
                ColumnarValue::Scalar(ScalarValue::Float64(target_distance_opt)),
            ) => {
                let (wkb1, wkb2, target_distance) = match (wkb1_opt, wkb2_opt, target_distance_opt)
                {
                    (Some(wkb1), Some(wkb2), Some(distance)) => (wkb1, wkb2, distance),
                    _ => {
                        return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
                    }
                };
                let result = match (Geometry::new_from_wkb(wkb1), Geometry::new_from_wkb(wkb2)) {
                    (Ok(geom1), Ok(geom2)) => {
                        match geom1.distance(&geom2) {
                            Ok(distance) => {
                                if distance <= *target_distance {
                                    Some(true)
                                } else {
                                    Some(false)
                                }
                            }
                            Err(_) => None,
                        }
                    }
                    _ => None,
                };

                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(result)))
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
        if arg_types.len() != 3 {
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
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView
        ) {
            return exec_err!(
                "unsupported data type '{}' for udf {}",
                arg_types[1],
                self.name()
            );
        }
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
            DataType::BinaryView,
            DataType::Float64,
        ])
    }
}
