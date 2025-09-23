use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, BinaryArray, BooleanArray, new_null_array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result as DFResult, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use geos::Geometry;

use super::geos_ext::GeosExt;

pub fn st_intersects() -> ScalarUDF {
    ScalarUDF::new_from_impl(IntersectsUDF {
        signature: Signature::exact(
            vec![DataType::Binary, DataType::Binary],
            Volatility::Immutable,
        ),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct IntersectsUDF {
    signature: Signature,
}

impl ScalarUDFImpl for IntersectsUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ST_Intersects"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = args.args;
        if args.len() != 2 {
            return exec_err!("invalid number of arguments for udf {}", self.name());
        }
        if args[0].data_type() != DataType::Binary {
            return exec_err!(
                "unsupported data type '{}' for udf {}",
                args[0].data_type(),
                self.name()
            );
        }
        if args[1].data_type() != DataType::Binary {
            return exec_err!(
                "unsupported data type '{}' for udf {}",
                args[1].data_type(),
                self.name()
            );
        }

        match (&args[0], &args[1]) {
            (ColumnarValue::Array(wkb_arr1), ColumnarValue::Array(wkb_arr2)) => {
                let wkb_arr1: &BinaryArray = wkb_arr1.as_any().downcast_ref().unwrap();
                let wkb_arr2: &BinaryArray = wkb_arr2.as_any().downcast_ref().unwrap();
                if wkb_arr1.len() != wkb_arr2.len() {
                    return exec_err!("array length mismatch for udf {}", self.name());
                }

                let result: BooleanArray = wkb_arr1
                    .iter()
                    .zip(wkb_arr2.iter())
                    .map(|opt| {
                        match opt {
                            (Some(wkb1), Some(wkb2)) => {
                                match (Geometry::new_from_wkb(wkb1), Geometry::new_from_wkb(wkb2)) {
                                    (Ok(geom1), Ok(geom2)) => geom1.st_intersects(&geom2),
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
                ColumnarValue::Scalar(ScalarValue::Binary(wkb_opt2)),
            ) => {
                let result = match wkb_opt2 {
                    Some(wkb2) => {
                        match Geometry::new_from_wkb(wkb2) {
                            Ok(geom2) => {
                                let wkb_arr1: &BinaryArray =
                                    wkb_arr1.as_any().downcast_ref().unwrap();
                                let result: BooleanArray = wkb_arr1
                                    .iter()
                                    .map(|opt| {
                                        opt.and_then(|wkb1| {
                                            Geometry::new_from_wkb(wkb1)
                                                .ok()
                                                .and_then(|geom1| geom1.st_intersects(&geom2))
                                        })
                                    })
                                    .collect();
                                Arc::new(result)
                            }
                            _ => new_null_array(&DataType::Boolean, wkb_arr1.len()),
                        }
                    }
                    None => new_null_array(&DataType::Boolean, wkb_arr1.len()),
                };
                Ok(ColumnarValue::Array(result))
            }
            (
                ColumnarValue::Scalar(ScalarValue::Binary(wkb_opt1)),
                ColumnarValue::Array(wkb_arr2),
            ) => {
                let result = match wkb_opt1 {
                    Some(wkb1) => {
                        match Geometry::new_from_wkb(wkb1) {
                            Ok(geom1) => {
                                let wkb_arr2: &BinaryArray =
                                    wkb_arr2.as_any().downcast_ref().unwrap();
                                let result: BooleanArray = wkb_arr2
                                    .iter()
                                    .map(|opt| {
                                        opt.and_then(|wkb2| {
                                            Geometry::new_from_wkb(wkb2)
                                                .ok()
                                                .and_then(|geom2| geom1.st_intersects(&geom2))
                                        })
                                    })
                                    .collect();
                                Arc::new(result)
                            }
                            _ => new_null_array(&DataType::Boolean, wkb_arr2.len()),
                        }
                    }
                    None => new_null_array(&DataType::Boolean, wkb_arr2.len()),
                };
                Ok(ColumnarValue::Array(result))
            }
            (
                ColumnarValue::Scalar(ScalarValue::Binary(wkb_opt1)),
                ColumnarValue::Scalar(ScalarValue::Binary(wkb_opt2)),
            ) => {
                let result = match (wkb_opt1, wkb_opt2) {
                    (Some(wkb1), Some(wkb2)) => {
                        match (Geometry::new_from_wkb(wkb1), Geometry::new_from_wkb(wkb2)) {
                            (Ok(geom1), Ok(geom2)) => geom1.st_intersects(&geom2),
                            _ => None,
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
}
