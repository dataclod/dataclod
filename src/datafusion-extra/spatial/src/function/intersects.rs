use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, BooleanArray, new_null_array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result as DFResult, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use geos::Geometry;

use crate::geos_ext::GeomExt;

pub fn st_intersects() -> ScalarUDF {
    ScalarUDF::new_from_impl(IntersectsUdf {
        signature: Signature::user_defined(Volatility::Immutable),
        aliases: vec!["st_intersects".to_owned()],
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct IntersectsUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl ScalarUDFImpl for IntersectsUdf {
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

        match (&args[0], &args[1]) {
            (ColumnarValue::Array(wkb_arr1), ColumnarValue::Array(wkb_arr2)) => {
                let wkb_arr1 = wkb_arr1.as_binary_view();
                let wkb_arr2 = wkb_arr2.as_binary_view();
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
                ColumnarValue::Scalar(ScalarValue::BinaryView(wkb_opt2)),
            ) => {
                let result = match wkb_opt2 {
                    Some(wkb2) => {
                        match Geometry::new_from_wkb(wkb2) {
                            Ok(geom2) => {
                                let wkb_arr1 = wkb_arr1.as_binary_view();
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
                ColumnarValue::Scalar(ScalarValue::BinaryView(wkb_opt1)),
                ColumnarValue::Array(wkb_arr2),
            ) => {
                let result = match wkb_opt1 {
                    Some(wkb1) => {
                        match Geometry::new_from_wkb(wkb1) {
                            Ok(geom1) => {
                                let wkb_arr2 = wkb_arr2.as_binary_view();
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
                ColumnarValue::Scalar(ScalarValue::BinaryView(wkb_opt1)),
                ColumnarValue::Scalar(ScalarValue::BinaryView(wkb_opt2)),
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
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView
        ) {
            return exec_err!(
                "unsupported data type '{}' for udf {}",
                arg_types[1],
                self.name()
            );
        }
        Ok(vec![DataType::BinaryView, DataType::BinaryView])
    }
}
