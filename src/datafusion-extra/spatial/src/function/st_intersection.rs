use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, AsArray, BinaryViewArray, new_null_array};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafabric_common_schema::schema_ext::FIELD_TARGET_TYPE;
use datafusion::common::{Result as DFResult, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use geos::{Geom, Geometry};

use crate::utils::GeosExt;

pub fn st_intersection() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(IntersectionUdf {
        signature: Signature::user_defined(Volatility::Immutable),
        aliases: vec!["st_intersection".to_owned()],
    }))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct IntersectionUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl ScalarUDFImpl for IntersectionUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ST_Intersection"
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
                "unsupported data type '{:?}' for udf {}",
                args[0].data_type(),
                self.name()
            );
        }
        if args[1].data_type() != DataType::BinaryView {
            return exec_err!(
                "unsupported data type '{:?}' for udf {}",
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

                let result: BinaryViewArray = wkb_arr1
                    .iter()
                    .zip(wkb_arr2.iter())
                    .map(|opt| {
                        match opt {
                            (Some(wkb1), Some(wkb2)) => {
                                match (Geometry::new_from_wkb(wkb1), Geometry::new_from_wkb(wkb2)) {
                                    (Ok(geom1), Ok(geom2)) => {
                                        geom1
                                            .intersection(&geom2)
                                            .and_then(|geom| geom.to_ewkb())
                                            .ok()
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
                ColumnarValue::Scalar(ScalarValue::BinaryView(wkb_opt2)),
            ) => {
                let result = match wkb_opt2 {
                    Some(wkb2) => {
                        match Geometry::new_from_wkb(wkb2) {
                            Ok(geom2) => {
                                let wkb_arr1 = wkb_arr1.as_binary_view();
                                let result: BinaryViewArray = wkb_arr1
                                    .iter()
                                    .map(|opt| {
                                        opt.and_then(|wkb1| {
                                            Geometry::new_from_wkb(wkb1)
                                                .and_then(|geom1| geom1.intersection(&geom2))
                                                .and_then(|geom| geom.to_ewkb())
                                                .ok()
                                        })
                                    })
                                    .collect();
                                Arc::new(result)
                            }
                            _ => new_null_array(&DataType::BinaryView, wkb_arr1.len()),
                        }
                    }
                    None => new_null_array(&DataType::BinaryView, wkb_arr1.len()),
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
                                let result: BinaryViewArray = wkb_arr2
                                    .iter()
                                    .map(|opt| {
                                        opt.and_then(|wkb2| {
                                            Geometry::new_from_wkb(wkb2)
                                                .and_then(|geom2| geom1.intersection(&geom2))
                                                .and_then(|geom| geom.to_ewkb())
                                                .ok()
                                        })
                                    })
                                    .collect();
                                Arc::new(result)
                            }
                            _ => new_null_array(&DataType::BinaryView, wkb_arr2.len()),
                        }
                    }
                    None => new_null_array(&DataType::BinaryView, wkb_arr2.len()),
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
                            (Ok(geom1), Ok(geom2)) => {
                                geom1
                                    .intersection(&geom2)
                                    .and_then(|geom| geom.to_ewkb())
                                    .ok()
                            }
                            _ => None,
                        }
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
