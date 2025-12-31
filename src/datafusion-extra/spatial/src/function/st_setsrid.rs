use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, AsArray, BinaryViewArray, new_null_array};
use arrow::datatypes::{DataType, Field, FieldRef, Int64Type};
use datafabric_common_schema::schema_ext::FIELD_TARGET_TYPE;
use datafusion::common::{Result as DFResult, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use geos::Geometry;

use crate::utils::GeosExt;

pub fn st_setsrid() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(SetSridUdf {
        signature: Signature::user_defined(Volatility::Immutable),
        aliases: vec!["st_setsrid".to_owned()],
    }))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SetSridUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl ScalarUDFImpl for SetSridUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ST_SetSrid"
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
        if args[1].data_type() != DataType::Int64 {
            return exec_err!(
                "unsupported data type '{}' for udf {}",
                args[1].data_type(),
                self.name()
            );
        }

        match (&args[0], &args[1]) {
            (ColumnarValue::Array(wkb_arr), ColumnarValue::Array(srid_arr)) => {
                let wkb_arr = wkb_arr.as_binary_view();
                let srid_arr = srid_arr.as_primitive::<Int64Type>();
                if wkb_arr.len() != srid_arr.len() {
                    return exec_err!("array length mismatch for udf {}", self.name());
                }

                let result: BinaryViewArray = wkb_arr
                    .iter()
                    .zip(srid_arr.iter())
                    .map(|opt| {
                        match opt {
                            (Some(wkb), Some(srid)) => {
                                Geometry::new_from_wkb(wkb)
                                    .and_then(|mut geom| {
                                        geom.set_srid(srid as libc::c_int);
                                        geom.to_ewkb()
                                    })
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
                ColumnarValue::Scalar(ScalarValue::Int64(srid_opt)),
            ) => {
                let result = match srid_opt {
                    Some(srid) => {
                        let wkb_arr = wkb_arr.as_binary_view();
                        let result: BinaryViewArray = wkb_arr
                            .iter()
                            .map(|opt| {
                                opt.and_then(|wkb| {
                                    Geometry::new_from_wkb(wkb)
                                        .and_then(|mut geom| {
                                            geom.set_srid(*srid as libc::c_int);
                                            geom.to_ewkb()
                                        })
                                        .ok()
                                })
                            })
                            .collect();
                        Arc::new(result)
                    }
                    None => wkb_arr.clone(),
                };
                Ok(ColumnarValue::Array(result))
            }
            (
                ColumnarValue::Scalar(ScalarValue::BinaryView(wkb_opt)),
                ColumnarValue::Array(srid_arr),
            ) => {
                let result = match wkb_opt {
                    Some(wkb) => {
                        match Geometry::new_from_wkb(wkb) {
                            Ok(mut geom) => {
                                let srid_arr = srid_arr.as_primitive::<Int64Type>();
                                let result: BinaryViewArray = srid_arr
                                    .iter()
                                    .map(|opt| {
                                        opt.and_then(|srid| {
                                            geom.set_srid(srid as libc::c_int);
                                            geom.to_ewkb().ok()
                                        })
                                    })
                                    .collect();
                                Arc::new(result)
                            }
                            _ => new_null_array(&DataType::BinaryView, srid_arr.len()),
                        }
                    }
                    None => new_null_array(&DataType::BinaryView, srid_arr.len()),
                };
                Ok(ColumnarValue::Array(result))
            }
            (
                ColumnarValue::Scalar(ScalarValue::BinaryView(wkb_opt)),
                ColumnarValue::Scalar(ScalarValue::Int64(srid_opt)),
            ) => {
                let result = match (wkb_opt, srid_opt) {
                    (Some(wkb), Some(srid)) => {
                        Geometry::new_from_wkb(wkb)
                            .and_then(|mut geom| {
                                geom.set_srid(*srid as libc::c_int);
                                geom.to_ewkb()
                            })
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
        Ok(vec![DataType::BinaryView, DataType::Int64])
    }
}
