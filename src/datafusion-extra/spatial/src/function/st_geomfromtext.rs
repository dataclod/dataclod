use std::any::Any;
use std::sync::Arc;

use arrow::array::{AsArray, BinaryViewArray, new_null_array};
use arrow::datatypes::{DataType, Field, FieldRef, Int64Type};
use datafabric_common_schema::schema_ext::FIELD_TARGET_TYPE;
use datafusion::common::{Result as DFResult, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use geos::Geometry;

use crate::utils::GeosExt;

pub fn st_geomfromtext() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(GeomFromTextUdf {
        signature: Signature::user_defined(Volatility::Immutable),
        aliases: vec!["st_geomfromtext".to_owned()],
    }))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct GeomFromTextUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl ScalarUDFImpl for GeomFromTextUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ST_GeomFromText"
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
        if args.len() != 1 && args.len() != 2 {
            return exec_err!("invalid number of arguments for udf {}", self.name());
        }
        if args[0].data_type() != DataType::Utf8View {
            return exec_err!(
                "unsupported data type '{}' for udf {}",
                args[0].data_type(),
                self.name()
            );
        }
        if args.len() == 2 && args[1].data_type() != DataType::Int64 {
            return exec_err!(
                "unsupported data type '{}' for udf {}",
                args[1].data_type(),
                self.name()
            );
        }

        if args.len() == 1 {
            match &args[0] {
                ColumnarValue::Array(arr) => {
                    let wkt_arr = arr.as_string_view();
                    let result: BinaryViewArray = wkt_arr
                        .iter()
                        .map(|opt| {
                            opt.and_then(|wkt| {
                                Geometry::new_from_wkt(wkt)
                                    .and_then(|geom| geom.to_ewkb())
                                    .ok()
                            })
                        })
                        .collect();

                    Ok(ColumnarValue::Array(Arc::new(result)))
                }
                ColumnarValue::Scalar(ScalarValue::Utf8View(opt)) => {
                    let result = opt.as_ref().and_then(|wkt| {
                        Geometry::new_from_wkt(wkt)
                            .and_then(|geom| geom.to_ewkb())
                            .ok()
                    });

                    Ok(ColumnarValue::Scalar(ScalarValue::BinaryView(result)))
                }
                other => {
                    exec_err!("unsupported data type '{other:?}' for udf {}", self.name())
                }
            }
        } else {
            match (&args[0], &args[1]) {
                (ColumnarValue::Array(wkt_arr), ColumnarValue::Array(srid_arr)) => {
                    let wkt_arr = wkt_arr.as_string_view();
                    let srid_arr = srid_arr.as_primitive::<Int64Type>();

                    let result: BinaryViewArray = wkt_arr
                        .iter()
                        .zip(srid_arr.iter())
                        .map(|opt| {
                            match opt {
                                (Some(wkt), Some(srid)) => {
                                    Geometry::new_from_wkt(wkt)
                                        .and_then(|mut geom| {
                                            geom.set_srid(srid as libc::c_int);
                                            geom.to_ewkb()
                                        })
                                        .ok()
                                }
                                (Some(wkt), None) => {
                                    Geometry::new_from_wkt(wkt)
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
                    ColumnarValue::Array(wkt_arr),
                    ColumnarValue::Scalar(ScalarValue::Int64(srid_opt)),
                ) => {
                    let result = match srid_opt {
                        Some(srid) => {
                            let wkt_arr = wkt_arr.as_string_view();
                            let result: BinaryViewArray = wkt_arr
                                .iter()
                                .map(|opt| {
                                    opt.and_then(|wkt| {
                                        Geometry::new_from_wkt(wkt)
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
                        None => {
                            let wkt_arr = wkt_arr.as_string_view();
                            let result: BinaryViewArray = wkt_arr
                                .iter()
                                .map(|opt| {
                                    opt.and_then(|wkt| {
                                        Geometry::new_from_wkt(wkt)
                                            .and_then(|geom| geom.to_ewkb())
                                            .ok()
                                    })
                                })
                                .collect();
                            Arc::new(result)
                        }
                    };
                    Ok(ColumnarValue::Array(result))
                }
                (
                    ColumnarValue::Scalar(ScalarValue::Utf8View(wkt_opt)),
                    ColumnarValue::Array(srid_arr),
                ) => {
                    let result = match wkt_opt {
                        Some(wkt) => {
                            let srid_arr = srid_arr.as_primitive::<Int64Type>();
                            let result: BinaryViewArray = srid_arr
                                .iter()
                                .map(|opt| {
                                    match opt {
                                        Some(srid) => {
                                            Geometry::new_from_wkt(wkt)
                                                .and_then(|mut geom| {
                                                    geom.set_srid(srid as libc::c_int);
                                                    geom.to_ewkb()
                                                })
                                                .ok()
                                        }
                                        None => {
                                            Geometry::new_from_wkt(wkt)
                                                .and_then(|geom| geom.to_ewkb())
                                                .ok()
                                        }
                                    }
                                })
                                .collect();
                            Arc::new(result)
                        }
                        None => new_null_array(&DataType::BinaryView, srid_arr.len()),
                    };
                    Ok(ColumnarValue::Array(result))
                }
                (
                    ColumnarValue::Scalar(ScalarValue::Utf8View(wkt_opt)),
                    ColumnarValue::Scalar(ScalarValue::Int64(srid_opt)),
                ) => {
                    let result = match (wkt_opt, srid_opt) {
                        (Some(wkt), Some(srid)) => {
                            Geometry::new_from_wkt(wkt)
                                .and_then(|mut geom| {
                                    geom.set_srid(*srid as libc::c_int);
                                    geom.to_ewkb()
                                })
                                .ok()
                        }
                        (Some(wkt), None) => {
                            Geometry::new_from_wkt(wkt)
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
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        let len = arg_types.len();
        if !(1..=2).contains(&len) {
            return exec_err!("invalid number of arguments for udf {}", self.name());
        }
        if !matches!(
            arg_types[0],
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ) {
            return exec_err!(
                "unsupported data type '{}' for udf {}",
                arg_types[0],
                self.name()
            );
        }
        if len == 1 {
            Ok(vec![DataType::Utf8View])
        } else {
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
            Ok(vec![DataType::Utf8View, DataType::Int64])
        }
    }
}
