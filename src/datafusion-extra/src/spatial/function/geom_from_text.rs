use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, BinaryArray, Int64Array, StringArray, new_null_array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result as DFResult, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use geos::Geometry;

use super::geos_ext::GeosExt;

pub fn st_geomfromtext() -> ScalarUDF {
    ScalarUDF::new_from_impl(GeomFromTextUDF {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64]),
            ],
            Volatility::Immutable,
        ),
        aliases: vec!["st_geomfromtext".to_owned()],
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct GeomFromTextUDF {
    signature: Signature,
    aliases: Vec<String>,
}

impl ScalarUDFImpl for GeomFromTextUDF {
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
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = args.args;
        if args.len() != 1 && args.len() != 2 {
            return exec_err!("invalid number of arguments for udf {}", self.name());
        }
        if args[0].data_type() != DataType::Utf8 {
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
                    let wkt_arr: &StringArray = arr.as_any().downcast_ref().unwrap();
                    let result: BinaryArray = wkt_arr
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
                ColumnarValue::Scalar(ScalarValue::Utf8(opt)) => {
                    let result = opt.as_ref().and_then(|wkt| {
                        Geometry::new_from_wkt(wkt)
                            .and_then(|geom| geom.to_ewkb())
                            .ok()
                    });

                    Ok(ColumnarValue::Scalar(ScalarValue::Binary(result)))
                }
                other => {
                    exec_err!("unsupported data type '{other:?}' for udf {}", self.name())
                }
            }
        } else {
            match (&args[0], &args[1]) {
                (ColumnarValue::Array(wkt_arr), ColumnarValue::Array(srid_arr)) => {
                    let wkt_arr: &StringArray = wkt_arr.as_any().downcast_ref().unwrap();
                    let srid_arr: &Int64Array = srid_arr.as_any().downcast_ref().unwrap();

                    let result: BinaryArray = wkt_arr
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
                            let wkt_arr: &StringArray = wkt_arr.as_any().downcast_ref().unwrap();
                            let result: BinaryArray = wkt_arr
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
                            let wkt_arr: &StringArray = wkt_arr.as_any().downcast_ref().unwrap();
                            let result: BinaryArray = wkt_arr
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
                    ColumnarValue::Scalar(ScalarValue::Utf8(wkt_opt)),
                    ColumnarValue::Array(srid_arr),
                ) => {
                    let result = match wkt_opt {
                        Some(wkt) => {
                            let srid_arr: &Int64Array = srid_arr.as_any().downcast_ref().unwrap();
                            let result: BinaryArray = srid_arr
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
                        None => new_null_array(&DataType::Binary, srid_arr.len()),
                    };
                    Ok(ColumnarValue::Array(result))
                }
                (
                    ColumnarValue::Scalar(ScalarValue::Utf8(wkt_opt)),
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

                    Ok(ColumnarValue::Scalar(ScalarValue::Binary(result)))
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
}
