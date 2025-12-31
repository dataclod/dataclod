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
use geos::Geom;
use geozero::{CoordDimensions, ToGeo, ToWkb};

use crate::utils::GeozeroExt;

pub fn st_simplifyvw() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(SimplifyVWUdf {
        signature: Signature::user_defined(Volatility::Immutable),
        aliases: vec!["st_simplifyvw".to_owned()],
    }))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SimplifyVWUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl ScalarUDFImpl for SimplifyVWUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ST_SimplifyVW"
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
        if args[1].data_type() != DataType::Float64 {
            return exec_err!(
                "unsupported data type '{:?}' for udf {}",
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
                                match (
                                    geos::Geometry::new_from_wkb(wkb),
                                    geozero::wkb::Ewkb(wkb.to_vec()).to_geo(),
                                ) {
                                    (Ok(geos_geom), Ok(geo_geom)) => {
                                        geo_geom.st_simplifyvw(tolerance).and_then(|geom| {
                                            geom.to_ewkb(
                                                CoordDimensions::xy(),
                                                geos_geom.get_srid().ok(),
                                            )
                                            .ok()
                                        })
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
                                    match (
                                        geos::Geometry::new_from_wkb(wkb),
                                        geozero::wkb::Ewkb(wkb.to_vec()).to_geo(),
                                    ) {
                                        (Ok(geos_geom), Ok(geo_geom)) => {
                                            geo_geom.st_simplifyvw(*tolerance).and_then(|geom| {
                                                geom.to_ewkb(
                                                    CoordDimensions::xy(),
                                                    geos_geom.get_srid().ok(),
                                                )
                                                .ok()
                                            })
                                        }
                                        _ => None,
                                    }
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
                        match (
                            geos::Geometry::new_from_wkb(wkb),
                            geozero::wkb::Ewkb(wkb.to_vec()).to_geo(),
                        ) {
                            (Ok(geos_geom), Ok(geo_geom)) => {
                                let tolerance_arr = tolerance_arr.as_primitive::<Float64Type>();
                                let result: BinaryViewArray = tolerance_arr
                                    .iter()
                                    .map(|opt| {
                                        opt.and_then(|tolerance| {
                                            geo_geom.st_simplifyvw(tolerance).and_then(|geom| {
                                                geom.to_ewkb(
                                                    CoordDimensions::xy(),
                                                    geos_geom.get_srid().ok(),
                                                )
                                                .ok()
                                            })
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
                        match (
                            geos::Geometry::new_from_wkb(wkb),
                            geozero::wkb::Ewkb(wkb.to_vec()).to_geo(),
                        ) {
                            (Ok(geos_geom), Ok(geo_geom)) => {
                                geo_geom.st_simplifyvw(*tolerance).and_then(|geom| {
                                    geom.to_ewkb(CoordDimensions::xy(), geos_geom.get_srid().ok())
                                        .ok()
                                })
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
