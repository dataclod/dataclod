use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, AsArray, BinaryViewArray, new_null_array};
use arrow::datatypes::{DataType, Field, FieldRef, Fields, Float64Type};
use datafabric_common_schema::schema_ext::FIELD_TARGET_TYPE;
use datafusion::common::{Result as DFResult, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::scalar::ScalarValue;
use itertools::multizip;
use lwgeom::{GBox, LWGeom};

fn box2d_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("xmin", DataType::Float64, true),
        Field::new("ymin", DataType::Float64, true),
        Field::new("xmax", DataType::Float64, true),
        Field::new("ymax", DataType::Float64, true),
    ]))
}

pub fn st_asmvtgeom() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(AsMVTGeomUdf {
        signature: Signature::user_defined(Volatility::Immutable),
        aliases: vec!["st_asmvtgeom".to_string()],
    }))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct AsMVTGeomUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl ScalarUDFImpl for AsMVTGeomUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ST_AsMVTGeom"
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
        if args.len() < 2 || args.len() > 5 {
            return exec_err!("invalid number of arguments for udf {}", self.name());
        }
        if args[0].data_type() != DataType::BinaryView {
            return exec_err!(
                "unsupported data type '{}' for udf {}",
                args[0].data_type(),
                self.name()
            );
        }
        if args[1].data_type() != box2d_type() {
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
        if args.len() > 3 && args[3].data_type() != DataType::Int64 {
            return exec_err!(
                "unsupported data type '{}' for udf {}",
                args[3].data_type(),
                self.name()
            );
        }
        if args.len() > 4 && args[4].data_type() != DataType::Boolean {
            return exec_err!(
                "unsupported data type '{}' for udf {}",
                args[4].data_type(),
                self.name()
            );
        }

        let extent =
            if let Some(ColumnarValue::Scalar(ScalarValue::Int64(Some(extent)))) = args.get(2) {
                *extent
            } else {
                4096
            };
        let buffer =
            if let Some(ColumnarValue::Scalar(ScalarValue::Int64(Some(buffer)))) = args.get(3) {
                *buffer
            } else {
                256
            };
        let clip_geom = if let Some(ColumnarValue::Scalar(ScalarValue::Boolean(Some(clip_geom)))) =
            args.get(4)
        {
            *clip_geom
        } else {
            true
        };

        match (&args[0], &args[1]) {
            (ColumnarValue::Array(geom_arr), ColumnarValue::Array(bounds_arr)) => {
                let geom_arr = geom_arr.as_binary_view();
                let bounds_arr = bounds_arr.as_struct();
                if geom_arr.len() != bounds_arr.len() {
                    return exec_err!("array length mismatch for udf {}", self.name());
                }
                let bounds_xmin_arr = bounds_arr.column(0).as_primitive::<Float64Type>();
                let bounds_ymin_arr = bounds_arr.column(1).as_primitive::<Float64Type>();
                let bounds_xmax_arr = bounds_arr.column(2).as_primitive::<Float64Type>();
                let bounds_ymax_arr = bounds_arr.column(3).as_primitive::<Float64Type>();

                let result: BinaryViewArray = multizip((
                    geom_arr,
                    bounds_xmin_arr,
                    bounds_ymin_arr,
                    bounds_xmax_arr,
                    bounds_ymax_arr,
                ))
                .map(|opt| {
                    match opt {
                        (
                            Some(ewkb),
                            Some(bounds_xmin),
                            Some(bounds_ymin),
                            Some(bounds_xmax),
                            Some(bounds_ymax),
                        ) => {
                            LWGeom::from_ewkb(ewkb).ok().and_then(|geom| {
                                let gbox = GBox::make_box(
                                    (bounds_xmin, bounds_ymin),
                                    (bounds_xmax, bounds_ymax),
                                );
                                geom.into_mvt_geom(&gbox, extent as u32, buffer as u32, clip_geom)
                                    .and_then(|geom| geom.as_ewkb().ok())
                            })
                        }
                        _ => None,
                    }
                })
                .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            (
                ColumnarValue::Array(geom_arr),
                ColumnarValue::Scalar(ScalarValue::Struct(bounds)),
            ) => {
                let bounds_xmin_arr = bounds.column(0).as_primitive::<Float64Type>();
                let bounds_ymin_arr = bounds.column(1).as_primitive::<Float64Type>();
                let bounds_xmax_arr = bounds.column(2).as_primitive::<Float64Type>();
                let bounds_ymax_arr = bounds.column(3).as_primitive::<Float64Type>();
                if bounds_xmin_arr.is_valid(0)
                    && bounds_ymin_arr.is_valid(0)
                    && bounds_xmax_arr.is_valid(0)
                    && bounds_ymax_arr.is_valid(0)
                {
                    let geom_arr = geom_arr.as_binary_view();
                    let bounds_xmin = bounds_xmin_arr.value(0);
                    let bounds_ymin = bounds_ymin_arr.value(0);
                    let bounds_xmax = bounds_xmax_arr.value(0);
                    let bounds_ymax = bounds_ymax_arr.value(0);
                    let result: BinaryViewArray = geom_arr
                        .iter()
                        .map(|opt| {
                            opt.and_then(|ewkb| {
                                LWGeom::from_ewkb(ewkb).ok().and_then(|geom| {
                                    let gbox = GBox::make_box(
                                        (bounds_xmin, bounds_ymin),
                                        (bounds_xmax, bounds_ymax),
                                    );
                                    geom.into_mvt_geom(
                                        &gbox,
                                        extent as u32,
                                        buffer as u32,
                                        clip_geom,
                                    )
                                    .and_then(|geom| geom.as_ewkb().ok())
                                })
                            })
                        })
                        .collect();
                    Ok(ColumnarValue::Array(Arc::new(result)))
                } else {
                    Ok(ColumnarValue::Array(new_null_array(
                        &DataType::BinaryView,
                        geom_arr.len(),
                    )))
                }
            }
            (
                ColumnarValue::Scalar(ScalarValue::BinaryView(geom_opt)),
                ColumnarValue::Array(bounds_arr),
            ) => {
                match geom_opt {
                    Some(ewkb) => {
                        let bounds_arr = bounds_arr.as_struct();
                        let bounds_xmin_arr = bounds_arr.column(0).as_primitive::<Float64Type>();
                        let bounds_ymin_arr = bounds_arr.column(1).as_primitive::<Float64Type>();
                        let bounds_xmax_arr = bounds_arr.column(2).as_primitive::<Float64Type>();
                        let bounds_ymax_arr = bounds_arr.column(3).as_primitive::<Float64Type>();

                        let result: BinaryViewArray = multizip((
                            bounds_xmin_arr,
                            bounds_ymin_arr,
                            bounds_xmax_arr,
                            bounds_ymax_arr,
                        ))
                        .map(|opt| {
                            match opt {
                                (
                                    Some(bounds_xmin),
                                    Some(bounds_ymin),
                                    Some(bounds_xmax),
                                    Some(bounds_ymax),
                                ) => {
                                    LWGeom::from_ewkb(ewkb).ok().and_then(|geom| {
                                        let gbox = GBox::make_box(
                                            (bounds_xmin, bounds_ymin),
                                            (bounds_xmax, bounds_ymax),
                                        );
                                        geom.into_mvt_geom(
                                            &gbox,
                                            extent as u32,
                                            buffer as u32,
                                            clip_geom,
                                        )
                                        .and_then(|geom| geom.as_ewkb().ok())
                                    })
                                }
                                _ => None,
                            }
                        })
                        .collect();
                        Ok(ColumnarValue::Array(Arc::new(result)))
                    }
                    None => {
                        Ok(ColumnarValue::Array(new_null_array(
                            &DataType::BinaryView,
                            bounds_arr.len(),
                        )))
                    }
                }
            }
            (
                ColumnarValue::Scalar(ScalarValue::BinaryView(geom_opt)),
                ColumnarValue::Scalar(ScalarValue::Struct(bounds)),
            ) => {
                let result = match geom_opt {
                    Some(ewkb) => {
                        let bounds_xmin_arr = bounds.column(0).as_primitive::<Float64Type>();
                        let bounds_ymin_arr = bounds.column(1).as_primitive::<Float64Type>();
                        let bounds_xmax_arr = bounds.column(2).as_primitive::<Float64Type>();
                        let bounds_ymax_arr = bounds.column(3).as_primitive::<Float64Type>();
                        if bounds_xmin_arr.is_valid(0)
                            && bounds_ymin_arr.is_valid(0)
                            && bounds_xmax_arr.is_valid(0)
                            && bounds_ymax_arr.is_valid(0)
                        {
                            let bounds_xmin = bounds_xmin_arr.value(0);
                            let bounds_ymin = bounds_ymin_arr.value(0);
                            let bounds_xmax = bounds_xmax_arr.value(0);
                            let bounds_ymax = bounds_ymax_arr.value(0);
                            LWGeom::from_ewkb(ewkb).ok().and_then(|geom| {
                                let gbox = GBox::make_box(
                                    (bounds_xmin, bounds_ymin),
                                    (bounds_xmax, bounds_ymax),
                                );
                                geom.into_mvt_geom(&gbox, extent as u32, buffer as u32, clip_geom)
                                    .and_then(|geom| geom.as_ewkb().ok())
                            })
                        } else {
                            None
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
        let len = arg_types.len();
        if !(2..=5).contains(&len) {
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
        if arg_types[1] != box2d_type() {
            return exec_err!(
                "unsupported data type '{}' for udf {}",
                arg_types[1],
                self.name()
            );
        }
        if len == 2 {
            Ok(vec![DataType::BinaryView, box2d_type()])
        } else if len == 3 {
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
            Ok(vec![DataType::BinaryView, box2d_type(), DataType::Int64])
        } else if len == 4 {
            if !matches!(
                arg_types[3],
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
                    arg_types[3],
                    self.name()
                );
            }
            Ok(vec![
                DataType::BinaryView,
                box2d_type(),
                DataType::Int64,
                DataType::Int64,
            ])
        } else {
            if !matches!(arg_types[4], DataType::Boolean) {
                return exec_err!(
                    "unsupported data type '{}' for udf {}",
                    arg_types[4],
                    self.name()
                );
            }
            Ok(vec![
                DataType::BinaryView,
                box2d_type(),
                DataType::Int64,
                DataType::Int64,
                DataType::Boolean,
            ])
        }
    }
}
