use std::any::Any;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, BinaryViewArray, new_null_array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Int64Type};
use datafusion::common::{Result as DFResult, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use geos::{Geom, Geometry};
use proj4rs::Proj;

use crate::geos_ext::GeosExt;

pub fn st_transform() -> ScalarUDF {
    ScalarUDF::new_from_impl(TransformUdf {
        signature: Signature::user_defined(Volatility::Immutable),
        aliases: vec!["st_transform".to_owned()],
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct TransformUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl ScalarUDFImpl for TransformUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ST_Transform"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::BinaryView)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DFResult<FieldRef> {
        if args
            .arg_fields
            .first()
            .and_then(|field| field.extension_type_name())
            != Some("Geometry")
        {
            return exec_err!(
                "argument 1 to {} must have extension type 'Geometry'",
                self.name()
            );
        }

        let mut field = Field::new(self.name(), DataType::BinaryView, true);
        field.try_with_extension_type(crate::extension::Geometry)?;
        Ok(Arc::new(field))
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

                let mut proj_cache = ProjCache::new();
                let result: BinaryViewArray = wkb_arr
                    .iter()
                    .zip(srid_arr.iter())
                    .map(|opt| {
                        match opt {
                            (Some(wkb), Some(dst_srid)) => {
                                if let Ok(geom) = Geometry::new_from_wkb(wkb) {
                                    if let Ok(src_srid) = geom.get_srid() {
                                        if dst_srid == src_srid as i64 {
                                            Some(wkb.to_vec())
                                        } else {
                                            match (
                                                proj_cache.get(src_srid as i64),
                                                proj_cache.get(dst_srid),
                                            ) {
                                                (Some(src_proj), Some(dst_proj)) => {
                                                    geom.st_transform(&src_proj, &dst_proj)
                                                        .and_then(|mut geom| {
                                                            geom.set_srid(dst_srid as libc::c_int);
                                                            geom.to_ewkb()
                                                        })
                                                        .ok()
                                                }
                                                _ => None,
                                            }
                                        }
                                    } else {
                                        None
                                    }
                                } else {
                                    None
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
                ColumnarValue::Scalar(ScalarValue::Int64(srid_opt)),
            ) => {
                let result = match srid_opt {
                    Some(dst_srid) => {
                        if let Ok(dst_proj) = Proj::from_epsg_code(*dst_srid as u16) {
                            let wkb_arr = wkb_arr.as_binary_view();

                            let mut proj_cache = ProjCache::new();
                            let result: BinaryViewArray = wkb_arr
                                .iter()
                                .map(|opt| {
                                    match opt {
                                        Some(wkb) => {
                                            if let Ok(geom) = Geometry::new_from_wkb(wkb) {
                                                if let Ok(src_srid) = geom.get_srid() {
                                                    if *dst_srid == src_srid as i64 {
                                                        Some(wkb.to_vec())
                                                    } else if let Some(src_proj) =
                                                        proj_cache.get(src_srid as i64)
                                                    {
                                                        geom.st_transform(&src_proj, &dst_proj)
                                                            .and_then(|mut geom| {
                                                                geom.set_srid(
                                                                    *dst_srid as libc::c_int,
                                                                );
                                                                geom.to_ewkb()
                                                            })
                                                            .ok()
                                                    } else {
                                                        None
                                                    }
                                                } else {
                                                    None
                                                }
                                            } else {
                                                None
                                            }
                                        }
                                        _ => None,
                                    }
                                })
                                .collect();
                            Arc::new(result)
                        } else {
                            new_null_array(&DataType::BinaryView, wkb_arr.len())
                        }
                    }
                    None => new_null_array(&DataType::BinaryView, wkb_arr.len()),
                };

                Ok(ColumnarValue::Array(result))
            }
            (
                ColumnarValue::Scalar(ScalarValue::BinaryView(wkb_opt)),
                ColumnarValue::Array(srid_arr),
            ) => {
                let result = match wkb_opt {
                    Some(wkb) => {
                        if let Ok(geom) = Geometry::new_from_wkb(wkb) {
                            if let Ok(src_srid) = geom.get_srid() {
                                if let Ok(src_proj) = Proj::from_epsg_code(src_srid as u16) {
                                    let srid_arr = srid_arr.as_primitive::<Int64Type>();

                                    let result: BinaryViewArray = srid_arr
                                        .iter()
                                        .map(|opt| {
                                            match opt {
                                                Some(dst_srid) => {
                                                    if dst_srid == src_srid as i64 {
                                                        Some(wkb.clone())
                                                    } else if let Ok(dst_proj) =
                                                        Proj::from_epsg_code(dst_srid as u16)
                                                    {
                                                        geom.st_transform(&src_proj, &dst_proj)
                                                            .and_then(|mut geom| {
                                                                geom.set_srid(
                                                                    dst_srid as libc::c_int,
                                                                );
                                                                geom.to_ewkb()
                                                            })
                                                            .ok()
                                                    } else {
                                                        None
                                                    }
                                                }
                                                _ => None,
                                            }
                                        })
                                        .collect();
                                    Arc::new(result)
                                } else {
                                    new_null_array(&DataType::BinaryView, srid_arr.len())
                                }
                            } else {
                                new_null_array(&DataType::BinaryView, srid_arr.len())
                            }
                        } else {
                            new_null_array(&DataType::BinaryView, srid_arr.len())
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
                    (Some(wkb), Some(dst_srid)) => {
                        match (
                            Geometry::new_from_wkb(wkb),
                            Proj::from_epsg_code(*dst_srid as u16),
                        ) {
                            (Ok(geom), Ok(dst_proj)) => {
                                if let Ok(src_srid) = geom.get_srid() {
                                    if *dst_srid == src_srid as i64 {
                                        Some(wkb.clone())
                                    } else if let Ok(src_proj) =
                                        Proj::from_epsg_code(src_srid as u16)
                                    {
                                        geom.st_transform(&src_proj, &dst_proj)
                                            .and_then(|mut geom| {
                                                geom.set_srid(*dst_srid as libc::c_int);
                                                geom.to_ewkb()
                                            })
                                            .ok()
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
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
        Ok(vec![DataType::BinaryView, DataType::Int64])
    }
}

struct ProjCache(HashMap<i64, Proj>);

impl ProjCache {
    fn new() -> Self {
        Self(HashMap::new())
    }
}

impl ProjCache {
    fn get(&mut self, srid: i64) -> Option<Proj> {
        match self.0.entry(srid) {
            Entry::Occupied(entry) => Some(entry.get().clone()),
            Entry::Vacant(entry) => {
                if let Ok(proj) = Proj::from_epsg_code(srid as u16) {
                    Some(entry.insert(proj).clone())
                } else {
                    None
                }
            }
        }
    }
}
