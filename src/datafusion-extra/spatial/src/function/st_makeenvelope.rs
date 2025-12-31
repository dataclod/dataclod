use std::any::Any;
use std::sync::Arc;

use arrow::array::{AsArray, BinaryViewArray};
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafabric_common_schema::schema_ext::FIELD_TARGET_TYPE;
use datafusion::common::{Result as DFResult, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use geos::Geometry;
use itertools::multizip;

use crate::utils::GeosExt;

pub fn st_makeenvelope() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(MakeEnvelopeUdf {
        signature: Signature::user_defined(Volatility::Immutable),
        aliases: vec!["st_makeenvelope".to_string()],
    }))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct MakeEnvelopeUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl ScalarUDFImpl for MakeEnvelopeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ST_MakeEnvelope"
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
        if args.len() != 4 && args.len() != 5 {
            return exec_err!("invalid number of arguments for udf {}", self.name());
        }
        if args[0].data_type() != DataType::Float64 {
            return exec_err!(
                "unsupported data type '{}' for udf {}",
                args[0].data_type(),
                self.name()
            );
        }
        if args[1].data_type() != DataType::Float64 {
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
        if args[3].data_type() != DataType::Float64 {
            return exec_err!(
                "unsupported data type '{}' for udf {}",
                args[3].data_type(),
                self.name()
            );
        }
        if args.len() == 5 && args[4].data_type() != DataType::Int64 {
            return exec_err!(
                "unsupported data type '{}' for udf {}",
                args[4].data_type(),
                self.name()
            );
        }

        let srid = if let Some(ColumnarValue::Scalar(ScalarValue::Int64(Some(srid)))) = args.get(4)
        {
            *srid
        } else {
            0
        };

        match (&args[0], &args[1], &args[2], &args[3]) {
            (
                ColumnarValue::Array(xmin_arr),
                ColumnarValue::Array(ymin_arr),
                ColumnarValue::Array(xmax_arr),
                ColumnarValue::Array(ymax_arr),
            ) => {
                let xmin_arr = xmin_arr.as_primitive::<Float64Type>();
                let ymin_arr = ymin_arr.as_primitive::<Float64Type>();
                let xmax_arr = xmax_arr.as_primitive::<Float64Type>();
                let ymax_arr = ymax_arr.as_primitive::<Float64Type>();

                let result: BinaryViewArray = multizip((xmin_arr, ymin_arr, xmax_arr, ymax_arr))
                    .map(|opt| {
                        match opt {
                            (Some(xmin), Some(ymin), Some(xmax), Some(ymax)) => {
                                Geometry::make_envelope(xmin, ymin, xmax, ymax)
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
                ColumnarValue::Scalar(ScalarValue::Float64(xmin)),
                ColumnarValue::Scalar(ScalarValue::Float64(ymin)),
                ColumnarValue::Scalar(ScalarValue::Float64(xmax)),
                ColumnarValue::Scalar(ScalarValue::Float64(ymax)),
            ) => {
                let result = match (xmin, ymin, xmax, ymax) {
                    (Some(xmin), Some(ymin), Some(xmax), Some(ymax)) => {
                        Geometry::make_envelope(*xmin, *ymin, *xmax, *ymax)
                            .and_then(|mut geom| {
                                geom.set_srid(srid as libc::c_int);
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
        let len = arg_types.len();
        if !(4..=5).contains(&len) {
            return exec_err!("invalid number of arguments for udf {}", self.name());
        }
        for dt in arg_types.iter().take(4) {
            if !matches!(
                dt,
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
                return exec_err!("unsupported data type '{}' for udf {}", dt, self.name());
            }
        }
        if len == 4 {
            Ok(vec![
                DataType::Float64,
                DataType::Float64,
                DataType::Float64,
                DataType::Float64,
            ])
        } else {
            if !matches!(
                arg_types[4],
                DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
            ) {
                return exec_err!(
                    "unsupported data type '{}' for udf {}",
                    arg_types[4],
                    self.name()
                );
            }
            Ok(vec![
                DataType::Float64,
                DataType::Float64,
                DataType::Float64,
                DataType::Float64,
                DataType::Int64,
            ])
        }
    }
}
