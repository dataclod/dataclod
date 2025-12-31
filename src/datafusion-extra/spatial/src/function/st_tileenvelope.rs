use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef};
use datafabric_common_schema::schema_ext::FIELD_TARGET_TYPE;
use datafusion::common::{Result as DFResult, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::scalar::ScalarValue;
use lwgeom::LWGeom;

pub fn st_tileenvelope() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(TileEnvelopeUdf {
        signature: Signature::user_defined(Volatility::Immutable),
        aliases: vec!["st_tileenvelope".to_owned()],
    }))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct TileEnvelopeUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl ScalarUDFImpl for TileEnvelopeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ST_TileEnvelope"
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
        if args.len() < 3 || args.len() > 5 {
            return exec_err!("invalid number of arguments for udf {}", self.name());
        }
        if args[0].data_type() != DataType::Int64 {
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
        if args[2].data_type() != DataType::Int64 {
            return exec_err!(
                "unsupported data type '{}' for udf {}",
                args[2].data_type(),
                self.name()
            );
        }
        if args.len() > 3 && args[3].data_type() != DataType::BinaryView {
            return exec_err!(
                "unsupported data type '{}' for udf {}",
                args[3].data_type(),
                self.name()
            );
        }
        if args.len() > 4 && args[4].data_type() != DataType::Float64 {
            return exec_err!(
                "unsupported data type '{}' for udf {}",
                args[4].data_type(),
                self.name()
            );
        }

        let bounds =
            if let Some(ColumnarValue::Scalar(ScalarValue::BinaryView(bounds))) = args.get(3) {
                bounds.as_ref().and_then(|wkb| LWGeom::from_ewkb(wkb).ok())
            } else {
                None
            };
        let margin = if let Some(ColumnarValue::Scalar(ScalarValue::Float64(margin))) = args.get(4)
        {
            *margin
        } else {
            None
        };

        match (&args[0], &args[1], &args[2]) {
            (
                ColumnarValue::Scalar(ScalarValue::Int64(zoom_opt)),
                ColumnarValue::Scalar(ScalarValue::Int64(x_opt)),
                ColumnarValue::Scalar(ScalarValue::Int64(y_opt)),
            ) => {
                let result = match (zoom_opt, x_opt, y_opt) {
                    (Some(zoom), Some(x), Some(y)) => {
                        LWGeom::tile_envelope(
                            *zoom as i32,
                            *x as i32,
                            *y as i32,
                            bounds.as_ref(),
                            margin,
                        )
                        .and_then(|geom| geom.as_ewkb())
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
        if !(3..=5).contains(&len) {
            return exec_err!("invalid number of arguments for udf {}", self.name());
        }
        for dt in arg_types.iter().take(3) {
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
        if len == 3 {
            Ok(vec![DataType::Int64, DataType::Int64, DataType::Int64])
        } else if len == 4 {
            if !matches!(
                arg_types[3],
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView
            ) {
                return exec_err!(
                    "unsupported data type '{}' for udf {}",
                    arg_types[3],
                    self.name()
                );
            }
            Ok(vec![
                DataType::Int64,
                DataType::Int64,
                DataType::Int64,
                DataType::BinaryView,
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
                    | DataType::Float16
                    | DataType::Float32
                    | DataType::Float64
            ) {
                return exec_err!(
                    "unsupported data type '{}' for udf {}",
                    arg_types[4],
                    self.name()
                );
            }
            Ok(vec![
                DataType::Int64,
                DataType::Int64,
                DataType::Int64,
                DataType::BinaryView,
                DataType::Float64,
            ])
        }
    }
}
