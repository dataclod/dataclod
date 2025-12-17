use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{AsArray, BinaryArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result as DFResult, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use geos::Geometry;

use crate::geos_ext::GeomExt;

pub fn st_geomfromewkt() -> ScalarUDF {
    ScalarUDF::new_from_impl(GeomFromEWKTUdf {
        signature: Signature::user_defined(Volatility::Immutable),
        aliases: vec!["st_geomfromewkt".to_owned()],
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct GeomFromEWKTUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl ScalarUDFImpl for GeomFromEWKTUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ST_GeomFromEWKT"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = args.args;
        if args.len() != 1 {
            return exec_err!("invalid number of arguments for udf {}", self.name());
        }
        if args[0].data_type() != DataType::Utf8 {
            return exec_err!(
                "unsupported data type '{}' for udf {}",
                args[0].data_type(),
                self.name()
            );
        }

        match &args[0] {
            ColumnarValue::Array(arr) => {
                let ewkt_arr = arr.as_string::<i32>();
                let result: BinaryArray = ewkt_arr
                    .iter()
                    .map(|opt| {
                        opt.and_then(|ewkt| {
                            Geometry::new_from_ewkt(ewkt)
                                .and_then(|geom| geom.to_ewkb())
                                .ok()
                        })
                    })
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(opt)) => {
                let result = opt.as_ref().and_then(|ewkt| {
                    Geometry::new_from_ewkt(ewkt)
                        .and_then(|geom| geom.to_ewkb())
                        .ok()
                });

                Ok(ColumnarValue::Scalar(ScalarValue::Binary(result)))
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
        if arg_types.len() != 1 {
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
        Ok(vec![DataType::Utf8])
    }
}
