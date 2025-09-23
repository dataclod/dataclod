use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, BinaryArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result as DFResult, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use geos::Geometry;

use super::geos_ext::GeosExt;

pub fn st_geomfromewkt() -> ScalarUDF {
    ScalarUDF::new_from_impl(GeomFromEWKTUDF {
        signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        aliases: vec!["st_geomfromewkt".to_owned()],
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct GeomFromEWKTUDF {
    signature: Signature,
    aliases: Vec<String>,
}

impl ScalarUDFImpl for GeomFromEWKTUDF {
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
                let ewkt_arr: &StringArray = arr.as_any().downcast_ref().unwrap();
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
}
