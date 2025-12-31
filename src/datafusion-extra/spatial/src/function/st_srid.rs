use std::any::Any;
use std::sync::Arc;

use arrow::array::{AsArray, UInt64Array};
use arrow::datatypes::DataType;
use datafusion::common::{Result as DFResult, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use geos::{Geom, Geometry};

pub fn st_srid() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(SridUdf {
        signature: Signature::user_defined(Volatility::Immutable),
        aliases: vec!["st_srid".to_owned()],
    }))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SridUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl ScalarUDFImpl for SridUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ST_Srid"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::UInt64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = args.args;
        if args.len() != 1 {
            return exec_err!("invalid number of arguments for udf {}", self.name());
        }
        if args[0].data_type() != DataType::BinaryView {
            return exec_err!(
                "unsupported data type '{}' for udf {}",
                args[0].data_type(),
                self.name()
            );
        }

        match &args[0] {
            ColumnarValue::Array(arr) => {
                let wkb_arr = arr.as_binary_view();
                let result: UInt64Array = wkb_arr
                    .iter()
                    .map(|opt| {
                        opt.and_then(|wkb| {
                            Geometry::new_from_wkb(wkb)
                                .and_then(|geom| geom.get_srid())
                                .map(|num| num as u64)
                                .ok()
                        })
                    })
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            ColumnarValue::Scalar(ScalarValue::BinaryView(opt)) => {
                let result = opt.as_ref().and_then(|wkb| {
                    Geometry::new_from_wkb(wkb)
                        .and_then(|geom| geom.get_srid())
                        .map(|num| num as u64)
                        .ok()
                });

                Ok(ColumnarValue::Scalar(ScalarValue::UInt64(result)))
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
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView
        ) {
            return exec_err!(
                "unsupported data type '{}' for udf {}",
                arg_types[0],
                self.name()
            );
        }
        Ok(vec![DataType::BinaryView])
    }
}
