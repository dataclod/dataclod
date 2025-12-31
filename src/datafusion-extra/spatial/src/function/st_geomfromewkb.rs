use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef};
use datafabric_common_schema::schema_ext::FIELD_TARGET_TYPE;
use datafusion::common::{Result as DFResult, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

pub fn st_geomfromewkb() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(GeomFromWKBUdf {
        signature: Signature::user_defined(Volatility::Immutable),
        aliases: vec!["st_geomfromewkb".to_owned()],
    }))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct GeomFromWKBUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl ScalarUDFImpl for GeomFromWKBUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ST_GeomFromEWKB"
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
            ColumnarValue::Array(arr) => Ok(ColumnarValue::Array(arr.clone())),
            ColumnarValue::Scalar(ScalarValue::BinaryView(opt)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::BinaryView(opt.clone())))
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
