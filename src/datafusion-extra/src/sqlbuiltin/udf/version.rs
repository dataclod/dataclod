use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{not_impl_err, Result as DFResult, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility};

const DATACLOD_VERSION: &str = "PostgreSQL 14.10 on dataclod";

pub fn create_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(Version {
        signature: Signature::any(0, Volatility::Immutable),
    })
}

#[derive(Debug)]
struct Version {
    signature: Signature,
}

impl ScalarUDFImpl for Version {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "version"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke(&self, _args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        not_impl_err!("version function does not accept arguments")
    }

    fn invoke_no_args(&self, _number_rows: usize) -> DFResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            DATACLOD_VERSION.to_owned(),
        ))))
    }
}
