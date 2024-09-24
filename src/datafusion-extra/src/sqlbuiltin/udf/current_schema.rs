use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result as DFResult, ScalarValue, not_impl_err};
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility};

pub fn create_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CurrentSchema {
        signature: Signature::any(0, Volatility::Immutable),
    })
}

#[derive(Debug)]
struct CurrentSchema {
    signature: Signature,
}

impl ScalarUDFImpl for CurrentSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "current_schema"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke(&self, _args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        not_impl_err!("current_schema function does not accept arguments")
    }

    fn invoke_no_args(&self, _number_rows: usize) -> DFResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "public".to_owned(),
        ))))
    }
}
