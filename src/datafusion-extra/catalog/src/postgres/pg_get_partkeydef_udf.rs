use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PgGetPartkeydefUDF {
    signature: Signature,
}

impl PgGetPartkeydefUDF {
    pub(crate) fn new() -> PgGetPartkeydefUDF {
        Self {
            signature: Signature::any(1, Volatility::Stable),
        }
    }

    pub fn into_scalar_udf(self) -> ScalarUDF {
        ScalarUDF::new_from_impl(self)
    }
}

impl ScalarUDFImpl for PgGetPartkeydefUDF {
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn name(&self) -> &str {
        "pg_catalog.pg_get_partkeydef"
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(&args.args)?;
        let oid = &args[0];

        let mut builder = StringBuilder::new();
        for _ in 0..oid.len() {
            builder.append_value("");
        }

        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub fn create_pg_get_partkeydef_udf() -> ScalarUDF {
    PgGetPartkeydefUDF::new().into_scalar_udf()
}
