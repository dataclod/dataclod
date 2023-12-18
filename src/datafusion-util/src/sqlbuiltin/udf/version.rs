use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::logical_expr::{ReturnTypeFunction, ScalarUDF, Signature, Volatility};
use datafusion::physical_plan::functions::make_scalar_function;

const DATACLOD_VERSION: &str = "PostgreSQL 14.10 on dataclod";

pub fn create_udf() -> ScalarUDF {
    let version = make_scalar_function(version);

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));

    ScalarUDF::new(
        "version",
        &Signature::any(0, Volatility::Immutable),
        &return_type,
        &version,
    )
}

fn version(_args: &[ArrayRef]) -> DFResult<ArrayRef> {
    Ok(Arc::new(StringArray::from(vec![DATACLOD_VERSION])))
}
