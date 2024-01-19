use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, ListBuilder, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::cast::as_boolean_array;
use datafusion::common::{plan_datafusion_err, DataFusionError, Result as DFResult};
use datafusion::logical_expr::{ReturnTypeFunction, ScalarUDF, Signature, Volatility};
use datafusion::physical_plan::functions::make_scalar_function;

pub fn create_udf() -> ScalarUDF {
    let current_schemas = make_scalar_function(current_schemas);

    let return_type: ReturnTypeFunction = Arc::new(|_| {
        Ok(Arc::new(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Utf8,
            true,
        )))))
    });

    ScalarUDF::new(
        "current_schemas",
        &Signature::exact(vec![DataType::Boolean], Volatility::Immutable),
        &return_type,
        &current_schemas,
    )
}

fn current_schemas(args: &[ArrayRef]) -> DFResult<ArrayRef> {
    let including_implicit = as_boolean_array(&args[0]).map_err(|_| {
        plan_datafusion_err!(
            "argument of current_schemas must be a boolean array, actual: {}",
            args[0].data_type()
        )
    })?;

    let values_builder = StringBuilder::with_capacity(2, 2);
    let mut builder = ListBuilder::new(values_builder);

    for i in 0..including_implicit.len() {
        if including_implicit.value(i) {
            builder.values().append_value("pg_catalog");
        }
        builder.values().append_value("public");
        builder.append(true);
    }

    Ok(Arc::new(builder.finish()))
}
