use std::sync::Arc;

use datafusion::arrow::array::{as_list_array, ArrayRef};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::cast::as_int64_array;
use datafusion::common::{not_impl_err, Result as DFResult};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ReturnTypeFunction, ScalarUDF, Signature, TypeSignature, Volatility,
};
use datafusion::physical_plan::functions::make_scalar_function;

pub fn create_udf() -> ScalarUDF {
    let fun = make_scalar_function(array_upper);

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int32)));

    ScalarUDF::new(
        "array_upper",
        &Signature::one_of(
            vec![TypeSignature::Any(1), TypeSignature::Any(2)],
            Volatility::Immutable,
        ),
        &return_type,
        &fun,
    )
}

fn array_upper(args: &[ArrayRef]) -> DFResult<ArrayRef> {
    match args[0].data_type() {
        DataType::List(_) => {}
        other => {
            return not_impl_err!(
                "anyarray argument must be a List of numeric values, actual: {}",
                other
            );
        }
    };

    let anyarray = as_list_array(&args[0]);
    let dims = if args.len() == 2 {
        as_int64_array(&args[1]).ok()
    } else {
        None
    };

    todo!()
}
