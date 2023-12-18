use std::cmp::Ordering;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int64Builder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::cast::{as_int64_array, as_list_array};
use datafusion::common::{exec_err, not_impl_err, Result as DFResult};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ReturnTypeFunction, ScalarUDF, Signature, TypeSignature, Volatility,
};
use datafusion::physical_plan::functions::make_scalar_function;

pub fn create_udf() -> ScalarUDF {
    let array_upper = make_scalar_function(array_upper);

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int64)));

    ScalarUDF::new(
        "array_upper",
        &Signature::one_of(
            vec![TypeSignature::Any(1), TypeSignature::Any(2)],
            Volatility::Immutable,
        ),
        &return_type,
        &array_upper,
    )
}

fn array_upper(args: &[ArrayRef]) -> DFResult<ArrayRef> {
    match args[0].data_type() {
        DataType::List(_) => {}
        other => {
            return exec_err!(
                "anyarray argument must be a List of numeric values, actual: {}",
                other
            );
        }
    };

    let anyarray = as_list_array(&args[0]).unwrap();
    let dims = if args.len() == 2 {
        as_int64_array(&args[1]).ok()
    } else {
        None
    };

    let mut builder = Int64Builder::with_capacity(anyarray.len());

    for (idx, element) in anyarray.iter().enumerate() {
        let dim = match dims {
            Some(dims) => {
                if dims.is_null(idx) {
                    -1
                } else {
                    dims.value(idx)
                }
            }
            None => 1,
        };

        match dim.cmp(&1) {
            Ordering::Less => builder.append_null(),
            Ordering::Equal => {
                match element {
                    None => builder.append_null(),
                    Some(arr) => {
                        if arr.len() == 0 {
                            builder.append_null()
                        } else {
                            builder.append_value(arr.len() as i64)
                        }
                    }
                }
            }
            Ordering::Greater => {
                return not_impl_err!(
                    "argument dim > 1 is not supported right now, actual: {}",
                    dim
                );
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}
