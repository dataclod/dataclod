use std::cmp::Ordering;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int64Builder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::cast::{as_int64_array, as_list_array};
use datafusion::common::{not_impl_err, plan_datafusion_err, Result as DFResult};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion::physical_plan::functions::make_scalar_function;

pub fn create_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(ArrayUpper {
        signature: Signature::one_of(
            vec![TypeSignature::Any(1), TypeSignature::Any(2)],
            Volatility::Immutable,
        ),
    })
}

#[derive(Debug)]
struct ArrayUpper {
    signature: Signature,
}

impl ScalarUDFImpl for ArrayUpper {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "array_upper"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        make_scalar_function(array_upper)(args)
    }
}

fn array_upper(args: &[ArrayRef]) -> DFResult<ArrayRef> {
    let anyarray = as_list_array(&args[0]).map_err(|_| {
        plan_datafusion_err!(
            "argument of array_upper must be an array, actual: {}",
            args[0].data_type()
        )
    })?;
    let dims = if args.len() == 2 {
        as_int64_array(&args[1]).ok()
    } else {
        None
    };

    let mut builder = Int64Builder::with_capacity(anyarray.len());
    for (idx, element) in anyarray.iter().enumerate() {
        let dim = dims.map_or(1, |dims| {
            if dims.is_null(idx) {
                -1
            } else {
                dims.value(idx)
            }
        });

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
