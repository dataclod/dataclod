//! Code generation for UDF invoke implementations.
//!
//! This module contains all the functions that generate the `invoke_with_args`
//! implementations for different function arities and patterns.

use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::Ident;

use crate::analyze::{ReturnTypeKind, UserFunctionAttr};
use crate::types::{ArgType, ReturnTypeInfo};

/// Generate the invoke implementation for a UDF.
pub fn generate_invoke_impl(
    user_fn: &UserFunctionAttr, sig_args: &[ArgType], return_info: &ReturnTypeInfo,
) -> TokenStream2 {
    let arg_count = user_fn.args.len();
    let fn_name = &user_fn.name;

    let arg_validation = quote! {
        if args.len() != #arg_count {
            return exec_err!(
                "invalid number of arguments for udf {}: expected {}, got {}",
                self.name(),
                #arg_count,
                args.len()
            );
        }
    };

    let core_logic = match arg_count {
        0 => generate_nullary_invoke(fn_name, &user_fn.return_type_kind, return_info),
        1 => generate_unary_invoke(user_fn, &sig_args[0], return_info),
        2 => generate_binary_invoke(user_fn, sig_args, return_info),
        _ => generate_nary_invoke(user_fn, sig_args, return_info),
    };

    quote! {
        #arg_validation
        #core_logic
    }
}

fn generate_nullary_invoke(
    fn_name: &Ident, return_kind: &ReturnTypeKind, return_info: &ReturnTypeInfo,
) -> TokenStream2 {
    let to_scalar = return_info.to_scalar_tokens(quote!(result));
    let call = quote! { #fn_name() };
    let wrapped_call = return_kind.wrap_call(call);

    quote! {
        let result = #wrapped_call;
        Ok(ColumnarValue::Scalar(#to_scalar))
    }
}

fn generate_unary_invoke(
    user_fn: &UserFunctionAttr, arg_type: &ArgType, return_info: &ReturnTypeInfo,
) -> TokenStream2 {
    let fn_name = &user_fn.name;
    let arg_name = &user_fn.args[0].0;
    let arg_is_option = user_fn.args_option[0];

    if return_info.is_struct() {
        return generate_unary_struct_invoke(user_fn, arg_type, return_info);
    }

    if return_info.is_list() {
        return generate_unary_list_invoke(user_fn, arg_type, return_info);
    }

    if let ArgType::Struct(_) = arg_type {
        return generate_unary_struct_input_invoke(user_fn, arg_type, return_info);
    }

    if arg_type.is_list() {
        return generate_unary_list_input_invoke(user_fn, arg_type, return_info);
    }

    let can_use_simd = arg_type.is_primitive()
        && return_info.is_primitive()
        && !arg_is_option
        && user_fn.return_type_kind == ReturnTypeKind::T;

    if can_use_simd {
        return generate_unary_simd_invoke(user_fn, arg_type, return_info);
    }

    let array_downcast = arg_type.array_type_tokens_named("arr");
    let scalar_pattern = arg_type.scalar_pattern_tokens();
    let result_array_type = return_info.array_type_tokens();
    let to_scalar = return_info.to_scalar_tokens(quote!(result));
    let convert_expr = arg_type.scalar_convert_expr("v");
    let scalar_opt = quote! { let opt = #convert_expr; };

    let process_value =
        generate_process_value(fn_name, arg_name, arg_is_option, &user_fn.return_type_kind);

    quote! {
        match &args[0] {
            ColumnarValue::Array(arr) => {
                let arr = #array_downcast;
                let result: #result_array_type = arr
                    .iter()
                    .map(|opt| {
                        #process_value
                    })
                    .collect();
                Ok(ColumnarValue::Array(std::sync::Arc::new(result)))
            }
            ColumnarValue::Scalar(#scalar_pattern) => {
                let result = {
                    #scalar_opt
                    #process_value
                };
                Ok(ColumnarValue::Scalar(#to_scalar))
            }
            other => {
                exec_err!("unsupported argument type '{other:?}' for udf {}", self.name())
            }
        }
    }
}

fn generate_unary_struct_input_invoke(
    user_fn: &UserFunctionAttr, arg_type: &ArgType, return_info: &ReturnTypeInfo,
) -> TokenStream2 {
    let fn_name = &user_fn.name;
    let arg_name = &user_fn.args[0].0;
    let arg_is_option = user_fn.args_option[0];

    let array_downcast = arg_type.array_downcast_method();
    let scalar_pattern = arg_type.scalar_pattern_tokens();
    let result_array_type = return_info.array_type_tokens();
    let to_scalar = return_info.to_scalar_tokens(quote!(result));
    let convert_expr = arg_type.scalar_convert_expr("v");
    let value_at_index = arg_type.value_at_index_tokens("arr", "i");

    let process_value =
        generate_process_value(fn_name, arg_name, arg_is_option, &user_fn.return_type_kind);

    quote! {
        match &args[0] {
            ColumnarValue::Array(a) => {
                let arr = a.#array_downcast;
                let len = arr.len();
                let result: #result_array_type = (0..len)
                    .map(|i| {
                        let opt = #value_at_index;
                        #process_value
                    })
                    .collect();
                Ok(ColumnarValue::Array(std::sync::Arc::new(result)))
            }
            ColumnarValue::Scalar(#scalar_pattern) => {
                let opt = #convert_expr;
                let result = #process_value;
                Ok(ColumnarValue::Scalar(#to_scalar))
            }
            other => {
                exec_err!("unsupported argument type '{other:?}' for udf {}", self.name())
            }
        }
    }
}

fn generate_unary_struct_invoke(
    user_fn: &UserFunctionAttr, arg_type: &ArgType, return_info: &ReturnTypeInfo,
) -> TokenStream2 {
    let fn_name = &user_fn.name;
    let arg_name = &user_fn.args[0].0;
    let arg_is_option = user_fn.args_option[0];

    let array_downcast = arg_type.array_type_tokens_named("arr");
    let scalar_pattern = arg_type.scalar_pattern_tokens();
    let convert_expr = arg_type.scalar_convert_expr("v");

    let builder_ident = return_info.struct_builder_ident().unwrap();

    let process_value =
        generate_process_value(fn_name, arg_name, arg_is_option, &user_fn.return_type_kind);

    quote! {
        match &args[0] {
            ColumnarValue::Array(arr) => {
                let arr = #array_downcast;
                let len = arr.len();
                let mut builder = #builder_ident::with_capacity(len);
                for opt in arr.iter() {
                    let result = #process_value;
                    builder.append_option(result);
                }
                Ok(ColumnarValue::Array(std::sync::Arc::new(builder.finish())))
            }
            ColumnarValue::Scalar(#scalar_pattern) => {
                let opt = #convert_expr;
                let result = #process_value;
                let mut builder = #builder_ident::with_capacity(1);
                builder.append_option(result);
                let arr = builder.finish();
                Ok(ColumnarValue::Scalar(ScalarValue::Struct(std::sync::Arc::new(arr))))
            }
            other => {
                exec_err!("unsupported argument type '{other:?}' for udf {}", self.name())
            }
        }
    }
}

fn generate_unary_list_invoke(
    user_fn: &UserFunctionAttr, arg_type: &ArgType, return_info: &ReturnTypeInfo,
) -> TokenStream2 {
    let fn_name = &user_fn.name;
    let arg_name = &user_fn.args[0].0;
    let arg_is_option = user_fn.args_option[0];

    let array_downcast = arg_type.array_type_tokens_named("arr");
    let scalar_pattern = arg_type.scalar_pattern_tokens();
    let convert_expr = arg_type.scalar_convert_expr("v");

    let inner_type = return_info.inner_type().unwrap();
    let inner_data_type = inner_type.data_type_tokens();

    let process_value =
        generate_process_value(fn_name, arg_name, arg_is_option, &user_fn.return_type_kind);

    quote! {
        match &args[0] {
            ColumnarValue::Array(arr) => {
                use datafusion::arrow::array::Array;
                let arr = #array_downcast;
                let len = arr.len();
                let field = std::sync::Arc::new(
                    datafusion::arrow::datatypes::Field::new("item", #inner_data_type, true)
                );

                let mut offsets = vec![0i32];
                let mut values: Vec<datafusion::arrow::array::ArrayRef> = Vec::with_capacity(len);
                let mut null_builder = datafusion::arrow::array::NullBufferBuilder::new(len);

                for opt in arr.iter() {
                    let result: Option<datafusion::arrow::array::ArrayRef> = #process_value;
                    match result {
                        Some(arr_ref) => {
                            offsets.push(offsets.last().unwrap() + arr_ref.len() as i32);
                            values.push(arr_ref);
                            null_builder.append_non_null();
                        }
                        None => {
                            offsets.push(*offsets.last().unwrap());
                            null_builder.append_null();
                        }
                    }
                }

                let values_array: datafusion::arrow::array::ArrayRef = if values.is_empty() {
                    datafusion::arrow::array::new_empty_array(&#inner_data_type)
                } else {
                    datafusion::arrow::compute::concat(&values.iter().map(|a| a.as_ref()).collect::<Vec<_>>())?
                };

                let list_array = datafusion::arrow::array::ListArray::new(
                    field,
                    datafusion::arrow::buffer::OffsetBuffer::new(offsets.into()),
                    values_array,
                    null_builder.finish(),
                );
                Ok(ColumnarValue::Array(std::sync::Arc::new(list_array)))
            }
            ColumnarValue::Scalar(#scalar_pattern) => {
                let opt = #convert_expr;
                let result: Option<datafusion::arrow::array::ArrayRef> = #process_value;
                let field = std::sync::Arc::new(
                    datafusion::arrow::datatypes::Field::new("item", #inner_data_type, true)
                );
                let scalar = match result {
                    Some(arr) => ScalarValue::List(std::sync::Arc::new(
                        datafusion::arrow::array::ListArray::new(
                            field,
                            datafusion::arrow::buffer::OffsetBuffer::from_lengths([arr.len()]),
                            arr,
                            None,
                        )
                    )),
                    None => ScalarValue::List(std::sync::Arc::new(
                        datafusion::arrow::array::ListArray::new_null(field, 1)
                    )),
                };
                Ok(ColumnarValue::Scalar(scalar))
            }
            other => {
                exec_err!("unsupported argument type '{other:?}' for udf {}", self.name())
            }
        }
    }
}

fn generate_unary_list_input_invoke(
    user_fn: &UserFunctionAttr, arg_type: &ArgType, return_info: &ReturnTypeInfo,
) -> TokenStream2 {
    let fn_name = &user_fn.name;
    let arg_name = &user_fn.args[0].0;
    let arg_is_option = user_fn.args_option[0];

    let array_downcast = arg_type.array_downcast_method();
    let scalar_pattern = arg_type.scalar_pattern_tokens();
    let result_array_type = return_info.array_type_tokens();
    let to_scalar = return_info.to_scalar_tokens(quote!(result));
    let convert_expr = arg_type.scalar_convert_expr("v");
    let value_at_index = arg_type.value_at_index_tokens("arr", "i");

    let process_value =
        generate_process_value(fn_name, arg_name, arg_is_option, &user_fn.return_type_kind);

    quote! {
        match &args[0] {
            ColumnarValue::Array(a) => {
                use datafusion::arrow::array::Array;
                let arr = a.#array_downcast;
                let len = arr.len();
                let result: #result_array_type = (0..len)
                    .map(|i| {
                        let opt = #value_at_index;
                        #process_value
                    })
                    .collect();
                Ok(ColumnarValue::Array(std::sync::Arc::new(result)))
            }
            ColumnarValue::Scalar(#scalar_pattern) => {
                let opt = #convert_expr;
                let result = #process_value;
                Ok(ColumnarValue::Scalar(#to_scalar))
            }
            other => {
                exec_err!("unsupported argument type '{other:?}' for udf {}", self.name())
            }
        }
    }
}

fn generate_unary_simd_invoke(
    user_fn: &UserFunctionAttr, arg_type: &ArgType, return_info: &ReturnTypeInfo,
) -> TokenStream2 {
    let fn_name = &user_fn.name;
    let array_downcast = arg_type.array_type_tokens_named("arr");
    let result_array_type = return_info.array_type_tokens();
    let scalar_pattern = arg_type.scalar_pattern_tokens();
    let to_scalar = return_info.to_scalar_tokens(quote!(result));
    let convert_expr = arg_type.scalar_convert_expr("v");

    quote! {
        match &args[0] {
            ColumnarValue::Array(arr) => {
                let arr = #array_downcast;
                let result: #result_array_type =
                    datafusion::arrow::compute::kernels::arity::unary(arr, #fn_name);
                Ok(ColumnarValue::Array(std::sync::Arc::new(result)))
            }
            ColumnarValue::Scalar(#scalar_pattern) => {
                let opt = #convert_expr;
                let result = opt.map(#fn_name);
                Ok(ColumnarValue::Scalar(#to_scalar))
            }
            other => {
                exec_err!("unsupported argument type '{other:?}' for udf {}", self.name())
            }
        }
    }
}

fn generate_process_value(
    fn_name: &Ident, arg_name: &Ident, arg_is_option: bool, return_kind: &ReturnTypeKind,
) -> TokenStream2 {
    let call = if arg_is_option {
        quote! { #fn_name(opt) }
    } else {
        quote! {
            match opt {
                Some(#arg_name) => #fn_name(#arg_name),
                None => return None,
            }
        }
    };

    let wrapped_call = return_kind.wrap_call(call);

    if arg_is_option {
        wrapped_call
    } else {
        quote! { (|| { #wrapped_call })() }
    }
}

fn generate_binary_invoke(
    user_fn: &UserFunctionAttr, sig_args: &[ArgType], return_info: &ReturnTypeInfo,
) -> TokenStream2 {
    let fn_name = &user_fn.name;
    let (arg1_name, _) = &user_fn.args[0];
    let (arg2_name, _) = &user_fn.args[1];
    let arg1_is_option = user_fn.args_option[0];
    let arg2_is_option = user_fn.args_option[1];

    if return_info.is_struct() {
        return generate_binary_struct_invoke(user_fn, sig_args, return_info);
    }

    if return_info.is_list() {
        return generate_binary_list_invoke(user_fn, sig_args, return_info);
    }

    let has_complex_input = sig_args[0].is_struct()
        || sig_args[0].is_list()
        || sig_args[1].is_struct()
        || sig_args[1].is_list();
    if has_complex_input {
        return generate_binary_complex_input_invoke(user_fn, sig_args, return_info);
    }

    let can_use_simd = sig_args[0].is_primitive()
        && sig_args[1].is_primitive()
        && return_info.is_primitive()
        && !arg1_is_option
        && !arg2_is_option
        && user_fn.return_type_kind == ReturnTypeKind::T;

    if can_use_simd {
        return generate_binary_simd_invoke(user_fn, sig_args, return_info);
    }

    let array_type1 = sig_args[0].array_type_tokens_named("a1");
    let array_type2 = sig_args[1].array_type_tokens_named("a2");
    let scalar_pattern1 = sig_args[0].scalar_pattern_tokens_named("opt1");
    let scalar_pattern2 = sig_args[1].scalar_pattern_tokens_named("opt2");
    let result_array_type = return_info.array_type_tokens();
    let return_data_type = return_info.data_type_tokens();
    let to_scalar = return_info.to_scalar_tokens(quote!(result));

    let convert1 = sig_args[0].scalar_convert_expr("opt1");
    let convert2 = sig_args[1].scalar_convert_expr("opt2");
    let scalar1_deref = quote! { let opt1 = #convert1; };
    let scalar2_deref = quote! { let opt2 = #convert2; };

    let call_fn = generate_binary_call(
        fn_name,
        arg1_name,
        arg2_name,
        arg1_is_option,
        arg2_is_option,
        &user_fn.return_type_kind,
    );

    let array_scalar_case = if !arg2_is_option {
        quote! {
            (ColumnarValue::Array(a1), ColumnarValue::Scalar(#scalar_pattern2)) => {
                let arr1 = #array_type1;
                #scalar2_deref
                if opt2.is_none() {
                    return Ok(ColumnarValue::Array(
                        datafusion::arrow::array::new_null_array(&#return_data_type, arr1.len())
                    ));
                }
                let result: #result_array_type = arr1
                    .iter()
                    .map(|opt1| { #call_fn })
                    .collect();
                Ok(ColumnarValue::Array(std::sync::Arc::new(result)))
            }
        }
    } else {
        quote! {
            (ColumnarValue::Array(a1), ColumnarValue::Scalar(#scalar_pattern2)) => {
                let arr1 = #array_type1;
                #scalar2_deref
                let result: #result_array_type = arr1
                    .iter()
                    .map(|opt1| { #call_fn })
                    .collect();
                Ok(ColumnarValue::Array(std::sync::Arc::new(result)))
            }
        }
    };

    let scalar_array_case = if !arg1_is_option {
        quote! {
            (ColumnarValue::Scalar(#scalar_pattern1), ColumnarValue::Array(a2)) => {
                let arr2 = #array_type2;
                #scalar1_deref
                if opt1.is_none() {
                    return Ok(ColumnarValue::Array(
                        datafusion::arrow::array::new_null_array(&#return_data_type, arr2.len())
                    ));
                }
                let result: #result_array_type = arr2
                    .iter()
                    .map(|opt2| { #call_fn })
                    .collect();
                Ok(ColumnarValue::Array(std::sync::Arc::new(result)))
            }
        }
    } else {
        quote! {
            (ColumnarValue::Scalar(#scalar_pattern1), ColumnarValue::Array(a2)) => {
                let arr2 = #array_type2;
                #scalar1_deref
                let result: #result_array_type = arr2
                    .iter()
                    .map(|opt2| { #call_fn })
                    .collect();
                Ok(ColumnarValue::Array(std::sync::Arc::new(result)))
            }
        }
    };

    quote! {
        match (&args[0], &args[1]) {
            (ColumnarValue::Array(a1), ColumnarValue::Array(a2)) => {
                let arr1 = #array_type1;
                let arr2 = #array_type2;
                if arr1.len() != arr2.len() {
                    return exec_err!("array length mismatch for udf {}", self.name());
                }
                let result: #result_array_type = arr1
                    .iter()
                    .zip(arr2.iter())
                    .map(|(opt1, opt2)| { #call_fn })
                    .collect();
                Ok(ColumnarValue::Array(std::sync::Arc::new(result)))
            }
            #array_scalar_case
            #scalar_array_case
            (ColumnarValue::Scalar(#scalar_pattern1), ColumnarValue::Scalar(#scalar_pattern2)) => {
                #scalar1_deref
                #scalar2_deref
                let result = { #call_fn };
                Ok(ColumnarValue::Scalar(#to_scalar))
            }
            _ => exec_err!("unsupported argument types for udf {}", self.name()),
        }
    }
}

fn generate_binary_struct_invoke(
    user_fn: &UserFunctionAttr, sig_args: &[ArgType], return_info: &ReturnTypeInfo,
) -> TokenStream2 {
    let fn_name = &user_fn.name;
    let (arg1_name, _) = &user_fn.args[0];
    let (arg2_name, _) = &user_fn.args[1];
    let arg1_is_option = user_fn.args_option[0];
    let arg2_is_option = user_fn.args_option[1];

    let array_type1 = sig_args[0].array_type_tokens_named("a1");
    let array_type2 = sig_args[1].array_type_tokens_named("a2");
    let scalar_pattern1 = sig_args[0].scalar_pattern_tokens_named("opt1");
    let scalar_pattern2 = sig_args[1].scalar_pattern_tokens_named("opt2");
    let convert1 = sig_args[0].scalar_convert_expr("opt1");
    let convert2 = sig_args[1].scalar_convert_expr("opt2");

    let builder_ident = return_info.struct_builder_ident().unwrap();

    let call_fn = generate_binary_call(
        fn_name,
        arg1_name,
        arg2_name,
        arg1_is_option,
        arg2_is_option,
        &user_fn.return_type_kind,
    );

    quote! {
        match (&args[0], &args[1]) {
            (ColumnarValue::Array(arr1), ColumnarValue::Array(arr2)) => {
                let a1 = #array_type1;
                let a2 = #array_type2;
                let len = a1.len();
                if len != a2.len() {
                    return exec_err!("array length mismatch for udf {}", self.name());
                }
                let mut builder = #builder_ident::with_capacity(len);
                for (opt1, opt2) in a1.iter().zip(a2.iter()) {
                    let result = #call_fn;
                    builder.append_option(result);
                }
                Ok(ColumnarValue::Array(std::sync::Arc::new(builder.finish())))
            }
            (ColumnarValue::Array(arr1), ColumnarValue::Scalar(#scalar_pattern2)) => {
                let a1 = #array_type1;
                let opt2 = #convert2;
                let len = a1.len();
                let mut builder = #builder_ident::with_capacity(len);
                for opt1 in a1.iter() {
                    let result = #call_fn;
                    builder.append_option(result);
                }
                Ok(ColumnarValue::Array(std::sync::Arc::new(builder.finish())))
            }
            (ColumnarValue::Scalar(#scalar_pattern1), ColumnarValue::Array(arr2)) => {
                let opt1 = #convert1;
                let a2 = #array_type2;
                let len = a2.len();
                let mut builder = #builder_ident::with_capacity(len);
                for opt2 in a2.iter() {
                    let result = #call_fn;
                    builder.append_option(result);
                }
                Ok(ColumnarValue::Array(std::sync::Arc::new(builder.finish())))
            }
            (ColumnarValue::Scalar(#scalar_pattern1), ColumnarValue::Scalar(#scalar_pattern2)) => {
                let opt1 = #convert1;
                let opt2 = #convert2;
                let result = #call_fn;
                let mut builder = #builder_ident::with_capacity(1);
                builder.append_option(result);
                let arr = builder.finish();
                Ok(ColumnarValue::Scalar(ScalarValue::Struct(std::sync::Arc::new(arr))))
            }
            _ => exec_err!("unsupported argument types for udf {}", self.name()),
        }
    }
}

fn generate_binary_simd_invoke(
    user_fn: &UserFunctionAttr, sig_args: &[ArgType], return_info: &ReturnTypeInfo,
) -> TokenStream2 {
    let fn_name = &user_fn.name;
    let array_type1 = sig_args[0].array_type_tokens_named("a1");
    let array_type2 = sig_args[1].array_type_tokens_named("a2");
    let result_array_type = return_info.array_type_tokens();
    let scalar_pattern1 = sig_args[0].scalar_pattern_tokens_named("opt1");
    let scalar_pattern2 = sig_args[1].scalar_pattern_tokens_named("opt2");
    let to_scalar = return_info.to_scalar_tokens(quote!(result));
    let convert1 = sig_args[0].scalar_convert_expr("opt1");
    let convert2 = sig_args[1].scalar_convert_expr("opt2");

    quote! {
        match (&args[0], &args[1]) {
            (ColumnarValue::Array(a1), ColumnarValue::Array(a2)) => {
                let arr1 = #array_type1;
                let arr2 = #array_type2;
                let result: #result_array_type =
                    datafusion::arrow::compute::kernels::arity::binary(arr1, arr2, #fn_name)?;
                Ok(ColumnarValue::Array(std::sync::Arc::new(result)))
            }
            (ColumnarValue::Array(a1), ColumnarValue::Scalar(#scalar_pattern2)) => {
                let arr1 = #array_type1;
                let opt2 = #convert2;
                if let Some(v2) = opt2 {
                    let result: #result_array_type =
                        datafusion::arrow::compute::kernels::arity::unary(arr1, |v1| #fn_name(v1, v2));
                    Ok(ColumnarValue::Array(std::sync::Arc::new(result)))
                } else {
                    Ok(ColumnarValue::Array(std::sync::Arc::new(
                        #result_array_type::new_null(arr1.len())
                    )))
                }
            }
            (ColumnarValue::Scalar(#scalar_pattern1), ColumnarValue::Array(a2)) => {
                let arr2 = #array_type2;
                let opt1 = #convert1;
                if let Some(v1) = opt1 {
                    let result: #result_array_type =
                        datafusion::arrow::compute::kernels::arity::unary(arr2, |v2| #fn_name(v1, v2));
                    Ok(ColumnarValue::Array(std::sync::Arc::new(result)))
                } else {
                    Ok(ColumnarValue::Array(std::sync::Arc::new(
                        #result_array_type::new_null(arr2.len())
                    )))
                }
            }
            (ColumnarValue::Scalar(#scalar_pattern1), ColumnarValue::Scalar(#scalar_pattern2)) => {
                let opt1 = #convert1;
                let opt2 = #convert2;
                let result = match (opt1, opt2) {
                    (Some(v1), Some(v2)) => Some(#fn_name(v1, v2)),
                    _ => None,
                };
                Ok(ColumnarValue::Scalar(#to_scalar))
            }
            _ => exec_err!("unsupported argument types for udf {}", self.name()),
        }
    }
}

/// Generate invoke for binary functions with Struct/List input parameters.
/// Uses index-based access similar to nary invoke for proper handling.
fn generate_binary_complex_input_invoke(
    user_fn: &UserFunctionAttr, sig_args: &[ArgType], return_info: &ReturnTypeInfo,
) -> TokenStream2 {
    let fn_name = &user_fn.name;
    let (arg1_name, _) = &user_fn.args[0];
    let (arg2_name, _) = &user_fn.args[1];
    let arg1_is_option = user_fn.args_option[0];
    let arg2_is_option = user_fn.args_option[1];

    let result_array_type = return_info.array_type_tokens();
    let to_scalar = return_info.to_scalar_tokens(quote!(result));

    let downcast1 = sig_args[0].array_downcast_method();
    let downcast2 = sig_args[1].array_downcast_method();
    let pattern1 = sig_args[0].scalar_pattern_tokens_named("s1");
    let pattern2 = sig_args[1].scalar_pattern_tokens_named("s2");
    let convert1 = sig_args[0].scalar_convert_expr("s1");
    let convert2 = sig_args[1].scalar_convert_expr("s2");
    let value_at1 = sig_args[0].value_at_index_tokens("arr1", "row_idx");
    let value_at2 = sig_args[1].value_at_index_tokens("arr2", "row_idx");

    let extract1 = if arg1_is_option {
        quote! { let #arg1_name = opt1; }
    } else {
        quote! { let #arg1_name = opt1?; }
    };
    let extract2 = if arg2_is_option {
        quote! { let #arg2_name = opt2; }
    } else {
        quote! { let #arg2_name = opt2?; }
    };

    let call = quote! { #fn_name(#arg1_name, #arg2_name) };
    let wrapped_call = user_fn.return_type_kind.wrap_call(call);

    let scalar_pattern1_extract = sig_args[0].scalar_pattern_tokens_named("opt1");
    let scalar_pattern2_extract = sig_args[1].scalar_pattern_tokens_named("opt2");
    let scalar_convert1 = sig_args[0].scalar_convert_expr("opt1");
    let scalar_convert2 = sig_args[1].scalar_convert_expr("opt2");

    let scalar_fast_path = if return_info.supports_scalar_value() {
        quote! {
            let all_scalars = args.iter().all(|a| matches!(a, ColumnarValue::Scalar(_)));
            if all_scalars {
                let opt1 = match &args[0] {
                    ColumnarValue::Scalar(#scalar_pattern1_extract) => #scalar_convert1,
                    _ => return exec_err!("expected scalar for argument 0 of udf {}", self.name()),
                };
                let opt2 = match &args[1] {
                    ColumnarValue::Scalar(#scalar_pattern2_extract) => #scalar_convert2,
                    _ => return exec_err!("expected scalar for argument 1 of udf {}", self.name()),
                };
                let result = (|| {
                    #extract1
                    #extract2
                    #wrapped_call
                })();
                return Ok(ColumnarValue::Scalar(#to_scalar));
            }
        }
    } else {
        quote! {}
    };

    quote! {
        #scalar_fast_path

        let is_arr1 = matches!(&args[0], ColumnarValue::Array(_));
        let arr1 = match &args[0] {
            ColumnarValue::Array(a) => Some(a.#downcast1),
            _ => None,
        };
        let scalar1 = match &args[0] {
            ColumnarValue::Scalar(#pattern1) => Some(#convert1),
            _ => None,
        };

        let is_arr2 = matches!(&args[1], ColumnarValue::Array(_));
        let arr2 = match &args[1] {
            ColumnarValue::Array(a) => Some(a.#downcast2),
            _ => None,
        };
        let scalar2 = match &args[1] {
            ColumnarValue::Scalar(#pattern2) => Some(#convert2),
            _ => None,
        };

        let mut len: Option<usize> = None;
        if is_arr1 {
            let arr_len = arr1.as_ref().unwrap().len();
            if let Some(l) = len {
                if arr_len != l {
                    return exec_err!("array length mismatch for udf {}", self.name());
                }
            } else {
                len = Some(arr_len);
            }
        }
        if is_arr2 {
            let arr_len = arr2.as_ref().unwrap().len();
            if let Some(l) = len {
                if arr_len != l {
                    return exec_err!("array length mismatch for udf {}", self.name());
                }
            } else {
                len = Some(arr_len);
            }
        }

        let len = match len {
            Some(l) => l,
            None => 1,
        };

        let result: #result_array_type = (0..len)
            .map(|row_idx| {
                let opt1 = if is_arr1 {
                    let arr1 = arr1.as_ref().unwrap();
                    #value_at1
                } else {
                    scalar1.clone().unwrap()
                };
                let opt2 = if is_arr2 {
                    let arr2 = arr2.as_ref().unwrap();
                    #value_at2
                } else {
                    scalar2.clone().unwrap()
                };
                (|| {
                    #extract1
                    #extract2
                    #wrapped_call
                })()
            })
            .collect();

        Ok(ColumnarValue::Array(std::sync::Arc::new(result)))
    }
}

/// Generate invoke for binary functions with List return type.
fn generate_binary_list_invoke(
    user_fn: &UserFunctionAttr, sig_args: &[ArgType], return_info: &ReturnTypeInfo,
) -> TokenStream2 {
    let fn_name = &user_fn.name;
    let (arg1_name, _) = &user_fn.args[0];
    let (arg2_name, _) = &user_fn.args[1];
    let arg1_is_option = user_fn.args_option[0];
    let arg2_is_option = user_fn.args_option[1];

    let inner_type = return_info.inner_type().unwrap();
    let inner_data_type = inner_type.data_type_tokens();

    let downcast1 = sig_args[0].array_downcast_method();
    let downcast2 = sig_args[1].array_downcast_method();
    let pattern1 = sig_args[0].scalar_pattern_tokens_named("s1");
    let pattern2 = sig_args[1].scalar_pattern_tokens_named("s2");
    let convert1 = sig_args[0].scalar_convert_expr("s1");
    let convert2 = sig_args[1].scalar_convert_expr("s2");
    let value_at1 = sig_args[0].value_at_index_tokens("arr1", "row_idx");
    let value_at2 = sig_args[1].value_at_index_tokens("arr2", "row_idx");

    let extract1 = if arg1_is_option {
        quote! { let #arg1_name = opt1; }
    } else {
        quote! { let #arg1_name = opt1?; }
    };
    let extract2 = if arg2_is_option {
        quote! { let #arg2_name = opt2; }
    } else {
        quote! { let #arg2_name = opt2?; }
    };

    let call = quote! { #fn_name(#arg1_name, #arg2_name) };
    let wrapped_call = user_fn.return_type_kind.wrap_call(call);

    let scalar_pattern1_extract = sig_args[0].scalar_pattern_tokens_named("opt1");
    let scalar_pattern2_extract = sig_args[1].scalar_pattern_tokens_named("opt2");
    let scalar_convert1 = sig_args[0].scalar_convert_expr("opt1");
    let scalar_convert2 = sig_args[1].scalar_convert_expr("opt2");

    quote! {
        let is_arr1 = matches!(&args[0], ColumnarValue::Array(_));
        let arr1 = match &args[0] {
            ColumnarValue::Array(a) => Some(a.#downcast1),
            _ => None,
        };
        let scalar1 = match &args[0] {
            ColumnarValue::Scalar(#pattern1) => Some(#convert1),
            _ => None,
        };

        let is_arr2 = matches!(&args[1], ColumnarValue::Array(_));
        let arr2 = match &args[1] {
            ColumnarValue::Array(a) => Some(a.#downcast2),
            _ => None,
        };
        let scalar2 = match &args[1] {
            ColumnarValue::Scalar(#pattern2) => Some(#convert2),
            _ => None,
        };

        let all_scalars = !is_arr1 && !is_arr2;
        if all_scalars {
            let opt1 = match &args[0] {
                ColumnarValue::Scalar(#scalar_pattern1_extract) => #scalar_convert1,
                _ => return exec_err!("expected scalar for argument 0 of udf {}", self.name()),
            };
            let opt2 = match &args[1] {
                ColumnarValue::Scalar(#scalar_pattern2_extract) => #scalar_convert2,
                _ => return exec_err!("expected scalar for argument 1 of udf {}", self.name()),
            };
            let result: Option<datafusion::arrow::array::ArrayRef> = (|| {
                #extract1
                #extract2
                #wrapped_call
            })();
            let field = std::sync::Arc::new(
                datafusion::arrow::datatypes::Field::new("item", #inner_data_type, true)
            );
            let scalar = match result {
                Some(arr) => ScalarValue::List(std::sync::Arc::new(
                    datafusion::arrow::array::ListArray::new(
                        field,
                        datafusion::arrow::buffer::OffsetBuffer::from_lengths([arr.len()]),
                        arr,
                        None,
                    )
                )),
                None => ScalarValue::List(std::sync::Arc::new(
                    datafusion::arrow::array::ListArray::new_null(field, 1)
                )),
            };
            return Ok(ColumnarValue::Scalar(scalar));
        }

        let mut len: Option<usize> = None;
        if is_arr1 {
            let arr_len = arr1.as_ref().unwrap().len();
            if let Some(l) = len {
                if arr_len != l {
                    return exec_err!("array length mismatch for udf {}", self.name());
                }
            } else {
                len = Some(arr_len);
            }
        }
        if is_arr2 {
            let arr_len = arr2.as_ref().unwrap().len();
            if let Some(l) = len {
                if arr_len != l {
                    return exec_err!("array length mismatch for udf {}", self.name());
                }
            } else {
                len = Some(arr_len);
            }
        }

        let len = len.unwrap_or(1);
        let field = std::sync::Arc::new(
            datafusion::arrow::datatypes::Field::new("item", #inner_data_type, true)
        );

        let mut offsets = vec![0i32];
        let mut values: Vec<datafusion::arrow::array::ArrayRef> = Vec::with_capacity(len);
        let mut null_builder = datafusion::arrow::array::NullBufferBuilder::new(len);

        for row_idx in 0..len {
            let opt1 = if is_arr1 {
                let arr1 = arr1.as_ref().unwrap();
                #value_at1
            } else {
                scalar1.clone().unwrap()
            };
            let opt2 = if is_arr2 {
                let arr2 = arr2.as_ref().unwrap();
                #value_at2
            } else {
                scalar2.clone().unwrap()
            };
            let result: Option<datafusion::arrow::array::ArrayRef> = (|| {
                #extract1
                #extract2
                #wrapped_call
            })();
            match result {
                Some(arr_ref) => {
                    offsets.push(offsets.last().unwrap() + arr_ref.len() as i32);
                    values.push(arr_ref);
                    null_builder.append_non_null();
                }
                None => {
                    offsets.push(*offsets.last().unwrap());
                    null_builder.append_null();
                }
            }
        }

        let values_array: datafusion::arrow::array::ArrayRef = if values.is_empty() {
            datafusion::arrow::array::new_empty_array(&#inner_data_type)
        } else {
            datafusion::arrow::compute::concat(&values.iter().map(|a| a.as_ref()).collect::<Vec<_>>())?
        };

        let list_array = datafusion::arrow::array::ListArray::new(
            field,
            datafusion::arrow::buffer::OffsetBuffer::new(offsets.into()),
            values_array,
            null_builder.finish(),
        );
        Ok(ColumnarValue::Array(std::sync::Arc::new(list_array)))
    }
}

fn generate_binary_call(
    fn_name: &Ident, arg1_name: &Ident, arg2_name: &Ident, arg1_is_option: bool,
    arg2_is_option: bool, return_kind: &ReturnTypeKind,
) -> TokenStream2 {
    let arg1_expr = if arg1_is_option {
        quote! { opt1 }
    } else {
        quote! { #arg1_name }
    };
    let arg2_expr = if arg2_is_option {
        quote! { opt2 }
    } else {
        quote! { #arg2_name }
    };

    let call = quote! { #fn_name(#arg1_expr, #arg2_expr) };
    let wrapped_call = return_kind.wrap_call(call);

    let extract1 = if arg1_is_option {
        quote! {}
    } else {
        quote! { let #arg1_name = opt1?; }
    };
    let extract2 = if arg2_is_option {
        quote! {}
    } else {
        quote! { let #arg2_name = opt2?; }
    };

    if !arg1_is_option || !arg2_is_option {
        quote! {
            (|| {
                #extract1
                #extract2
                #wrapped_call
            })()
        }
    } else {
        wrapped_call
    }
}

fn generate_nary_invoke(
    user_fn: &UserFunctionAttr, sig_args: &[ArgType], return_info: &ReturnTypeInfo,
) -> TokenStream2 {
    let fn_name = &user_fn.name;
    let to_scalar = return_info.to_scalar_tokens(quote!(result));

    let arg_extracts: Vec<TokenStream2> = user_fn
        .args
        .iter()
        .zip(user_fn.args_option.iter())
        .enumerate()
        .map(|(i, ((arg_name, _), is_option))| {
            let opt_name = format_ident!("opt{}", i);
            if *is_option {
                quote! { let #arg_name = #opt_name; }
            } else {
                quote! { let #arg_name = #opt_name?; }
            }
        })
        .collect();

    let arg_names: Vec<&Ident> = user_fn.args.iter().map(|(n, _)| n).collect();
    let call = quote! { #fn_name(#(#arg_names),*) };
    let wrapped_call = user_fn.return_type_kind.wrap_call(call);

    if return_info.is_struct() {
        return generate_mixed_struct_invoke(sig_args, return_info, &arg_extracts, &wrapped_call);
    }

    generate_mixed_invoke(
        sig_args,
        return_info,
        &arg_extracts,
        &wrapped_call,
        &to_scalar,
    )
}

fn generate_mixed_struct_invoke(
    sig_args: &[ArgType], return_info: &ReturnTypeInfo, arg_extracts: &[TokenStream2],
    wrapped_call: &TokenStream2,
) -> TokenStream2 {
    let builder_ident = return_info.struct_builder_ident().unwrap();

    let arg_setup: Vec<TokenStream2> = sig_args
        .iter()
        .enumerate()
        .map(|(i, arg_type)| {
            let arr_name = format_ident!("arr{}", i);
            let scalar_name = format_ident!("scalar{}", i);
            let downcast_method = arg_type.array_downcast_method();
            let pattern = arg_type.scalar_pattern_tokens_named(&format!("s{i}"));
            let convert = arg_type.scalar_convert_expr(&format!("s{i}"));

            quote! {
                let #arr_name;
                let #scalar_name;
                match &args[#i] {
                    ColumnarValue::Array(a) => {
                        #arr_name = Some(a.#downcast_method);
                        #scalar_name = None;
                    }
                    ColumnarValue::Scalar(#pattern) => {
                        #arr_name = None;
                        #scalar_name = Some(#convert);
                    }
                    _ => return exec_err!("unsupported argument type for udf"),
                }
            }
        })
        .collect();

    let opt_extracts: Vec<TokenStream2> = sig_args
        .iter()
        .enumerate()
        .map(|(i, arg_type)| {
            let opt_name = format_ident!("opt{}", i);
            let arr_name = format_ident!("arr{}", i);
            let scalar_name = format_ident!("scalar{}", i);
            let value_at = arg_type.value_at_index_tokens(&format!("arr{i}"), "i");

            quote! {
                let #opt_name = if let Some(arr) = &#arr_name {
                    let #arr_name = *arr;
                    #value_at
                } else {
                    #scalar_name.clone()
                };
            }
        })
        .collect();

    let len_checks: Vec<TokenStream2> = sig_args
        .iter()
        .enumerate()
        .map(|(i, _)| {
            let arr_name = format_ident!("arr{}", i);
            quote! {
                if let Some(a) = &#arr_name {
                    if let Some(l) = len {
                        if a.len() != l {
                            return exec_err!("array length mismatch for udf {}", self.name());
                        }
                    } else {
                        len = Some(a.len());
                    }
                }
            }
        })
        .collect();

    let scalar_checks: Vec<TokenStream2> = sig_args
        .iter()
        .enumerate()
        .map(|(i, _)| {
            let arr_name = format_ident!("arr{}", i);
            quote! { #arr_name.is_none() }
        })
        .collect();

    quote! {
        #(#arg_setup)*

        let mut len: Option<usize> = None;
        #(#len_checks)*

        let len = len.unwrap_or(1);

        let all_scalars = #(#scalar_checks)&&*;

        if all_scalars {
            let i = 0usize;
            let mut builder = #builder_ident::with_capacity(1);
            #(#opt_extracts)*
            let result = (|| {
                #(#arg_extracts)*
                #wrapped_call
            })();
            builder.append_option(result);
            let arr = builder.finish();
            return Ok(ColumnarValue::Scalar(ScalarValue::Struct(std::sync::Arc::new(arr))));
        }

        let mut builder = #builder_ident::with_capacity(len);
        for i in 0..len {
            #(#opt_extracts)*
            let result = (|| {
                #(#arg_extracts)*
                #wrapped_call
            })();
            builder.append_option(result);
        }
        Ok(ColumnarValue::Array(std::sync::Arc::new(builder.finish())))
    }
}

fn generate_mixed_invoke(
    sig_args: &[ArgType], return_info: &ReturnTypeInfo, arg_extracts: &[TokenStream2],
    wrapped_call: &TokenStream2, to_scalar: &TokenStream2,
) -> TokenStream2 {
    let result_array_type = return_info.array_type_tokens();
    let arg_count = sig_args.len();

    let arg_setup: Vec<TokenStream2> = sig_args
        .iter()
        .enumerate()
        .map(|(i, arg_type)| {
            let is_arr_name = format_ident!("is_arr{}", i);
            let arr_name = format_ident!("arr{}", i);
            let scalar_name = format_ident!("scalar{}", i);
            let downcast_method = arg_type.array_downcast_method();
            let pattern = arg_type.scalar_pattern_tokens_named(&format!("s{i}"));
            let convert = arg_type.scalar_convert_expr(&format!("s{i}"));

            quote! {
                let #is_arr_name = matches!(&args[#i], ColumnarValue::Array(_));
                let #arr_name = match &args[#i] {
                    ColumnarValue::Array(a) => Some(a.#downcast_method),
                    _ => None,
                };
                let #scalar_name = match &args[#i] {
                    ColumnarValue::Scalar(#pattern) => Some(#convert),
                    _ => None,
                };
            }
        })
        .collect();

    let value_extracts: Vec<TokenStream2> = sig_args
        .iter()
        .enumerate()
        .map(|(i, arg_type)| {
            let opt_name = format_ident!("opt{}", i);
            let is_arr_name = format_ident!("is_arr{}", i);
            let arr_name = format_ident!("arr{}", i);
            let scalar_name = format_ident!("scalar{}", i);
            let value_access = arg_type.value_at_index_tokens("arr", "row_idx");

            quote! {
                let #opt_name = if #is_arr_name {
                    let arr = &#arr_name.as_ref().unwrap();
                    #value_access
                } else {
                    #scalar_name.clone().unwrap()
                };
            }
        })
        .collect();

    let len_checks: Vec<TokenStream2> = (0..arg_count)
        .map(|i| {
            let is_arr_name = format_ident!("is_arr{}", i);
            let arr_name = format_ident!("arr{}", i);
            quote! {
                if #is_arr_name {
                    let arr_len = #arr_name.as_ref().unwrap().len();
                    if let Some(l) = len {
                        if arr_len != l {
                            return exec_err!("array length mismatch for udf {}", self.name());
                        }
                    } else {
                        len = Some(arr_len);
                    }
                }
            }
        })
        .collect();

    let scalar_extractions: Vec<TokenStream2> = sig_args
        .iter()
        .enumerate()
        .map(|(i, arg_type)| {
            let opt_name = format_ident!("opt{}", i);
            let pattern = arg_type.scalar_pattern_tokens_named(&format!("opt{i}"));
            let convert_expr = arg_type.scalar_convert_expr(&format!("opt{i}"));
            quote! {
                let #opt_name = match &args[#i] {
                    ColumnarValue::Scalar(#pattern) => #convert_expr,
                    _ => return exec_err!("expected scalar for argument {} of udf {}", #i, self.name()),
                };
            }
        })
        .collect();

    let scalar_fast_path = if return_info.supports_scalar_value() {
        quote! {
            let all_scalars = args.iter().all(|a| matches!(a, ColumnarValue::Scalar(_)));
            if all_scalars {
                #(#scalar_extractions)*
                let result = (|| {
                    #(#arg_extracts)*
                    #wrapped_call
                })();
                return Ok(ColumnarValue::Scalar(#to_scalar));
            }
        }
    } else {
        quote! {}
    };

    quote! {
        #scalar_fast_path

        #(#arg_setup)*

        let mut len: Option<usize> = None;
        #(#len_checks)*

        let len = match len {
            Some(l) => l,
            None => 1,
        };

        let result: #result_array_type = (0..len)
            .map(|row_idx| {
                #(#value_extracts)*
                (|| {
                    #(#arg_extracts)*
                    #wrapped_call
                })()
            })
            .collect();

        Ok(ColumnarValue::Array(std::sync::Arc::new(result)))
    }
}

/// Generate invoke implementation for combined signatures with fewer args.
pub fn generate_combined_invoke_impl(
    user_fn: &UserFunctionAttr, sig_args: &[ArgType], return_info: &ReturnTypeInfo,
    fn_arg_count: usize,
) -> TokenStream2 {
    let sig_arg_count = sig_args.len();

    if sig_arg_count == fn_arg_count {
        return generate_invoke_impl(user_fn, sig_args, return_info);
    }

    let fn_name = &user_fn.name;
    let result_array_type = return_info.array_type_tokens();
    let to_scalar = return_info.to_scalar_tokens(quote!(result));

    let arg_setup: Vec<TokenStream2> = sig_args
        .iter()
        .enumerate()
        .map(|(i, arg_type)| {
            let is_arr_name = format_ident!("is_arr{}", i);
            let arr_name = format_ident!("arr{}", i);
            let scalar_name = format_ident!("scalar{}", i);
            let downcast_method = arg_type.array_downcast_method();
            let pattern = arg_type.scalar_pattern_tokens_named(&format!("s{i}"));
            let convert = arg_type.scalar_convert_expr(&format!("s{i}"));

            quote! {
                let #is_arr_name = matches!(&args[#i], ColumnarValue::Array(_));
                let #arr_name = match &args[#i] {
                    ColumnarValue::Array(a) => Some(a.#downcast_method),
                    _ => None,
                };
                let #scalar_name = match &args[#i] {
                    ColumnarValue::Scalar(#pattern) => Some(#convert),
                    _ => None,
                };
            }
        })
        .collect();

    let value_extracts: Vec<TokenStream2> = sig_args
        .iter()
        .enumerate()
        .map(|(i, arg_type)| {
            let opt_name = format_ident!("opt{}", i);
            let is_arr_name = format_ident!("is_arr{}", i);
            let arr_name = format_ident!("arr{}", i);
            let scalar_name = format_ident!("scalar{}", i);
            let value_access = arg_type.value_at_index_tokens("arr", "row_idx");

            quote! {
                let #opt_name = if #is_arr_name {
                    let arr = &#arr_name.as_ref().unwrap();
                    #value_access
                } else {
                    #scalar_name.clone().unwrap()
                };
            }
        })
        .collect();

    let mut arg_extracts = Vec::new();
    let mut fn_call_args = Vec::new();

    for i in 0..fn_arg_count {
        let (arg_name, _) = &user_fn.args[i];
        let is_option = user_fn.args_option[i];

        if i < sig_arg_count {
            let opt_name = format_ident!("opt{}", i);
            if is_option {
                arg_extracts.push(quote! { let #arg_name = #opt_name; });
            } else {
                arg_extracts.push(quote! { let #arg_name = #opt_name?; });
            }
            fn_call_args.push(quote! { #arg_name });
        } else {
            fn_call_args.push(quote! { None });
        }
    }

    let call = quote! { #fn_name(#(#fn_call_args),*) };
    let wrapped_call = user_fn.return_type_kind.wrap_call(call);

    let scalar_extractions: Vec<TokenStream2> = sig_args
        .iter()
        .enumerate()
        .map(|(i, arg_type)| {
            let opt_name = format_ident!("opt{}", i);
            let pattern = arg_type.scalar_pattern_tokens_named(&format!("opt{i}"));
            let convert_expr = arg_type.scalar_convert_expr(&format!("opt{i}"));
            quote! {
                let #opt_name = match &args[#i] {
                    ColumnarValue::Scalar(#pattern) => #convert_expr,
                    _ => return exec_err!("expected scalar for argument {} of udf {}", #i, self.name()),
                };
            }
        })
        .collect();

    let len_checks: Vec<TokenStream2> = (0..sig_arg_count)
        .map(|i| {
            let is_arr_name = format_ident!("is_arr{}", i);
            let arr_name = format_ident!("arr{}", i);
            quote! {
                if #is_arr_name {
                    let arr_len = #arr_name.as_ref().unwrap().len();
                    if let Some(l) = len {
                        if arr_len != l {
                            return exec_err!("array length mismatch for udf {}", self.name());
                        }
                    } else {
                        len = Some(arr_len);
                    }
                }
            }
        })
        .collect();

    let scalar_fast_path = if return_info.supports_scalar_value() {
        quote! {
            let all_scalars = args.iter().all(|a| matches!(a, ColumnarValue::Scalar(_)));
            if all_scalars {
                #(#scalar_extractions)*
                let result = (|| {
                    #(#arg_extracts)*
                    #wrapped_call
                })();
                return Ok(ColumnarValue::Scalar(#to_scalar));
            }
        }
    } else {
        quote! {}
    };

    quote! {
        #scalar_fast_path

        #(#arg_setup)*

        let mut len: Option<usize> = None;
        #(#len_checks)*

        let len = match len {
            Some(l) => l,
            None => 1,
        };

        let result: #result_array_type = (0..len)
            .map(|row_idx| {
                #(#value_extracts)*
                (|| {
                    #(#arg_extracts)*
                    #wrapped_call
                })()
            })
            .collect();

        Ok(ColumnarValue::Array(std::sync::Arc::new(result)))
    }
}
