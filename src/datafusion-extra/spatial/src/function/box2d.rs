use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, AsArray, Float64Array, Float64Builder, NullBufferBuilder, StructArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::{Result as DFResult, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use geos::{Geom, Geometry};

pub fn box2d() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(Box2DUdf {
        signature: Signature::user_defined(Volatility::Immutable),
        aliases: vec!["box2d".to_owned()],
    }))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Box2DUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl ScalarUDFImpl for Box2DUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "Box2D"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Struct(Fields::from(vec![
            Field::new("xmin", DataType::Float64, true),
            Field::new("ymin", DataType::Float64, true),
            Field::new("xmax", DataType::Float64, true),
            Field::new("ymax", DataType::Float64, true),
        ])))
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
            ColumnarValue::Array(arr) => {
                let wkb_arr = arr.as_binary_view();
                let len = wkb_arr.len();
                let mut xmin_builder = Float64Builder::with_capacity(len);
                let mut ymin_builder = Float64Builder::with_capacity(len);
                let mut xmax_builder = Float64Builder::with_capacity(len);
                let mut ymax_builder = Float64Builder::with_capacity(len);
                let mut nulls_builder = NullBufferBuilder::new(len);
                for i in 0..len {
                    if wkb_arr.is_null(i) {
                        xmin_builder.append_null();
                        ymin_builder.append_null();
                        xmax_builder.append_null();
                        ymax_builder.append_null();
                        nulls_builder.append(false);
                    } else {
                        let wkb = wkb_arr.value(i);
                        match Geometry::new_from_wkb(wkb) {
                            Ok(geom) => {
                                xmin_builder.append_option(geom.get_x_min().ok());
                                ymin_builder.append_option(geom.get_y_min().ok());
                                xmax_builder.append_option(geom.get_x_max().ok());
                                ymax_builder.append_option(geom.get_y_max().ok());
                                nulls_builder.append(true);
                            }
                            _ => {
                                xmin_builder.append_null();
                                ymin_builder.append_null();
                                xmax_builder.append_null();
                                ymax_builder.append_null();
                                nulls_builder.append(false);
                            }
                        }
                    }
                }
                let result = StructArray::new(
                    Fields::from(vec![
                        Field::new("xmin", DataType::Float64, true),
                        Field::new("ymin", DataType::Float64, true),
                        Field::new("xmax", DataType::Float64, true),
                        Field::new("ymax", DataType::Float64, true),
                    ]),
                    vec![
                        Arc::new(xmin_builder.finish()),
                        Arc::new(ymin_builder.finish()),
                        Arc::new(xmax_builder.finish()),
                        Arc::new(ymax_builder.finish()),
                    ],
                    nulls_builder.finish(),
                );

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            ColumnarValue::Scalar(ScalarValue::BinaryView(opt)) => {
                let box2d = opt.as_ref().and_then(|wkb| {
                    match Geometry::new_from_wkb(wkb) {
                        Ok(geom) => {
                            Some((
                                geom.get_x_min().ok(),
                                geom.get_y_min().ok(),
                                geom.get_x_max().ok(),
                                geom.get_y_max().ok(),
                            ))
                        }
                        _ => None,
                    }
                });
                let result = match box2d {
                    Some((xmin, ymin, xmax, ymax)) => {
                        StructArray::new(
                            Fields::from(vec![
                                Field::new("xmin", DataType::Float64, true),
                                Field::new("ymin", DataType::Float64, true),
                                Field::new("xmax", DataType::Float64, true),
                                Field::new("ymax", DataType::Float64, true),
                            ]),
                            vec![
                                Arc::new(Float64Array::from(vec![xmin])),
                                Arc::new(Float64Array::from(vec![ymin])),
                                Arc::new(Float64Array::from(vec![xmax])),
                                Arc::new(Float64Array::from(vec![ymax])),
                            ],
                            Some(NullBuffer::new_valid(1)),
                        )
                    }
                    None => {
                        StructArray::new(
                            Fields::from(vec![
                                Field::new("xmin", DataType::Float64, true),
                                Field::new("ymin", DataType::Float64, true),
                                Field::new("xmax", DataType::Float64, true),
                                Field::new("ymax", DataType::Float64, true),
                            ]),
                            vec![
                                Arc::new(Float64Array::new_null(1)),
                                Arc::new(Float64Array::new_null(1)),
                                Arc::new(Float64Array::new_null(1)),
                                Arc::new(Float64Array::new_null(1)),
                            ],
                            Some(NullBuffer::new_null(1)),
                        )
                    }
                };

                Ok(ColumnarValue::Scalar(ScalarValue::Struct(Arc::new(result))))
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
