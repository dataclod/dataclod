use datafusion_udf_macros::extension_type;

#[extension_type(
    name = "Geometry",
    data_type = datafusion::arrow::datatypes::DataType::BinaryView
)]
pub struct Geometry;
