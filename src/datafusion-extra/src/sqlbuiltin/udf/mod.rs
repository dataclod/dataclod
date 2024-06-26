mod array_upper;
mod current_schema;
mod current_schemas;
mod format_type;
mod version;

use datafusion::execution::context::SessionContext;

pub fn register_udf(ctx: &SessionContext) {
    ctx.register_udf(array_upper::create_udf());
    ctx.register_udf(current_schema::create_udf());
    ctx.register_udf(current_schemas::create_udf());
    ctx.register_udf(version::create_udf());
    ctx.register_udf(format_type::create_udf());
}
