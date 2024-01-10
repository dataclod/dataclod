mod array_upper;
mod current_schemas;
mod version;

use datafusion::execution::context::SessionContext;

pub fn register_udf(ctx: &SessionContext) {
    ctx.register_udf(array_upper::create_udf());
    ctx.register_udf(current_schemas::create_udf());
    ctx.register_udf(version::create_udf());
}