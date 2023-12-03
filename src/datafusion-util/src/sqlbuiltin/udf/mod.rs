mod array_upper;

use datafusion::execution::context::SessionContext;

pub fn register_udf(ctx: &SessionContext) {
    ctx.register_udf(array_upper::create_udf());
}
