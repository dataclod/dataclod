mod array_upper;
mod current_schema;
mod current_schemas;
mod format_type;
mod version;

use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::ScalarUDF;

pub fn sqlbuiltin_udfs() -> Vec<Arc<ScalarUDF>> {
    vec![
        Arc::new(array_upper::create_udf()),
        Arc::new(current_schema::create_udf()),
        Arc::new(current_schemas::create_udf()),
        Arc::new(version::create_udf()),
        Arc::new(format_type::create_udf()),
    ]
}

pub fn register_sqlbuiltin_udfs(ctx: &SessionContext) {
    for udf in sqlbuiltin_udfs() {
        ctx.register_udf(udf.as_ref().clone());
    }
}
