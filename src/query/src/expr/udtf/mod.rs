use std::sync::Arc;

use datafusion::execution::context::SessionContext;

mod postgres_scan;

pub fn register_udtf(ctx: &SessionContext) {
    ctx.register_udtf(
        "postgres_scan",
        Arc::new(postgres_scan::PostgresScanUDTF {}),
    );
}
