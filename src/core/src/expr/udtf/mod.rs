mod postgres_scan;
mod sqlite_scan;

use std::sync::Arc;

use datafusion::execution::context::SessionContext;

pub fn register_udtf(ctx: &SessionContext) {
    ctx.register_udtf("postgres_scan", Arc::new(postgres_scan::PostgresScanUDTF));
    ctx.register_udtf("sqlite_scan", Arc::new(sqlite_scan::SqliteScanUDTF));
}
