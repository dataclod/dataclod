mod duckdb_scan;
mod mysql_scan;
mod postgres_scan;
mod sqlite_scan;

use std::sync::Arc;

use datafusion::execution::context::SessionContext;

pub fn register_udtf(ctx: &SessionContext) {
    ctx.register_udtf("duckdb_scan", Arc::new(duckdb_scan::DuckDBScanUDTF));
    ctx.register_udtf("mysql_scan", Arc::new(mysql_scan::MySQLScanUDTF));
    ctx.register_udtf("postgres_scan", Arc::new(postgres_scan::PostgresScanUDTF));
    ctx.register_udtf("sqlite_scan", Arc::new(sqlite_scan::SqliteScanUDTF));
}
