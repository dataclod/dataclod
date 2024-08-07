use std::sync::Arc;

use datafusion::common::{exec_datafusion_err, plan_err, ScalarValue};
use datafusion::datasource::function::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Expr;
use datafusion::sql::TableReference;
use datafusion_table_providers::sql::db_connection_pool::sqlitepool::SqliteConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::Mode;
use datafusion_table_providers::sqlite::SqliteTableFactory;
use tokio::runtime::Handle;

pub struct SqliteScanUDTF;

impl TableFunctionImpl for SqliteScanUDTF {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if exprs.len() < 2 {
            return plan_err!("postgres_scan takes 2 arguments");
        }

        match (&exprs[0], &exprs[1]) {
            (
                Expr::Literal(ScalarValue::Utf8(Some(db_path))),
                Expr::Literal(ScalarValue::Utf8(Some(table))),
            ) => {
                let sqlite_pool = tokio::task::block_in_place(|| {
                    Handle::current()
                        .block_on(async { SqliteConnectionPool::new(db_path, Mode::File).await })
                })
                .map_err(|e| exec_datafusion_err!("{}", e))?;
                let sqlite_table_factory = SqliteTableFactory::new(Arc::new(sqlite_pool));

                tokio::task::block_in_place(|| {
                    Handle::current().block_on(async {
                        sqlite_table_factory
                            .table_provider(TableReference::bare(table.as_str()))
                            .await
                    })
                })
                .map_err(|e| exec_datafusion_err!("{}", e))
            }
            _ => plan_err!("sqlite_scan arguments must be string literals"),
        }
    }
}
