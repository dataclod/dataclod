use std::sync::Arc;
use std::time::Duration;

use datafusion::common::{ScalarValue, exec_datafusion_err, plan_err};
use datafusion::datasource::TableProvider;
use datafusion::datasource::function::TableFunctionImpl;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Expr;
use datafusion::sql::TableReference;
use datafusion_table_providers::sql::db_connection_pool::Mode;
use datafusion_table_providers::sql::db_connection_pool::sqlitepool::SqliteConnectionPoolFactory;
use datafusion_table_providers::sqlite::SqliteTableFactory;
use tokio::runtime::Handle;

pub struct SqliteScanUDTF;

impl TableFunctionImpl for SqliteScanUDTF {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if exprs.len() < 2 {
            return plan_err!("sqlite_scan takes 2 arguments");
        }

        match (&exprs[0], &exprs[1]) {
            (
                Expr::Literal(ScalarValue::Utf8(Some(db_path))),
                Expr::Literal(ScalarValue::Utf8(Some(table))),
            ) => {
                let pool = tokio::task::block_in_place(|| {
                    Handle::current().block_on(async {
                        SqliteConnectionPoolFactory::new(
                            db_path,
                            Mode::File,
                            Duration::from_secs(5),
                        )
                        .build()
                        .await
                    })
                })
                .map_err(|e| exec_datafusion_err!("{}", e))?;
                let table_factory = SqliteTableFactory::new(Arc::new(pool));

                tokio::task::block_in_place(|| {
                    Handle::current().block_on(async {
                        table_factory
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
