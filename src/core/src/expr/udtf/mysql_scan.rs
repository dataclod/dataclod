use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::{exec_datafusion_err, plan_err, ScalarValue};
use datafusion::datasource::function::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Expr;
use datafusion::sql::TableReference;
use datafusion_table_providers::mysql::MySQLTableFactory;
use datafusion_table_providers::sql::db_connection_pool::mysqlpool::MySQLConnectionPool;
use datafusion_table_providers::util::secrets::to_secret_map;
use tokio::runtime::Handle;

pub struct MySQLScanUDTF;

impl TableFunctionImpl for MySQLScanUDTF {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if exprs.len() < 3 {
            return plan_err!("mysql_scan takes 3 arguments");
        }

        match (&exprs[0], &exprs[1], &exprs[2]) {
            (
                Expr::Literal(ScalarValue::Utf8(Some(dsn))),
                Expr::Literal(ScalarValue::Utf8(Some(db))),
                Expr::Literal(ScalarValue::Utf8(Some(table))),
            ) => {
                let params = to_secret_map(HashMap::from([(
                    "connection_string".to_owned(),
                    dsn.to_owned(),
                )]));
                let pool = tokio::task::block_in_place(|| {
                    Handle::current().block_on(async { MySQLConnectionPool::new(params).await })
                })
                .map_err(|e| exec_datafusion_err!("{}", e))?;
                let table_factory = MySQLTableFactory::new(Arc::new(pool));

                tokio::task::block_in_place(|| {
                    Handle::current().block_on(async {
                        table_factory
                            .table_provider(TableReference::partial(db.as_str(), table.as_str()))
                            .await
                    })
                })
                .map_err(|e| exec_datafusion_err!("{}", e))
            }
            _ => plan_err!("mysql_scan arguments must be string literals"),
        }
    }
}