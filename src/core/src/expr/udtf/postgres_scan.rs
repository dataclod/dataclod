use std::sync::Arc;

use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::{exec_datafusion_err, not_impl_datafusion_err, plan_err, ScalarValue};
use datafusion::datasource::function::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Expr;
use tokio::runtime::Handle;
use tokio_postgres::NoTls;
use tracing::error;

use crate::datasource::{postgres_to_arrow, PostgresTable};

pub struct PostgresScanUDTF;

impl TableFunctionImpl for PostgresScanUDTF {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if exprs.len() < 3 {
            return plan_err!("postgres_scan takes 3 arguments");
        }

        match (&exprs[0], &exprs[1], &exprs[2]) {
            (
                Expr::Literal(ScalarValue::Utf8(Some(dsn))),
                Expr::Literal(ScalarValue::Utf8(Some(db))),
                Expr::Literal(ScalarValue::Utf8(Some(table))),
            ) => {
                let query = format!("SELECT * FROM {db}.{table}");
                // HACK: better way to do this?
                let (client, stmt) = tokio::task::block_in_place(|| {
                    Handle::current().block_on(async {
                        let (client, conn) = tokio_postgres::connect(dsn, NoTls).await?;
                        tokio::spawn(async move {
                            if let Err(e) = conn.await {
                                error!("connection error: {}", e);
                            }
                        });
                        let stmt = client.prepare(&query).await?;
                        Ok::<_, tokio_postgres::Error>((client, stmt))
                    })
                })
                .map_err(|e| exec_datafusion_err!("{}", e))?;

                let columns = stmt.columns();
                let mut fields = Vec::with_capacity(columns.len());
                for column in columns {
                    let filed_type = postgres_to_arrow(column.type_())
                        .map_err(|e| not_impl_datafusion_err!("{}", e))?;
                    let field = Field::new(column.name(), filed_type, true);
                    fields.push(field);
                }
                let schema = Arc::new(Schema::new(fields));

                Ok(Arc::new(PostgresTable {
                    client: Arc::new(client),
                    schema,
                    query,
                }))
            }
            _ => plan_err!("postgres_scan arguments must be string literals"),
        }
    }
}
