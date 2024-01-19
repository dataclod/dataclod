use std::sync::Arc;

use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::{plan_err, DataFusionError, ScalarValue};
use datafusion::datasource::function::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use tokio::runtime::Handle;
use tokio_postgres::NoTls;
use tracing::error;

use crate::datasource::{postgres_to_arrow, PostgresTable};

pub struct PostgresScanUDTF {}

impl TableFunctionImpl for PostgresScanUDTF {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if exprs.len() != 3 {
            return plan_err!("postgres_scan takes 3 arguments");
        }

        let dsn = &exprs[0];
        let db = &exprs[1];
        let table = &exprs[2];

        match (dsn, db, table) {
            (
                Expr::Literal(ScalarValue::Utf8(Some(dsn))),
                Expr::Literal(ScalarValue::Utf8(Some(db))),
                Expr::Literal(ScalarValue::Utf8(Some(table))),
            ) => {
                let query = format!("SELECT * FROM {}.{}", db, table);
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
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;

                let columns = stmt.columns();
                let mut fields = Vec::with_capacity(columns.len());
                for column in columns {
                    let filed_type = postgres_to_arrow(column.type_())
                        .map_err(|e| DataFusionError::NotImplemented(e.to_string()))?;
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
