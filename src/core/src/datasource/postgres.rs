use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{exec_datafusion_err, not_impl_datafusion_err, Result};
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use futures::{pin_mut, TryStreamExt};
use tokio_postgres::Client;

use super::types::encode_postgres_rows;

pub struct PostgresTable {
    // XXX: close connection on drop?
    pub client: Arc<Client>,
    pub schema: SchemaRef,
    pub query: String,
}

#[async_trait]
impl TableProvider for PostgresTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self, _state: &SessionState, projection: Option<&Vec<usize>>, _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let stream = PostgresStream {
            client: self.client.clone(),
            schema: self.schema(),
            query: self.query.clone(),
        };

        Ok(Arc::new(StreamingTableExec::try_new(
            self.schema(),
            vec![Arc::new(stream)],
            projection,
            [],
            false,
            limit,
        )?))
    }
}

struct PostgresStream {
    client: Arc<Client>,
    schema: SchemaRef,
    query: String,
}

impl PartitionStream for PostgresStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut builder = RecordBatchReceiverStreamBuilder::new(self.schema.clone(), 2);
        let tx = builder.tx();
        let client = self.client.clone();
        let query = self.query.clone();
        let schema = self.schema.clone();

        builder.spawn(async move {
            let row_stream = client
            // HACK: unable to infer type
                .query_raw::<_, bool, _>(&query, Vec::new())
                .await
                .map_err(|e| exec_datafusion_err!("Failed to execute query: {}", e))?;
            let row_stream = row_stream.try_chunks(128);
            pin_mut!(row_stream);

            while let Some(rows) = row_stream
                .try_next()
                .await
                .map_err(|e| exec_datafusion_err!("Failed to receive rows from database: {}", e))?
            {
                tx.send(
                    encode_postgres_rows(&rows, &schema)
                        .map_err(|e| not_impl_datafusion_err!("{}", e)),
                )
                .await
                .map_err(|e| {
                    exec_datafusion_err!("Failed to send record batch to receiver: {}", e)
                })?;
            }

            Ok(())
        });

        builder.build()
    }
}
