use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
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
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let stream = Arc::new(PostgresStream {
            client: self.client.clone(),
            schema: self.schema(),
            query: self.query.to_owned(),
        });

        Ok(Arc::new(StreamingTableExec::try_new(
            self.schema(),
            vec![stream],
            projection,
            None,
            false,
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
        let query = self.query.to_owned();
        let schema = self.schema.clone();
        builder.spawn(async move {
            let row_stream = client
                .query_raw::<_, bool, _>(&query, Vec::new())
                .await
                .unwrap()
                .try_chunks(128);
            pin_mut!(row_stream);
            while let Some(rows) = row_stream.try_next().await.unwrap() {
                tx.blocking_send(
                    encode_postgres_rows(&rows, &schema)
                        .map_err(|e| DataFusionError::NotImplemented(e.to_string())),
                )
                .unwrap();
            }
            Ok(())
        });

        builder.build()
    }
}
