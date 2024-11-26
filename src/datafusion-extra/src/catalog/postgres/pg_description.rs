use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, StringBuilder, UInt32Builder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::memory::MemoryExec;

struct PgCatalogDescriptionBuilder {
    objoid: UInt32Builder,
    description: StringBuilder,
}

impl PgCatalogDescriptionBuilder {
    fn new() -> Self {
        let capacity = 64;

        Self {
            objoid: UInt32Builder::with_capacity(capacity),
            description: StringBuilder::with_capacity(capacity, 0),
        }
    }

    fn finish(&mut self) -> Vec<ArrayRef> {
        vec![
            Arc::new(self.objoid.finish()),
            Arc::new(self.description.finish()),
        ]
    }
}

#[derive(Debug)]
pub struct PgDescriptionTable {
    data: Vec<ArrayRef>,
}

impl PgDescriptionTable {
    pub fn new() -> Self {
        let mut builder = PgCatalogDescriptionBuilder::new();

        Self {
            data: builder.finish(),
        }
    }
}

#[async_trait]
impl TableProvider for PgDescriptionTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("objoid", DataType::UInt32, false),
            Field::new("description", DataType::Utf8, false),
        ]))
    }

    async fn scan(
        &self, _state: &dyn Session, projection: Option<&Vec<usize>>, _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let batch = RecordBatch::try_new(self.schema(), self.data.clone())?;

        Ok(Arc::new(MemoryExec::try_new(
            &[vec![batch]],
            self.schema(),
            projection.cloned(),
        )?))
    }
}
