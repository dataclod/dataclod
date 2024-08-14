use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, StringBuilder, UInt32Builder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::Result as DFResult;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;

struct _PgClass<'a> {
    oid: u32,
    relkind: &'a str,
}

struct PgCatalogClassBuilder {
    oid: UInt32Builder,
    relkind: StringBuilder,
}

impl PgCatalogClassBuilder {
    fn new() -> Self {
        let capacity = 64;

        Self {
            oid: UInt32Builder::with_capacity(capacity),
            relkind: StringBuilder::with_capacity(capacity, 0),
        }
    }

    fn _add_class(&mut self, class: &_PgClass) {
        self.oid.append_value(class.oid);
        self.relkind.append_value(class.relkind);
    }

    fn finish(&mut self) -> Vec<ArrayRef> {
        vec![Arc::new(self.oid.finish()), Arc::new(self.relkind.finish())]
    }
}

pub struct PgClassTable {
    data: Vec<ArrayRef>,
}

impl PgClassTable {
    pub fn new() -> Self {
        let mut builder = PgCatalogClassBuilder::new();

        Self {
            data: builder.finish(),
        }
    }
}

#[async_trait]
impl TableProvider for PgClassTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("relkind", DataType::Utf8, false),
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
