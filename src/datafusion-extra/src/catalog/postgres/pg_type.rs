use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, StringBuilder, UInt32Builder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result as DFResult;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;

use super::utils::PgType;

struct PgTypeBuilder {
    oid: UInt32Builder,
    typname: StringBuilder,
    typnamespace: UInt32Builder,
}

impl PgTypeBuilder {
    fn new() -> Self {
        let capacity = 100;

        Self {
            oid: UInt32Builder::with_capacity(capacity),
            typname: StringBuilder::with_capacity(capacity, capacity),
            typnamespace: UInt32Builder::with_capacity(capacity),
        }
    }

    fn add_row(&mut self, typ: &PgType) {
        self.oid.append_value(typ.oid);
        self.typname.append_value(typ.typname);
        self.typnamespace.append_value(typ.typnamespace);
    }

    fn finish(mut self) -> Vec<ArrayRef> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.oid.finish()),
            Arc::new(self.typname.finish()),
            Arc::new(self.typnamespace.finish()),
        ];

        columns
    }
}

pub struct PgTypeTable {
    data: Arc<Vec<ArrayRef>>,
}

impl PgTypeTable {
    pub fn new() -> Self {
        let mut builder = PgTypeBuilder::new();

        for typ in PgType::get_all() {
            builder.add_row(typ);
        }

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

impl Default for PgTypeTable {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TableProvider for PgTypeTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("typname", DataType::Utf8, false),
            Field::new("typnamespace", DataType::UInt32, false),
        ]))
    }

    async fn scan(
        &self, _state: &SessionState, projection: Option<&Vec<usize>>, _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let batch = RecordBatch::try_new(self.schema(), self.data.to_vec())?;

        Ok(Arc::new(MemoryExec::try_new(
            &[vec![batch]],
            self.schema(),
            projection.cloned(),
        )?))
    }
}
