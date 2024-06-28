use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use common_utils::PgType;
use datafusion::arrow::array::{ArrayRef, Int64Builder, StringBuilder, UInt32Builder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result as DFResult;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;

struct PgTypeBuilder {
    oid: UInt32Builder,
    typname: StringBuilder,
    typnamespace: UInt32Builder,
    typcategory: StringBuilder,
    typrelid: UInt32Builder,
    typelem: UInt32Builder,
    typbasetype: UInt32Builder,
    typtypmod: Int64Builder,
}

impl PgTypeBuilder {
    fn new() -> Self {
        let capacity = 64;

        Self {
            oid: UInt32Builder::with_capacity(capacity),
            typname: StringBuilder::with_capacity(capacity, 0),
            typnamespace: UInt32Builder::with_capacity(capacity),
            typcategory: StringBuilder::with_capacity(capacity, 0),
            typrelid: UInt32Builder::with_capacity(capacity),
            typelem: UInt32Builder::with_capacity(capacity),
            typbasetype: UInt32Builder::with_capacity(capacity),
            typtypmod: Int64Builder::with_capacity(capacity),
        }
    }

    fn add_row(&mut self, typ: &PgType) {
        self.oid.append_value(typ.oid);
        self.typname.append_value(typ.typname);
        self.typnamespace.append_value(typ.typnamespace);
        self.typcategory.append_value(typ.typcategory);
        self.typrelid.append_value(typ.typrelid);
        self.typelem.append_value(typ.typelem);
        self.typbasetype.append_value(typ.typbasetype);
        self.typtypmod.append_value(-1);
    }

    fn finish(&mut self) -> Vec<ArrayRef> {
        vec![
            Arc::new(self.oid.finish()),
            Arc::new(self.typname.finish()),
            Arc::new(self.typnamespace.finish()),
            Arc::new(self.typcategory.finish()),
            Arc::new(self.typrelid.finish()),
            Arc::new(self.typelem.finish()),
            Arc::new(self.typbasetype.finish()),
            Arc::new(self.typtypmod.finish()),
        ]
    }
}

pub struct PgTypeTable {
    data: Vec<ArrayRef>,
}

impl PgTypeTable {
    pub fn new() -> Self {
        let mut builder = PgTypeBuilder::new();

        for typ in PgType::get_all() {
            builder.add_row(typ);
        }

        Self {
            data: builder.finish(),
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
            Field::new("typcategory", DataType::Utf8, false),
            Field::new("typrelid", DataType::UInt32, false),
            Field::new("typelem", DataType::UInt32, false),
            Field::new("typbasetype", DataType::UInt32, false),
            Field::new("typtypmod", DataType::Int64, false),
        ]))
    }

    async fn scan(
        &self, _state: &SessionState, projection: Option<&Vec<usize>>, _filters: &[Expr],
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
