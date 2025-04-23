use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, StringBuilder, UInt32Builder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::Result as DFResult;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;

struct PgNamespace {
    oid: u32,
    nspname: &'static str,
}

struct PgCatalogNamespaceBuilder {
    oid: UInt32Builder,
    nspname: StringBuilder,
}

impl PgCatalogNamespaceBuilder {
    fn new() -> Self {
        let capacity = 64;

        Self {
            oid: UInt32Builder::with_capacity(capacity),
            nspname: StringBuilder::with_capacity(capacity, 0),
        }
    }

    fn add_row(&mut self, ns: &PgNamespace) {
        self.oid.append_value(ns.oid);
        self.nspname.append_value(ns.nspname);
    }

    fn finish(&mut self) -> Vec<ArrayRef> {
        vec![Arc::new(self.oid.finish()), Arc::new(self.nspname.finish())]
    }
}

#[derive(Debug)]
pub struct PgNamespaceTable {
    data: Vec<ArrayRef>,
}

impl PgNamespaceTable {
    pub fn new() -> Self {
        let mut builder = PgCatalogNamespaceBuilder::new();
        builder.add_row(&PgNamespace {
            oid: 11,
            nspname: "pg_catalog",
        });
        builder.add_row(&PgNamespace {
            oid: 2200,
            nspname: "public",
        });
        builder.add_row(&PgNamespace {
            oid: 13676,
            nspname: "information_schema",
        });

        Self {
            data: builder.finish(),
        }
    }
}

#[async_trait]
impl TableProvider for PgNamespaceTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("nspname", DataType::Utf8, false),
        ]))
    }

    async fn scan(
        &self, _state: &dyn Session, projection: Option<&Vec<usize>>, _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let batch = RecordBatch::try_new(self.schema(), self.data.clone())?;

        Ok(MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            self.schema(),
            projection.cloned(),
        )?)
    }
}
