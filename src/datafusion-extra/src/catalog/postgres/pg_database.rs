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

struct PgDatabase<'a> {
    oid: u32,
    datname: &'a str,
}

struct PgDatabaseBuilder {
    oid: UInt32Builder,
    datname: StringBuilder,
    datlastsysoid: UInt32Builder,
}

impl PgDatabaseBuilder {
    fn new() -> Self {
        let capacity = 1;

        Self {
            oid: UInt32Builder::with_capacity(capacity),
            datname: StringBuilder::with_capacity(capacity, 0),
            datlastsysoid: UInt32Builder::with_capacity(capacity),
        }
    }

    fn add_row(&mut self, database: &PgDatabase) {
        self.oid.append_value(database.oid);
        self.datname.append_value(database.datname);
        self.datlastsysoid.append_value(13756);
    }

    fn finish(mut self) -> Vec<ArrayRef> {
        vec![
            Arc::new(self.oid.finish()),
            Arc::new(self.datname.finish()),
            Arc::new(self.datlastsysoid.finish()),
        ]
    }
}

pub struct PgDatabaseTable {
    data: Vec<ArrayRef>,
}

impl PgDatabaseTable {
    pub fn new(datname: &str) -> Self {
        let mut builder = PgDatabaseBuilder::new();
        builder.add_row(&PgDatabase {
            oid: 13757,
            datname,
        });

        Self {
            data: builder.finish(),
        }
    }
}

#[async_trait]
impl TableProvider for PgDatabaseTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("datname", DataType::Utf8, false),
            Field::new("datlastsysoid", DataType::UInt32, false),
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
