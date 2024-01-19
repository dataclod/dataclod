mod pg_namespace;
mod pg_type;
mod utils;

use std::sync::Arc;

use anyhow::Result;
use datafusion::catalog::schema::{MemorySchemaProvider, SchemaProvider};
use datafusion::execution::context::SessionContext;
use pg_namespace::PgNamespaceTable;
use pg_type::PgTypeTable;

pub fn with_pg_catalog(ctx: &SessionContext) -> Result<()> {
    let pg_catalog = MemorySchemaProvider::new();
    pg_catalog.register_table("pg_type".to_string(), Arc::new(PgTypeTable::new()))?;

    ctx.register_table("public.pg_namespace", Arc::new(PgNamespaceTable::new()))?;

    let default_catalog = ctx
        .catalog(&ctx.state().config_options().catalog.default_catalog)
        .expect("Failed to get default catalog");
    default_catalog.register_schema("pg_catalog", Arc::new(pg_catalog))?;
    Ok(())
}
