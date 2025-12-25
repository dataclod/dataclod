mod pg_class;
mod pg_database;
mod pg_description;
mod pg_get_expr_udf;
mod pg_get_partkeydef_udf;
mod pg_namespace;
mod pg_type;

use std::sync::Arc;

use anyhow::Result;
use datafusion::catalog::{MemorySchemaProvider, SchemaProvider};
use datafusion::execution::context::SessionContext;
use pg_class::PgClassTable;
use pg_database::PgDatabaseTable;
use pg_description::PgDescriptionTable;
use pg_namespace::PgNamespaceTable;
use pg_type::PgTypeTable;

const CURRENT_DATABASE: &str = "postgres";

pub fn with_pg_catalog(ctx: &SessionContext) -> Result<()> {
    let pg_catalog = MemorySchemaProvider::new();
    pg_catalog.register_table("pg_type".to_owned(), Arc::new(PgTypeTable::new()))?;
    pg_catalog.register_table("pg_namespace".to_owned(), Arc::new(PgNamespaceTable::new()))?;
    pg_catalog.register_table("pg_class".to_owned(), Arc::new(PgClassTable::new()))?;
    pg_catalog.register_table(
        "pg_description".to_owned(),
        Arc::new(PgDescriptionTable::new()),
    )?;

    ctx.register_table("public.pg_type", Arc::new(PgTypeTable::new()))?;
    ctx.register_table("public.pg_namespace", Arc::new(PgNamespaceTable::new()))?;
    ctx.register_table("public.pg_class", Arc::new(PgClassTable::new()))?;
    ctx.register_table("public.pg_description", Arc::new(PgDescriptionTable::new()))?;
    ctx.register_table(
        "public.pg_database",
        Arc::new(PgDatabaseTable::new(CURRENT_DATABASE)),
    )?;

    ctx.register_udf(pg_get_expr_udf::create_pg_get_expr_udf());
    ctx.register_udf(pg_get_partkeydef_udf::create_pg_get_partkeydef_udf());

    let default_catalog = ctx
        .catalog(&ctx.state().config_options().catalog.default_catalog)
        .expect("Failed to get default catalog");
    default_catalog.register_schema("pg_catalog", Arc::new(pg_catalog))?;
    Ok(())
}
