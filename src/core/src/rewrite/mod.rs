mod postgres_stmt;

use std::fmt::Debug;
use std::sync::Arc;

use anyhow::Result;
use datafusion::sql::parser::Statement;

use crate::QueryContext;

pub trait StatementRewrite: Debug {
    fn name(&self) -> &str;

    fn rewrite(&self, stmt: Statement) -> Result<Statement>;
}

pub fn register_postgres_stmt_rewrites(ctx: &mut QueryContext) {
    ctx.register_statement_rewrite(Arc::new(postgres_stmt::PostgresStmtRewrite));
    ctx.register_statement_rewrite(Arc::new(postgres_stmt::PostgresStmtVisitorRewrite));
}
