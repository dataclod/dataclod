use std::sync::Arc;

use crate::rewrite::StatementRewrite;

pub struct QueryState {
    stmt_rewrites: Vec<Arc<dyn StatementRewrite + Send + Sync>>,
}

impl Default for QueryState {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryState {
    pub fn new() -> Self {
        Self {
            stmt_rewrites: Vec::new(),
        }
    }

    pub fn add_stmt_rewrite(&mut self, rewrite: Arc<dyn StatementRewrite + Send + Sync>) {
        self.stmt_rewrites.push(rewrite);
    }

    pub fn stmt_rewrites(&self) -> &[Arc<dyn StatementRewrite + Send + Sync>] {
        &self.stmt_rewrites
    }
}
