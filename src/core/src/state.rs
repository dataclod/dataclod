use std::sync::Arc;

use crate::rewrite::StatementRewrite;

pub struct QueryState {
    stmt_rewrites: Vec<Arc<dyn StatementRewrite + Send + Sync>>,
    set_prefixes: Vec<&'static str>,
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
            set_prefixes: vec!["datafusion.", "dataclod."],
        }
    }

    pub fn add_stmt_rewrite(&mut self, rewrite: Arc<dyn StatementRewrite + Send + Sync>) {
        self.stmt_rewrites.push(rewrite);
    }

    pub fn stmt_rewrites(&self) -> &[Arc<dyn StatementRewrite + Send + Sync>] {
        &self.stmt_rewrites
    }

    pub fn allow_set_prefix(&mut self, prefix: &'static str) {
        if !self.set_prefixes.contains(&prefix) {
            self.set_prefixes.push(prefix);
        }
    }

    pub fn allows_set_variable(&self, variable: &str) -> bool {
        self.set_prefixes
            .iter()
            .any(|prefix| variable.starts_with(prefix))
    }
}
