use dataclod::QueryContext;
use log::info;

/// Context for running tests
pub struct TestContext {
    /// Context for running queries
    ctx: QueryContext,
}

impl TestContext {
    pub fn new(ctx: QueryContext) -> Self {
        Self { ctx }
    }

    /// Create a `QueryContext`, configured for the specific sqllogictest
    /// test(.slt file) , if possible.
    ///
    /// If `None` is returned (e.g. because some needed feature is not
    /// enabled), the file should be skipped
    pub fn try_new_for_test_file() -> Option<Self> {
        info!("Using DataClod QueryContext with extensions");
        Some(TestContext::new(QueryContext::new()))
    }

    /// Returns a reference to the internal `QueryContext`
    pub fn query_ctx(&self) -> &QueryContext {
        &self.ctx
    }
}
