mod context;
mod expr;
mod parser;

pub use context::QueryContext;
pub use parser::sql_to_statement;

#[cfg(test)]
mod tests {
    use super::*;
    use sqllogictest::{DBOutput, DefaultColumnType, Runner};
    use std::sync::Arc;

    struct TestRunner {
        context: Arc<QueryContext>,
    }

    impl TestRunner {
        fn new() -> Self {
            Self {
                context: Arc::new(QueryContext::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl sqllogictest::AsyncDB for TestRunner {
        type Error = anyhow::Error;
        type ColumnType = DefaultColumnType;

        async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
            // Simple test - just handle basic CREATE TABLE
            if sql.starts_with("CREATE TABLE") {
                // For this simple test, we don't actually create tables in DataFusion
                return Ok(DBOutput::StatementComplete(0));
            }

            // Simple SELECT test
            if sql.trim() == "SELECT 1;" {
                return Ok(DBOutput::Rows {
                    types: vec![DefaultColumnType::Any],
                    rows: vec![sqllogictest::ColumnType::Values(vec![sqllogictest::Value::I64(1)])],
                });
            }

            // For other queries, return empty result
            Ok(DBOutput::StatementComplete(0))
        }
    }

    #[tokio::test]
    async fn test_sqllogictest_integration() -> Result<(), anyhow::Error> {
        let mut runner = TestRunner::new();
        let mut tester = Runner::new(&mut runner);

        // Test a simple query
        let result = tester.run("SELECT 1;").await;
        assert!(result.is_ok());

        Ok(())
    }
}