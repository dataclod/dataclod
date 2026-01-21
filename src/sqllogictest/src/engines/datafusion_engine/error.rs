use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use datafusion::sql::sqlparser::parser::ParserError;
use sqllogictest::TestError;
use thiserror::Error;

pub type Result<T, E = DCSqlLogicTestError> = std::result::Result<T, E>;

/// `DataClod` sql-logicaltest error
#[derive(Debug, Error)]
pub enum DCSqlLogicTestError {
    /// Error from sqllogictest-rs
    #[error("SqlLogicTest error(from sqllogictest-rs crate): {0}")]
    SqlLogicTest(#[from] Box<TestError>),
    /// Error from datafusion
    #[error("DataFusion error: {}", .0.strip_backtrace())]
    DataFusion(#[from] DataFusionError),
    /// Error returned when SQL is syntactically incorrect.
    #[error("SQL Parser error: {0}")]
    Sql(#[from] ParserError),
    /// Error from arrow-rs
    #[error("Arrow error: {0}")]
    Arrow(#[from] ArrowError),
    /// Generic error
    #[error("Other Error: {0}")]
    Other(String),
}

impl From<String> for DCSqlLogicTestError {
    fn from(value: String) -> Self {
        DCSqlLogicTestError::Other(value)
    }
}
