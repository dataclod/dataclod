use datafusion::common::{not_impl_err, DataFusionError, Result as DFResult};
use datafusion::sql::parser::{DFParser, Statement};

pub fn sql_to_statement(sql: &str) -> DFResult<Statement> {
    let mut statements = DFParser::parse_sql(sql)?;
    if statements.len() > 1 {
        return not_impl_err!("The context currently only supports a single SQL statement");
    }
    let statement = statements.pop_front().ok_or_else(|| {
        DataFusionError::NotImplemented("The context requires a statement!".to_string())
    })?;
    Ok(statement)
}
