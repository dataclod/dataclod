use datafusion::common::{Result as DFResult, not_impl_datafusion_err, not_impl_err};
use datafusion::sql::parser::{DFParser, Statement};

pub fn sql_to_statement(sql: &str) -> DFResult<Statement> {
    let mut statements = DFParser::parse_sql(sql)?;
    if statements.len() > 1 {
        return not_impl_err!("The context currently only supports a single SQL statement");
    }

    let statement = statements
        .pop_front()
        .ok_or_else(|| not_impl_datafusion_err!("The context requires a statement!"))?;
    Ok(statement)
}
