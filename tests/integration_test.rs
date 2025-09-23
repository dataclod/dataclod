use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use dataclod::QueryContext;
use postgres::{Client, NoTls};
use sqllogictest::{DBOutput, DefaultColumnType, Runner};
use tokio::sync::Mutex;
use tokio::time::sleep;

/// Test runner that uses QueryContext directly
struct QueryContextRunner {
    context: Arc<QueryContext>,
}

impl QueryContextRunner {
    fn new(context: Arc<QueryContext>) -> Self {
        Self { context }
    }
}

#[async_trait::async_trait]
impl sqllogictest::AsyncDB for QueryContextRunner {
    type Error = anyhow::Error;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        // Handle special statements
        if sql.trim().to_lowercase() == "begin" {
            return Ok(DBOutput::StatementComplete(0));
        }
        if sql.trim().to_lowercase() == "commit" {
            return Ok(DBOutput::StatementComplete(0));
        }
        if sql.trim().to_lowercase() == "rollback" || sql.trim().to_lowercase() == "abort" {
            return Ok(DBOutput::StatementComplete(0));
        }

        let df = self.context.sql(sql).await?;
        let batches = df.collect().await?;

        if batches.is_empty() {
            return Ok(DBOutput::StatementComplete(0));
        }

        let batch = &batches[0];
        let rows = batch
            .rows()
            .map(|row| {
                sqllogictest::ColumnType::Values(
                    row.columns()
                        .iter()
                        .map(|col| {
                            // Convert arrow data types to sqllogictest types
                            match col.data_type() {
                                arrow::datatypes::DataType::Int64 => {
                                    let val = row.get::<usize, Option<i64>>(col.index()).unwrap_or(0);
                                    sqllogictest::Value::I64(val)
                                }
                                arrow::datatypes::DataType::Float64 => {
                                    let val = row.get::<usize, Option<f64>>(col.index()).unwrap_or(0.0);
                                    sqllogictest::Value::F64(val)
                                }
                                arrow::datatypes::DataType::Utf8 => {
                                    let val = row.get::<usize, Option<&str>>(col.index()).unwrap_or("");
                                    sqllogictest::Value::Str(val.to_string())
                                }
                                arrow::datatypes::DataType::Decimal128(_, _) => {
                                    let val = row.get::<usize, Option<i128>>(col.index()).unwrap_or(0);
                                    sqllogictest::Value::Str(val.to_string())
                                }
                                _ => {
                                    let val = row.get::<usize, Option<String>>(col.index()).unwrap_or_else(|| "NULL".to_string());
                                    sqllogictest::Value::Str(val)
                                }
                            }
                        })
                        .collect(),
                )
            })
            .collect::<Vec<_>>();

        Ok(DBOutput::Rows {
            types: vec![DefaultColumnType::Any; batch.num_columns()],
            rows,
        })
    }
}

/// Test runner that connects via pgwire server
struct PostgresRunner {
    client: Arc<Mutex<Client>>,
}

impl PostgresRunner {
    async fn new() -> Result<Self, anyhow::Error> {
        // Start the server in the background
        let server_handle = tokio::spawn(async {
            server::postgres::server("127.0.0.1:9974".to_string()).await;
        });

        // Wait for server to start
        sleep(Duration::from_millis(100)).await;

        // Connect to the server
        let client = Client::connect("postgresql://127.0.0.1:9974", NoTls)?;
        let client = Arc::new(Mutex::new(client));

        Ok(Self { client })
    }
}

#[async_trait::async_trait]
impl sqllogictest::AsyncDB for PostgresRunner {
    type Error = anyhow::Error;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let mut client = self.client.lock().await;

        // Handle special statements
        if sql.trim().to_lowercase() == "begin" {
            let _ = client.execute("BEGIN", &[]);
            return Ok(DBOutput::StatementComplete(0));
        }
        if sql.trim().to_lowercase() == "commit" {
            let _ = client.execute("COMMIT", &[]);
            return Ok(DBOutput::StatementComplete(0));
        }
        if sql.trim().to_lowercase() == "rollback" || sql.trim().to_lowercase() == "abort" {
            let _ = client.execute("ROLLBACK", &[]);
            return Ok(DBOutput::StatementComplete(0));
        }

        let rows = client.query(sql, &[])?;

        if rows.is_empty() {
            return Ok(DBOutput::StatementComplete(0));
        }

        let types = vec![DefaultColumnType::Any; rows[0].columns().len()];
        let mut output_rows = Vec::new();

        for row in rows {
            let values = (0..row.columns().len())
                .map(|i| {
                    let col = &row.columns()[i];
                    match col.type_().name() {
                        "int8" | "int4" | "int2" => {
                            let val = row.get::<usize, Option<i64>>(i).unwrap_or(0);
                            sqllogictest::Value::I64(val)
                        }
                        "float8" | "float4" => {
                            let val = row.get::<usize, Option<f64>>(i).unwrap_or(0.0);
                            sqllogictest::Value::F64(val)
                        }
                        "numeric" => {
                            let val = row.get::<usize, Option<String>>(i).unwrap_or_else(|| "0".to_string());
                            sqllogictest::Value::Str(val)
                        }
                        "text" | "varchar" => {
                            let val = row.get::<usize, Option<String>>(i).unwrap_or_else(|| "".to_string());
                            sqllogictest::Value::Str(val)
                        }
                        _ => {
                            let val = row.get::<usize, Option<String>>(i).unwrap_or_else(|| "NULL".to_string());
                            sqllogictest::Value::Str(val)
                        }
                    }
                })
                .collect::<Vec<_>>();

            output_rows.push(sqllogictest::ColumnType::Values(values));
        }

        Ok(DBOutput::Rows {
            types,
            rows: output_rows,
        })
    }
}

async fn run_tests_with_runner<D: sqllogictest::AsyncDB>(
    runner: &mut D,
    test_files: &[&str],
) -> Result<(), anyhow::Error> {
    let mut tester = Runner::new(runner);

    for test_file in test_files {
        println!("Running tests from: {}", test_file);
        tester.run_file(test_file).await?;
    }

    Ok(())
}

#[tokio::test]
async fn test_query_context_direct() -> Result<(), anyhow::Error> {
    let context = Arc::new(QueryContext::new());
    let mut runner = QueryContextRunner::new(context);

    let test_files = [
        "tests/data/basic_test.slt",
        "tests/data/advanced_test.slt",
    ];

    run_tests_with_runner(&mut runner, &test_files).await
}

#[tokio::test]
async fn test_postgres_server_connection() -> Result<(), anyhow::Error> {
    let mut runner = PostgresRunner::new().await?;

    let test_files = [
        "tests/data/basic_test.slt",
        "tests/data/advanced_test.slt",
    ];

    run_tests_with_runner(&mut runner, &test_files).await
}

#[tokio::test]
async fn test_both_methods_consistency() -> Result<(), anyhow::Error> {
    println!("Testing QueryContext direct method...");
    let context = Arc::new(QueryContext::new());
    let mut direct_runner = QueryContextRunner::new(context);

    println!("Testing PostgreSQL server connection method...");
    let mut server_runner = PostgresRunner::new().await?;

    let test_files = [
        "tests/data/basic_test.slt",
        "tests/data/advanced_test.slt",
    ];

    let mut direct_results = HashMap::new();
    let mut server_results = HashMap::new();

    // Collect results from both methods
    for test_file in &test_files {
        let mut direct_tester = Runner::new(&mut direct_runner);
        let direct_result = direct_tester.run_file(test_file).await;
        direct_results.insert(test_file.to_string(), direct_result);

        let mut server_tester = Runner::new(&mut server_runner);
        let server_result = server_tester.run_file(test_file).await;
        server_results.insert(test_file.to_string(), server_result);
    }

    // Compare results
    for test_file in &test_files {
        let direct_result = &direct_results[test_file];
        let server_result = &server_results[test_file];

        match (direct_result, server_result) {
            (Ok(_), Ok(_)) => {
                println!("✅ {}: Both methods passed", test_file);
            }
            (Err(e), Ok(_)) => {
                println!("❌ {}: Direct method failed, server method passed", test_file);
                println!("   Direct error: {:?}", e);
                return Err(anyhow::anyhow!("Inconsistency in test results for {}", test_file));
            }
            (Ok(_), Err(e)) => {
                println!("❌ {}: Server method failed, direct method passed", test_file);
                println!("   Server error: {:?}", e);
                return Err(anyhow::anyhow!("Inconsistency in test results for {}", test_file));
            }
            (Err(de), Err(se)) => {
                println!("❌ {}: Both methods failed", test_file);
                println!("   Direct error: {:?}", de);
                println!("   Server error: {:?}", se);
                return Err(anyhow::anyhow!("Both methods failed for {}", test_file));
            }
        }
    }

    println!("✅ All tests passed consistently between both methods!");
    Ok(())
}