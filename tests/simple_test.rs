use dataclod::QueryContext;
use std::sync::Arc;

/// Simple test to verify sqllogictest integration works
#[tokio::test]
async fn test_basic_sqllogictest_integration() -> Result<(), anyhow::Error> {
    let context = Arc::new(QueryContext::new());

    // Test basic SQL execution
    let df = context.sql("SELECT 1 as id, 'test' as name").await?;
    let batches = df.collect().await?;

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);

    // Check the result
    let id = batches[0].column(0).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    let name = batches[0].column(1).as_any().downcast_ref::<arrow::array::Utf8Array>().unwrap();

    assert_eq!(id.value(0), 1);
    assert_eq!(name.value(0), "test");

    println!("✅ Basic SQL execution test passed");
    Ok(())
}

/// Test table creation and insertion
#[tokio::test]
async fn test_table_operations() -> Result<(), anyhow::Error> {
    let context = Arc::new(QueryContext::new());

    // Create table
    let _ = context.sql("CREATE TABLE test_users (id INTEGER, name TEXT, age INTEGER)").await?;

    // Insert data
    let _ = context.sql("INSERT INTO test_users VALUES (1, 'Alice', 25), (2, 'Bob', 30)").await?;

    // Query data
    let df = context.sql("SELECT * FROM test_users ORDER BY id").await?;
    let batches = df.collect().await?;

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 2);

    println!("✅ Table operations test passed");
    Ok(())
}

/// Test aggregation functions
#[tokio::test]
async fn test_aggregation() -> Result<(), anyhow::Error> {
    let context = Arc::new(QueryContext::new());

    // Create and populate table
    let _ = context.sql("CREATE TABLE test_scores (student_id INTEGER, score INTEGER)").await?;
    let _ = context.sql("INSERT INTO test_scores VALUES (1, 85), (1, 90), (2, 95), (2, 88)").await?;

    // Test aggregation
    let df = context.sql("SELECT student_id, AVG(score) as avg_score FROM test_scores GROUP BY student_id ORDER BY student_id").await?;
    let batches = df.collect().await?;

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 2);

    println!("✅ Aggregation test passed");
    Ok(())
}