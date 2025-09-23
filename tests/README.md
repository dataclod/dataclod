# SQL Logic Tests for DataClod

这个目录包含了使用 sqllogictest 包对 DataClod 项目进行的完整测试套件。

## 测试结构

我们提供了两种测试方式，确保结果的一致性：

### 1. 直接使用 QueryContext 测试 (`QueryContextRunner`)

直接通过 `QueryContext` 执行 SQL 查询，测试核心查询处理逻辑。

### 2. 通过 pgwire 服务器连接测试 (`PostgresRunner`)

启动 pgwire 服务器并通过 PostgreSQL 协议进行连接测试，验证网络协议层的正确性。

## 测试文件

- `tests/data/basic_test.slt` - 基础 SQL 功能测试
- `tests/data/advanced_test.slt` - 高级 SQL 功能测试
- `tests/integration_test.rs` - 集成测试主文件

## 运行测试

### 运行所有集成测试

```bash
cargo test integration_test -- --nocapture
```

### 运行特定测试

```bash
# 仅测试 QueryContext 直接方式
cargo test test_query_context_direct -- --nocapture

# 仅测试 PostgreSQL 服务器连接方式
cargo test test_postgres_server_connection -- --nocapture

# 测试两种方式的一致性
cargo test test_both_methods_consistency -- --nocapture
```

## 测试覆盖的功能

### 基础测试 (`basic_test.slt`)
- 基本 SELECT 语句
- WHERE 子句
- ORDER BY 子句
- 聚合函数 (AVG, SUM, COUNT)
- GROUP BY 子句
- JOIN 操作
- 子查询
- LIMIT 子句
- 数学函数
- 字符串函数

### 高级测试 (`advanced_test.slt`)
- 复杂的多表 JOIN
- 窗口函数（如果支持）
- 公共表表达式 (CTE)
- 子查询在 SELECT 中的使用
- EXISTS 子查询
- CASE 表达式

## 测试数据格式

测试文件使用 sqllogictest 格式：

```sql
# 注释
statement ok
CREATE TABLE test_table (id INTEGER, name TEXT);

query I
SELECT id FROM test_table;
----
1
2
3
```

- `statement ok` - 执行语句，不检查结果
- `query <types>` - 执行查询并验证结果
  - `I` = INTEGER
  - `T` = TEXT
  - `R` = REAL
- `----` - 分隔符
- 期望的结果

## 确保测试一致性

`test_both_methods_consistency` 测试确保两种测试方式产生相同的结果：

1. 收集两种方式的测试结果
2. 比较每个测试文件的输出
3. 报告任何不一致的地方

## 添加新测试

1. 在 `tests/data/` 目录下创建新的 `.slt` 文件
2. 在 `integration_test.rs` 中的测试文件列表中添加新文件
3. 运行测试验证新功能

## 依赖要求

测试需要以下系统依赖：

```bash
sudo apt-get install -y libssl-dev pkg-config libgeos-dev
```

## 故障排除

如果测试失败：

1. 检查 DataFusion 和 PostgreSQL 协议的兼容性
2. 验证数据类型转换是否正确
3. 确保服务器正确启动和关闭
4. 检查连接参数和超时设置