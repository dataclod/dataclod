# DataClod sqllogictest

This crate is a submodule of DataClod that contains an implementation of [sqllogictest].

[sqllogictest]: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki

## Overview

This crate uses [sqllogictest-rs] to parse and run `.slt` files in the [`test_files`] directory.

[sqllogictest-rs]: https://github.com/risinglightdb/sqllogictest-rs
[`test_files`]: test_files

## Running tests: TLDR Examples

```shell
# Run all tests
cargo test -p dataclod-sqllogictest --test sqllogictests
```

```shell
# Run all tests, with debug logging enabled
RUST_LOG=debug cargo test -p dataclod-sqllogictest --test sqllogictests
```

```shell
# Run only the tests in `spatial_udf.slt`
cargo test -p dataclod-sqllogictest --test sqllogictests -- spatial_udf
```

```shell
# Automatically update spatial_udf.slt with expected output
cargo test -p dataclod-sqllogictest --test sqllogictests -- spatial_udf --complete
```

## Cookbook: Adding Tests

1. Add queries

Add the setup and queries you want to run to a `.slt` file
(`my_awesome_test.slt` in this example) using the following format:

```text
query
CREATE TABLE foo AS VALUES (1);

query
SELECT * from foo;
```

2. Fill in expected answers with `--complete` mode

Running the following command will update `my_awesome_test.slt` with the expected output:

```shell
cargo test -p dataclod-sqllogictest --test sqllogictests -- my_awesome_test --complete
```

3. Verify the content

In the case above, `my_awesome_test.slt` will look something like

```text
statement ok
CREATE TABLE foo AS VALUES (1);

query I
SELECT * from foo;
----
1
```

Assuming it looks good, check it in!

## Cookbook: Testing for whitespace

The `sqllogictest` runner will automatically strip trailing whitespace, meaning
it requires an additional effort to verify that trailing whitespace is correctly produced.

For example, the following test can't distinguish between `Andrew` and `Andrew `
(with trailing space):

```text
query T
select substr('Andrew Lamb', 1, 7)
----
Andrew
```

To test trailing whitespace, project additional non-whitespace column on the
right. For example, by selecting `'|'` after the column of interest, the test
can distinguish between `Andrew` and `Andrew `:

```text
# Note two spaces between `Andrew` and `|`
query TT
select substr('Andrew Lamb', 1, 7), '|'
----
Andrew  |

# Note only one space between `Andrew` and `|`
query TT
select substr('Andrew Lamb', 1, 6), '|'
----
Andrew |
```

## Cookbook: Ignoring volatile output

Sometimes parts of a result change every run (timestamps, counters, etc.). To keep the rest of the snapshot checked in, replace those fragments with the `<slt:ignore>` marker inside the expected block. During validation the marker acts like a wildcard, so only the surrounding text must match.

```text
query TT
EXPLAIN ANALYZE SELECT * FROM generate_series(100);
----
Plan with Metrics LazyMemoryExec: partitions=1, batch_generators=[generate_series: start=0, end=100, batch_size=8192], metrics=[output_rows=101, elapsed_compute=<slt:ignore>, output_bytes=<slt:ignore>]
```

# Reference

## Running tests: Validation Mode

In this mode, `sqllogictests` runs the statements and queries in a `.slt` file, comparing the expected output in the file to the output produced by that run.

For example, to run all tests suites in validation mode

```shell
cargo test -p dataclod-sqllogictest --test sqllogictests
```

sqllogictests also supports `cargo test` style substring matches on file names to restrict which tests to run

```shell
# spatial_udf.slt matches due to substring matching `spatial`
cargo test -p dataclod-sqllogictest --test sqllogictests -- spatial
```

Additionally, executing specific tests within a file is also supported. Tests are identified by line number within the .slt file; for example, the following command will run the test in line `50` for file `spatial_udf.slt` along with any other preparatory statements:

```shell
cargo test -p dataclod-sqllogictest --test sqllogictests -- spatial_udf:50
```

## Updating tests: Completion Mode

In test script completion mode, `sqllogictests` reads a prototype script and runs the statements and queries against the database engine. The output is a full script that is a copy of the prototype script with result inserted.

You can update the tests / generate expected output by passing the `--complete` argument.

```shell
# Update spatial_udf.slt with output from running
cargo test -p dataclod-sqllogictest --test sqllogictests -- spatial_udf --complete
```

## Running tests: `scratchdir`

The DataClod sqllogictest runner automatically creates a directory
named `test_files/scratch/<filename>`, creating it if needed and
clearing any file contents if it exists.

For example, the `test_files/copy.slt` file should use scratch
directory `test_files/scratch/copy`.

Tests that need to write temporary files should write (only) to this
directory to ensure they do not interfere with others concurrently
running tests.

## `.slt` file format

[`sqllogictest`] was originally written for SQLite to verify the
correctness of SQL queries against the SQLite engine. The format is designed
engine-agnostic and can parse sqllogictest files (`.slt`), runs
queries against an SQL engine and compares the output to the expected
output.

[`sqllogictest`]: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki

Tests in the `.slt` file are a sequence of query records generally
starting with `CREATE` statements to populate tables and then further
queries to test the populated data.

Each `.slt` file runs in its own, isolated `QueryContext`, to make the test setup explicit and so they can run in
parallel. Thus it important to keep the tests from having externally visible side effects (like writing to a global
location such as `/tmp/`)

Query records follow the format:

```sql
# <test_name>
query <type_string> <sort_mode>
<sql_query>
----
<expected_result>
```

- `test_name`: Uniquely identify the test name
- `type_string`: A short string that specifies the number of result columns and the expected datatype of each result
  column. There is one character in the `<type_string>` for each result column. The characters codes are:
  - 'B' - **B**oolean,
  - 'D' - **D**atetime,
  - 'I' - **I**nteger,
  - 'P' - timestam**P**,
  - 'R' - floating-point results,
  - 'T' - **T**ext,
  - "?" - any other types
- `expected_result`: In the results section, some values are converted according to some rules:
  - floating point values are rounded to the scale of "12",
  - NULL values are rendered as `NULL`,
  - empty strings are rendered as `(empty)`,
  - boolean values are rendered as `true`/`false`,
  - this list can be not exhaustive, check the `src/engines/conversion.rs` for details.
- `sort_mode`: If included, it must be one of `nosort` (**default**), `rowsort`, or `valuesort`. In `nosort` mode, the
  results appear in exactly the order in which they were received from the database engine. The `nosort` mode should
  only be used on queries that have an `ORDER BY` clause or which only have a single row of result, since otherwise the
  order of results is undefined and might vary from one database engine to another. The `rowsort` mode gathers all
  output from the database engine then sorts it by rows on the client side. Sort comparisons
  use [sort_unstable](https://doc.rust-lang.org/std/primitive.slice.html#method.sort_unstable) on the rendered text
  representation of the values. Hence, "9" sorts after "10", not before. The `valuesort` mode works like `rowsort`
  except that it does not honor row groupings. Each individual result value is sorted on its own.

> :warning: It is encouraged to either apply `order by`, or use `rowsort` for queries without explicit `order by`
> clauses.

### Example

```sql
# group_by_distinct
query TTI
SELECT a, b, COUNT(DISTINCT c) FROM my_table GROUP BY a, b ORDER BY a, b
----
foo bar 10
foo baz 5
foo     4
        3
```
