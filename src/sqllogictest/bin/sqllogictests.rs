use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

use clap::Parser;
use dataclod_sqllogictest::{
    CurrentlyExecutingSqlTracker, DataClod, Filter, TestContext, df_value_validator,
    read_dir_recursive, setup_scratch_dir, should_skip_file, should_skip_record, value_normalizer,
};
use datafusion::common::instant::Instant;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::utils::get_available_parallelism;
use datafusion::common::{DataFusionError, Result, exec_datafusion_err, exec_err};
use futures::FutureExt;
use futures::stream::StreamExt;
use indicatif::{HumanDuration, MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use log::info;
use sqllogictest::{
    AsyncDB, Condition, MakeConnection, Record, parse_file, strict_column_validator,
};

const TEST_DIRECTORY: &str = "test_files/";
const ERRS_PER_FILE_LIMIT: usize = 10;

pub fn main() -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run_tests())
}

async fn run_tests() -> Result<()> {
    // Enable logging (e.g. set RUST_LOG=debug to see debug logs)
    env_logger::init();

    let options: Options = Parser::parse();
    if options.list {
        // nextest parses stdout, so print messages to stderr
        eprintln!("NOTICE: --list option unsupported, quitting");
        // return Ok, not error so that tools like nextest which are listing all
        // workspace tests (by running `cargo test ... --list --format terse`)
        // do not fail when they encounter this binary. Instead, print nothing
        // to stdout and return OK so they can continue listing other tests.
        return Ok(());
    }

    options.warn_on_ignored();

    // Run all tests in parallel, reporting failures at the end
    //
    // Doing so is safe because each slt file runs with its own
    // `SessionContext` and should not have side effects (like
    // modifying shared state like `/tmp/`)
    let m = MultiProgress::with_draw_target(ProgressDrawTarget::stderr_with_hz(1));
    let m_style = ProgressStyle::with_template(
        "[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}",
    )
    .unwrap()
    .progress_chars("##-");

    let start = Instant::now();

    let test_files = read_test_files(&options)?;

    // Perform scratch file sanity check
    let scratch_errors = scratch_file_check(&test_files);
    if !scratch_errors.is_empty() {
        eprintln!("Scratch file sanity check failed:");
        for error in &scratch_errors {
            eprintln!("  {error}");
        }

        eprintln!(
            "\nTemporary file check failed. Please ensure that within each test file, any scratch file created is placed under a folder with the same name as the test file (without extension).\nExample: inside `join.slt`, temporary files must be created under `.../scratch/join/`\n"
        );

        return exec_err!("sqllogictests scratch file check failed");
    }

    let num_tests = test_files.len();
    let errors: Vec<_> = futures::stream::iter(test_files)
        .map(|test_file| {
            let m_clone = m.clone();
            let m_style_clone = m_style.clone();
            let filters = options.filters.clone();

            let relative_path = test_file.relative_path.clone();

            let currently_running_sql_tracker = CurrentlyExecutingSqlTracker::new();
            let currently_running_sql_tracker_clone = currently_running_sql_tracker.clone();
            SpawnedTask::spawn(async move {
                if options.complete {
                    run_complete_file(
                        test_file,
                        m_clone,
                        m_style_clone,
                        currently_running_sql_tracker_clone,
                    )
                    .await?
                } else {
                    run_test_file(
                        test_file,
                        m_clone,
                        m_style_clone,
                        filters.as_ref(),
                        currently_running_sql_tracker_clone,
                    )
                    .await?
                }
                Ok(())
            })
            .join()
            .map(move |result| (result, relative_path, currently_running_sql_tracker))
        })
        // run up to num_cpus streams in parallel
        .buffer_unordered(options.test_threads)
        .flat_map(|(result, test_file_path, current_sql)| {
            // Filter out any Ok() leaving only the DataFusionErrors
            futures::stream::iter(match result {
                // Tokio panic error
                Err(e) => {
                    let error = DataFusionError::External(Box::new(e));
                    let current_sql = current_sql.get_currently_running_sqls();

                    if current_sql.is_empty() {
                        Some(error.context(format!(
                            "failure in {} with no currently running sql tracked",
                            test_file_path.display()
                        )))
                    } else if current_sql.len() == 1 {
                        let sql = &current_sql[0];
                        Some(error.context(format!(
                            "failure in {} for sql {sql}",
                            test_file_path.display()
                        )))
                    } else {
                        let sqls = current_sql
                            .iter()
                            .enumerate()
                            .map(|(i, sql)| format!("\n[{}]: {}", i + 1, sql))
                            .collect::<String>();
                        Some(error.context(format!(
                            "failure in {} for multiple currently running sqls: {}",
                            test_file_path.display(),
                            sqls
                        )))
                    }
                }
                Ok(thread_result) => thread_result.err(),
            })
        })
        .collect()
        .await;

    m.println(format!(
        "Completed {} test files in {}",
        num_tests,
        HumanDuration(start.elapsed())
    ))?;

    // report on any errors
    if !errors.is_empty() {
        for e in &errors {
            println!("{e}");
        }
        exec_err!("{} failures", errors.len())
    } else {
        Ok(())
    }
}

async fn run_test_file(
    test_file: TestFile, mp: MultiProgress, mp_style: ProgressStyle, filters: &[Filter],
    currently_executing_sql_tracker: CurrentlyExecutingSqlTracker,
) -> Result<()> {
    let TestFile {
        path,
        relative_path,
    } = test_file;
    let Some(test_ctx) = TestContext::try_new_for_test_file() else {
        info!("Skipping: {}", path.display());
        return Ok(());
    };
    setup_scratch_dir(&relative_path)?;

    let count = get_record_count(&path, "DataClod".to_owned());
    let pb = mp.add(ProgressBar::new(count));

    pb.set_style(mp_style);
    pb.set_message(format!("{:?}", &relative_path));

    let mut runner = sqllogictest::Runner::new(|| {
        async {
            Ok(DataClod::new(
                test_ctx.query_ctx().clone(),
                relative_path.clone(),
                pb.clone(),
            )
            .with_currently_executing_sql_tracker(currently_executing_sql_tracker.clone()))
        }
    });
    runner.add_label("DataClod");
    runner.with_column_validator(strict_column_validator);
    runner.with_normalizer(value_normalizer);
    runner.with_validator(df_value_validator);
    let result = run_file_in_runner(path, runner, filters).await;
    pb.finish_and_clear();
    result
}

async fn run_file_in_runner<D: AsyncDB, M: MakeConnection<Conn = D>>(
    path: PathBuf, mut runner: sqllogictest::Runner<D, M>, filters: &[Filter],
) -> Result<()> {
    let path = path.canonicalize()?;
    let records = parse_file(&path).map_err(|e| DataFusionError::External(Box::new(e)))?;
    let mut errs = vec![];
    for record in records {
        if let Record::Halt { .. } = record {
            break;
        }
        if should_skip_record::<D>(&record, filters) {
            continue;
        }
        if let Err(err) = runner.run_async(record).await {
            errs.push(format!("{err}"));
        }
    }

    if !errs.is_empty() {
        let mut msg = format!("{} errors in file {}\n\n", errs.len(), path.display());
        for (i, err) in errs.iter().enumerate() {
            if i >= ERRS_PER_FILE_LIMIT {
                msg.push_str(&format!(
                    "... other {} errors in {} not shown ...\n\n",
                    errs.len() - ERRS_PER_FILE_LIMIT,
                    path.display()
                ));
                break;
            }
            msg.push_str(&format!("{}. {err}\n\n", i + 1));
        }
        return Err(DataFusionError::External(msg.into()));
    }

    Ok(())
}

#[expect(clippy::needless_pass_by_value)]
fn get_record_count(path: &PathBuf, label: String) -> u64 {
    let records: Vec<Record<<DataClod as AsyncDB>::ColumnType>> = parse_file(path).unwrap();
    let mut count: u64 = 0;

    records.iter().for_each(|rec| {
        match rec {
            Record::Query { conditions, .. } => {
                if conditions.is_empty()
                    || !conditions.contains(&Condition::SkipIf {
                        label: label.clone(),
                    })
                    || conditions.contains(&Condition::OnlyIf {
                        label: label.clone(),
                    })
                {
                    count += 1;
                }
            }
            Record::Statement { conditions, .. } => {
                if conditions.is_empty()
                    || !conditions.contains(&Condition::SkipIf {
                        label: label.clone(),
                    })
                    || conditions.contains(&Condition::OnlyIf {
                        label: label.clone(),
                    })
                {
                    count += 1;
                }
            }
            _ => {}
        }
    });

    count
}

async fn run_complete_file(
    test_file: TestFile, mp: MultiProgress, mp_style: ProgressStyle,
    currently_executing_sql_tracker: CurrentlyExecutingSqlTracker,
) -> Result<()> {
    let TestFile {
        path,
        relative_path,
    } = test_file;

    info!("Using complete mode to complete: {}", path.display());

    let Some(test_ctx) = TestContext::try_new_for_test_file() else {
        info!("Skipping: {}", path.display());
        return Ok(());
    };
    setup_scratch_dir(&relative_path)?;

    let count = get_record_count(&path, "DataClod".to_owned());
    let pb = mp.add(ProgressBar::new(count));

    pb.set_style(mp_style);
    pb.set_message(format!("{:?}", &relative_path));

    let mut runner = sqllogictest::Runner::new(|| {
        async {
            Ok(DataClod::new(
                test_ctx.query_ctx().clone(),
                relative_path.clone(),
                pb.clone(),
            )
            .with_currently_executing_sql_tracker(currently_executing_sql_tracker.clone()))
        }
    });

    let col_separator = " ";
    let res = runner
        .update_test_file(
            path,
            col_separator,
            df_value_validator,
            value_normalizer,
            strict_column_validator,
        )
        .await
        // Can't use e directly because it isn't marked Send, so turn it into a string.
        .map_err(|e| exec_datafusion_err!("Error completing {relative_path:?}: {e}"));

    pb.finish_and_clear();

    res
}

/// Represents a parsed test file
#[derive(Debug)]
struct TestFile {
    /// The absolute path to the file
    pub path: PathBuf,
    /// The relative path of the file (used for display)
    pub relative_path: PathBuf,
}

impl TestFile {
    fn new(path: PathBuf) -> Self {
        let p = path.to_string_lossy();
        let relative_path = PathBuf::from(if p.starts_with(TEST_DIRECTORY) {
            p.strip_prefix(TEST_DIRECTORY).unwrap()
        } else {
            ""
        });

        Self {
            path,
            relative_path,
        }
    }

    fn is_slt_file(&self) -> bool {
        self.path.extension() == Some(OsStr::new("slt"))
    }
}

fn read_test_files(options: &Options) -> Result<Vec<TestFile>> {
    let paths: Vec<TestFile> = read_dir_recursive(TEST_DIRECTORY)?
        .into_iter()
        .map(TestFile::new)
        .filter(|f| options.check_test_file(&f.path))
        .filter(|f| f.is_slt_file())
        .collect();

    Ok(paths)
}

/// Parsed command line options
///
/// This structure attempts to mimic the command line options of the built-in
/// rust test runner accepted by IDEs such as `CLion` that pass arguments
///
/// See <https://github.com/apache/datafusion/issues/8287> for more details
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about= None)]
struct Options {
    #[clap(long, help = "Auto complete mode to fill out expected results")]
    complete: bool,

    #[clap(
        action,
        help = "test filter (substring match on filenames with optional :{line_number} suffix)"
    )]
    filters: Vec<Filter>,

    #[clap(
        long,
        help = "IGNORED (for compatibility with built in rust test runner)"
    )]
    format: Option<String>,

    #[clap(
        short = 'Z',
        long,
        help = "IGNORED (for compatibility with built in rust test runner)"
    )]
    z_options: Option<String>,

    #[clap(
        long,
        help = "IGNORED (for compatibility with built in rust test runner)"
    )]
    show_output: bool,

    #[clap(
        long,
        help = "Quits immediately, not listing anything (for compatibility with built-in rust test runner)"
    )]
    list: bool,

    #[clap(
        long,
        help = "IGNORED (for compatibility with built-in rust test runner)"
    )]
    ignored: bool,

    #[clap(
        long,
        help = "IGNORED (for compatibility with built-in rust test runner)"
    )]
    nocapture: bool,

    #[clap(
        long,
        help = "Number of threads used for running tests in parallel",
        default_value_t = get_available_parallelism()
    )]
    test_threads: usize,
}

impl Options {
    /// Because this test can be run as a cargo test, commands like
    ///
    /// ```shell
    /// cargo test foo
    /// ```
    ///
    /// Will end up passing `foo` as a command line argument.
    ///
    /// To be compatible with this, treat the command line arguments as a
    /// filter and that does a substring match on each input.  returns
    /// true f this path should be run
    fn check_test_file(&self, path: &Path) -> bool {
        !should_skip_file(path, &self.filters)
    }

    /// Logs warning messages to stdout if any ignored options are passed
    fn warn_on_ignored(&self) {
        if self.format.is_some() {
            eprintln!("WARNING: Ignoring `--format` compatibility option");
        }

        if self.z_options.is_some() {
            eprintln!("WARNING: Ignoring `-Z` compatibility option");
        }

        if self.show_output {
            eprintln!("WARNING: Ignoring `--show-output` compatibility option");
        }
    }
}

/// Performs scratch file check for all test files.
///
/// Scratch file rule: In each .slt test file, the temporary file created must
/// be under a folder that is has the same name as the test file.
/// e.g. In `join.slt`, temporary files must be created under
/// `.../scratch/join/`
///
/// See: <https://github.com/apache/datafusion/tree/main/datafusion/sqllogictest#running-tests-scratchdir>
///
/// This function searches for `scratch/[target]/...` patterns and verifies
/// that the target matches the file name.
///
/// Returns a vector of error strings for incorrectly created scratch files.
fn scratch_file_check(test_files: &[TestFile]) -> Vec<String> {
    let mut errors = Vec::new();

    // Search for any scratch/[target]/... patterns and check if they match the file
    // name
    let scratch_pattern = regex::Regex::new(r"scratch/([^/]+)/").unwrap();

    for test_file in test_files {
        // Get the file content
        let content = match fs::read_to_string(&test_file.path) {
            Ok(content) => content,
            Err(e) => {
                errors.push(format!(
                    "Failed to read file {}: {}",
                    test_file.path.display(),
                    e
                ));
                continue;
            }
        };

        // Get the expected target name (file name without extension)
        let expected_target = match test_file.path.file_stem() {
            Some(stem) => stem.to_string_lossy().to_string(),
            None => {
                errors.push(format!("File {} has no stem", test_file.path.display()));
                continue;
            }
        };

        let lines: Vec<&str> = content.lines().collect();

        for (line_num, line) in lines.iter().enumerate() {
            if let Some(captures) = scratch_pattern.captures(line)
                && let Some(found_target) = captures.get(1)
            {
                let found_target = found_target.as_str();
                if found_target != expected_target {
                    errors.push(format!(
                        "File {}:{}: scratch target '{}' does not match file name '{}'",
                        test_file.path.display(),
                        line_num + 1,
                        found_target,
                        expected_target
                    ));
                }
            }
        }
    }

    errors
}
