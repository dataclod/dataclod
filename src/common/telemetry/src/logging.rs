use std::str::FromStr;
use std::sync::{Arc, LazyLock, Mutex, Once};

use tracing::Level;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::Registry;
use tracing_subscriber::fmt::Layer;
use tracing_subscriber::fmt::writer::MakeWriterExt;
use tracing_subscriber::layer::SubscriberExt;

pub fn init_logging() -> Vec<WorkerGuard> {
    let mut guards = vec![];

    let (stdout_write, stdout_guard) = tracing_appender::non_blocking(std::io::stdout());
    let log_level = std::env::var("DATACLOD_LOG_LEVEL").unwrap_or("INFO".to_owned());
    let log_level = Level::from_str(log_level.as_str()).unwrap_or(Level::INFO);
    let stdout_layer = Layer::new()
        .with_writer(stdout_write.with_max_level(log_level))
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(false);
    guards.push(stdout_guard);

    #[cfg(not(feature = "console"))]
    {
        let subscriber = Registry::default().with(stdout_layer);
        tracing::subscriber::set_global_default(subscriber).unwrap();
    }
    #[cfg(feature = "console")]
    {
        let subscriber = Registry::default()
            .with(stdout_layer)
            .with(console_subscriber::spawn());
        tracing::subscriber::set_global_default(subscriber).unwrap();
    }

    guards
}

static GLOBAL_TEST_LOG_GUARDS: LazyLock<Arc<Mutex<Option<Vec<WorkerGuard>>>>> =
    LazyLock::new(|| Arc::new(Mutex::new(None)));

pub fn init_test_logging() {
    static START: Once = Once::new();

    START.call_once(|| {
        let mut guards = GLOBAL_TEST_LOG_GUARDS.as_ref().lock().unwrap();

        *guards = Some(init_logging());
    });
}
