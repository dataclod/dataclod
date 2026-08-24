use std::sync::Arc;

use dataclod::collect_scalar_udfs;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion_distributed::{Worker, WorkerQueryContext};

pub(crate) fn configure_worker_session(mut builder: SessionStateBuilder) -> SessionStateBuilder {
    {
        let slot = builder.config().get_or_insert_default();
        let mut next = slot.clone();
        next = next.set_bool(
            "datafusion.execution.skip_physical_aggregate_schema_check",
            true,
        );
        next = datafusion_spatial::add_dataclod_option_extension(next);
        *slot = next;
    }
    builder
        .scalar_functions()
        .get_or_insert_with(Vec::new)
        .extend(collect_scalar_udfs());
    builder
}

pub(crate) fn build_worker(runtime: Arc<RuntimeEnv>, version: impl Into<String>) -> Worker {
    datafusion_spatial::init_tggeom_alloc();
    Worker::from_session_builder(move |ctx: WorkerQueryContext| {
        async move { Ok(configure_worker_session(ctx.builder).build()) }
    })
    .with_runtime_env(runtime)
    .with_version(version.into())
}
