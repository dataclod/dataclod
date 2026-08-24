use std::sync::Arc;

use dataclod::{ClusterConfig, ProcessRole, QueryContext, QueryContextOptions};
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() {
    let _guards = common_telemetry::init_logging();

    let cfg = ClusterConfig::from_env().expect("invalid cluster configuration");
    cfg.validate_for_role()
        .expect("cluster configuration failed validation");
    match cfg.role {
        ProcessRole::Standalone => {
            let bind = cfg
                .postgres_bind()
                .expect("invalid PostgreSQL bind address");
            let ctx = QueryContext::try_new(QueryContextOptions {
                cluster: cfg,
                runtime: None,
            })
            .expect("failed to build standalone QueryContext");
            server::postgres::server_with_context(bind, Arc::new(ctx)).await;
        }
        ProcessRole::Coordinator => {
            dataclod_distributed::run_coordinator(cfg)
                .await
                .expect("coordinator exited")
        }
        ProcessRole::Worker => {
            dataclod_distributed::run_worker(cfg)
                .await
                .expect("worker exited")
        }
    }
}
