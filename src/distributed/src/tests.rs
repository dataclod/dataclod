use std::sync::Arc;
use std::time::Duration;

use dataclod::{
    ClusterConfig, ProcessRole, QueryContext, QueryContextOptions, make_runtime_from_cluster,
};
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result as DFResult;
use datafusion::config::ConfigOptions;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_distributed::{
    DistributedExec, DistributedExt, GetWorkerInfoResponse, NetworkBoundaryExt,
    SessionStateBuilderExt, TaskEstimation, TaskEstimator, WorkerResolver,
};
use tokio::net::TcpListener;

use crate::coordinator::apply_coordinator_session;
use crate::resolver::StaticWorkerResolver;

#[derive(Debug, Clone, Copy)]
struct ForceTwoTasks;

impl TaskEstimator for ForceTwoTasks {
    fn task_estimation(
        &self, plan: &Arc<dyn ExecutionPlan>, _cfg: &ConfigOptions,
    ) -> Option<TaskEstimation> {
        if plan.children().is_empty() || plan.name() == "SpatialJoinExec" {
            Some(TaskEstimation::desired(2))
        } else {
            None
        }
    }

    fn scale_up_leaf_node(
        &self, _plan: &Arc<dyn ExecutionPlan>, _task_count: usize, _cfg: &ConfigOptions,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }
}
use crate::roles::{
    get_worker_info, ping_workers, run_coordinator, spawn_flight_server, with_worker_info_timeout,
};
use crate::worker::build_worker;

async fn write_parquet(dir: &std::path::Path) {
    std::fs::create_dir_all(dir).unwrap();
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
        ],
    )
    .unwrap();
    for name in ["t.parquet", "t1.parquet", "t2.parquet"] {
        ctx.read_batch(batch.clone())
            .unwrap()
            .write_parquet(
                dir.join(name).to_str().unwrap(),
                DataFrameWriteOptions::new(),
                None,
            )
            .await
            .unwrap();
    }
}

fn coordinator_opts(data_dir: &std::path::Path, worker_url: &str) -> QueryContextOptions {
    let mut cluster = ClusterConfig::standalone();
    cluster.role = ProcessRole::Coordinator;
    cluster.data_dir = data_dir.to_string_lossy().into_owned();
    cluster.advertised_flight_url = worker_url.to_owned();
    cluster.worker_urls = vec![worker_url.to_owned()];
    QueryContextOptions {
        cluster,
        runtime: None,
    }
}

fn coordinator_ctx(data_dir: &std::path::Path) -> QueryContext {
    let mut opts = coordinator_opts(data_dir, "http://127.0.0.1:9100");
    opts.cluster.worker_urls = vec![
        "http://127.0.0.1:9100".to_owned(),
        "http://127.0.0.1:9101".to_owned(),
        "http://127.0.0.1:9102".to_owned(),
    ];
    let cluster = opts.cluster.clone();
    QueryContext::try_new_with_builder(opts, move |mut builder| {
        {
            let slot = builder.config().get_or_insert_default();
            *slot = slot.clone().with_target_partitions(2);
        }
        let builder = apply_coordinator_session(builder, &cluster)?;
        builder
            .with_distributed_file_scan_config_bytes_per_partition(1)
            .map_err(|err| anyhow::anyhow!(err))
    })
    .unwrap()
}

fn plan_contains_distributed(plan: &dyn ExecutionPlan) -> bool {
    if plan.name() == "DistributedExec"
        || plan.name().contains("Network")
        || plan.as_network_boundary().is_some()
        || plan.downcast_ref::<DistributedExec>().is_some()
    {
        return true;
    }
    plan.children()
        .iter()
        .any(|child| plan_contains_distributed(child.as_ref()))
}

fn plan_contains_name(plan: &dyn ExecutionPlan, name: &str) -> bool {
    plan.name() == name
        || plan
            .children()
            .iter()
            .any(|child| plan_contains_name(child.as_ref(), name))
}

async fn geom_point_batch() -> RecordBatch {
    let ctx = SessionContext::new();
    datafusion_spatial::register_spatial_udfs(&ctx);
    let batch = ctx
        .sql("SELECT 0 AS k, ST_MakePoint(0, 0) AS g UNION ALL SELECT 1, ST_MakePoint(1, 0)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .remove(0);
    let fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| {
            if field.name() == "g" {
                let mut metadata = field.metadata().clone();
                metadata.insert("target_type".to_owned(), "geometry".to_owned());
                field.as_ref().clone().with_metadata(metadata)
            } else {
                field.as_ref().clone()
            }
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec()).unwrap()
}

fn register_geom_tables(ctx: &QueryContext, batch: RecordBatch) {
    ctx.session_context()
        .register_batch("a", batch.clone())
        .unwrap();
    ctx.session_context().register_batch("b", batch).unwrap();
}

async fn bind_local() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, format!("http://127.0.0.1:{port}"))
}

#[tokio::test]
async fn coordinator_file_groupby_is_distributed() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path()).await;
    let ctx = coordinator_ctx(dir.path());
    let planner = format!("{:?}", ctx.session_context().state().query_planner());
    assert!(
        planner.contains("EncodeSafeQueryPlanner"),
        "planner={planner}"
    );
    let logical = ctx
        .create_logical_plan("SELECT k, sum(v) FROM 'dataclod://data/' GROUP BY k")
        .await
        .unwrap();

    let mut raw_opts = coordinator_opts(dir.path(), "http://127.0.0.1:9100");
    raw_opts.cluster.worker_urls = vec![
        "http://127.0.0.1:9100".to_owned(),
        "http://127.0.0.1:9101".to_owned(),
        "http://127.0.0.1:9102".to_owned(),
    ];
    let raw_cluster = raw_opts.cluster.clone();
    let raw_ctx = QueryContext::try_new_with_builder(raw_opts, move |mut builder| {
        {
            let slot = builder.config().get_or_insert_default();
            *slot = slot.clone().with_target_partitions(2);
        }
        let builder = crate::coordinator::apply_coordinator_session_raw(builder, &raw_cluster)?;
        builder
            .with_distributed_file_scan_config_bytes_per_partition(1)
            .map_err(|err| anyhow::anyhow!(err))
    })
    .unwrap();
    let raw = raw_ctx
        .session_context()
        .state()
        .create_physical_plan(&logical)
        .await
        .unwrap();
    let dry =
        crate::encode_fallback::dry_run_encode_stages(&raw, &raw_ctx.session_context().state());
    assert!(
        plan_contains_distributed(raw.as_ref()),
        "raw_plan_name={} dry={dry:?} raw={raw:?}",
        raw.name()
    );

    let physical = ctx
        .session_context()
        .state()
        .create_physical_plan(&logical)
        .await
        .unwrap();
    assert!(
        plan_contains_distributed(physical.as_ref()),
        "planner={planner} dry={dry:?} plan={physical:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn duckdb_scan_agg_falls_back_and_matches_standalone() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("t.duckdb");
    {
        let conn = duckdb::Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t(k INTEGER, v INTEGER)", [])
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 10), (1, 20), (2, 30)", [])
            .unwrap();
    }
    let sql = format!(
        "SELECT k, sum(v) FROM duckdb_scan('{}', 'main', 't') GROUP BY k ORDER BY k",
        db.display()
    );
    let standalone = QueryContext::new();
    let expected = standalone.sql(&sql).await.unwrap().collect().await.unwrap();

    let ctx = coordinator_ctx(dir.path());
    let got = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    assert_eq!(format!("{got:?}"), format!("{expected:?}"));
}

#[tokio::test]
async fn spatial_join_falls_back_and_matches_standalone() {
    let sql = "SELECT a.k, count(*) FROM a CROSS JOIN b \
               WHERE ST_Intersects(a.g, b.g) GROUP BY a.k ORDER BY a.k";
    let batch = geom_point_batch().await;
    let standalone = QueryContext::new();
    register_geom_tables(&standalone, batch.clone());
    let expected = standalone.sql(sql).await.unwrap().collect().await.unwrap();
    let standalone_logical = standalone.create_logical_plan(sql).await.unwrap();
    let standalone_physical = standalone
        .session_context()
        .state()
        .create_physical_plan(&standalone_logical)
        .await
        .unwrap();
    assert!(
        plan_contains_name(standalone_physical.as_ref(), "SpatialJoinExec"),
        "standalone must plan SpatialJoinExec: {standalone_physical:?}"
    );

    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path()).await;
    let ctx = coordinator_ctx(dir.path());
    register_geom_tables(&ctx, batch.clone());

    let logical = ctx.create_logical_plan(sql).await.unwrap();
    let mut raw_opts = coordinator_opts(dir.path(), "http://127.0.0.1:9100");
    raw_opts.cluster.worker_urls = vec![
        "http://127.0.0.1:9100".to_owned(),
        "http://127.0.0.1:9101".to_owned(),
        "http://127.0.0.1:9102".to_owned(),
    ];
    let raw_cluster = raw_opts.cluster.clone();
    let raw_ctx = QueryContext::try_new_with_builder(raw_opts, move |mut builder| {
        {
            let slot = builder.config().get_or_insert_default();
            *slot = slot.clone().with_target_partitions(2);
        }
        let resolver = crate::resolver::StaticWorkerResolver::parse(&raw_cluster.worker_urls)?;
        Ok(builder
            .with_distributed_worker_resolver(resolver)
            .with_distributed_task_estimator(ForceTwoTasks)
            .with_distributed_planner())
    })
    .unwrap();
    register_geom_tables(&raw_ctx, batch);
    let raw = raw_ctx
        .session_context()
        .state()
        .create_physical_plan(&logical)
        .await
        .unwrap();
    assert!(
        plan_contains_name(raw.as_ref(), "SpatialJoinExec"),
        "raw must contain SpatialJoinExec: {raw:?}"
    );
    let dry =
        crate::encode_fallback::dry_run_encode_stages(&raw, &raw_ctx.session_context().state());
    assert!(
        dry.is_err(),
        "SpatialJoinExec stage must fail encode: {dry:?}"
    );

    let physical = ctx
        .session_context()
        .state()
        .create_physical_plan(&logical)
        .await
        .unwrap();
    assert!(
        plan_contains_name(physical.as_ref(), "SpatialJoinExec"),
        "fallback plan must keep SpatialJoinExec: {physical:?}"
    );
    assert!(
        !plan_contains_distributed(physical.as_ref()),
        "encode-safe fallback must not keep network stages: {physical:?}"
    );

    let got = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    assert_eq!(format!("{got:?}"), format!("{expected:?}"));
}

#[tokio::test]
async fn coordinator_set_distributed_reaches_config() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path()).await;
    let ctx = coordinator_ctx(dir.path());
    ctx.allow_set_prefix("distributed.");
    ctx.sql("SET distributed.compression = 'none'")
        .await
        .unwrap();
    let value = ctx
        .session_context()
        .state()
        .config()
        .options()
        .extensions
        .get::<datafusion_distributed::DistributedConfig>()
        .expect("DistributedConfig")
        .compression
        .clone();
    assert_eq!(value, "none");
}

#[tokio::test(flavor = "multi_thread")]
async fn build_worker_set_plan_keeps_udfs_and_distributes() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path()).await;

    let (listener_a, url_a) = bind_local().await;
    let (listener_b, url_b) = bind_local().await;

    let mut cluster = ClusterConfig::standalone();
    cluster.role = ProcessRole::Coordinator;
    cluster.data_dir = dir.path().to_string_lossy().into_owned();
    cluster.advertised_flight_url = url_a.clone();
    cluster.worker_urls = vec![url_a.clone(), url_b.clone()];
    let runtime = make_runtime_from_cluster(&cluster).unwrap();
    spawn_flight_server(build_worker(runtime.clone(), "test-a"), listener_a)
        .await
        .unwrap();
    spawn_flight_server(build_worker(runtime.clone(), "test-b"), listener_b)
        .await
        .unwrap();
    let info_a = get_worker_info(&url_a).await.unwrap();
    let info_b = get_worker_info(&url_b).await.unwrap();
    assert_eq!(info_a.version, "test-a");
    assert_eq!(info_b.version, "test-b");

    let cluster_for_builder = cluster.clone();
    let ctx = QueryContext::try_new_with_builder(
        QueryContextOptions {
            cluster,
            runtime: Some(runtime),
        },
        move |builder| {
            let mut builder = apply_coordinator_session(builder, &cluster_for_builder)?;
            {
                let slot = builder.config().get_or_insert_default();
                *slot = slot.clone().with_target_partitions(2);
            }
            builder
                .with_distributed_file_scan_config_bytes_per_partition(1)
                .map_err(|err| anyhow::anyhow!(err))
        },
    )
    .unwrap();

    let sql = "SELECT abs(k) AS k, ST_Intersects(ST_MakePoint(k, 0), ST_MakePoint(k, 0)) AS hit, \
               sum(v) AS s FROM 'dataclod://data/' GROUP BY abs(k), \
               ST_Intersects(ST_MakePoint(k, 0), ST_MakePoint(k, 0)) ORDER BY k";
    let logical = ctx.create_logical_plan(sql).await.unwrap();
    let physical = ctx
        .session_context()
        .state()
        .create_physical_plan(&logical)
        .await
        .unwrap();
    assert!(
        plan_contains_distributed(physical.as_ref()),
        "plan must hit Flight SetPlan path: {physical:?}"
    );

    let standalone = {
        let mut standalone_cluster = ClusterConfig::standalone();
        standalone_cluster.data_dir = dir.path().to_string_lossy().into_owned();
        QueryContext::try_new(QueryContextOptions {
            cluster: standalone_cluster,
            runtime: None,
        })
        .unwrap()
    };
    let expected = standalone.sql(sql).await.unwrap().collect().await.unwrap();
    let got = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    assert_eq!(format!("{got:?}"), format!("{expected:?}"));
}

#[tokio::test]
async fn ping_workers_require_ready_fails_when_dead() {
    let err = ping_workers(&["http://127.0.0.1:1".to_owned()], true)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("DATACLOD_REQUIRE_WORKERS_READY") || err.contains("GetWorkerInfo"),
        "{err}"
    );
}

#[tokio::test]
async fn worker_info_timeout_wraps_connection_and_rpc_future() {
    let err = with_worker_info_timeout(
        "http://pending.invalid",
        Duration::from_millis(10),
        std::future::pending::<anyhow::Result<GetWorkerInfoResponse>>(),
    )
    .await
    .unwrap_err()
    .to_string();

    assert!(err.contains("timed out GetWorkerInfo"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn run_coordinator_require_workers_ready_fails_on_dead_peer() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path()).await;
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let flight_port = probe.local_addr().unwrap().port();
    drop(probe);
    let pg = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let core_port = pg.local_addr().unwrap().port();
    drop(pg);

    let mut cfg = ClusterConfig::standalone();
    cfg.role = ProcessRole::Coordinator;
    cfg.bind_host = "127.0.0.1".to_owned();
    cfg.core_port = core_port;
    cfg.flight_port = flight_port;
    cfg.advertised_flight_url = format!("http://127.0.0.1:{flight_port}");
    cfg.worker_urls = vec![
        cfg.advertised_flight_url.clone(),
        "http://127.0.0.1:1".to_owned(),
    ];
    cfg.require_workers_ready = true;
    cfg.data_dir = dir.path().to_string_lossy().into_owned();

    let err = run_coordinator(cfg).await.unwrap_err().to_string();
    assert!(
        err.contains("DATACLOD_REQUIRE_WORKERS_READY") || err.contains("GetWorkerInfo"),
        "{err}"
    );
}

#[test]
fn resolver_dedups_normalized_worker_urls() {
    let resolver = StaticWorkerResolver::parse(&[
        "http://127.0.0.1:9100".to_owned(),
        "http://127.0.0.1:9100/".to_owned(),
        "HTTP://127.0.0.1:9101".to_owned(),
    ])
    .unwrap();
    let urls = resolver.get_urls().unwrap();
    assert_eq!(urls.len(), 2, "{urls:?}");
    assert_eq!(urls[0].as_str(), "http://127.0.0.1:9100/");
    assert_eq!(urls[1].as_str(), "http://127.0.0.1:9101/");
}
