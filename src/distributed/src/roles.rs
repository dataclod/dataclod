use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::{Context as _, Result};
use dataclod::{ClusterConfig, QueryContext, QueryContextOptions, make_runtime_from_cluster};
use datafusion_distributed::{
    ChannelResolver, DefaultChannelResolver, GetWorkerInfoRequest, GetWorkerInfoResponse, Worker,
};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::Stream;
use tonic::transport::Server;
use url::Url;

use crate::coordinator::apply_coordinator_session;
use crate::worker::build_worker;

const WORKER_INFO_TIMEOUT: Duration = Duration::from_secs(2);

pub async fn run_coordinator(cfg: ClusterConfig) -> Result<()> {
    cfg.validate_for_role()?;
    let flight_bind = cfg.flight_bind()?;
    let postgres_bind = cfg.postgres_bind()?;
    if cfg.data_dir.is_empty() {
        tracing::warn!("DATACLOD_DATA_DIR is empty; coordinator can plan but cannot read files");
    }

    let runtime = make_runtime_from_cluster(&cfg)?;
    let worker = build_worker(runtime.clone(), cfg.worker_version.clone());

    let listener = TcpListener::bind(&flight_bind)
        .await
        .with_context(|| format!("bind Flight {flight_bind}"))?;
    spawn_flight_server(worker, listener).await?;
    get_worker_info(&cfg.advertised_flight_url)
        .await
        .with_context(|| {
            format!(
                "advertised Flight URL `{}` failed GetWorkerInfo",
                cfg.advertised_flight_url
            )
        })?;
    ping_workers(&cfg.worker_urls, cfg.require_workers_ready).await?;

    let opts = QueryContextOptions {
        cluster: cfg.clone(),
        runtime: Some(runtime.clone()),
    };
    let cluster = cfg.clone();
    let ctx = QueryContext::try_new_with_builder(opts, move |builder| {
        apply_coordinator_session(builder, &cluster)
    })?;
    anyhow::ensure!(
        Arc::ptr_eq(&runtime, &ctx.runtime_env()),
        "coordinator QueryContext must share the Worker RuntimeEnv"
    );
    ctx.allow_set_prefix("distributed.");

    tracing::info!(
        role = %cfg.role,
        postgres = %postgres_bind,
        flight = %flight_bind,
        advertised = %cfg.advertised_flight_url,
        workers = ?cfg.worker_urls,
        memory_bytes = cfg.memory_bytes,
        spill_dir = ?cfg.spill_dir,
        data_dir = %cfg.data_dir,
        "starting coordinator"
    );

    server::postgres::server_with_context(postgres_bind, Arc::new(ctx)).await;
    Ok(())
}

pub async fn run_worker(cfg: ClusterConfig) -> Result<()> {
    cfg.validate_for_role()?;
    let flight_bind = cfg.flight_bind()?;
    if cfg.data_dir.is_empty() {
        tracing::warn!(
            "DATACLOD_DATA_DIR is empty; worker can execute stages but cannot read files"
        );
    }
    let runtime = make_runtime_from_cluster(&cfg)?;
    let worker = build_worker(runtime, cfg.worker_version.clone());
    let listener = TcpListener::bind(&flight_bind)
        .await
        .with_context(|| format!("bind Flight {flight_bind}"))?;
    tracing::info!(
        role = %cfg.role,
        flight = %flight_bind,
        spill_dir = ?cfg.spill_dir,
        data_dir = %cfg.data_dir,
        "starting worker"
    );
    Server::builder()
        .add_service(worker.into_worker_server())
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
        .await
        .context("Flight worker server")?;
    Ok(())
}

pub(crate) async fn spawn_flight_server(worker: Worker, listener: TcpListener) -> Result<()> {
    let (serving_tx, serving_rx) = oneshot::channel();
    tokio::spawn(async move {
        let incoming = ServingNotify {
            inner: tokio_stream::wrappers::TcpListenerStream::new(listener),
            notify: Some(serving_tx),
        };
        if let Err(err) = Server::builder()
            .add_service(worker.into_worker_server())
            .serve_with_incoming(incoming)
            .await
        {
            tracing::error!(error = %err, "Flight worker server exited");
        }
    });
    serving_rx.await.context("Flight serving signal")?;
    Ok(())
}

pub(crate) async fn get_worker_info(url: &str) -> Result<GetWorkerInfoResponse> {
    let request = async {
        let parsed = Url::parse(url).with_context(|| format!("invalid worker URL `{url}`"))?;
        let resolver = DefaultChannelResolver::default();
        let mut client = resolver
            .get_worker_client_for_url(&parsed)
            .await
            .map_err(|err| anyhow::anyhow!(err))?;
        let resp = client
            .get_worker_info(GetWorkerInfoRequest::default())
            .await
            .with_context(|| format!("GetWorkerInfo `{url}` failed"))?;
        Ok(resp.into_inner())
    };
    with_worker_info_timeout(url, WORKER_INFO_TIMEOUT, request).await
}

pub(crate) async fn with_worker_info_timeout<F>(
    url: &str, timeout: Duration, request: F,
) -> Result<GetWorkerInfoResponse>
where
    F: Future<Output = Result<GetWorkerInfoResponse>>,
{
    tokio::time::timeout(timeout, request)
        .await
        .with_context(|| format!("timed out GetWorkerInfo `{url}`"))?
}

pub(crate) async fn ping_workers(urls: &[String], require_ready: bool) -> Result<()> {
    let mut first_err: Option<anyhow::Error> = None;
    for url in urls {
        match get_worker_info(url).await {
            Ok(info) => {
                tracing::info!(url, version = %info.version, "worker ready");
            }
            Err(err) => {
                tracing::warn!(url, error = %err, "worker GetWorkerInfo failed");
                if first_err.is_none() {
                    first_err = Some(err);
                }
            }
        }
    }
    if let Some(err) = first_err
        && require_ready
    {
        return Err(err).context("DATACLOD_REQUIRE_WORKERS_READY");
    }
    Ok(())
}

struct ServingNotify<S> {
    inner: S,
    notify: Option<oneshot::Sender<()>>,
}

impl<S: Stream + Unpin> Stream for ServingNotify<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(tx) = self.notify.take() {
            let _ = tx.send(());
        }
        Pin::new(&mut self.inner).poll_next(cx)
    }
}
