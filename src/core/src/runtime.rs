use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use datafusion::execution::memory_pool::TrackConsumersPool;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use object_store::local::LocalFileSystem;
use url::Url;

use crate::cluster::ClusterConfig;
use crate::dataclod_url::DATACLOD_OBJECT_STORE_URL;
use crate::memory_pool::{DEFAULT_UNSPILLABLE_RESERVE_RATIO, QueryFairSpillPool};

pub fn make_runtime_from_cluster(cluster: &ClusterConfig) -> Result<Arc<RuntimeEnv>> {
    let track_capacity = NonZeroUsize::new(10).expect("track capacity must be non-zero");
    let memory_pool = Arc::new(TrackConsumersPool::new(
        QueryFairSpillPool::new(cluster.memory_bytes, DEFAULT_UNSPILLABLE_RESERVE_RATIO),
        track_capacity,
    ));
    let mut builder = RuntimeEnvBuilder::new().with_memory_pool(memory_pool);
    if let Some(spill) = &cluster.spill_dir {
        builder = builder.with_temp_file_path(spill);
    }
    let runtime = builder.build_arc().context("build datafusion runtime")?;
    if !cluster.data_dir.is_empty() {
        register_dataclod_object_store(&runtime, &cluster.data_dir)?;
    }
    Ok(runtime)
}

fn register_dataclod_object_store(runtime: &RuntimeEnv, data_dir: &str) -> Result<()> {
    let dir = PathBuf::from(data_dir);
    if !dir.is_absolute() {
        bail!("DATACLOD_DATA_DIR must be an absolute path, got `{data_dir}`");
    }
    if !dir.is_dir() {
        bail!("DATACLOD_DATA_DIR `{data_dir}` is not a directory");
    }
    let store = LocalFileSystem::new_with_prefix(&dir)
        .with_context(|| format!("open LocalFileSystem at `{data_dir}`"))?;
    let url = Url::parse(DATACLOD_OBJECT_STORE_URL).expect("static dataclod://data URL");
    runtime.register_object_store(&url, Arc::new(store));
    Ok(())
}
