use anyhow::{Context, Result};
use datafusion::common::DataFusionError;
use datafusion_distributed::WorkerResolver;
use url::Url;

#[derive(Debug, Clone)]
pub struct StaticWorkerResolver {
    urls: Vec<Url>,
}

impl StaticWorkerResolver {
    pub fn parse(urls: &[String]) -> Result<Self> {
        let normalized = dataclod::normalize_worker_urls(urls.iter().cloned())?;
        let parsed = normalized
            .iter()
            .map(|url| Url::parse(url).with_context(|| format!("invalid worker URL `{url}`")))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { urls: parsed })
    }
}

impl WorkerResolver for StaticWorkerResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        Ok(self.urls.clone())
    }
}
