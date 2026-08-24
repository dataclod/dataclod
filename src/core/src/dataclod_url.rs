use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{DynamicFileCatalog, UrlTableFactory};
use datafusion::datasource::dynamic_file::DynamicListTableFactory;
use datafusion::error::Result as DFResult;
use datafusion::execution::context::SessionContext;
use url::Url;

pub(crate) const DATACLOD_OBJECT_STORE_URL: &str = "dataclod://data";
const DATACLOD_URL_AUTHORITY: &str = "data";

#[derive(Debug)]
struct DataclodUrlTableFactory {
    inner: DynamicListTableFactory,
    data_dir: PathBuf,
}

impl DataclodUrlTableFactory {
    fn new(data_dir: PathBuf) -> Self {
        Self {
            inner: DynamicListTableFactory::new(Default::default()),
            data_dir,
        }
    }

    fn bind_session(&self, ctx: &SessionContext) {
        self.inner.session_store().with_state(ctx.state_weak_ref());
    }
}

#[async_trait]
impl UrlTableFactory for DataclodUrlTableFactory {
    async fn try_new(
        &self, url: &str,
    ) -> DFResult<Option<Arc<dyn datafusion::catalog::TableProvider>>> {
        if url.contains("://") {
            validate_dataclod_url(url, &self.data_dir)?;
            return self.inner.try_new(url).await;
        }
        if looks_like_filesystem_path(url) {
            return datafusion::common::plan_err!("only dataclod:// URLs are allowed, got `{url}`");
        }
        Ok(None)
    }
}

pub(crate) fn install_dataclod_url_catalog(ctx: SessionContext, data_dir: &str) -> SessionContext {
    if data_dir.is_empty() {
        return ctx;
    }
    let factory = Arc::new(DataclodUrlTableFactory::new(PathBuf::from(data_dir)));
    let factory_ref: Arc<dyn UrlTableFactory> = factory.clone();
    let catalog_list = Arc::new(DynamicFileCatalog::new(
        Arc::clone(ctx.state().catalog_list()),
        factory_ref,
    ));
    let state = ctx
        .into_state_builder()
        .with_catalog_list(catalog_list)
        .build();
    let ctx = SessionContext::new_with_state(state);
    factory.bind_session(&ctx);
    ctx
}

fn looks_like_filesystem_path(name: &str) -> bool {
    name.starts_with('/')
        || name.starts_with('.')
        || name.ends_with(".parquet")
        || name.ends_with(".csv")
        || name.ends_with(".json")
}

fn validate_dataclod_url(name: &str, data_dir: &Path) -> DFResult<()> {
    use datafusion::common::plan_err;
    use object_store::path::Path as ObjectPath;

    if name.split('/').any(|seg| seg == "..") {
        return plan_err!("dataclod URL must not contain `..`");
    }
    let parsed = Url::parse(name).map_err(|err| {
        datafusion::error::DataFusionError::Plan(format!("invalid table URL `{name}`: {err}"))
    })?;
    if parsed.scheme() != "dataclod" {
        return plan_err!("only dataclod:// URLs are allowed, got `{name}`");
    }
    if parsed.host_str() != Some(DATACLOD_URL_AUTHORITY) {
        return plan_err!(
            "dataclod URL must use authority `{DATACLOD_URL_AUTHORITY}` (e.g. dataclod://data/t.parquet)"
        );
    }
    // ListingTableUrl opens Path::from_url_path, which percent-decodes first.
    let object_path = ObjectPath::from_url_path(parsed.path()).map_err(|err| {
        datafusion::error::DataFusionError::Plan(format!("invalid dataclod URL `{name}`: {err}"))
    })?;
    if object_path.is_root() {
        return Ok(());
    }

    let canonical_root = data_dir.canonicalize().map_err(|err| {
        datafusion::error::DataFusionError::Plan(format!(
            "cannot canonicalize DATACLOD_DATA_DIR: {err}"
        ))
    })?;
    let mut joined = canonical_root.clone();
    for part in object_path.parts() {
        joined.push(part.as_ref());
    }
    if let Ok(canonical) = joined.canonicalize()
        && !canonical.starts_with(&canonical_root)
    {
        return plan_err!("dataclod URL escapes DATACLOD_DATA_DIR");
    }
    Ok(())
}
