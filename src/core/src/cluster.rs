use std::net::IpAddr;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use url::{Host, Url};

pub const DEFAULT_CORE_PORT: u16 = 9973;
pub const DEFAULT_FLIGHT_PORT: u16 = 9100;
pub const DEFAULT_MEMORY_BYTES: usize = 8 * 1024 * 1024 * 1024;
pub const DEFAULT_BIND_HOST: &str = "0.0.0.0";
pub const DEFAULT_FLIGHT_BIND_HOST: &str = "127.0.0.1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessRole {
    Standalone,
    Coordinator,
    Worker,
}

impl ProcessRole {
    pub fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "standalone" => Ok(Self::Standalone),
            "coordinator" => Ok(Self::Coordinator),
            "worker" => Ok(Self::Worker),
            other => bail!("unknown DATACLOD_ROLE `{other}`"),
        }
    }

    /// `DATACLOD_ROLE` is optional once the binary encodes the role. If it is
    /// set, it must name this process.
    fn reject_mismatch(self, raw: Option<&str>) -> Result<()> {
        let Some(raw) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
            return Ok(());
        };
        let parsed = Self::parse(raw)?;
        if parsed != self {
            bail!("DATACLOD_ROLE `{parsed}` does not match this `{self}` binary");
        }
        Ok(())
    }
}

impl std::fmt::Display for ProcessRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Standalone => "standalone",
            Self::Coordinator => "coordinator",
            Self::Worker => "worker",
        })
    }
}

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub role: ProcessRole,
    pub bind_host: String,
    pub core_port: u16,
    pub flight_bind_host: String,
    pub flight_port: u16,
    pub advertised_flight_url: String,
    pub worker_urls: Vec<String>,
    pub memory_bytes: usize,
    pub spill_dir: Option<PathBuf>,
    pub data_dir: String,
    pub worker_version: String,
    pub require_workers_ready: bool,
    pub flight_expose: bool,
}

impl ClusterConfig {
    pub fn standalone() -> Self {
        Self {
            role: ProcessRole::Standalone,
            bind_host: DEFAULT_BIND_HOST.to_owned(),
            core_port: DEFAULT_CORE_PORT,
            flight_bind_host: DEFAULT_FLIGHT_BIND_HOST.to_owned(),
            flight_port: DEFAULT_FLIGHT_PORT,
            advertised_flight_url: format!("http://127.0.0.1:{DEFAULT_FLIGHT_PORT}"),
            worker_urls: Vec::new(),
            memory_bytes: DEFAULT_MEMORY_BYTES,
            spill_dir: None,
            data_dir: String::new(),
            worker_version: env!("CARGO_PKG_VERSION").to_owned(),
            require_workers_ready: false,
            flight_expose: false,
        }
    }

    pub fn from_env() -> Result<Self> {
        let mut cfg = Self::standalone();
        if let Ok(role) = std::env::var("DATACLOD_ROLE") {
            cfg.role = ProcessRole::parse(&role)?;
        }
        if let Ok(host) = std::env::var("DATACLOD_BIND_HOST")
            && !host.is_empty()
        {
            cfg.bind_host = host;
        }
        if let Ok(port) = std::env::var("DATACLOD_CORE_PORT") {
            cfg.core_port = port.parse().context("DATACLOD_CORE_PORT")?;
        }
        if let Ok(host) = std::env::var("DATACLOD_FLIGHT_BIND_HOST")
            && !host.is_empty()
        {
            cfg.flight_bind_host = host;
        }
        if let Ok(port) = std::env::var("DATACLOD_FLIGHT_PORT") {
            cfg.flight_port = port.parse().context("DATACLOD_FLIGHT_PORT")?;
        }
        cfg.advertised_flight_url = normalize_flight_url(
            &std::env::var("DATACLOD_ADVERTISED_FLIGHT_URL")
                .unwrap_or(format!("http://127.0.0.1:{}", cfg.flight_port)),
        )?;
        if let Ok(urls) = std::env::var("DATACLOD_WORKER_URLS") {
            cfg.worker_urls = normalize_worker_urls(
                urls.split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(ToOwned::to_owned),
            )?;
        }
        if let Ok(bytes) = std::env::var("DATACLOD_MEMORY_BYTES") {
            cfg.memory_bytes = bytes.parse().context("DATACLOD_MEMORY_BYTES")?;
        }
        if let Ok(dir) = std::env::var("DATACLOD_SPILL_DIR")
            && !dir.is_empty()
        {
            cfg.spill_dir = Some(PathBuf::from(dir));
        }
        if let Ok(dir) = std::env::var("DATACLOD_DATA_DIR") {
            cfg.data_dir = dir;
        }
        if let Ok(version) = std::env::var("DATACLOD_WORKER_VERSION")
            && !version.is_empty()
        {
            cfg.worker_version = version;
        }
        cfg.require_workers_ready = env_flag("DATACLOD_REQUIRE_WORKERS_READY");
        cfg.flight_expose = env_flag("DATACLOD_FLIGHT_EXPOSE");
        Ok(cfg)
    }

    /// Load env config and pin `role` to this binary.
    pub fn from_env_for(role: ProcessRole) -> Result<Self> {
        role.reject_mismatch(std::env::var("DATACLOD_ROLE").ok().as_deref())?;
        let mut cfg = Self::from_env()?;
        cfg.role = role;
        Ok(cfg)
    }

    pub fn is_coordinator(&self) -> bool {
        self.role == ProcessRole::Coordinator
    }

    pub fn is_worker(&self) -> bool {
        self.role == ProcessRole::Worker
    }

    pub fn postgres_bind(&self) -> Result<String> {
        format_bind_address(&self.bind_host, self.core_port)
    }

    pub fn flight_bind(&self) -> Result<String> {
        format_bind_address(&self.flight_bind_host, self.flight_port)
    }

    pub fn validate_for_role(&self) -> Result<()> {
        match self.role {
            ProcessRole::Standalone => Ok(()),
            ProcessRole::Worker | ProcessRole::Coordinator => {
                self.validate_flight_bind()?;
                self.validate_advertised_url()?;
                if self.role == ProcessRole::Coordinator {
                    self.validate_coordinator_urls()?;
                }
                Ok(())
            }
        }
    }

    fn validate_flight_bind(&self) -> Result<()> {
        if !self.flight_expose && !is_loopback_host(&self.flight_bind_host) {
            bail!(
                "Flight bind host `{}` is not loopback; set DATACLOD_FLIGHT_EXPOSE=1 to allow",
                self.flight_bind_host
            );
        }
        Ok(())
    }

    fn validate_advertised_url(&self) -> Result<()> {
        let url = parse_flight_url(&self.advertised_flight_url).with_context(|| {
            format!(
                "DATACLOD_ADVERTISED_FLIGHT_URL `{}`",
                self.advertised_flight_url
            )
        })?;
        let host = url.host_str().unwrap_or("");
        if host == "0.0.0.0" || host == "[::]" || host == "::" {
            bail!("advertised Flight URL must not use bind-any host `{host}`");
        }
        Ok(())
    }

    fn validate_coordinator_urls(&self) -> Result<()> {
        if self.worker_urls.is_empty() {
            bail!("DATACLOD_WORKER_URLS is required for coordinator");
        }
        let advertised = normalize_flight_url(&self.advertised_flight_url)?;
        let mut found = false;
        for raw in &self.worker_urls {
            let normalized = normalize_flight_url(raw)?;
            if normalized == advertised {
                found = true;
            }
        }
        if !found {
            bail!("DATACLOD_WORKER_URLS must contain advertised URL `{advertised}`");
        }
        Ok(())
    }
}

fn env_flag(name: &str) -> bool {
    matches!(
        std::env::var(name).as_deref(),
        Ok("1" | "true" | "TRUE" | "yes" | "YES")
    )
}

pub fn parse_flight_url(raw: &str) -> Result<Url> {
    Url::parse(raw.trim()).with_context(|| format!("invalid Flight URL `{raw}`"))
}

pub fn normalize_flight_url(raw: &str) -> Result<String> {
    let url = parse_flight_url(raw)?;
    let scheme = url.scheme().to_ascii_lowercase();
    let host = url.host().context("Flight URL must include a host")?;
    let port = url.port_or_known_default();
    let mut normalized = format!("{scheme}://{host}");
    if let Some(port) = port {
        normalized.push(':');
        normalized.push_str(&port.to_string());
    }
    Ok(normalized)
}

pub fn normalize_worker_urls(urls: impl IntoIterator<Item = String>) -> Result<Vec<String>> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for raw in urls {
        let normalized = normalize_flight_url(&raw)?;
        if seen.insert(normalized.clone()) {
            out.push(normalized);
        }
    }
    Ok(out)
}

fn is_loopback_host(host: &str) -> bool {
    match parse_bind_host(host) {
        Ok(Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
        Ok(Host::Ipv4(ip)) => ip.is_loopback(),
        Ok(Host::Ipv6(ip)) => ip.is_loopback(),
        Err(_) => false,
    }
}

fn parse_bind_host(raw: &str) -> Result<Host<String>> {
    let raw = raw.trim();
    if let Ok(ip) = raw.parse::<IpAddr>() {
        return Ok(match ip {
            IpAddr::V4(ip) => Host::Ipv4(ip),
            IpAddr::V6(ip) => Host::Ipv6(ip),
        });
    }
    Host::parse(raw).with_context(|| format!("invalid bind host `{raw}`"))
}

fn format_bind_address(host: &str, port: u16) -> Result<String> {
    let host = parse_bind_host(host)?;
    Ok(format!("{host}:{port}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_process_role() {
        assert_eq!(ProcessRole::parse("").unwrap(), ProcessRole::Standalone);
        assert_eq!(
            ProcessRole::parse("standalone").unwrap(),
            ProcessRole::Standalone
        );
        assert_eq!(
            ProcessRole::parse("Coordinator").unwrap(),
            ProcessRole::Coordinator
        );
        assert_eq!(ProcessRole::parse("WORKER").unwrap(), ProcessRole::Worker);
        assert!(ProcessRole::parse("replica").is_err());
    }

    #[test]
    fn env_role_may_be_omitted() {
        ProcessRole::Coordinator.reject_mismatch(None).unwrap();
        ProcessRole::Worker.reject_mismatch(Some("")).unwrap();
        ProcessRole::Standalone
            .reject_mismatch(Some("standalone"))
            .unwrap();
    }

    #[test]
    fn env_role_must_match_binary() {
        let err = ProcessRole::Standalone
            .reject_mismatch(Some("coordinator"))
            .unwrap_err();
        assert!(err.to_string().contains("does not match"));
        ProcessRole::Coordinator
            .reject_mismatch(Some("coordinator"))
            .unwrap();
    }
}
