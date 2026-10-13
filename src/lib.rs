//! Shared helpers of the `run` and `test` binaries: `cfg/services.json` and service configs.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use serde_json::Value;
use skw_lib_shared::prelude::consts::SERVICE_BIND_ADDRESS;

const SERVICES_FILE: &str = "services.json";

/// `services/test` crate dir
pub fn crate_dir() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

pub fn cfg_dir() -> PathBuf {
    crate_dir().join("cfg")
}

/// One entry of `cfg/services.json`; array order is the start order.
#[derive(Debug, Clone)]
pub struct ServiceEntry {
    pub bin: String,
    /// absolute path of the service config
    pub config: PathBuf,
    /// service accepts connections on `/service/bindAddress` of its config
    pub listens: bool,
}

pub fn read_json(path: &Path) -> Result<Value> {
    let json = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    serde_json::from_str(&json).with_context(|| format!("parse {}", path.display()))
}

pub fn load_services() -> Result<Vec<ServiceEntry>> {
    let path = cfg_dir().join(SERVICES_FILE);
    let list = read_json(&path)?;
    let entries = list
        .as_array()
        .ok_or_else(|| anyhow!("{} must be an array", path.display()))?;

    entries
        .iter()
        .map(|e| {
            let bin = e["bin"]
                .as_str()
                .ok_or_else(|| anyhow!("{SERVICES_FILE}: entry without \"bin\": {e}"))?;
            let config = e["config"]
                .as_str()
                .ok_or_else(|| anyhow!("{SERVICES_FILE}: {bin} has no \"config\""))?;
            Ok(ServiceEntry {
                bin: bin.to_string(),
                config: cfg_dir().join(config),
                listens: e["listens"].as_bool().unwrap_or(false),
            })
        })
        .collect()
}

pub fn find_service(bin: &str) -> Result<ServiceEntry> {
    load_services()?
        .into_iter()
        .find(|s| s.bin == bin)
        .ok_or_else(|| anyhow!("{bin} is not defined in cfg/{SERVICES_FILE}"))
}

/// Port of a `listens` service, from `/service/bindAddress` (`"0.0.0.0:9001"` → `9001`).
pub fn listen_port(service: &ServiceEntry) -> Result<Option<u16>> {
    if !service.listens {
        return Ok(None);
    }
    let cfg = read_json(&service.config)?;
    cfg.pointer(SERVICE_BIND_ADDRESS)
        .and_then(Value::as_str)
        .and_then(|b| b.rsplit(':').next()?.parse().ok())
        .map(Some)
        .ok_or_else(|| {
            anyhow!(
                "{} listens but has no valid {SERVICE_BIND_ADDRESS} in {}",
                service.bin,
                service.config.display()
            )
        })
}
