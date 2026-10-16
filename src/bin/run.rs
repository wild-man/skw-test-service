//! `cargo run --bin run` — starts every service from `cfg/services.json` and streams their output
//! prefixed with `[bin]`. Ctrl-C / SIGTERM stops all spawned services.
//!
//! Spawns services in `services.json` order, waiting for the port of `listens` ones, since later
//! services may depend on them (e.g. stream consumers need topics created by the gates).
//! Developer's responsibility beforehand: infra (iggy, postgres, redis, rabbitmq) running,
//! binaries built (`cargo build`), migrations applied.
//!
//! Also serves the workspace `frontend/` folder over https on `FRONTEND_BIND_ADDRESS`
//! (default `127.0.0.1:8000`) with the gates' self-signed cert (`conf/skw-api-gate.*.pem`), so
//! pages can talk to the gates from a browser (api-gate allows this origin via `/service/cors`).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use axum::{
    Router,
    http::{HeaderValue, header},
};
use axum_server::{
    Server,
    tls_rustls::{RustlsAcceptor, RustlsConfig},
};
use serde_json::Value;
use skw_lib_shared::prelude::consts::CONFIG_FILE_ENV;
use skw_test_service::{ServiceEntry, crate_dir, listen_port, load_services};
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::net::TcpStream;
use tokio::process::{Child, Command};
use tokio::signal::unix::{SignalKind, signal};
use tower_http::{services::ServeDir, set_header::SetResponseHeaderLayer};

const FRONTEND_BIND_ADDRESS_ENV: &str = "FRONTEND_BIND_ADDRESS";
const FRONTEND_DEFAULT_BIND_ADDRESS: &str = "127.0.0.1:8000";
/// workspace-relative; same cert as the gates (SAN: localhost, 127.0.0.1)
const FRONTEND_TLS_CERT: &str = "conf/skw-api-gate.cert.pem";
const FRONTEND_TLS_KEY: &str = "conf/skw-api-gate.key.pem";
const LISTEN_TIMEOUT: Duration = Duration::from_secs(30);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const EXIT_CHECK_INTERVAL: Duration = Duration::from_millis(500);

struct Workspace {
    target_dir: PathBuf,
    /// bin name → crate dir
    bins: HashMap<String, PathBuf>,
}

impl Workspace {
    fn bin_path(&self, bin: &str) -> PathBuf {
        self.target_dir.join("debug").join(bin)
    }
}

struct Running {
    bin: String,
    child: Child,
    exited: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let services = load_services()?;
    let ws = workspace()?;

    let mut daemons = vec![];
    for s in &services {
        let crate_dir = ws
            .bins
            .get(&s.bin)
            .ok_or_else(|| anyhow!("services.json: no workspace binary named {}", s.bin))?;
        if !ws.bin_path(&s.bin).exists() {
            bail!(
                "{} is not built ({}); run `cargo build` first",
                s.bin,
                ws.bin_path(&s.bin).display()
            );
        }
        daemons.push((s, crate_dir.as_path()));
    }

    // bound before any service is spawned, so a busy port fails fast
    let frontend = frontend_server().await?;
    tokio::spawn(serve_frontend(frontend));

    let mut running: Vec<Running> = vec![];
    let started = tokio::select! {
        r = start_all(&ws, &daemons, &mut running) => r,
        _ = shutdown_signal() => Err(anyhow!("interrupted during startup")),
    };

    if let Err(e) = &started {
        eprintln!("[run] {e:#}");
    } else {
        println!("[run] all services started; Ctrl-C to stop");
        tokio::select! {
            _ = shutdown_signal() => {}
            _ = watch_exits(&mut running) => println!("[run] all services exited"),
        }
    }

    shutdown(&mut running).await;
    started
}

async fn start_all(ws: &Workspace, daemons: &[(&ServiceEntry, &Path)], running: &mut Vec<Running>) -> Result<()> {
    for (s, crate_dir) in daemons {
        let child = spawn(ws, s, crate_dir)?;
        running.push(Running {
            bin: s.bin.clone(),
            child,
            exited: false,
        });

        if let Some(port) = listen_port(s)? {
            let deadline = Instant::now() + LISTEN_TIMEOUT;
            while TcpStream::connect(("127.0.0.1", port)).await.is_err() {
                let r = running.last_mut().expect("just pushed");
                if let Some(status) = r.child.try_wait()? {
                    r.exited = true;
                    bail!(
                        "[{}] exited with {status} before listening on :{port}",
                        s.bin
                    );
                }
                if Instant::now() > deadline {
                    bail!(
                        "[{}] did not listen on :{port} within {LISTEN_TIMEOUT:?}",
                        s.bin
                    );
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    Ok(())
}

fn workspace_dir() -> PathBuf {
    crate_dir().join("../..")
}

fn frontend_dir() -> PathBuf {
    workspace_dir().join("frontend")
}

async fn frontend_server() -> Result<Server<std::net::SocketAddr, RustlsAcceptor>> {
    let addr = std::env::var(FRONTEND_BIND_ADDRESS_ENV).unwrap_or_else(|_| FRONTEND_DEFAULT_BIND_ADDRESS.into());
    let dir = frontend_dir();
    if !dir.is_dir() {
        bail!("frontend dir {} not found", dir.display());
    }

    // the build may enable both rustls backends (gates use aws-lc-rs), so pick one explicitly
    let _ = rustls::crypto::ring::default_provider().install_default();
    let tls = RustlsConfig::from_pem_file(
        workspace_dir().join(FRONTEND_TLS_CERT),
        workspace_dir().join(FRONTEND_TLS_KEY),
    )
    .await
    .with_context(|| format!("frontend: load {FRONTEND_TLS_CERT} / {FRONTEND_TLS_KEY}"))?;

    let listener = std::net::TcpListener::bind(&addr).with_context(|| format!("frontend: bind {addr}"))?;
    listener.set_nonblocking(true)?;
    let server = axum_server::from_tcp_rustls(listener, tls)?;

    println!(
        "[frontend] serving {} on https://{addr}/",
        dir.canonicalize().unwrap_or(dir).display()
    );
    Ok(server)
}

/// Static files of `frontend/` (`index.html` for directories), never cached so edits show up on reload.
async fn serve_frontend(server: Server<std::net::SocketAddr, RustlsAcceptor>) {
    let app = Router::new()
        .fallback_service(ServeDir::new(frontend_dir()))
        .layer(SetResponseHeaderLayer::overriding(
            header::CACHE_CONTROL,
            HeaderValue::from_static("no-store"),
        ));

    if let Err(e) = server.serve(app.into_make_service()).await {
        eprintln!("[frontend] server stopped: {e}");
    }
}

/// Reports crashed services; returns when none is left running.
async fn watch_exits(running: &mut [Running]) {
    loop {
        for r in running.iter_mut().filter(|r| !r.exited) {
            if let Ok(Some(status)) = r.child.try_wait() {
                r.exited = true;
                eprintln!("[run] {} exited with {status}", r.bin);
            }
        }
        if running.iter().all(|r| r.exited) {
            return;
        }
        tokio::time::sleep(EXIT_CHECK_INTERVAL).await;
    }
}

async fn shutdown_signal() {
    let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

/// SIGTERM every service, SIGKILL whatever is still alive after `SHUTDOWN_TIMEOUT`.
async fn shutdown(running: &mut [Running]) {
    println!("[run] stopping services");
    for r in running.iter().filter(|r| !r.exited) {
        if let Some(pid) = r.child.id() {
            let _ = std::process::Command::new("kill")
                .args(["-TERM", &pid.to_string()])
                .status();
        }
    }

    let deadline = tokio::time::Instant::now() + SHUTDOWN_TIMEOUT;
    for r in running.iter_mut().filter(|r| !r.exited) {
        match tokio::time::timeout_at(deadline, r.child.wait()).await {
            Ok(Ok(status)) => println!("[run] {} stopped ({status})", r.bin),
            _ => {
                let _ = r.child.kill().await;
                println!("[run] {} killed", r.bin);
            }
        }
    }
}

fn spawn(ws: &Workspace, service: &ServiceEntry, crate_dir: &Path) -> Result<Child> {
    let mut child = Command::new(ws.bin_path(&service.bin))
        // CWD = crate dir: relative paths in configs (e.g. tls certs) resolve from there
        .current_dir(crate_dir)
        .env("ENV", "local") // cfg/ holds copies of the crates' local.json
        .env(CONFIG_FILE_ENV, &service.config)
        .env(
            "RUST_LOG",
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
        )
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // own process group: terminal Ctrl-C reaches only `run`, which then stops services itself
        .process_group(0)
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("spawn {}", service.bin))?;

    let prefix = format!("[{}]", service.bin);
    if let Some(out) = child.stdout.take() {
        tokio::spawn(forward(out, prefix.clone(), false));
    }
    if let Some(err) = child.stderr.take() {
        tokio::spawn(forward(err, prefix, true));
    }
    Ok(child)
}

async fn forward(stream: impl AsyncRead + Unpin, prefix: String, to_stderr: bool) {
    let mut lines = BufReader::new(stream).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        if to_stderr {
            eprintln!("{prefix} {line}");
        } else {
            println!("{prefix} {line}");
        }
    }
}

/// Locates binaries and their crate dirs via `cargo metadata` (read-only, nothing is built).
fn workspace() -> Result<Workspace> {
    let out = std::process::Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".into()))
        .current_dir(crate_dir())
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .output()
        .context("spawn cargo metadata")?;
    if !out.status.success() {
        bail!(
            "cargo metadata failed:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let meta: Value = serde_json::from_slice(&out.stdout).context("parse cargo metadata")?;

    let mut ws = Workspace {
        target_dir: meta["target_directory"]
            .as_str()
            .map(PathBuf::from)
            .ok_or_else(|| anyhow!("cargo metadata: no target_directory"))?,
        bins: HashMap::new(),
    };

    for pkg in meta["packages"].as_array().into_iter().flatten() {
        let crate_dir = Path::new(pkg["manifest_path"].as_str().unwrap_or_default())
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_default();

        for target in pkg["targets"].as_array().into_iter().flatten() {
            let is_bin = target["kind"]
                .as_array()
                .is_some_and(|k| k.iter().any(|k| k == "bin"));
            let Some(name) = target["name"].as_str().filter(|_| is_bin) else {
                continue;
            };
            ws.bins.insert(name.to_string(), crate_dir.clone());
        }
    }

    Ok(ws)
}
