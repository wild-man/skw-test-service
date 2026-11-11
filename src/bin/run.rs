//! `cargo run --bin run` — starts every service from `cfg/services.json` and streams their output
//! prefixed with `[bin]`. Ctrl-C / SIGTERM stops all spawned services.
//!
//! Spawns services in `services.json` order, waiting for the port of `listens` ones, since later
//! services may depend on them (e.g. stream consumers need topics created by the gates).
//! Developer's responsibility beforehand: infra (iggy, postgres, redis, rabbitmq) running,
//! binaries built (`cargo build`), migrations applied.
//!
//! Needs no cargo: binaries are taken from `SKW_BIN_DIR` (required, e.g. `../../target/debug`;
//! relative to the CWD of `run`), configs from `./cfg`. Every service runs with the CWD of `run`,
//! so relative paths in `cfg/` configs (tls certs, `frontend/`) resolve from there.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use skw_lib_shared::prelude::consts::CONFIG_FILE_ENV;
use skw_test_service::{ServiceEntry, listen_port, load_services};
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::net::TcpStream;
use tokio::process::{Child, Command};
use tokio::signal::unix::{SignalKind, signal};

/// directory holding the service binaries (required)
const BIN_DIR_ENV: &str = "SKW_BIN_DIR";
const LISTEN_TIMEOUT: Duration = Duration::from_secs(30);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const EXIT_CHECK_INTERVAL: Duration = Duration::from_millis(500);

struct Running {
    bin: String,
    child: Child,
    exited: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let bin_dir = bin_dir()?;
    let services = load_services()?;
    // every binary checked before anything is spawned
    let daemons = services
        .iter()
        .map(|s| Ok((s, bin_path(&bin_dir, &s.bin)?)))
        .collect::<Result<Vec<_>>>()?;

    let mut running: Vec<Running> = vec![];
    let started = tokio::select! {
        r = start_all(&daemons, &mut running) => r,
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

async fn start_all(daemons: &[(&ServiceEntry, PathBuf)], running: &mut Vec<Running>) -> Result<()> {
    for (s, bin) in daemons {
        let child = spawn(s, bin)?;
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

fn spawn(service: &ServiceEntry, bin: &Path) -> Result<Child> {
    // CWD inherited from `run`: relative paths in cfg/ configs (e.g. tls certs) resolve from there
    let mut child = Command::new(bin)
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

/// `SKW_BIN_DIR`, made absolute; `run` refuses to start without it.
fn bin_dir() -> Result<PathBuf> {
    let Some(dir) = std::env::var_os(BIN_DIR_ENV).filter(|d| !d.is_empty()) else {
        bail!("{BIN_DIR_ENV} is not set; point it at the dir of the built binaries, e.g. {BIN_DIR_ENV}=../../target/debug");
    };
    let dir = PathBuf::from(dir);
    let abs = dir
        .canonicalize()
        .with_context(|| format!("{BIN_DIR_ENV} {}", dir.display()))?;
    if !abs.is_dir() {
        bail!("{BIN_DIR_ENV} {} is not a directory", dir.display());
    }
    Ok(abs)
}

/// The executable for a `services.json` `bin`: a plain file name (no path) of an executable file in `bin_dir`.
fn bin_path(bin_dir: &Path, bin: &str) -> Result<PathBuf> {
    let is_plain_name = !bin.is_empty() && Path::new(bin).file_name().is_some_and(|n| n == bin);
    if !is_plain_name || bin.starts_with('.') {
        bail!("services.json: invalid bin {bin:?}, expected a binary file name");
    }
    let path = bin_dir.join(bin);
    let meta = std::fs::metadata(&path).with_context(|| format!("services.json: bin {bin}: {} not found", path.display()))?;
    if !meta.is_file() || meta.permissions().mode() & 0o111 == 0 {
        bail!(
            "services.json: bin {bin}: {} is not an executable file",
            path.display()
        );
    }
    Ok(path)
}
