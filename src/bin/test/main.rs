//! `cargo run --bin test [-- <libtest args>]` — runs the integration tests of this binary via
//! `cargo test --bin test`. Services must already be running (`cargo run --bin run`).
//!
//! Tests talk to the system only over HTTP / WebSocket through `api-gate` or `ws`.
//! Shared scenarios run once per gate provider (`gate::Gate`): filter with `-- ::api::` / `-- ::ws::`.

#[cfg(test)]
mod common;
#[cfg(test)]
mod gate;
#[cfg(test)]
mod keys;
#[cfg(test)]
mod tests;

use std::net::TcpStream;
use std::process::{Command, ExitCode};
use std::time::Duration;

use skw_test_service::{crate_dir, find_service, listen_port};

const GATES: &[&str] = &["api-gate", "ws"];

fn main() -> ExitCode {
    for gate in GATES {
        let port = find_service(gate).and_then(|s| listen_port(&s));
        match port {
            Ok(Some(port)) if TcpStream::connect_timeout(&([127, 0, 0, 1], port).into(), Duration::from_secs(1)).is_ok() => {}
            Ok(_) => {
                eprintln!("[test] {gate} is not running; start services first: cargo run --bin run");
                return ExitCode::FAILURE;
            }
            Err(e) => {
                eprintln!("[test] {e:#}");
                return ExitCode::FAILURE;
            }
        }
    }

    let mut cmd = Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".into()));
    // `cargo run` exports per-package vars; build scripts (ring) watch them → endless rebuilds
    for (key, _) in std::env::vars_os() {
        let k = key.to_string_lossy();
        if k.starts_with("CARGO_PKG_")
            || k.starts_with("CARGO_MANIFEST_")
            || matches!(
                &*k,
                "CARGO_CRATE_NAME" | "CARGO_BIN_NAME" | "CARGO_PRIMARY_PACKAGE" | "OUT_DIR"
            )
        {
            cmd.env_remove(&key);
        }
    }

    let status = cmd
        .current_dir(crate_dir())
        .args(["test", "--bin", "test", "--"])
        .args(std::env::args().skip(1))
        .status();

    match status {
        Ok(s) if s.success() => ExitCode::SUCCESS,
        Ok(_) => ExitCode::FAILURE,
        Err(e) => {
            eprintln!("[test] spawn cargo test: {e}");
            ExitCode::FAILURE
        }
    }
}
