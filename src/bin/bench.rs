// Closed-loop HTTP load generator for the api-gate. Fires requests from a fixed
// pool of concurrent workers for a fixed duration and reports throughput/latency.
//
// Two scenarios, selected with --scenario:
//   plain  (default) - GET with no body, no auth headers.
//   signed            - QUERY with a freshly built {"Ts":...,"Method":"Ping","Params":{}}
//                        body, ed25519-signed on every single request (signing cost is
//                        included in measured latency). Requires API_KEY (an entity uuid)
//                        and PRIVATE_KEY (that entity's bare-base64 PKCS#8 DER private key,
//                        same format as stored in the auth service's `entities` table) to
//                        be set as environment variables.
//
// Usage:
//   cargo run --release --bin bench -- --scenario plain --url https://localhost:8443/ping/ \
//     --concurrency 50 --duration 30
//
//   API_KEY=01a01500-98d5-71fc-9038-71acb78d61c4 \
//   PRIVATE_KEY=MC4CAQAwBQYDK2VwBCIEIBciTyz1f9ELrN3rZ+tcxxvQa14krR0sxY6HTJOLcWbK \
//     cargo run --release --bin bench -- --scenario signed --url https://localhost:8443/ping/ \
//     --concurrency 50 --duration 30

use std::{
    process::exit,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use base64::prelude::*;
use chrono::{SecondsFormat, Utc};
use ed25519_dalek::{Signer, SigningKey, pkcs8::DecodePrivateKey};
use reqwest::Method;
use tokio::sync::Mutex;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Scenario {
    Plain,
    Signed,
}

impl Scenario {
    fn parse(s: &str) -> Self {
        match s {
            "plain" => Scenario::Plain,
            "signed" => Scenario::Signed,
            other => {
                eprintln!("invalid --scenario {other}: expected \"plain\" or \"signed\"");
                exit(1);
            }
        }
    }

    fn default_method(self) -> &'static str {
        match self {
            Scenario::Plain => "GET",
            Scenario::Signed => "QUERY",
        }
    }
}

struct Args {
    url: String,
    method: Option<String>,
    scenario: Scenario,
    body: Option<String>,
    concurrency: usize,
    duration: Duration,
    insecure: bool,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            url: "https://localhost:8443/ping/".into(),
            method: None,
            scenario: Scenario::Plain,
            body: None,
            concurrency: 50,
            duration: Duration::from_secs(100),
            insecure: true,
        }
    }
}

fn parse_args() -> Args {
    let mut args = Args::default();
    let mut it = std::env::args().skip(1);

    while let Some(flag) = it.next() {
        let mut next = || {
            it.next().unwrap_or_else(|| {
                eprintln!("missing value for {flag}");
                exit(1);
            })
        };

        match flag.as_str() {
            "--url" => args.url = next(),
            "--method" => args.method = Some(next()),
            "--scenario" => args.scenario = Scenario::parse(&next()),
            "--body" => args.body = Some(next()),
            "--body-file" => args.body = Some(std::fs::read_to_string(next()).expect("failed to read --body-file")),
            "--concurrency" => args.concurrency = next().parse().expect("--concurrency must be a number"),
            "--duration" => args.duration = Duration::from_secs(next().parse().expect("--duration must be seconds")),
            "--insecure" => args.insecure = next().parse().expect("--insecure must be true/false"),
            "-h" | "--help" => {
                println!(
                    "usage: bench --scenario plain|signed [--url <url>] [--method <verb>]\n\
                     \x20      [--body <json> | --body-file <path>] (plain scenario only)\n\
                     \x20      [--concurrency N] [--duration SECS] [--insecure true|false]\n\
                     \n\
                     signed scenario requires API_KEY and PRIVATE_KEY environment variables."
                );
                exit(0);
            }
            other => {
                eprintln!("unknown flag: {other}");
                exit(1);
            }
        }
    }

    if args.scenario == Scenario::Signed && args.body.is_some() {
        eprintln!("--body/--body-file cannot be combined with --scenario signed: the signed body is always generated internally so it matches what gets signed");
        exit(1);
    }

    args
}

/// Key material for the signed scenario, loaded once from env vars at startup.
struct SignedAuth {
    api_key: String,
    signing_key: SigningKey,
}

impl SignedAuth {
    fn from_env() -> Self {
        let api_key = std::env::var("API_KEY").unwrap_or_else(|_| {
            eprintln!("API_KEY env var is required for --scenario signed");
            exit(1);
        });
        let private_key = std::env::var("PRIVATE_KEY").unwrap_or_else(|_| {
            eprintln!("PRIVATE_KEY env var is required for --scenario signed");
            exit(1);
        });
        let private_key_der = BASE64_STANDARD.decode(private_key.as_bytes()).unwrap_or_else(|e| {
            eprintln!("PRIVATE_KEY is not valid base64: {e}");
            exit(1);
        });
        let signing_key = SigningKey::from_pkcs8_der(&private_key_der).unwrap_or_else(|e| {
            eprintln!("PRIVATE_KEY is not a valid PKCS#8 DER ed25519 key: {e}");
            exit(1);
        });

        Self { api_key, signing_key }
    }

    /// Builds a fresh Ping envelope and signs it, for use on a single request.
    fn sign_fresh_ping_body(&self) -> (String, String) {
        let ts = Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true);
        let body = format!(r#"{{"Ts":"{ts}","Method":"Ping","Params":{{}}}}"#);
        let signature = self.signing_key.sign(body.as_bytes());
        let sig_b64 = BASE64_STANDARD.encode(signature.to_bytes());
        (body, sig_b64)
    }
}

struct WorkerStats {
    latencies: Vec<Duration>,
    status_counts: std::collections::HashMap<String, u64>,
}

#[tokio::main]
async fn main() {
    let args = parse_args();

    let method_str = args.method.clone().unwrap_or_else(|| args.scenario.default_method().to_string());
    let method = Method::from_bytes(method_str.as_bytes()).expect("invalid HTTP method");

    let signed_auth = match args.scenario {
        Scenario::Plain => None,
        Scenario::Signed => Some(Arc::new(SignedAuth::from_env())),
    };

    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(args.insecure)
        .build()
        .expect("failed to build http client");

    println!(
        "benchmarking {} {} | scenario={} concurrency={} duration={}s",
        method_str,
        args.url,
        if args.scenario == Scenario::Signed { "signed" } else { "plain" },
        args.concurrency,
        args.duration.as_secs()
    );

    let sent = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + args.duration;
    let results = Arc::new(Mutex::new(Vec::<WorkerStats>::new()));

    let mut handles = Vec::with_capacity(args.concurrency);
    for _ in 0..args.concurrency {
        let client = client.clone();
        let method = method.clone();
        let url = args.url.clone();
        let body = args.body.clone();
        let signed_auth = signed_auth.clone();
        let sent = sent.clone();
        let results = results.clone();

        handles.push(tokio::spawn(async move {
            let mut stats = WorkerStats {
                latencies: Vec::new(),
                status_counts: std::collections::HashMap::new(),
            };

            while Instant::now() < deadline {
                let start = Instant::now();
                let mut req = client
                    .request(method.clone(), &url)
                    .header("Content-Type", "application/json");

                if let Some(auth) = &signed_auth {
                    let (body, sig_b64) = auth.sign_fresh_ping_body();
                    req = req.header("API-KEY", &auth.api_key).header("PAYLOAD-SIGNATURE", sig_b64).body(body);
                } else if let Some(body) = &body {
                    req = req.body(body.clone());
                }

                let resp = req.send().await;

                let elapsed = start.elapsed();
                sent.fetch_add(1, Ordering::Relaxed);
                stats.latencies.push(elapsed);

                let label = match resp {
                    Ok(r) => r.status().as_u16().to_string(),
                    Err(e) => format!("error:{}", classify_reqwest_error(&e)),
                };
                *stats.status_counts.entry(label).or_insert(0) += 1;
            }

            results.lock().await.push(stats);
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    report(results.lock().await.drain(..).collect(), args.duration);
}

fn classify_reqwest_error(e: &reqwest::Error) -> &'static str {
    if e.is_timeout() {
        "timeout"
    } else if e.is_connect() {
        "connect"
    } else {
        "other"
    }
}

fn report(all: Vec<WorkerStats>, duration: Duration) {
    let mut latencies: Vec<Duration> = all
        .iter()
        .flat_map(|s| s.latencies.iter().copied())
        .collect();
    let mut status_counts: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    for s in &all {
        for (k, v) in &s.status_counts {
            *status_counts.entry(k.clone()).or_insert(0) += v;
        }
    }

    let total = latencies.len() as u64;
    if total == 0 {
        println!("no requests completed");
        return;
    }

    latencies.sort();
    let pct = |p: f64| -> Duration {
        let idx = ((total as f64 - 1.0) * p).round() as usize;
        latencies[idx]
    };
    let sum: Duration = latencies.iter().sum();
    let success = status_counts.get("200").copied().unwrap_or(0);

    println!();
    println!("total requests : {total}");
    println!(
        "throughput     : {:.1} req/s",
        total as f64 / duration.as_secs_f64()
    );
    println!(
        "success (200)  : {success} ({:.2}%)",
        success as f64 / total as f64 * 100.0
    );
    println!("latency avg    : {:?}", sum / total as u32);
    println!("latency p50    : {:?}", pct(0.50));
    println!("latency p90    : {:?}", pct(0.90));
    println!("latency p99    : {:?}", pct(0.99));
    println!("latency max    : {:?}", latencies.last().unwrap());
    println!();
    println!("status breakdown:");
    let mut entries: Vec<_> = status_counts.into_iter().collect();
    entries.sort_by_key(|a| std::cmp::Reverse(a.1));
    for (status, count) in entries {
        println!("  {status:<12} {count}");
    }
}
