// Closed-loop HTTP load generator for the api-gate. Fires requests from a fixed
// pool of concurrent workers for a fixed duration and reports throughput/latency.
//
// Defaults reproduce the sample QUERY /ping curl call as-is. The gate verifies
// PAYLOAD-SIGNATURE against the raw request body bytes only (no ts/nonce replay
// check), so the same signed body can be reused for every request.
//
// Usage:
//   cargo run --release --bin bench -- \
//     --url https://localhost:8443/ping/ \
//     --method QUERY \
//     --api-key 01a01500-98d5-71fc-9038-71acb78d61c4 \
//     --signature "nmivI6Z9v9bGpGnhIfUUxsTwH+u/WMEJOhy60tQG/IVFBgPb4DKUnA3Ub2R1HvgAOG2R2DaXcBgNSF8XHkn6Dw==" \
//     --body '{"Ts":"2026-08-25T12:49:50.264895652Z", "Method":"Ping", "Params":{}}' \
//     --concurrency 50 --duration 30

use std::{
    process::exit,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use reqwest::Method;
use tokio::sync::Mutex;

struct Args {
    url: String,
    method: String,
    api_key: Option<String>,
    signature: Option<String>,
    body: Option<String>,
    concurrency: usize,
    duration: Duration,
    insecure: bool,
}

impl Default for Args {
    fn default() -> Self {
        // Self {
        //     url: "https://localhost:8443/ping/".into(),
        //     method: "QUERY".into(),
        //     api_key: Some("01a01500-98d5-71fc-9038-71acb78d61c4".into()),
        //     signature: Some("nmivI6Z9v9bGpGnhIfUUxsTwH+u/WMEJOhy60tQG/IVFBgPb4DKUnA3Ub2R1HvgAOG2R2DaXcBgNSF8XHkn6Dw==".into()),
        //     body: Some(r#"{"Ts":"2026-08-25T12:49:50.264895652Z", "Method":"Ping", "Params":{}}"#.into()),
        //     concurrency: 50,
        //     duration: Duration::from_secs(100),
        //     insecure: true,
        // }

        Self {
            url: "https://localhost:8443/ping/".into(),
            method: "GET".into(),
            api_key: None,
            signature: None,
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
            "--method" => args.method = next(),
            "--api-key" => args.api_key = Some(next()),
            "--signature" => args.signature = Some(next()),
            "--body" => args.body = Some(next()),
            "--body-file" => args.body = Some(std::fs::read_to_string(next()).expect("failed to read --body-file")),
            "--concurrency" => args.concurrency = next().parse().expect("--concurrency must be a number"),
            "--duration" => args.duration = Duration::from_secs(next().parse().expect("--duration must be seconds")),
            "--insecure" => args.insecure = next().parse().expect("--insecure must be true/false"),
            "-h" | "--help" => {
                println!(
                    "usage: bench --url <url> [--method QUERY] [--api-key <uuid>] [--signature <base64>]\n\
                     \x20      [--body <json> | --body-file <path>] [--concurrency N] [--duration SECS] [--insecure true|false]"
                );
                exit(0);
            }
            other => {
                eprintln!("unknown flag: {other}");
                exit(1);
            }
        }
    }

    args
}

struct WorkerStats {
    latencies: Vec<Duration>,
    status_counts: std::collections::HashMap<String, u64>,
}

#[tokio::main]
async fn main() {
    let args = parse_args();

    let method = Method::from_bytes(args.method.as_bytes()).expect("invalid HTTP method");

    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(args.insecure)
        .build()
        .expect("failed to build http client");

    println!(
        "benchmarking {} {} | concurrency={} duration={}s",
        args.method,
        args.url,
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
        let api_key = args.api_key.clone();
        let signature = args.signature.clone();
        let body = args.body.clone();
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
                if let Some(api_key) = &api_key {
                    req = req.header("API-KEY", api_key);
                }
                if let Some(signature) = &signature {
                    req = req.header("PAYLOAD-SIGNATURE", signature);
                }
                if let Some(body) = &body {
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
    entries.sort_by(|a, b| b.1.cmp(&a.1));
    for (status, count) in entries {
        println!("  {status:<12} {count}");
    }
}
