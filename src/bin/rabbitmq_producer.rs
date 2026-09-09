// Standalone RabbitMQ test producer: publishes JSON task messages onto
// ping-mq-service's queue, for manually exercising that consumer without
// wiring up the full skw-lib-shared config/AppContext machinery.
//
// Usage:
//   cargo run --bin rabbitmq_producer -- --message "hello" --count 3
//   RABBITMQ_URL=amqp://rabbitmq:Secret123!@localhost:5672 cargo run --bin rabbitmq_producer

use std::process::exit;

use lapin::{
    BasicProperties, Connection, ConnectionProperties,
    options::{BasicPublishOptions, ConfirmSelectOptions, QueueDeclareOptions},
    types::FieldTable,
};
use skw_lib_task_protos::ping::PingTask;

// Must match skw_get_queue_name("ping-task") from libs/shared, i.e.
// "ping-task-{skw-lib-shared's CARGO_PKG_VERSION}". Update if that version changes.
const QUEUE_NAME: &str = "ping-task-0.20.0";

struct Args {
    url: String,
    message: String,
    count: u32,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            url: std::env::var("RABBITMQ_URL").unwrap_or_else(|_| "amqp://rabbitmq:Secret123!@localhost:5672".into()),
            message: "hello from test producer".into(),
            count: 1,
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
            "--message" => args.message = next(),
            "--count" => args.count = next().parse().expect("--count must be a number"),
            "-h" | "--help" => {
                println!(
                    "usage: rabbitmq_producer [--url <amqp-url>] [--message <text>] [--count N]\n\
                     \x20      publishes to the hardcoded queue \"{QUEUE_NAME}\""
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = parse_args();

    let conn = Connection::connect(&args.url, ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await?;

    channel
        .queue_declare(
            QUEUE_NAME.into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;

    for i in 0..args.count {
        let task = PingTask {
            message: format!("{} #{}", args.message, i + 1),
        };
        let payload = serde_json::to_string(&task)?;

        let confirm = channel
            .basic_publish(
                "".into(), // default exchange: routes directly to the queue named by routing_key
                QUEUE_NAME.into(),
                BasicPublishOptions::default(),
                payload.as_bytes(),
                BasicProperties::default().with_delivery_mode(2), // persistent
            )
            .await?
            .await?;

        if confirm.is_ack() {
            println!("published #{}: {}", i + 1, payload);
        } else {
            eprintln!("broker did not confirm publish #{}: {:?}", i + 1, confirm);
        }
    }

    Ok(())
}
