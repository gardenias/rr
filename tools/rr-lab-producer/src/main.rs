//! rr-lab-producer — drives a real Kafka-compatible broker (e.g. Redpanda in
//! Docker) with a deterministic, verifiable command stream for the rr
//! Kafka-ingestor path.
//!
//! Every message is a bincode-encoded [`KvRequest`], value-only (no message
//! key): the rr ingestor derives its idempotency key from the Kafka offset
//! itself, not from the message key. The workload is deterministic given the
//! global message index `i`, so verification replays can recompute the
//! expected keys and values:
//!
//! - every message: `Increment { key: "lab:counter", delta: 1 }` — a
//!   non-idempotent op, so the final counter value must equal the number of
//!   applied increments (exactly-once apply becomes observable);
//! - every 10th message (`i % 10 == 0`): additionally
//!   `Put { key: "lab:seq:{i}", value: "v{i}" }`.
//!
//! Sends are paced at `--rate` by a tokio interval. Delivery futures are
//! queued in a `JoinSet` and reaped as they complete — never awaited
//! one-by-one — and the producer is flushed with a bounded timeout before
//! exit. On completion or SIGTERM/SIGINT a final machine-greppable line is
//! printed: `PRODUCER-DONE increments=<N> puts=<M> errors=<E>`.
//!
//! Run:
//! ```bash
//! cargo run -p rr-lab-producer -- --brokers 127.0.0.1:9092 --rate 500
//! ```

use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use clap::Parser;
use kv_store::types::KvRequest;
use rdkafka::config::ClientConfig;
use rdkafka::producer::future_producer::OwnedDeliveryResult;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info, warn};

/// How long to wait for the broker to come up before giving up (lab startup
/// race: Redpanda may still be booting when the producer starts).
const STARTUP_CONNECT_TIMEOUT: Duration = Duration::from_secs(120);

/// Per-probe broker metadata timeout.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// How long `send` retries when the librdkafka queue is full before the
/// delivery future resolves to `QueueFull` (counted into `errors`).
const QUEUE_TIMEOUT: Duration = Duration::from_secs(1);

/// Bounded producer flush on shutdown.
const FLUSH_TIMEOUT: Duration = Duration::from_secs(10);

/// Extra bound for awaiting in-flight delivery futures after the flush.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Backpressure cap on outstanding delivery futures.
const MAX_IN_FLIGHT: usize = 10_000;

/// The single counter key every Increment lands on.
const COUNTER_KEY: &str = "lab:counter";

type DeliveryOutcome = std::result::Result<OwnedDeliveryResult, tokio::task::JoinError>;

#[derive(Parser)]
#[command(
    name = "rr-lab-producer",
    version,
    about = "Produce a deterministic, verifiable KvRequest command stream to Kafka"
)]
struct Cli {
    /// Comma-separated bootstrap brokers, e.g. "127.0.0.1:9092"
    #[arg(long)]
    brokers: String,

    /// Topic to produce to (group 1's input topic on the rr side)
    #[arg(long, default_value = "input_1")]
    topic: String,

    /// Messages per second (each message is one Increment; every 10th adds a Put)
    #[arg(long, default_value_t = 500)]
    rate: u64,

    /// Total messages to send; 0 = run until SIGTERM/SIGINT
    #[arg(long, default_value_t = 0)]
    total: u64,

    /// Progress report interval, in seconds
    #[arg(long, default_value_t = 5)]
    report_every_secs: u64,
}

#[derive(Default)]
struct Stats {
    sent_increments: u64,
    sent_puts: u64,
    errors: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();
    if cli.rate == 0 {
        bail!("--rate must be >= 1");
    }
    if cli.report_every_secs == 0 {
        bail!("--report-every-secs must be >= 1");
    }

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &cli.brokers)
        .set("acks", "all")
        // Producer-side idempotence keeps broker-level retries from creating
        // duplicate offsets, so any counter drift observed in the lab points
        // at the rr dedup path, not at the producer. The framework's
        // offset-based dedup remains the mechanism under test.
        .set("enable.idempotence", "true")
        .create()?;

    let mut stats = Stats::default();

    match wait_for_broker(&producer).await {
        Ok(true) => {}
        Ok(false) => {
            // Shutdown signal during the startup wait — clean exit.
            print_done(&stats);
            return Ok(());
        }
        Err(e) => {
            print_done(&stats);
            return Err(e);
        }
    }

    let mut in_flight = produce_loop(&cli, &producer, &mut stats).await;

    info!("flushing producer");
    let p = producer.clone();
    match tokio::task::spawn_blocking(move || p.flush(FLUSH_TIMEOUT)).await {
        Ok(Ok(())) => info!("producer flushed"),
        Ok(Err(e)) => warn!(error = %e, "flush failed"),
        Err(e) => warn!(error = %e, "flush task failed"),
    }
    drain_in_flight(&mut in_flight, &mut stats).await;

    print_done(&stats);
    Ok(())
}

/// Probe broker metadata with exponential backoff until it answers, the
/// startup deadline hits, or a shutdown signal arrives. Returns `Ok(false)`
/// when interrupted by a signal.
async fn wait_for_broker(producer: &FutureProducer) -> Result<bool> {
    let deadline = Instant::now() + STARTUP_CONNECT_TIMEOUT;
    let mut backoff = Duration::from_millis(500);
    loop {
        let p = producer.clone();
        let probe =
            tokio::task::spawn_blocking(move || p.client().fetch_metadata(None, PROBE_TIMEOUT));
        tokio::select! {
            res = probe => match res {
                Ok(Ok(_metadata)) => {
                    info!("broker reachable; starting production");
                    return Ok(true);
                }
                Ok(Err(e)) => warn!(error = %e, "broker not ready"),
                Err(e) => warn!(error = %e, "metadata probe task failed"),
            },
            _ = shutdown_signal() => return Ok(false),
        }
        if Instant::now() >= deadline {
            bail!(
                "broker not reachable within {:?} — giving up",
                STARTUP_CONNECT_TIMEOUT
            );
        }
        info!(
            retry_in_ms = backoff.as_millis() as u64,
            "retrying broker connection"
        );
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = shutdown_signal() => return Ok(false),
        }
        backoff = (backoff * 2).min(Duration::from_secs(5));
    }
}

/// Pace sends at `--rate` until `--total` is reached or a shutdown signal
/// arrives. Returns the still-outstanding delivery futures so the caller can
/// flush and drain them.
async fn produce_loop(
    cli: &Cli,
    producer: &FutureProducer,
    stats: &mut Stats,
) -> JoinSet<OwnedDeliveryResult> {
    let mut tick = tokio::time::interval(Duration::from_secs_f64(1.0 / cli.rate as f64));
    // No burst catch-up after a stall — keep the pace steady.
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut report = tokio::time::interval(Duration::from_secs(cli.report_every_secs));
    let mut in_flight: JoinSet<OwnedDeliveryResult> = JoinSet::new();
    let mut index: u64 = 0;

    loop {
        tokio::select! {
            _ = tick.tick() => {
                // Backpressure: stop sending when the broker goes away mid-run
                // and delivery futures pile up.
                while in_flight.len() >= MAX_IN_FLIGHT {
                    handle_delivery(in_flight.join_next().await, stats);
                }
                spawn_sends(&mut in_flight, producer, &cli.topic, index, stats);
                index += 1;
            }
            _ = report.tick() => {
                reap(&mut in_flight, stats);
                info!(
                    sent_increments = stats.sent_increments,
                    sent_puts = stats.sent_puts,
                    errors = stats.errors,
                    "progress"
                );
            }
            res = in_flight.join_next(), if !in_flight.is_empty() => {
                handle_delivery(res, stats);
            }
            _ = shutdown_signal() => {
                info!("shutdown signal received");
                break;
            }
        }
        if cli.total > 0 && index >= cli.total {
            info!(total = cli.total, "message total reached");
            break;
        }
    }
    in_flight
}

/// Enqueue the deterministic workload for global message index `i`: always an
/// Increment on `lab:counter`, plus a `lab:seq:{i}` Put on every 10th
/// message. Messages are value-only — no Kafka message key.
fn spawn_sends(
    in_flight: &mut JoinSet<OwnedDeliveryResult>,
    producer: &FutureProducer,
    topic: &str,
    index: u64,
    stats: &mut Stats,
) {
    let increment = KvRequest::Increment {
        key: COUNTER_KEY.to_string(),
        delta: 1,
    };
    if spawn_send(in_flight, producer, topic, &increment, stats) {
        stats.sent_increments += 1;
    }

    if index.is_multiple_of(10) {
        let put = KvRequest::Put {
            key: format!("lab:seq:{index}"),
            value: format!("v{index}").into_bytes(),
        };
        if spawn_send(in_flight, producer, topic, &put, stats) {
            stats.sent_puts += 1;
        }
    }
}

/// Encode `req` and queue its delivery future. Never awaits the future.
fn spawn_send(
    in_flight: &mut JoinSet<OwnedDeliveryResult>,
    producer: &FutureProducer,
    topic: &str,
    req: &KvRequest,
    stats: &mut Stats,
) -> bool {
    let payload = match bincode::serde::encode_to_vec(req, bincode::config::standard()) {
        Ok(p) => p,
        Err(e) => {
            stats.errors += 1;
            error!(error = %e, "failed to encode KvRequest");
            return false;
        }
    };
    let producer = producer.clone();
    let topic = topic.to_string();
    in_flight.spawn(async move {
        let record = FutureRecord::<[u8], _>::to(topic.as_str()).payload(&payload);
        producer.send(record, QUEUE_TIMEOUT).await
    });
    true
}

/// Reap every completed delivery future without blocking.
fn reap(in_flight: &mut JoinSet<OwnedDeliveryResult>, stats: &mut Stats) {
    while let Some(res) = in_flight.try_join_next() {
        handle_delivery(Some(res), stats);
    }
}

/// Await remaining delivery futures after the flush, with a bounded timeout.
async fn drain_in_flight(in_flight: &mut JoinSet<OwnedDeliveryResult>, stats: &mut Stats) {
    while !in_flight.is_empty() {
        match tokio::time::timeout(DRAIN_TIMEOUT, in_flight.join_next()).await {
            Ok(res) => handle_delivery(res, stats),
            Err(_) => {
                warn!(
                    pending = in_flight.len(),
                    "timed out draining delivery futures"
                );
                break;
            }
        }
    }
}

fn handle_delivery(res: Option<DeliveryOutcome>, stats: &mut Stats) {
    match res {
        Some(Ok(Ok((partition, offset)))) => {
            debug!(partition, offset, "delivered");
        }
        Some(Ok(Err((e, _msg)))) => {
            stats.errors += 1;
            warn!(error = %e, "delivery failed");
        }
        Some(Err(e)) => {
            stats.errors += 1;
            warn!(error = %e, "delivery task failed");
        }
        None => {}
    }
}

/// Resolve on SIGINT or SIGTERM.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Final machine-greppable summary line for lab harnesses.
fn print_done(stats: &Stats) {
    println!(
        "PRODUCER-DONE increments={} puts={} errors={}",
        stats.sent_increments, stats.sent_puts, stats.errors
    );
}
