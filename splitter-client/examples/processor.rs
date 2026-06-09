//! Phase-3 smoke: build a Dispatcher with a Processor whose Range logs every
//! lifecycle transition. Mirrors `examples/join.rs` in usage but drives the
//! full 5-step state machine via a reusable `RecordingRange`.
//!
//! Usage:
//!     cargo run --example processor -- <endpoint> <tenant/service> [region] [node]

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use splitter_client::processor::RecordingRange;
use splitter_client::{
    DispatcherBuilder, GrantId, Instance, InstanceId, Location, Ownership, Processor,
    QualifiedServiceName, RangeFactory, Shard,
};
use tonic::transport::Channel;

struct LoggingFactory {
    log: Arc<Mutex<Vec<String>>>,
}

impl RangeFactory for LoggingFactory {
    type Range = RecordingRange;

    fn build(&self, id: GrantId, shard: Shard, _o: Arc<Ownership>) -> Option<RecordingRange> {
        let label = format!("{shard}|{id}");
        Some(RecordingRange::new(
            label,
            Duration::from_millis(100),
            Duration::from_millis(500),
            self.log.clone(),
        ))
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,splitter_client=debug".into()),
        )
        .init();

    let mut args = std::env::args().skip(1);
    let endpoint = args
        .next()
        .unwrap_or_else(|| "http://localhost:50051".to_string());
    let svc_arg = args.next().ok_or("second arg must be tenant/service")?;
    let service = QualifiedServiceName::parse(&svc_arg)
        .ok_or("tenant/service format required, e.g. acme/widgets")?;
    let region = args.next().unwrap_or_else(|| "local".to_string());
    let node = args.next().unwrap_or_else(|| "local".to_string());

    tracing::info!(%endpoint, %service, %region, %node, "connecting");

    let channel = Channel::from_shared(endpoint)?.connect().await?;
    let instance = Instance {
        id: InstanceId::new(),
        location: Location::new(region, node),
        name: "splitter-rs/processor-example".into(),
        created: SystemTime::now(),
        endpoint: Default::default(),
    };

    let log = Arc::new(Mutex::new(Vec::new()));
    let factory = Arc::new(LoggingFactory { log: log.clone() });
    let processor = Processor::new(factory);

    let dispatcher = DispatcherBuilder::new(service, instance)
        .filter(processor.clone())
        .build(channel)
        .await?;

    tracing::info!(id = %dispatcher.id(), "dispatcher online");

    // Periodically flush the log so the user sees lifecycle transitions.
    let log_printer = log.clone();
    let printer_task = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let mut buf = log_printer.lock().expect("log poisoned");
            if !buf.is_empty() {
                for line in buf.drain(..) {
                    tracing::info!(target: "range", "{line}");
                }
            }
        }
    });

    tokio::signal::ctrl_c().await?;
    tracing::info!("SIGINT received, draining dispatcher (5s)");
    printer_task.abort();
    dispatcher.drain(Duration::from_secs(5)).await?;
    tracing::info!("done");
    Ok(())
}
