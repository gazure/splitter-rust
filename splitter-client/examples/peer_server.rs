//! Demonstrates the full Phase-5 integration: stand up a local peer gRPC
//! server, advertise its endpoint in the Dispatcher's consumer instance, and
//! run a Processor that logs each grant lifecycle.
//!
//! In a real service the `Router` passed to `PeerServerBuilder::serve` would
//! register the user's application services (the ones other consumers
//! forward to via `splitter_client::handle`). For this demo we register no
//! services — the port still opens and the endpoint is wired through so the
//! plumbing is visible.
//!
//! Usage:
//!     cargo run --example peer_server -- <endpoint> <tenant/service> [peer_port] [region] [node]

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use splitter_client::processor::RecordingRange;
use splitter_client::{
    DispatcherBuilder, GrantId, Instance, InstanceId, Location, Ownership, Processor,
    QualifiedServiceName, RangeFactory, Shard,
};
use splitter_peer::PeerServerBuilder;
use tonic::transport::{Channel, Server};

struct LoggingFactory {
    log: Arc<Mutex<Vec<String>>>,
}

impl RangeFactory for LoggingFactory {
    type Range = RecordingRange;
    fn build(&self, id: GrantId, shard: Shard, _o: Arc<Ownership>) -> Option<RecordingRange> {
        Some(RecordingRange::new(
            format!("{shard}|{id}"),
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
    let peer_port: u16 = args
        .next()
        .as_deref()
        .unwrap_or("0")
        .parse()
        .map_err(|e| format!("bad peer_port: {e}"))?;
    let region = args.next().unwrap_or_else(|| "local".to_string());
    let node = args.next().unwrap_or_else(|| "local".to_string());

    // --- 1. Stand up the peer server first so we know its endpoint ---
    let bind: SocketAddr = format!("127.0.0.1:{peer_port}").parse()?;
    let router = Server::builder().add_routes(tonic::service::Routes::default());
    let peer = PeerServerBuilder::new(bind).serve(router).await?;
    tracing::info!(endpoint = %peer.endpoint, local = %peer.local_addr, "peer server up");

    // --- 2. Connect to the coordinator and build the Dispatcher ---
    let channel = Channel::from_shared(endpoint.clone())?.connect().await?;
    let instance = Instance {
        id: InstanceId::new(),
        location: Location::new(region, node),
        name: "splitter-rs/peer-example".into(),
        created: SystemTime::now(),
        endpoint: peer.endpoint.clone().into(),
    };

    let log = Arc::new(Mutex::new(Vec::new()));
    let processor = Processor::new(Arc::new(LoggingFactory { log: log.clone() }));
    let dispatcher = DispatcherBuilder::new(service, instance)
        .filter(processor.clone())
        .build(channel)
        .await?;

    tracing::info!(id = %dispatcher.id(), "dispatcher online");

    // --- 3. Periodically flush lifecycle log ---
    let log_printer = log.clone();
    let printer_task = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let mut buf = log_printer.lock().expect("log poisoned");
            for line in buf.drain(..) {
                tracing::info!(target: "range", "{line}");
            }
        }
    });

    tokio::signal::ctrl_c().await?;
    tracing::info!("SIGINT received; draining");
    printer_task.abort();
    dispatcher.drain(Duration::from_secs(5)).await?;
    peer.shutdown().await?;
    Ok(())
}
