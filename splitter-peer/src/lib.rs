//! Thin gRPC-server harness for splitter consumers that need to handle
//! peer-to-peer forwarded requests.
//!
//! The splitter wire protocol (see `pkg/model/proxy.go`) delivers cross-peer
//! requests to a consumer's *own* gRPC server, whose endpoint is published
//! in the `Register` message's `Instance.endpoint` field. Users bring their
//! own `tonic` services; this crate just wraps the bind/serve/shutdown
//! plumbing and the "endpoint advertised in Register" bookkeeping.
//!
//! # Example
//!
//! ```ignore
//! use std::net::SocketAddr;
//! use splitter_peer::PeerServerBuilder;
//! use tonic::transport::Server;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let router = Server::builder()
//!     .add_service(my_service::MyServiceServer::new(MyImpl));
//!
//! let peer = PeerServerBuilder::new("0.0.0.0:0".parse()?)
//!     .advertise("10.0.0.5:50052".into())
//!     .serve(router)
//!     .await?;
//!
//! // peer.endpoint is what you put into Instance.endpoint when joining:
//! println!("peer serving at {}", peer.endpoint);
//! # peer.shutdown().await?;
//! # Ok(()) }
//! ```

use std::io;
use std::net::SocketAddr;

use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::server::Router;

/// Builder for a peer gRPC server.
pub struct PeerServerBuilder {
    bind: SocketAddr,
    advertised: Option<String>,
}

impl PeerServerBuilder {
    pub fn new(bind: SocketAddr) -> Self {
        Self {
            bind,
            advertised: None,
        }
    }

    /// Override the endpoint string published in `Instance.endpoint`. If the
    /// bind address is `0.0.0.0` (or any wildcard), advertise the
    /// externally-reachable address here. Defaults to the resolved local
    /// address prefixed with `http://`.
    pub fn advertise(mut self, endpoint: impl Into<String>) -> Self {
        self.advertised = Some(endpoint.into());
        self
    }

    /// Bind, spawn, and return a running server handle. The caller supplies a
    /// fully-constructed `tonic::transport::server::Router` (typically via
    /// `Server::builder().add_service(...)`). Graceful shutdown is triggered
    /// via [`PeerServer::shutdown`].
    pub async fn serve(self, router: Router) -> io::Result<PeerServer> {
        let listener = TcpListener::bind(self.bind).await?;
        let local_addr = listener.local_addr()?;
        let endpoint = self
            .advertised
            .unwrap_or_else(|| format!("http://{local_addr}"));
        let incoming = TcpListenerStream::new(listener);

        let (tx, rx) = oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            router
                .serve_with_incoming_shutdown(incoming, async move {
                    let _ = rx.await;
                })
                .await
        });

        Ok(PeerServer {
            endpoint,
            local_addr,
            shutdown: Some(tx),
            task: Some(task),
        })
    }
}

/// Running peer server handle. Drop aborts the accept task; use
/// [`PeerServer::shutdown`] for graceful termination.
pub struct PeerServer {
    /// The string to publish in `Instance.endpoint` when joining splitter.
    pub endpoint: String,
    /// Actual bound address (useful when bind was `0.0.0.0:0`).
    pub local_addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<(), tonic::transport::Error>>>,
}

impl PeerServer {
    /// Fire the shutdown signal and await orderly termination of the server
    /// task. Idempotent — calling twice returns `Ok(())` the second time.
    pub async fn shutdown(mut self) -> Result<(), ServeError> {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            match task.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(ServeError::Transport(e)),
                Err(e) if e.is_cancelled() => Ok(()),
                Err(e) => Err(ServeError::Join(e.to_string())),
            }
        } else {
            Ok(())
        }
    }
}

impl Drop for PeerServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    #[error("tonic transport: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("server task: {0}")]
    Join(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tonic::transport::Server;

    #[tokio::test]
    async fn serve_advertises_local_addr_by_default() {
        // Empty router is legal — the server just refuses to route anything
        // but the port still opens.
        let router = Server::builder().add_routes(tonic::service::Routes::default());
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let peer = PeerServerBuilder::new(addr).serve(router).await.unwrap();
        assert!(peer.endpoint.starts_with("http://127.0.0.1:"));
        assert_eq!(peer.local_addr.ip().to_string(), "127.0.0.1");
        peer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn explicit_advertise_override() {
        let router = Server::builder().add_routes(tonic::service::Routes::default());
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let peer = PeerServerBuilder::new(addr)
            .advertise("splitter-rs.internal:50052")
            .serve(router)
            .await
            .unwrap();
        assert_eq!(peer.endpoint, "splitter-rs.internal:50052");
        peer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn drop_before_shutdown_still_aborts() {
        let router = Server::builder().add_routes(tonic::service::Routes::default());
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let peer = PeerServerBuilder::new(addr).serve(router).await.unwrap();
        drop(peer);
        // Should not deadlock; give tokio a beat to run the abort.
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
