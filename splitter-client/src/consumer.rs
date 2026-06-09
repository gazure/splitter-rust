//! Consumer client: drives the `ConsumerService.Join` bidi stream and owns the
//! per-grant `Ownership` state machine.
//!
//! On each inbound `ClientMessage` the driver mutates its grant table and
//! closes the appropriate `Ownership` latches, then broadcasts a
//! [`GrantEvent`] to downstream consumers (the Dispatcher in Phase 3 or the
//! example in Phase 1/2). User-side signal latches (`loader.load`,
//! `unloader.unload`, `request_revoke`) are observed by small per-grant
//! watcher tasks that turn them into outbound `ClientMessage`s.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use splitter_proto::pb;
use splitter_proto::pb::consumer_service_client::ConsumerServiceClient;
use splitter_proto::pb::{client_message, consumer_message, join_message, ClientMessage};
use splitter_proto::pb::{ConsumerMessage, JoinMessage};
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinHandle as TaskHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tonic::Streaming;
use tracing::{debug, info, warn};

use crate::cluster::ClusterMap;
use crate::error::{ClientError, Result};
use crate::ids::{DomainKeyName, GrantId, GrantState, Instance, QualifiedServiceName, Shard};
use crate::latch::{Latch, LatchReader};
use crate::ownership::Ownership;
use crate::session;
use crate::wrap::{grant_state_from_i32, timestamp_to_system_time};

/// User-facing options for `ConsumerClient::join`.
#[derive(Clone, Debug, Default)]
pub struct ConsumerOptions {
    pub key_names: Vec<DomainKeyName>,
    pub capacity_limit: u64,
}

impl ConsumerOptions {
    pub fn with_key_name(mut self, name: DomainKeyName) -> Self {
        self.key_names.push(name);
        self
    }
    pub fn with_capacity_limit(mut self, limit: u64) -> Self {
        self.capacity_limit = limit;
        self
    }
}

/// Emitted to broadcast subscribers whenever the grant table changes.
#[derive(Clone, Debug)]
pub enum GrantEvent {
    Assigned {
        id: GrantId,
        shard: Shard,
        ownership: Arc<Ownership>,
    },
    Removed {
        id: GrantId,
    },
}

/// Handle to a running Join stream.
pub struct JoinHandle {
    pub cluster: watch::Receiver<Arc<ClusterMap>>,
    pub grants: broadcast::Receiver<GrantEvent>,
    closed: LatchReader,
    shutdown: Latch,
    task: Option<TaskHandle<Result<()>>>,
}

impl JoinHandle {
    pub fn closed(&self) -> LatchReader {
        self.closed.clone()
    }

    /// Subscribe a new receiver to grant events.
    pub fn subscribe_grants(&self) -> broadcast::Receiver<GrantEvent> {
        self.grants.resubscribe()
    }

    /// Signal the driver to send Deregister + session Closed and exit.
    pub async fn shutdown(mut self) -> Result<()> {
        self.shutdown.close();
        if let Some(task) = self.task.take() {
            match task.await {
                Ok(res) => res,
                Err(e) if e.is_cancelled() => Ok(()),
                Err(e) => Err(ClientError::SessionClosed(e.to_string())),
            }
        } else {
            Ok(())
        }
    }
}

#[derive(Clone)]
pub struct ConsumerClient {
    inner: ConsumerServiceClient<Channel>,
}

impl ConsumerClient {
    pub fn new(channel: Channel) -> Self {
        Self {
            inner: ConsumerServiceClient::new(channel),
        }
    }

    /// Start a Join stream.
    pub async fn join(
        &self,
        consumer: Instance,
        service: QualifiedServiceName,
        opts: ConsumerOptions,
    ) -> Result<JoinHandle> {
        let (outbound_tx, outbound_rx) = mpsc::channel::<JoinMessage>(64);
        let (cluster_tx, cluster_rx) = watch::channel(Arc::new(ClusterMap::empty()));
        let (grants_tx, grants_rx) = broadcast::channel::<GrantEvent>(256);
        let closed = Latch::new();
        let shutdown = Latch::new();

        // Pre-queue Establish + Register into the outbound buffer before we
        // initiate the RPC. Server-side `ReadEstablish` runs a 5s timer the
        // moment the stream opens (`session/server.go:18`); having the first
        // item ready avoids any scheduling-delay window.
        let session_id = uuid::Uuid::new_v4().to_string();
        outbound_tx
            .send(session::establish_frame(
                consumer.clone().into(),
                session_id,
            ))
            .await
            .map_err(|_| ClientError::SessionClosed("outbound closed before Establish".into()))?;
        outbound_tx
            .send(build_register(consumer.clone(), service.clone(), &opts))
            .await
            .map_err(|_| ClientError::SessionClosed("outbound closed before Register".into()))?;

        let request_stream = ReceiverStream::new(outbound_rx);
        let mut client = self.inner.clone();
        let response = client.join(request_stream).await?;
        let inbound: Streaming<JoinMessage> = response.into_inner();

        let driver = Driver {
            outbound: outbound_tx,
            inbound,
            cluster: cluster_tx,
            grants_tx,
            closed: closed.clone(),
            shutdown: shutdown.clone(),
            grants: HashMap::new(),
        };

        let task = tokio::spawn(driver.run());

        Ok(JoinHandle {
            cluster: cluster_rx,
            grants: grants_rx,
            closed: closed.reader(),
            shutdown,
            task: Some(task),
        })
    }
}

struct ActiveGrant {
    pb_grant: pb::Grant,
    ownership: Arc<Ownership>,
    /// Whether the coordinator has asked us to revoke this grant. Tracked so
    /// Extend only refreshes non-revoked grants (matches `workpool.go:186`).
    revoked: bool,
    /// Background helper tasks (load/unload/request_revoke watchers, expiration
    /// timer). Aborted when the grant is removed.
    helpers: Vec<TaskHandle<()>>,
}

struct Driver {
    outbound: mpsc::Sender<JoinMessage>,
    inbound: Streaming<JoinMessage>,
    cluster: watch::Sender<Arc<ClusterMap>>,
    grants_tx: broadcast::Sender<GrantEvent>,
    closed: Latch,
    shutdown: Latch,
    grants: HashMap<GrantId, ActiveGrant>,
}

impl Driver {
    async fn run(mut self) -> Result<()> {
        let mut heartbeat = tokio::time::interval(Duration::from_secs(60));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        heartbeat.tick().await;

        let result = loop {
            tokio::select! {
                _ = self.shutdown.closed(), if !self.shutdown.is_closed() => {
                    let _ = self.outbound.send(wrap_client(client_message::Msg::Deregister(
                        client_message::Deregister {},
                    ))).await;
                    let _ = self.outbound.send(session::closed_frame("client shutdown")).await;
                    break Ok(());
                }
                _ = heartbeat.tick() => {
                    if self.outbound.send(session::heartbeat_frame()).await.is_err() {
                        break Err(ClientError::SessionClosed("outbound dropped".into()));
                    }
                }
                msg = self.inbound.next() => {
                    match msg {
                        Some(Ok(join)) => {
                            if let Some(ttl) = self.handle_inbound(join).await? {
                                let delay = session::heartbeat_delay(ttl, SystemTime::now());
                                heartbeat = tokio::time::interval(delay);
                                heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                                heartbeat.tick().await;
                            }
                        }
                        Some(Err(status)) => break Err(ClientError::Transport(status)),
                        None => break Err(ClientError::SessionClosed("stream ended".into())),
                    }
                }
            }
        };

        // Clean up all grants on exit.
        let ids: Vec<GrantId> = self.grants.keys().cloned().collect();
        for id in ids {
            self.remove_grant(id);
        }
        self.closed.close();
        result
    }

    async fn handle_inbound(&mut self, join: JoinMessage) -> Result<Option<SystemTime>> {
        let Some(msg) = join.msg else {
            return Ok(None);
        };
        match msg {
            join_message::Msg::Session(session_msg) => {
                match session::classify(&session_msg) {
                    Some(session::SessionEvent::Established { ttl }) => {
                        info!(?ttl, "session established");
                        Ok(Some(ttl))
                    }
                    Some(session::SessionEvent::HeartbeatAck { ttl }) => {
                        debug!(?ttl, "heartbeat ack");
                        Ok(Some(ttl))
                    }
                    Some(session::SessionEvent::Closed { error }) => {
                        Err(ClientError::SessionClosed(error))
                    }
                    None => Ok(None),
                }
            }
            join_message::Msg::Consumer(ConsumerMessage { msg: Some(sub) }) => {
                match sub {
                    consumer_message::Msg::Cluster(cm) => {
                        let current = self.cluster.borrow().clone();
                        match current.apply(cm) {
                            Ok(next) => {
                                let _ = self.cluster.send(Arc::new(next));
                            }
                            Err(e) => warn!(error = %e, "failed to apply cluster update"),
                        }
                        Ok(None)
                    }
                    consumer_message::Msg::Client(client) => {
                        self.handle_client_message(client).await;
                        Ok(None)
                    }
                }
            }
            join_message::Msg::Consumer(ConsumerMessage { msg: None }) => Ok(None),
        }
    }

    async fn handle_client_message(&mut self, msg: ClientMessage) {
        let Some(variant) = msg.msg else { return };
        match variant {
            client_message::Msg::Assign(a) => {
                for g in a.grants {
                    self.on_assign(g);
                }
            }
            client_message::Msg::Promote(p) => {
                for g in p.grants {
                    self.on_promote(g);
                }
            }
            client_message::Msg::Revoke(r) => {
                for g in r.grants {
                    self.on_revoke(g);
                }
            }
            client_message::Msg::Notify(n) => {
                self.on_notify(n);
            }
            client_message::Msg::Extend(e) => {
                self.on_extend(e);
            }
            // Consumer→coordinator only; ignore if ever seen on the inbound side.
            client_message::Msg::Register(_)
            | client_message::Msg::Deregister(_)
            | client_message::Msg::Released(_)
            | client_message::Msg::Update(_)
            | client_message::Msg::Status(_) => {}
        }
    }

    fn on_assign(&mut self, g: pb::Grant) {
        let Some(shard) = g.shard.clone().and_then(|s| Shard::try_from(s).ok()) else {
            warn!(id = %g.id, "Assign with missing/invalid shard");
            return;
        };
        let id = GrantId(g.id.clone());
        let state = grant_state_from_i32(g.state);
        let expiration = g
            .lease
            .as_ref()
            .map(timestamp_to_system_time)
            .unwrap_or_else(|| SystemTime::now() + Duration::from_secs(30));

        // If re-assigning an existing grant (reconnect replay) drop the old
        // entry entirely — v1 has no reconnect, but the server may still send
        // duplicates which we should handle idempotently.
        if self.grants.contains_key(&id) {
            self.remove_grant(id.clone());
        }

        let ownership = Ownership::new(state, expiration);
        let expired = ownership.expired.reader();
        let helpers = vec![
            spawn_signal_watcher(expired.clone(), ownership.loader.load.reader(), {
                let g = g.clone();
                let out = self.outbound.clone();
                move || send_update(out, g, pb::GrantState::AllocatedLoaded)
            }),
            spawn_signal_watcher(expired.clone(), ownership.unloader.unload.reader(), {
                let g = g.clone();
                let out = self.outbound.clone();
                move || send_update(out, g, pb::GrantState::RevokedUnloaded)
            }),
            spawn_signal_watcher(
                expired,
                ownership.request_revoke_signal.reader(),
                {
                    let g = g.clone();
                    let out = self.outbound.clone();
                    move || send_revoke(out, g)
                },
            ),
            spawn_expiration_timer(ownership.clone()),
        ];

        self.grants.insert(
            id.clone(),
            ActiveGrant {
                pb_grant: g,
                ownership: ownership.clone(),
                revoked: matches!(
                    state,
                    GrantState::Revoked | GrantState::RevokedUnloaded
                ),
                helpers,
            },
        );

        let _ = self.grants_tx.send(GrantEvent::Assigned {
            id,
            shard,
            ownership,
        });
    }

    fn on_promote(&mut self, g: pb::Grant) {
        let id = GrantId(g.id.clone());
        if let Some(entry) = self.grants.get_mut(&id) {
            entry.pb_grant = g;
            entry.ownership.activate();
        } else {
            warn!(id = %id, "Promote for unknown grant");
        }
    }

    fn on_revoke(&mut self, g: pb::Grant) {
        let id = GrantId(g.id.clone());
        let new_expiration = g.lease.as_ref().map(timestamp_to_system_time);
        if let Some(entry) = self.grants.get_mut(&id) {
            entry.revoked = true;
            entry.pb_grant = g;
            if let Some(exp) = new_expiration {
                entry.ownership.set_expiration(exp);
            }
            entry.ownership.revoke();
        } else {
            warn!(id = %id, "Revoke for unknown grant");
        }
    }

    fn on_notify(&mut self, n: client_message::Notify) {
        let Some(target) = n.target else { return };
        let Some(update) = n.update else { return };
        let id = GrantId(target.id);
        let update_state = grant_state_from_i32(update.state);
        let Some(entry) = self.grants.get(&id) else {
            debug!(id = %id, "Notify for unknown grant, ignoring");
            return;
        };
        match update_state {
            // Counterpart now loaded → let our unloader proceed.
            GrantState::AllocatedLoaded => entry.ownership.counterpart_loaded(),
            // Counterpart now unloaded → let our loader proceed.
            GrantState::RevokedUnloaded => entry.ownership.counterpart_unloaded(),
            other => warn!(?other, "Notify with unexpected update state"),
        }
    }

    fn on_extend(&mut self, e: client_message::Extend) {
        let Some(new_lease) = e.lease.as_ref().map(timestamp_to_system_time) else {
            return;
        };
        for entry in self.grants.values() {
            if !entry.revoked {
                entry.ownership.set_expiration(new_lease);
            }
        }
    }

    fn remove_grant(&mut self, id: GrantId) {
        if let Some(entry) = self.grants.remove(&id) {
            entry.ownership.expire();
            for h in entry.helpers {
                h.abort();
            }
            let _ = self.grants_tx.send(GrantEvent::Removed { id });
        }
    }
}

// ---------- Per-grant watcher tasks ----------

/// Race a user-driven `signal` latch against the grant's `expired` latch.
/// If `signal` fires first, invoke `on_signal` (typically to push an outbound
/// message); on expiration, exit silently. Used by the three lifecycle
/// watchers — load, unload, request-revoke — that all share this shape.
fn spawn_signal_watcher<F, Fut>(
    expired: LatchReader,
    signal: LatchReader,
    on_signal: F,
) -> TaskHandle<()>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        tokio::select! {
            _ = signal.closed() => {
                on_signal().await;
            }
            _ = expired.closed() => {}
        }
    })
}

async fn send_update(
    outbound: mpsc::Sender<JoinMessage>,
    grant: pb::Grant,
    state: pb::GrantState,
) {
    let updated = pb::Grant {
        state: state as i32,
        ..grant
    };
    let _ = outbound
        .send(wrap_client(client_message::Msg::Update(
            client_message::Update {
                grant: Some(updated),
            },
        )))
        .await;
}

async fn send_revoke(outbound: mpsc::Sender<JoinMessage>, grant: pb::Grant) {
    let _ = outbound
        .send(wrap_client(client_message::Msg::Revoke(
            client_message::Revoke {
                grants: vec![grant],
            },
        )))
        .await;
}

fn spawn_expiration_timer(ownership: Arc<Ownership>) -> TaskHandle<()> {
    tokio::spawn(async move {
        loop {
            if ownership.expired.is_closed() {
                return;
            }
            let deadline = ownership.expiration();
            let now = SystemTime::now();
            let sleep_for = deadline.duration_since(now).unwrap_or(Duration::ZERO);

            if sleep_for.is_zero() {
                ownership.expire();
                return;
            }
            tokio::select! {
                _ = tokio::time::sleep(sleep_for) => {
                    // Re-check under the current expiration, which may have been extended.
                    let deadline = ownership.expiration();
                    if SystemTime::now() >= deadline {
                        ownership.expire();
                        return;
                    }
                }
                _ = ownership.expiration_changed.notified() => {
                    // Loop around and sleep for the new duration.
                }
            }
        }
    })
}

// ---------- Outbound message builders ----------

fn build_register(
    consumer: Instance,
    service: QualifiedServiceName,
    opts: &ConsumerOptions,
) -> JoinMessage {
    let register = pb::client_message::Register {
        consumer: Some(consumer.into()),
        service: Some(service.into()),
        domains: Vec::new(),
        active: Vec::new(),
        options: Some(pb::client_message::register::Options {
            names: opts.key_names.iter().cloned().map(Into::into).collect(),
            capacity_limit: opts.capacity_limit,
        }),
    };
    wrap_client(pb::client_message::Msg::Register(register))
}

fn wrap_client(msg: pb::client_message::Msg) -> JoinMessage {
    JoinMessage {
        msg: Some(join_message::Msg::Consumer(ConsumerMessage {
            msg: Some(consumer_message::Msg::Client(ClientMessage {
                msg: Some(msg),
            })),
        })),
    }
}

