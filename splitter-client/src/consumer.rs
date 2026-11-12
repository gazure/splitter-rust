//! Consumer sessions, recoverable assignment snapshots, and grant lifecycle signals.
//!
//! Broadcast grant events are observational and can lag. Ownership consumers
//! should reconcile [`JoinHandle::assignments`], as the dispatcher does.

use crate::{
    cluster::ClusterMap,
    error::{ClientError, Result},
    ids::{DomainKeyName, GrantId, GrantState, Instance, QualifiedServiceName, Shard},
    latch::{Latch, LatchReader},
    ownership::Ownership,
    session,
    wrap::{grant_state_from_i32, timestamp_to_system_time},
};
use futures::{Stream, StreamExt};
use splitter_proto::pb::{
    self, ClientMessage, ConsumerMessage, JoinMessage, client_message, consumer_message,
    consumer_service_client::ConsumerServiceClient, join_message,
};
use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{
    sync::{broadcast, mpsc, watch},
    task::JoinHandle as TaskHandle,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tracing::{debug, info, warn};

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

/// Current local grant assignment, including the identity of its ownership instance.
#[derive(Clone, Debug)]
pub struct GrantAssignment {
    /// Domain, region, and key bounds covered by the grant.
    pub shard: Shard,
    /// Shared lifecycle signals. Pointer identity distinguishes replacements.
    pub ownership: Arc<Ownership>,
}

/// Recoverable set of current assignments. Removed or replaced instances are expired.
pub type AssignmentSnapshot = Arc<HashMap<GrantId, GrantAssignment>>;

/// Observational events. A lagging subscriber must reconcile the assignment snapshot.
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

/// Handle to a running Join stream. Dropping it force-stops the driver.
pub struct JoinHandle {
    pub cluster: watch::Receiver<Arc<ClusterMap>>,
    pub grants: broadcast::Receiver<GrantEvent>,
    /// Current assignments, including those delivered before this handle was returned.
    pub assignments: watch::Receiver<AssignmentSnapshot>,
    closed: LatchReader,
    shutdown: Latch,
    task: Option<TaskHandle<Result<()>>>,
}

impl JoinHandle {
    pub fn closed(&self) -> LatchReader {
        self.closed.clone()
    }
    /// Subscribes to future observational events; use `assignments` for ownership.
    pub fn subscribe_grants(&self) -> broadcast::Receiver<GrantEvent> {
        self.grants.resubscribe()
    }
    /// Deregisters and keeps processing handover until all grants are released or expire.
    ///
    /// Direct consumer users must finish their handlers with `Ownership::release`.
    /// Use a timeout to bound this operation. Cancelling it aborts the driver.
    pub async fn shutdown(mut self) -> Result<()> {
        self.shutdown.close();
        let result = match self.task.as_mut() {
            Some(task) => match task.await {
                Ok(result) => result,
                Err(error) => Err(ClientError::SessionClosed(error.to_string())),
            },
            None => Ok(()),
        };
        self.task.take();
        result
    }
}

impl Drop for JoinHandle {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[derive(Clone)]
pub struct ConsumerClient {
    inner: ConsumerServiceClient<Channel>,
}

type Inbound = Pin<Box<dyn Stream<Item = std::result::Result<JoinMessage, tonic::Status>> + Send>>;
struct Transport {
    outbound: mpsc::Sender<JoinMessage>,
    inbound: Inbound,
}

impl ConsumerClient {
    pub fn new(channel: Channel) -> Self {
        Self {
            inner: ConsumerServiceClient::new(channel),
        }
    }

    /// Starts a session for `consumer` and `service` with `opts`.
    /// Returns connection errors before spawning the driver.
    pub async fn join(
        &self,
        consumer: Instance,
        service: QualifiedServiceName,
        opts: ConsumerOptions,
    ) -> Result<JoinHandle> {
        let transport =
            open_session(self.inner.clone(), consumer, service, opts, Vec::new()).await?;
        let (driver, mut handle) = Driver::new(transport);
        handle.task = Some(tokio::spawn(driver.run()));
        Ok(handle)
    }
}

async fn open_session(
    mut client: ConsumerServiceClient<Channel>,
    consumer: Instance,
    service: QualifiedServiceName,
    opts: ConsumerOptions,
    active: Vec<pb::Grant>,
) -> Result<Transport> {
    let (outbound, receiver) = mpsc::channel(64);
    // The server's establishment deadline starts as soon as the stream opens.
    outbound
        .send(session::establish_frame(
            consumer.clone().into(),
            uuid::Uuid::new_v4().to_string(),
        ))
        .await
        .map_err(|_| ClientError::Cancelled)?;
    outbound
        .send(build_register(consumer, service, &opts, active))
        .await
        .map_err(|_| ClientError::Cancelled)?;
    let response = client.join(ReceiverStream::new(receiver)).await?;
    Ok(Transport {
        outbound,
        inbound: Box::pin(response.into_inner()),
    })
}

struct ActiveGrant {
    pb_grant: pb::Grant,
    ownership: Arc<Ownership>,
    revoked: bool,
    watcher: TaskHandle<()>,
}

impl Drop for ActiveGrant {
    fn drop(&mut self) {
        self.ownership.expire();
        self.watcher.abort();
    }
}

#[derive(Clone, Copy)]
enum Action {
    Load,
    Unload,
    Revoke,
    Release,
    Expired,
}
struct GrantAction {
    id: GrantId,
    ownership: Arc<Ownership>,
    action: Action,
}

struct Driver {
    transport: Transport,
    cluster: watch::Sender<Arc<ClusterMap>>,
    grants_tx: broadcast::Sender<GrantEvent>,
    assignments: watch::Sender<AssignmentSnapshot>,
    closed: Latch,
    shutdown: Latch,
    grants: HashMap<GrantId, ActiveGrant>,
    actions_tx: mpsc::UnboundedSender<GrantAction>,
    actions: mpsc::UnboundedReceiver<GrantAction>,
}

impl Drop for Driver {
    fn drop(&mut self) {
        self.grants.clear();
        self.assignments.send_replace(Arc::new(HashMap::new()));
        self.cluster.send_replace(Arc::new(ClusterMap::empty()));
        self.closed.close();
    }
}

impl Driver {
    fn new(transport: Transport) -> (Self, JoinHandle) {
        let (cluster, cluster_rx) = watch::channel(Arc::new(ClusterMap::empty()));
        let (grants_tx, grants) = broadcast::channel(256);
        let (assignments, assignments_rx) = watch::channel(Arc::new(HashMap::new()));
        let (actions_tx, actions) = mpsc::unbounded_channel();
        let closed = Latch::new();
        let shutdown = Latch::new();
        let handle = JoinHandle {
            cluster: cluster_rx,
            grants,
            assignments: assignments_rx,
            closed: closed.reader(),
            shutdown: shutdown.clone(),
            task: None,
        };
        (
            Self {
                transport,
                cluster,
                grants_tx,
                assignments,
                closed,
                shutdown,
                grants: HashMap::new(),
                actions_tx,
                actions,
            },
            handle,
        )
    }

    async fn send(&mut self, message: JoinMessage) -> Result<()> {
        tokio::time::timeout(
            Duration::from_secs(5),
            self.transport.outbound.send(message),
        )
        .await
        .map_err(|_| ClientError::SessionClosed("outbound stalled".into()))?
        .map_err(|_| ClientError::SessionClosed("outbound dropped".into()))
    }

    async fn run(mut self) -> Result<()> {
        let mut heartbeat = heartbeat_timer(None);
        let mut draining = false;
        loop {
            if draining && self.grants.is_empty() {
                self.send(session::closed_frame("client shutdown")).await?;
                return Ok(());
            }
            tokio::select! {
                biased;
                _ = self.shutdown.closed(), if !draining => {
                    draining = true;
                    self.send(wrap_client(client_message::Msg::Deregister(client_message::Deregister {}))).await?;
                }
                Some(action) = self.actions.recv() => self.on_action(action).await?,
                msg = self.transport.inbound.next() => {
                    match msg {
                        Some(Ok(join)) => {
                            if let Some(ttl) = self.handle_inbound(join).await? {
                                heartbeat = heartbeat_timer(Some(ttl));
                            }
                        }
                        Some(Err(status)) => return Err(ClientError::Transport(status)),
                        None => return Err(ClientError::SessionClosed("stream ended".into())),
                    }
                }
                _ = heartbeat.tick() => self.send(session::heartbeat_frame()).await?,
            }
        }
    }

    async fn handle_inbound(&mut self, join: JoinMessage) -> Result<Option<SystemTime>> {
        let Some(msg) = join.msg else {
            return Ok(None);
        };
        match msg {
            join_message::Msg::Session(session_msg) => match session::classify(&session_msg) {
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
            },
            join_message::Msg::Consumer(ConsumerMessage { msg: Some(sub) }) => match sub {
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
                    self.handle_client_message(client).await?;
                    Ok(None)
                }
            },
            join_message::Msg::Consumer(ConsumerMessage { msg: None }) => Ok(None),
        }
    }

    async fn handle_client_message(&mut self, msg: ClientMessage) -> Result<()> {
        let Some(variant) = msg.msg else {
            return Ok(());
        };
        match variant {
            client_message::Msg::Assign(a) => {
                for g in a.grants {
                    self.on_assign(g).await?;
                }
            }
            client_message::Msg::Promote(p) => {
                for g in p.grants {
                    self.on_promote(g);
                }
            }
            client_message::Msg::Revoke(r) => {
                for g in r.grants {
                    self.on_revoke(g).await?;
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
        Ok(())
    }
    async fn on_assign(&mut self, g: pb::Grant) -> Result<()> {
        let id = GrantId(g.id.clone());
        if self.shutdown.is_closed() {
            // A replay does not prove that the existing handler has stopped.
            // Keep its current lifecycle until it releases or its lease expires.
            if self.grants.contains_key(&id) {
                return Ok(());
            }
            return self.send_released(g).await;
        }
        let Some(shard) = g.shard.clone().and_then(|s| Shard::try_from(s).ok()) else {
            return Err(ClientError::InvalidMessage(
                "Assign without a valid shard".into(),
            ));
        };
        let expiration = g
            .lease
            .as_ref()
            .map(timestamp_to_system_time)
            .ok_or_else(|| ClientError::InvalidMessage("Assign without a lease".into()))?;
        let state = grant_state_from_i32(g.state);
        if let Some(entry) = self.grants.get_mut(&id) {
            // Replay must preserve application state, but cannot revive an expired
            // instance or reverse a terminal transition.
            if !entry.ownership.expired.is_closed()
                && SystemTime::now() < entry.ownership.expiration()
                && entry.pb_grant.shard == g.shard
                && !entry.revoked
                && matches!(
                    state,
                    GrantState::Allocated | GrantState::AllocatedLoaded | GrantState::Active
                )
            {
                let current = grant_state_from_i32(entry.pb_grant.state);
                if state == current
                    || (current == GrantState::AllocatedLoaded && state == GrantState::Allocated)
                    || (matches!(current, GrantState::Allocated | GrantState::AllocatedLoaded)
                        && state == GrantState::Active)
                {
                    entry.ownership.set_expiration(expiration);
                    if state == GrantState::Active {
                        entry.ownership.activate();
                    }
                    let mut replay = g;
                    if current == GrantState::AllocatedLoaded && state == GrantState::Allocated {
                        replay.state = pb::GrantState::AllocatedLoaded as i32;
                    }
                    entry.pb_grant = replay.clone();
                    let revoke_requested = entry.ownership.request_revoke_signal.is_closed();
                    if current == GrantState::AllocatedLoaded && state == GrantState::Allocated {
                        self.send(wrap_client(client_message::Msg::Update(
                            client_message::Update {
                                grant: Some(replay.clone()),
                            },
                        )))
                        .await?;
                    }
                    if revoke_requested {
                        self.send(wrap_client(client_message::Msg::Revoke(
                            client_message::Revoke {
                                grants: vec![replay],
                            },
                        )))
                        .await?;
                    }
                    return Ok(());
                }
            }
            self.remove_grant(&id);
        }
        if expiration <= SystemTime::now() {
            return Ok(());
        }
        let ownership = Ownership::new(state, expiration);
        let watcher = spawn_grant_watcher(id.clone(), ownership.clone(), self.actions_tx.clone());
        self.grants.insert(
            id.clone(),
            ActiveGrant {
                pb_grant: g,
                ownership: ownership.clone(),
                revoked: matches!(state, GrantState::Revoked | GrantState::RevokedUnloaded),
                watcher,
            },
        );
        self.assignments.send_modify(|snapshot| {
            Arc::make_mut(snapshot).insert(
                id.clone(),
                GrantAssignment {
                    shard: shard.clone(),
                    ownership: ownership.clone(),
                },
            );
        });
        let _ = self.grants_tx.send(GrantEvent::Assigned {
            id,
            shard,
            ownership,
        });
        Ok(())
    }

    fn on_promote(&mut self, g: pb::Grant) {
        if let Some(entry) = self.grants.get_mut(&GrantId(g.id.clone())) {
            if entry.revoked
                || entry.ownership.expired.is_closed()
                || SystemTime::now() >= entry.ownership.expiration()
            {
                return;
            }
            if let Some(expiration) = g.lease.as_ref().map(timestamp_to_system_time) {
                entry.ownership.set_expiration(expiration);
            }
            entry.pb_grant = g;
            entry.ownership.activate();
        }
    }

    async fn on_revoke(&mut self, mut g: pb::Grant) -> Result<()> {
        if let Some(entry) = self.grants.get_mut(&GrantId(g.id.clone())) {
            if entry.ownership.expired.is_closed()
                || SystemTime::now() >= entry.ownership.expiration()
            {
                return Ok(());
            }
            let already_unloaded =
                grant_state_from_i32(entry.pb_grant.state) == GrantState::RevokedUnloaded;
            entry.revoked = true;
            if let Some(expiration) = g.lease.as_ref().map(timestamp_to_system_time) {
                entry.ownership.set_expiration(expiration);
            }
            if already_unloaded {
                g.state = pb::GrantState::RevokedUnloaded as i32;
            }
            entry.pb_grant = g.clone();
            entry.ownership.revoke();
            if already_unloaded {
                self.send(wrap_client(client_message::Msg::Update(
                    client_message::Update { grant: Some(g) },
                )))
                .await?;
            }
        }
        Ok(())
    }

    fn on_notify(&mut self, n: client_message::Notify) {
        let (Some(target), Some(update)) = (n.target, n.update) else {
            return;
        };
        if let Some(entry) = self.grants.get(&GrantId(target.id)) {
            match grant_state_from_i32(update.state) {
                GrantState::AllocatedLoaded => entry.ownership.counterpart_loaded(),
                GrantState::RevokedUnloaded => entry.ownership.counterpart_unloaded(),
                _ => {}
            }
        }
    }

    fn on_extend(&mut self, e: client_message::Extend) {
        let Some(expiration) = e.lease.as_ref().map(timestamp_to_system_time) else {
            return;
        };
        for entry in self.grants.values_mut() {
            if !entry.revoked
                && !entry.ownership.expired.is_closed()
                && SystemTime::now() < entry.ownership.expiration()
            {
                entry.ownership.set_expiration(expiration);
                entry.pb_grant.lease = e.lease;
            }
        }
    }

    async fn on_action(&mut self, signal: GrantAction) -> Result<()> {
        let Some(entry) = self.grants.get_mut(&signal.id) else {
            return Ok(());
        };
        if !Arc::ptr_eq(&entry.ownership, &signal.ownership) {
            return Ok(());
        }
        let state = grant_state_from_i32(entry.pb_grant.state);
        match signal.action {
            Action::Expired => {
                self.remove_grant(&signal.id);
            }
            Action::Release => {
                let grant = entry.pb_grant.clone();
                let unexpired = SystemTime::now() < entry.ownership.expiration();
                self.remove_grant(&signal.id);
                if unexpired {
                    self.send_released(grant).await?;
                }
            }
            Action::Load | Action::Unload => {
                if entry.ownership.expired.is_closed()
                    || SystemTime::now() >= entry.ownership.expiration()
                {
                    return Ok(());
                }
                let next = match (signal.action, state) {
                    (Action::Load, GrantState::Allocated) => pb::GrantState::AllocatedLoaded,
                    (Action::Unload, GrantState::Revoked) => pb::GrantState::RevokedUnloaded,
                    _ => return Ok(()),
                };
                entry.pb_grant.state = next as i32;
                let grant = entry.pb_grant.clone();
                self.send(wrap_client(client_message::Msg::Update(
                    client_message::Update { grant: Some(grant) },
                )))
                .await?;
            }
            Action::Revoke => {
                if !entry.revoked && !entry.ownership.expired.is_closed() {
                    let grant = entry.pb_grant.clone();
                    self.send(wrap_client(client_message::Msg::Revoke(
                        client_message::Revoke {
                            grants: vec![grant],
                        },
                    )))
                    .await?;
                }
            }
        }
        Ok(())
    }

    async fn send_released(&mut self, grant: pb::Grant) -> Result<()> {
        self.send(wrap_client(client_message::Msg::Released(
            client_message::Released {
                grants: vec![grant],
            },
        )))
        .await
    }

    fn remove_grant(&mut self, id: &GrantId) {
        if self.grants.remove(id).is_some() {
            self.assignments.send_modify(|snapshot| {
                Arc::make_mut(snapshot).remove(id);
            });
            let _ = self.grants_tx.send(GrantEvent::Removed { id: id.clone() });
        }
    }
}

fn heartbeat_timer(ttl: Option<SystemTime>) -> tokio::time::Interval {
    let delay = ttl.map_or(Duration::from_secs(60), |ttl| {
        session::heartbeat_delay(ttl, SystemTime::now())
    });
    let mut timer = tokio::time::interval_at(tokio::time::Instant::now() + delay, delay);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    timer
}

fn spawn_grant_watcher(
    id: GrantId,
    ownership: Arc<Ownership>,
    actions: mpsc::UnboundedSender<GrantAction>,
) -> TaskHandle<()> {
    tokio::spawn(async move {
        let (mut loaded, mut unloaded, mut requested) = (false, false, false);
        loop {
            let action = tokio::select! {
                biased;
                _ = ownership.released.closed() => Action::Release,
                _ = ownership.wait_for_expiration() => Action::Expired,
                _ = ownership.loader.load.closed(), if !loaded => { loaded = true; Action::Load },
                _ = ownership.unloader.unload.closed(), if !unloaded => { unloaded = true; Action::Unload },
                _ = ownership.request_revoke_signal.closed(), if !requested => { requested = true; Action::Revoke },
            };
            if actions
                .send(GrantAction {
                    id: id.clone(),
                    ownership: ownership.clone(),
                    action,
                })
                .is_err()
                || matches!(action, Action::Release | Action::Expired)
            {
                return;
            }
        }
    })
}

fn build_register(
    consumer: Instance,
    service: QualifiedServiceName,
    opts: &ConsumerOptions,
    active: Vec<pb::Grant>,
) -> JoinMessage {
    let register = pb::client_message::Register {
        consumer: Some(consumer.into()),
        service: Some(service.into()),
        domains: Vec::new(),
        active,
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

#[cfg(test)]
mod tests;

#[cfg(test)]
mod shutdown_tests;
