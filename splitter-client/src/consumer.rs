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
use futures::{Stream, StreamExt, future::BoxFuture};
use splitter_proto::pb::{
    self, ClientMessage, ConsumerMessage, JoinMessage, client_message, consumer_message,
    consumer_service_client::ConsumerServiceClient, join_message,
};
use std::{
    collections::HashMap,
    future::Future,
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

/// Reconnection policy. Grants retain only their existing leases during outages.
#[derive(Clone, Debug)]
pub struct ReconnectOptions {
    /// Maximum continuous disconnect before draining. `None` retries indefinitely.
    /// Includes connection attempts and receipt of a valid cluster snapshot.
    pub timeout: Option<Duration>,
    /// Delay between connection attempts. Must be nonzero.
    pub backoff: Duration,
}

impl Default for ReconnectOptions {
    fn default() -> Self {
        Self {
            timeout: None,
            backoff: Duration::from_secs(1),
        }
    }
}

/// User-facing options for `ConsumerClient::join`.
#[derive(Clone, Debug)]
#[must_use]
pub struct ConsumerOptions {
    pub key_names: Vec<DomainKeyName>,
    pub capacity_limit: u64,
    /// Maximum duration of each connection attempt, including session establishment
    /// when reconnecting. Defaults to 30 seconds. Must be nonzero.
    pub join_timeout: Duration,
    /// Maximum graceful drain duration before remaining grants are force-closed.
    /// Applies to explicit shutdown and disconnect timeout. Defaults to 30 seconds.
    pub drain_timeout: Duration,
    /// Retries session failures by default, including initial connection failures,
    /// RPC rejection, server closure, and invalid protocol messages.
    /// Set to `None` to return initial connection errors and stop on session failure.
    pub reconnect: Option<ReconnectOptions>,
}

impl Default for ConsumerOptions {
    fn default() -> Self {
        Self {
            key_names: Vec::new(),
            capacity_limit: 0,
            join_timeout: Duration::from_secs(30),
            drain_timeout: Duration::from_secs(30),
            reconnect: Some(ReconnectOptions::default()),
        }
    }
}

impl ConsumerOptions {
    /// Sets the per-attempt connection timeout. Zero durations are rejected by `join`.
    pub fn with_join_timeout(mut self, timeout: Duration) -> Self {
        self.join_timeout = timeout;
        self
    }

    pub fn with_key_name(mut self, name: DomainKeyName) -> Self {
        self.key_names.push(name);
        self
    }
    pub fn with_capacity_limit(mut self, limit: u64) -> Self {
        self.capacity_limit = limit;
        self
    }
    /// Configures recovery. Zero or unrepresentable durations are rejected by `join`.
    pub fn with_reconnect(mut self, options: ReconnectOptions) -> Self {
        self.reconnect = Some(options);
        self
    }
    /// Stops retrying after `timeout` without a valid cluster snapshot, then drains.
    pub fn with_disconnect_timeout(mut self, timeout: Duration) -> Self {
        self.reconnect
            .get_or_insert_with(ReconnectOptions::default)
            .timeout = Some(timeout);
        self
    }

    /// Sets the maximum graceful shutdown duration, including offline drain.
    pub fn with_drain_timeout(mut self, timeout: Duration) -> Self {
        self.drain_timeout = timeout;
        self
    }

    /// Disables recovery and returns initial connection errors directly from `join`.
    pub fn without_reconnect(mut self) -> Self {
        self.reconnect = None;
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

/// Handle to a running consumer. Dropping it force-stops the driver.
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
    /// Deregisters and processes handover until grants finish or the drain timeout expires.
    ///
    /// While disconnected, revokes grants locally and stops reconnecting.
    /// Direct consumer users must finish their handlers with `Ownership::release`.
    /// Cancelling this future aborts the driver. Returns `DrainTimeout` if forced to stop.
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
type Connector = Arc<dyn Fn(Vec<pb::Grant>) -> BoxFuture<'static, Result<Transport>> + Send + Sync>;

const SESSION_ESTABLISH_TIMEOUT: Duration = Duration::from_secs(30);

enum SessionProgress {
    Continue,
    Renew(SystemTime),
    Closed,
    StreamEnded,
    DeadlineElapsed,
}

impl ConsumerClient {
    pub fn new(channel: Channel) -> Self {
        Self {
            inner: ConsumerServiceClient::new(channel),
        }
    }

    /// Starts a consumer for `consumer` and `service` with `opts`.
    ///
    /// With recovery enabled, returns a handle before connecting. The driver retries
    /// until shutdown or the optional disconnect timeout; this is not a readiness signal.
    /// Without recovery, waits for the initial Join RPC and returns its errors directly.
    /// Invalid timeout settings are rejected before starting the driver.
    pub async fn join(
        &self,
        consumer: Instance,
        service: QualifiedServiceName,
        opts: ConsumerOptions,
    ) -> Result<JoinHandle> {
        let inner = self.inner.clone();
        let options = opts.clone();
        let connector: Connector = Arc::new(move |active| {
            Box::pin(open_session(
                inner.clone(),
                consumer.clone(),
                service.clone(),
                options.clone(),
                active,
            ))
        });
        start_join(connector, opts).await
    }
}

async fn start_join(connector: Connector, opts: ConsumerOptions) -> Result<JoinHandle> {
    let valid_duration = |duration: Duration| {
        !duration.is_zero() && tokio::time::Instant::now().checked_add(duration).is_some()
    };
    if !valid_duration(opts.join_timeout)
        || !valid_duration(opts.drain_timeout)
        || opts.reconnect.as_ref().is_some_and(|r| {
            !valid_duration(r.backoff) || r.timeout.is_some_and(|timeout| !valid_duration(timeout))
        })
    {
        return Err(ClientError::InvalidMessage(
            "consumer durations must be nonzero and representable".into(),
        ));
    }
    let transport = if opts.reconnect.is_none() {
        let deadline = tokio::time::Instant::now() + opts.join_timeout;
        Some(tokio::select! {
            biased;
            () = tokio::time::sleep_until(deadline) => return Err(ClientError::JoinTimeout),
            result = async { connector(Vec::new()).await } => result?,
        })
    } else {
        None
    };
    let (mut driver, mut handle) = Driver::new(transport);
    driver.join_timeout = opts.join_timeout;
    driver.drain_timeout = opts.drain_timeout;
    driver.reconnect = opts.reconnect.map(|options| (options, connector));
    handle.task = Some(tokio::spawn(driver.run()));
    Ok(handle)
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
    Ok(Transport {
        outbound,
        inbound: Box::pin(
            client
                .join(ReceiverStream::new(receiver))
                .await?
                .into_inner(),
        ),
    })
}

struct ActiveGrant {
    pb_grant: pb::Grant,
    ownership: Arc<Ownership>,
    revoked: bool,
    confirmed: bool,
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
    transport: Option<Transport>,
    cluster: watch::Sender<Arc<ClusterMap>>,
    cluster_needs_snapshot: bool,
    grants_tx: broadcast::Sender<GrantEvent>,
    assignments: watch::Sender<AssignmentSnapshot>,
    closed: Latch,
    shutdown: Latch,
    grants: HashMap<GrantId, ActiveGrant>,
    lease: Option<prost_types::Timestamp>,
    actions_tx: mpsc::UnboundedSender<GrantAction>,
    actions: mpsc::UnboundedReceiver<GrantAction>,
    reconnect: Option<(ReconnectOptions, Connector)>,
    join_timeout: Duration,
    drain_timeout: Duration,
    disconnect_deadline: Option<tokio::time::Instant>,
    drain_deadline: Option<tokio::time::Instant>,
    deregistered: bool,
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
    fn new(transport: Option<Transport>) -> (Self, JoinHandle) {
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
                cluster_needs_snapshot: true,
                grants_tx,
                assignments,
                closed,
                shutdown,
                grants: HashMap::new(),
                lease: None,
                actions_tx,
                actions,
                reconnect: None,
                join_timeout: Duration::from_secs(30),
                drain_timeout: Duration::from_secs(30),
                disconnect_deadline: None,
                drain_deadline: None,
                deregistered: false,
            },
            handle,
        )
    }

    async fn send(&mut self, message: JoinMessage) -> Result<()> {
        let Some(transport) = &self.transport else {
            // Local completion still advances state while there is no coordinator.
            return Ok(());
        };
        tokio::time::timeout(Duration::from_secs(5), transport.outbound.send(message))
            .await
            .map_err(|_| ClientError::SessionClosed("outbound stalled".into()))?
            .map_err(|_| ClientError::SessionClosed("outbound dropped".into()))
    }

    async fn run(mut self) -> Result<()> {
        match self.run_sessions().await {
            Err(ClientError::Cancelled) if self.shutdown.is_closed() => {
                self.drain_disconnected(None).await
            }
            Err(ClientError::DisconnectTimeout) => {
                self.drain_disconnected(Some(ClientError::DisconnectTimeout))
                    .await
            }
            result => result,
        }
    }

    async fn run_sessions(&mut self) -> Result<()> {
        let ttl = if self.transport.is_none() {
            Some(self.reconnect().await?)
        } else {
            None
        };
        let mut heartbeat = heartbeat_timer(ttl);
        let mut deadline = tokio::time::Instant::now()
            + ttl.map_or(SESSION_ESTABLISH_TIMEOUT, |ttl| {
                ttl.duration_since(SystemTime::now()).unwrap_or_default()
            });
        let mut draining = false;
        loop {
            let shutdown = self.shutdown.clone();
            // Deadlines include message handling and outbound sends. Failed sends
            // and expired sessions return to the same reconnection loop.
            let progress = tokio::select! {
                biased;
                () = wait_until(self.drain_deadline) => return Err(ClientError::DrainTimeout),
                () = shutdown.closed(), if !draining => {
                    self.begin_drain();
                    draining = true;
                    continue;
                }
                () = wait_until(self.disconnect_deadline), if !draining => {
                    return Err(ClientError::DisconnectTimeout);
                }
                () = tokio::time::sleep_until(deadline) => Ok(SessionProgress::DeadlineElapsed),
                result = self.step(&mut heartbeat, draining) => result,
            };
            let ttl = match progress {
                Ok(SessionProgress::Continue) => continue,
                Ok(SessionProgress::Closed) => return Ok(()),
                Ok(SessionProgress::Renew(ttl)) => ttl,
                Ok(SessionProgress::StreamEnded) => {
                    self.recover(draining, ClientError::SessionClosed("stream ended".into()))
                        .await?
                }
                Ok(SessionProgress::DeadlineElapsed) => {
                    self.recover(
                        draining,
                        ClientError::SessionClosed("session deadline elapsed".into()),
                    )
                    .await?
                }
                Err(error) => self.recover(draining, error).await?,
            };
            deadline = tokio::time::Instant::now()
                + ttl.duration_since(SystemTime::now()).unwrap_or_default();
            heartbeat = heartbeat_timer(Some(ttl));
        }
    }

    async fn step(
        &mut self,
        heartbeat: &mut tokio::time::Interval,
        draining: bool,
    ) -> Result<SessionProgress> {
        if draining && !self.deregistered {
            self.deregistered = true;
            self.send(wrap_client(client_message::Msg::Deregister(
                client_message::Deregister {},
            )))
            .await?;
        }
        if draining && self.grants.is_empty() {
            self.send(session::closed_frame("client shutdown")).await?;
            return Ok(SessionProgress::Closed);
        }
        tokio::select! {
            biased;
            Some(action) = self.actions.recv() => self.on_action(action).await?,
            msg = self.transport.as_mut().expect("connected session").inbound.next() => {
                match msg {
                    Some(Ok(join)) => {
                        if let Some(ttl) = self.handle_inbound(join).await? {
                            return Ok(SessionProgress::Renew(ttl));
                        }
                    }
                    Some(Err(status)) => return Err(ClientError::Transport(status)),
                    None => return Ok(SessionProgress::StreamEnded),
                }
            }
            _ = heartbeat.tick() => self.send(session::heartbeat_frame()).await?,
        }
        Ok(SessionProgress::Continue)
    }

    async fn recover(&mut self, draining: bool, error: ClientError) -> Result<SystemTime> {
        if draining || self.shutdown.is_closed() {
            return Err(ClientError::Cancelled);
        }
        if self.reconnect.is_none() {
            return Err(error);
        }
        warn!(%error, "recovering consumer session");
        self.reconnect().await
    }

    fn disconnect(&mut self) {
        self.transport = None;
        self.lease = None;
        for grant in self.grants.values_mut() {
            grant.confirmed = false;
        }
        self.cluster_needs_snapshot = true;
        if self.disconnect_deadline.is_none() {
            self.disconnect_deadline = self
                .reconnect
                .as_ref()
                .and_then(|(options, _)| options.timeout)
                .map(|timeout| tokio::time::Instant::now() + timeout);
        }
    }

    async fn reconnect(&mut self) -> Result<SystemTime> {
        let (options, connect) = self.reconnect.clone().expect("reconnect configured");
        let mut delay = self.transport.is_some();
        self.disconnect();
        loop {
            if delay {
                self.while_disconnected(tokio::time::sleep(options.backoff))
                    .await?;
            }
            delay = true;
            let active = self
                .grants
                .values()
                .filter(|g| {
                    !g.revoked
                        && !g.ownership.expired.is_closed()
                        && SystemTime::now() < g.ownership.expiration()
                })
                .map(|g| g.pb_grant.clone())
                .collect();
            let attempt = async {
                let mut transport = connect(active).await?;
                match transport.inbound.next().await {
                    Some(Ok(JoinMessage {
                        msg: Some(join_message::Msg::Session(message)),
                    })) => match session::classify(&message) {
                        Some(session::SessionEvent::Established { ttl })
                            if ttl > SystemTime::now() =>
                        {
                            Ok((transport, ttl))
                        }
                        Some(session::SessionEvent::Closed { error }) => {
                            Err(ClientError::SessionClosed(error))
                        }
                        _ => Err(ClientError::InvalidMessage(
                            "expected unexpired session Established".into(),
                        )),
                    },
                    Some(Err(status)) => Err(ClientError::Transport(status)),
                    None => Err(ClientError::SessionClosed(
                        "stream ended during establishment".into(),
                    )),
                    _ => Err(ClientError::InvalidMessage(
                        "expected session Established during reconnect".into(),
                    )),
                }
            };
            let attempt_deadline = tokio::time::Instant::now() + self.join_timeout;
            let result = self
                .while_disconnected(async {
                    tokio::select! {
                        biased;
                        () = tokio::time::sleep_until(attempt_deadline) => Err(ClientError::JoinTimeout),
                        result = attempt => result,
                    }
                })
                .await?;
            match result {
                Ok((transport, ttl)) => {
                    self.transport = Some(transport);
                    return Ok(ttl);
                }
                Err(error) => warn!(%error, "consumer connection attempt failed; retrying"),
            }
        }
    }

    async fn while_disconnected<T>(&mut self, future: impl Future<Output = T>) -> Result<T> {
        tokio::pin!(future);
        loop {
            tokio::select! {
                biased;
                () = self.shutdown.closed() => return Err(ClientError::Cancelled),
                () = wait_until(self.disconnect_deadline) => return Err(ClientError::DisconnectTimeout),
                Some(action) = self.actions.recv() => self.on_action(action).await?,
                result = &mut future => return Ok(result),
            }
        }
    }

    fn begin_drain(&mut self) {
        self.shutdown.close();
        self.drain_deadline
            .get_or_insert_with(|| tokio::time::Instant::now() + self.drain_timeout);
    }

    async fn drain_disconnected(&mut self, reason: Option<ClientError>) -> Result<()> {
        self.disconnect();
        self.begin_drain();
        self.cluster.send_replace(Arc::new(ClusterMap::empty()));
        for grant in self.grants.values_mut() {
            if !grant.revoked {
                grant.revoked = true;
                grant.pb_grant.state = pb::GrantState::Revoked as i32;
                grant.ownership.revoke();
            }
        }
        while !self.grants.is_empty() {
            tokio::select! {
                biased;
                () = wait_until(self.drain_deadline) => return Err(reason.unwrap_or(ClientError::DrainTimeout)),
                Some(action) = self.actions.recv() => self.on_action(action).await?,
            }
        }
        reason.map_or(Ok(()), Err)
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
                    let snapshot = matches!(cm.msg, Some(pb::cluster_message::Msg::Snapshot(_)));
                    if self.cluster_needs_snapshot && !snapshot {
                        return Err(ClientError::InvalidCluster(
                            "expected a fresh cluster snapshot".into(),
                        ));
                    }
                    let current = self.cluster.borrow().clone();
                    match current.apply(cm) {
                        Ok(next) => {
                            self.cluster_needs_snapshot = false;
                            self.disconnect_deadline = None;
                            self.cluster.send_replace(Arc::new(next));
                        }
                        Err(error) => {
                            self.cluster_needs_snapshot = true;
                            return Err(ClientError::InvalidCluster(error.to_string()));
                        }
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
                    self.on_promote(g).await?;
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
    async fn on_assign(&mut self, mut g: pb::Grant) -> Result<()> {
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
        let state = grant_state_from_i32(g.state);
        if matches!(
            state,
            GrantState::Active | GrantState::Allocated | GrantState::AllocatedLoaded
        ) && let Some(lease) = self.lease
        {
            // Active and allocated grants share the consumer lease. A replay's
            // embedded deadline can predate the latest Extend message.
            g.lease = Some(lease);
        }
        let expiration = g
            .lease
            .as_ref()
            .map(timestamp_to_system_time)
            .ok_or_else(|| ClientError::InvalidMessage("Assign without a lease".into()))?;
        if expiration <= SystemTime::now() {
            // A stale replay must not relinquish a handler covered by a newer lease.
            if self.grants.get(&id).is_some_and(|entry| {
                !entry.ownership.expired.is_closed()
                    && SystemTime::now() < entry.ownership.expiration()
            }) {
                return Ok(());
            }
            self.remove_grant(&id);
            return Ok(());
        }
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
                    entry.confirmed = true;
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
        let ownership = Ownership::new(state, expiration);
        let watcher = spawn_grant_watcher(id.clone(), ownership.clone(), self.actions_tx.clone());
        self.grants.insert(
            id.clone(),
            ActiveGrant {
                pb_grant: g,
                ownership: ownership.clone(),
                revoked: matches!(state, GrantState::Revoked | GrantState::RevokedUnloaded),
                confirmed: true,
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

    async fn on_promote(&mut self, mut g: pb::Grant) -> Result<()> {
        let id = GrantId(g.id.clone());
        let Some(entry) = self.grants.get_mut(&id).filter(|entry| {
            entry.confirmed
                && !entry.revoked
                && !entry.ownership.expired.is_closed()
                && SystemTime::now() < entry.ownership.expiration()
                && matches!(
                    grant_state_from_i32(entry.pb_grant.state),
                    GrantState::Allocated | GrantState::AllocatedLoaded
                )
        }) else {
            // Stop local ownership before acknowledging the rejected promotion.
            // Otherwise the coordinator can keep renewing a grant with no handler.
            self.remove_grant(&id);
            return self.send_released(g).await;
        };
        if let Some(lease) = self.lease {
            g.lease = Some(lease);
        }
        if let Some(expiration) = g.lease.as_ref().map(timestamp_to_system_time) {
            entry.ownership.set_expiration(expiration);
        }
        entry.pb_grant = g;
        entry.ownership.activate();
        Ok(())
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
            entry.confirmed = true;
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
        self.lease = e.lease;
        for entry in self.grants.values_mut() {
            if !entry.revoked
                && entry.confirmed
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
                if !entry.confirmed {
                    return Ok(());
                }
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

async fn wait_until(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
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
                () = ownership.released.closed() => Action::Release,
                () = ownership.wait_for_expiration() => Action::Expired,
                () = ownership.loader.load.closed(), if !loaded => { loaded = true; Action::Load },
                () = ownership.unloader.unload.closed(), if !unloaded => { unloaded = true; Action::Unload },
                () = ownership.request_revoke_signal.closed(), if !requested => { requested = true; Action::Revoke },
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
mod tests {
    use super::*;
    use crate::{
        ids::{DomainType, QualifiedDomainName},
        wrap::system_time_to_timestamp,
    };
    use futures::{FutureExt, future::ready};
    use tokio::time::timeout;

    mod cluster {
        use super::*;
        use crate::ids::{DomainKey, Endpoint, Instance, InstanceId, Location, QualifiedDomainKey};
        use splitter_proto::pb::cluster_message as cm;

        fn snapshot(assignment: bool) -> JoinMessage {
            let grant = grant("peer-grant", pb::GrantState::Active);
            let shard = grant.shard.unwrap();
            let consumer = Instance {
                id: InstanceId::new(),
                location: Location::new("region", "peer"),
                name: "peer".into(),
                created: SystemTime::UNIX_EPOCH,
                endpoint: Endpoint::new("peer:8080"),
            };
            JoinMessage {
                msg: Some(join_message::Msg::Consumer(ConsumerMessage {
                    msg: Some(consumer_message::Msg::Cluster(pb::ClusterMessage {
                        version: if assignment { 1 } else { 2 },
                        msg: Some(cm::Msg::Snapshot(cm::Snapshot {
                            origin: Some(consumer.clone().into()),
                            shards: vec![shard.clone()],
                            assignments: if assignment {
                                vec![cm::Assignment {
                                    consumer: Some(consumer.into()),
                                    grants: vec![cm::GrantInfo {
                                        id: grant.id,
                                        shard: Some(shard),
                                        state: grant.state,
                                    }],
                                }]
                            } else {
                                vec![]
                            },
                        })),
                        ..Default::default()
                    })),
                })),
            }
        }

        fn assert_peer_route(cluster: &ClusterMap) {
            let owner = cluster
                .lookup(
                    &QualifiedDomainKey {
                        domain: QualifiedDomainName {
                            service: QualifiedServiceName::new("test", "lifecycle"),
                            name: "ranges".into(),
                        },
                        key: DomainKey {
                            key: "key".into(),
                            ..Default::default()
                        },
                    },
                    &[],
                )
                .expect("peer route must survive recovery");
            assert_eq!(owner.id.as_str(), "peer-grant");
            assert_eq!(
                cluster.consumers()[&owner.consumer]
                    .consumer
                    .endpoint
                    .as_str(),
                "peer:8080"
            );
        }

        #[tokio::test(start_paused = true)]
        async fn reconnect_preserves_routes_until_partial_snapshot_and_clears_on_close() {
            let mut h = Harness::new();
            h.driver.handle_inbound(snapshot(true)).await.unwrap();
            let cluster = h.handle.cluster.clone();
            let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let captured_calls = calls.clone();
            let (incoming, receiver) = mpsc::channel(8);
            incoming.send(Ok(established())).await.unwrap();
            let (outbound, _outgoing) = mpsc::channel(8);
            let transport = Arc::new(std::sync::Mutex::new(Some(Transport {
                outbound,
                inbound: Box::pin(ReceiverStream::new(receiver)),
            })));
            h.driver.reconnect = Some((
                ReconnectOptions {
                    timeout: Some(Duration::from_secs(10)),
                    backoff: Duration::from_secs(1),
                },
                Arc::new(move |_| {
                    assert_peer_route(&cluster.borrow());
                    if captured_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                        Box::pin(async {
                            Err(ClientError::Transport(tonic::Status::unavailable("retry")))
                        })
                    } else {
                        ready(Ok(transport.lock().unwrap().take().unwrap())).boxed()
                    }
                }),
            ));
            h.driver.reconnect().await.unwrap();
            assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
            assert_peer_route(&h.handle.cluster.borrow());
            h.driver.handle_inbound(snapshot(false)).await.unwrap();
            assert_eq!(h.handle.cluster.borrow().id().version, 2);
            assert_peer_route(&h.handle.cluster.borrow());
            drop(h.driver);
            assert!(h.handle.cluster.borrow().grants().is_empty());
            assert!(h.handle.cluster.borrow().consumers().is_empty());
            assert!(h.handle.closed().is_closed());
        }

        #[tokio::test(start_paused = true)]
        async fn failed_recovery_clears_retained_routes() {
            let mut h = Harness::new();
            h.driver.handle_inbound(snapshot(true)).await.unwrap();
            let cluster = h.handle.cluster.clone();
            h.driver.reconnect = Some((
                ReconnectOptions {
                    timeout: Some(Duration::from_secs(2)),
                    backoff: Duration::from_secs(1),
                },
                Arc::new(move |_| {
                    assert_peer_route(&cluster.borrow());
                    Box::pin(std::future::pending())
                }),
            ));
            h.inbound
                .send(Err(tonic::Status::unavailable("restart")))
                .await
                .unwrap();
            assert!(matches!(
                h.driver.run().await,
                Err(ClientError::DisconnectTimeout)
            ));
            assert!(h.handle.closed().is_closed());
            assert!(h.handle.cluster.borrow().grants().is_empty());
            assert!(h.handle.cluster.borrow().consumers().is_empty());
        }

        fn incremental(cluster: &ClusterMap, version: i64) -> JoinMessage {
            JoinMessage {
                msg: Some(join_message::Msg::Consumer(ConsumerMessage {
                    msg: Some(consumer_message::Msg::Cluster(pb::ClusterMessage {
                        id: cluster.id().origin.as_ref().unwrap().id.to_string(),
                        version,
                        msg: Some(cm::Msg::Change(cm::Change::default())),
                        ..Default::default()
                    })),
                })),
            }
        }

        #[tokio::test]
        async fn invalid_cluster_update_preserves_map_and_requires_snapshot() {
            let mut h = Harness::new();
            h.driver.handle_inbound(snapshot(true)).await.unwrap();
            let previous = h.handle.cluster.borrow().clone();
            assert!(matches!(
                h.driver.handle_inbound(incremental(&previous, 3)).await,
                Err(ClientError::InvalidCluster(_))
            ));
            assert!(Arc::ptr_eq(&previous, &h.handle.cluster.borrow()));
            // Even a correctly sequenced delta cannot repair a broken stream history.
            assert!(matches!(
                h.driver.handle_inbound(incremental(&previous, 2)).await,
                Err(ClientError::InvalidCluster(_))
            ));
            h.driver.handle_inbound(snapshot(false)).await.unwrap();
            let repaired = h.handle.cluster.borrow().clone();
            h.driver
                .handle_inbound(incremental(&repaired, 3))
                .await
                .unwrap();
            assert_eq!(h.handle.cluster.borrow().id().version, 3);
            assert_peer_route(&h.handle.cluster.borrow());
        }

        #[tokio::test(start_paused = true)]
        async fn invalid_cluster_update_reconnects_and_accepts_fresh_snapshot() {
            let mut h = Harness::new();
            h.driver.handle_inbound(snapshot(true)).await.unwrap();
            let previous = h.handle.cluster.borrow().clone();
            let (incoming, receiver) = mpsc::channel(8);
            incoming.send(Ok(established())).await.unwrap();
            incoming.send(Ok(snapshot(false))).await.unwrap();
            let (outbound, _outgoing) = mpsc::channel(8);
            let transport = Arc::new(std::sync::Mutex::new(Some(Transport {
                outbound,
                inbound: Box::pin(ReceiverStream::new(receiver)),
            })));
            h.driver.reconnect = Some((
                ReconnectOptions {
                    timeout: Some(Duration::from_secs(10)),
                    backoff: Duration::from_secs(1),
                },
                Arc::new(move |_| {
                    ready(Ok(transport
                        .lock()
                        .unwrap()
                        .take()
                        .expect("one recovery attempt")))
                    .boxed()
                }),
            ));
            h.inbound
                .send(Ok(incremental(&previous, 99)))
                .await
                .unwrap();
            h.handle.task = Some(tokio::spawn(h.driver.run()));
            timeout(Duration::from_secs(5), async {
                while h.handle.cluster.borrow().id().version != 2 {
                    h.handle.cluster.changed().await.unwrap();
                }
            })
            .await
            .unwrap();
            assert_peer_route(&h.handle.cluster.borrow());
            assert!(!h.handle.closed().is_closed());
            h.handle.shutdown().await.unwrap();
        }

        #[tokio::test(start_paused = true)]
        async fn invalid_cluster_update_without_recovery_closes_and_clears_routes() {
            let mut h = Harness::new();
            h.driver.handle_inbound(snapshot(true)).await.unwrap();
            let previous = h.handle.cluster.borrow().clone();
            h.inbound
                .send(Ok(incremental(&previous, 99)))
                .await
                .unwrap();
            assert!(matches!(
                h.driver.run().await,
                Err(ClientError::InvalidCluster(_))
            ));
            assert!(h.handle.closed().is_closed());
            assert!(h.handle.cluster.borrow().grants().is_empty());
        }

        #[tokio::test(start_paused = true)]
        async fn cluster_recovery_requires_snapshot_within_total_recovery_budget() {
            let mut h = Harness::new();
            h.driver.handle_inbound(snapshot(true)).await.unwrap();
            let previous = h.handle.cluster.borrow().clone();
            let (incoming, receiver) = mpsc::channel(8);
            incoming.send(Ok(established())).await.unwrap();
            let (outbound, _outgoing) = mpsc::channel(8);
            let transport = Arc::new(std::sync::Mutex::new(Some(Transport {
                outbound,
                inbound: Box::pin(ReceiverStream::new(receiver)),
            })));
            h.driver.reconnect = Some((
                ReconnectOptions {
                    timeout: Some(Duration::from_secs(3)),
                    backoff: Duration::from_secs(1),
                },
                Arc::new(move |_| ready(Ok(transport.lock().unwrap().take().unwrap())).boxed()),
            ));
            h.inbound
                .send(Ok(incremental(&previous, 99)))
                .await
                .unwrap();
            let start = tokio::time::Instant::now();
            let error = h.driver.run().await.unwrap_err();
            assert!(matches!(error, ClientError::DisconnectTimeout));
            assert_eq!(start.elapsed(), Duration::from_secs(3));
            assert!(h.handle.cluster.borrow().grants().is_empty());
        }
    }
    mod deadline {
        use super::*;
        use splitter_proto::session_pb::{self, message::Request};

        pub(super) fn session_frame(ttl: SystemTime, ack: bool) -> JoinMessage {
            let ttl = Some(system_time_to_timestamp(ttl));
            let request = if ack {
                Request::Ack(session_pb::message::HeartbeatAck { ttl })
            } else {
                Request::Established(session_pb::message::Established {
                    ttl,
                    ..Default::default()
                })
            };
            JoinMessage {
                msg: Some(join_message::Msg::Session(session_pb::Message {
                    request: Some(request),
                })),
            }
        }

        async fn advance(duration: Duration) {
            tokio::time::advance(duration).await;
            tokio::task::yield_now().await;
        }

        async fn start(h: &mut Harness, ttl: Duration) {
            h.inbound
                .send(Ok(session_frame(SystemTime::now() + ttl, false)))
                .await
                .unwrap();
            // Move the driver without replacing its externally held channel endpoints.
            let replacement = Harness::new();
            let driver = std::mem::replace(&mut h.driver, replacement.driver);
            h.handle.task = Some(tokio::spawn(driver.run()));
            tokio::task::yield_now().await;
        }

        #[tokio::test(start_paused = true)]
        async fn silent_session_expires_and_cleans_up_live_grants() {
            let mut h = Harness::new();
            h.driver
                .on_assign(grant("g", pb::GrantState::Active))
                .await
                .unwrap();
            let ownership = h.driver.grants[&GrantId("g".into())].ownership.clone();
            start(&mut h, Duration::from_secs(1)).await;

            advance(Duration::from_secs(2)).await;
            assert!(
                h.handle.closed().is_closed(),
                "silent session must expire without EOF"
            );
            assert!(ownership.expired().is_closed());
            assert!(h.handle.assignments.borrow().is_empty());
            assert!(matches!(
                h.handle.shutdown().await,
                Err(ClientError::SessionClosed(_))
            ));
        }

        #[tokio::test(start_paused = true)]
        async fn heartbeat_ack_renews_session_but_not_forever() {
            let mut h = Harness::new();
            start(&mut h, Duration::from_secs(1)).await;
            advance(Duration::from_millis(500)).await;
            h.inbound
                .send(Ok(session_frame(
                    SystemTime::now() + Duration::from_secs(3),
                    true,
                )))
                .await
                .unwrap();
            tokio::task::yield_now().await;

            advance(Duration::from_secs(1)).await;
            assert!(
                !h.handle.closed().is_closed(),
                "ack must replace the original deadline"
            );
            advance(Duration::from_secs(3)).await;
            assert!(
                h.handle.closed().is_closed(),
                "renewed session still expires without another ack"
            );
        }

        #[tokio::test(start_paused = true)]
        async fn blocked_heartbeat_send_cannot_delay_session_expiration() {
            let mut h = Harness::new();
            while h
                .driver
                .transport
                .as_ref()
                .unwrap()
                .outbound
                .try_send(JoinMessage::default())
                .is_ok()
            {}
            start(&mut h, Duration::from_secs(1)).await;

            advance(Duration::from_millis(500)).await;
            assert!(!h.handle.closed().is_closed());
            advance(Duration::from_secs(1)).await;
            assert!(
                h.handle.closed().is_closed(),
                "session deadline must interrupt the five-second send wait"
            );
        }

        #[tokio::test(start_paused = true)]
        async fn heartbeat_ack_can_shorten_the_session_deadline() {
            let mut h = Harness::new();
            start(&mut h, Duration::from_secs(30)).await;
            h.inbound
                .send(Ok(session_frame(
                    SystemTime::now() + Duration::from_secs(1),
                    true,
                )))
                .await
                .unwrap();
            tokio::task::yield_now().await;
            advance(Duration::from_secs(2)).await;
            assert!(h.handle.closed().is_closed());
        }

        #[tokio::test(start_paused = true)]
        async fn unrelated_inbound_messages_do_not_extend_the_session() {
            let mut h = Harness::new();
            start(&mut h, Duration::from_secs(1)).await;
            advance(Duration::from_millis(500)).await;
            h.inbound.send(Ok(JoinMessage::default())).await.unwrap();
            tokio::task::yield_now().await;
            advance(Duration::from_millis(750)).await;
            assert!(h.handle.closed().is_closed());
        }

        #[tokio::test(start_paused = true)]
        async fn queued_ack_cannot_revive_an_expired_session() {
            let mut h = Harness::new();
            h.inbound
                .send(Ok(session_frame(SystemTime::UNIX_EPOCH, false)))
                .await
                .unwrap();
            h.inbound
                .send(Ok(session_frame(
                    SystemTime::now() + Duration::from_secs(60),
                    true,
                )))
                .await
                .unwrap();
            h.handle.task = Some(tokio::spawn(h.driver.run()));
            tokio::task::yield_now().await;
            assert!(h.handle.closed().is_closed());
            assert!(matches!(
                h.handle.shutdown().await,
                Err(ClientError::SessionClosed(_))
            ));
        }

        #[tokio::test(start_paused = true)]
        async fn initial_stream_without_established_has_a_deadline() {
            let mut h = Harness::new();
            h.handle.task = Some(tokio::spawn(h.driver.run()));
            tokio::task::yield_now().await;
            advance(Duration::from_secs(31)).await;
            assert!(h.handle.closed().is_closed());
            assert!(matches!(
                h.handle.shutdown().await,
                Err(ClientError::SessionClosed(_))
            ));
        }

        fn pending_reconnect() -> (ReconnectOptions, Connector) {
            (
                ReconnectOptions {
                    timeout: Some(Duration::from_secs(2)),
                    backoff: Duration::from_millis(100),
                },
                Arc::new(|_| Box::pin(std::future::pending())),
            )
        }

        #[tokio::test(start_paused = true)]
        async fn session_expiration_reconnects_and_preserves_valid_ownership() {
            let mut h = Harness::new();
            let g = grant("g", pb::GrantState::Active);
            h.driver.on_assign(g.clone()).await.unwrap();
            let ownership = h.driver.grants[&GrantId("g".into())].ownership.clone();
            let (incoming, receiver) = mpsc::channel(8);
            incoming
                .send(Ok(session_frame(
                    SystemTime::now() + Duration::from_secs(10),
                    false,
                )))
                .await
                .unwrap();
            incoming
                .send(Ok(reconnection::empty_snapshot()))
                .await
                .unwrap();
            incoming
                .send(Ok(wrap_client(client_message::Msg::Assign(
                    client_message::Assign { grants: vec![g] },
                ))))
                .await
                .unwrap();
            let (outbound, mut outgoing) = mpsc::channel(8);
            let transport = std::sync::Mutex::new(Some(Transport {
                outbound,
                inbound: Box::pin(ReceiverStream::new(receiver)),
            }));
            let (replay_tx, mut replay_rx) = mpsc::channel(1);
            h.driver.reconnect = Some((
                pending_reconnect().0,
                Arc::new(move |active| {
                    replay_tx.try_send(active).unwrap();
                    let next = transport.lock().unwrap().take().expect("one reconnect");
                    Box::pin(async move { Ok(next) })
                }),
            ));
            start(&mut h, Duration::from_secs(1)).await;
            advance(Duration::from_secs(1)).await;
            advance(Duration::from_millis(200)).await;

            let replay = replay_rx
                .try_recv()
                .expect("expired session must reconnect");
            assert_eq!(replay.len(), 1);
            assert_eq!(replay[0].id, "g");
            let assignment = h.handle.assignments.borrow()[&GrantId("g".into())].clone();
            assert!(Arc::ptr_eq(&ownership, &assignment.ownership));
            assert!(!ownership.expired().is_closed());
            advance(Duration::from_secs(2)).await;
            assert!(
                !h.handle.closed().is_closed(),
                "recovery must install the new session deadline"
            );
            assert!(
                outgoing.try_recv().is_err(),
                "old heartbeat interval must also be replaced"
            );
            ownership.release();
            h.handle.shutdown().await.unwrap();
        }

        #[tokio::test(start_paused = true)]
        async fn failed_recovery_after_session_expiration_is_bounded() {
            let mut h = Harness::new();
            h.driver.reconnect = Some(pending_reconnect());
            start(&mut h, Duration::from_secs(1)).await;
            advance(Duration::from_secs(1)).await;
            assert!(
                !h.handle.closed().is_closed(),
                "recovery gets its own bounded window"
            );
            advance(Duration::from_secs(3)).await;
            assert!(h.handle.closed().is_closed());
            assert!(matches!(
                h.handle.shutdown().await,
                Err(ClientError::DisconnectTimeout)
            ));
        }

        #[tokio::test(start_paused = true)]
        async fn shutdown_interrupts_recovery_triggered_by_session_expiration() {
            let mut h = Harness::new();
            h.driver.reconnect = Some(pending_reconnect());
            start(&mut h, Duration::from_secs(1)).await;
            advance(Duration::from_secs(1)).await;
            h.handle.shutdown().await.unwrap();
        }

        #[tokio::test(start_paused = true)]
        async fn draining_session_expires_without_reconnecting() {
            let mut h = Harness::new();
            h.driver
                .on_assign(grant("g", pb::GrantState::Active))
                .await
                .unwrap();
            h.driver.reconnect = Some((
                pending_reconnect().0,
                Arc::new(|_| {
                    panic!("shutdown must not start a new session");
                }),
            ));
            start(&mut h, Duration::from_secs(1)).await;
            h.handle.shutdown.close();
            tokio::task::yield_now().await;
            advance(Duration::from_secs(2)).await;
            assert!(
                !h.handle.closed().is_closed(),
                "offline drain retains grants until completion or timeout"
            );
            advance(Duration::from_secs(30)).await;
            assert!(h.handle.closed().is_closed());
            assert!(h.handle.assignments.borrow().is_empty());
            assert!(matches!(
                h.handle.shutdown().await,
                Err(ClientError::DrainTimeout)
            ));
        }

        #[tokio::test(start_paused = true)]
        async fn expired_reconnect_handshakes_do_not_reset_the_recovery_budget() {
            let mut h = Harness::new();
            h.driver.reconnect = Some((
                pending_reconnect().0,
                Arc::new(|_| {
                    Box::pin(async {
                        let (outbound, _receiver) = mpsc::channel(1);
                        Ok(Transport {
                            outbound,
                            inbound: Box::pin(futures::stream::iter([Ok(session_frame(
                                SystemTime::UNIX_EPOCH,
                                false,
                            ))])),
                        })
                    })
                }),
            ));
            let started = tokio::time::Instant::now();
            assert!(matches!(
                h.driver.reconnect().await,
                Err(ClientError::DisconnectTimeout)
            ));
            assert!(started.elapsed() < Duration::from_secs(3));
        }
    }
    mod expired_assignments {
        use super::*;

        fn extend(h: &mut Harness) -> SystemTime {
            let lease = grant("unused", pb::GrantState::Active).lease;
            h.driver.on_extend(client_message::Extend { lease });
            timestamp_to_system_time(&lease.unwrap())
        }

        fn expired_assignment(state: pb::GrantState) -> pb::Grant {
            let mut assignment = grant("late", state);
            assignment.lease = Some(system_time_to_timestamp(SystemTime::UNIX_EPOCH));
            assignment
        }

        #[tokio::test]
        async fn assignment_uses_shared_lease_received_before_assignment() {
            for state in [
                pb::GrantState::Active,
                pb::GrantState::Allocated,
                pb::GrantState::AllocatedLoaded,
            ] {
                let mut h = Harness::new();
                let deadline = extend(&mut h);
                h.driver.on_assign(expired_assignment(state)).await.unwrap();
                let ownership = &h.handle.assignments.borrow()[&GrantId("late".into())].ownership;
                assert_eq!(ownership.expiration(), deadline);
                assert!(!ownership.expired().is_closed());
                assert!(h.outbound.try_recv().is_err());
            }
        }

        #[tokio::test]
        async fn stale_assignment_replay_preserves_handler_and_shared_lease() {
            let mut h = Harness::new();
            let deadline = extend(&mut h);
            h.driver
                .on_assign(grant("late", pb::GrantState::Active))
                .await
                .unwrap();
            let id = GrantId("late".into());
            let ownership = h.driver.grants[&id].ownership.clone();
            h.driver
                .on_assign(expired_assignment(pb::GrantState::Active))
                .await
                .unwrap();
            assert!(Arc::ptr_eq(
                &h.handle.assignments.borrow()[&id].ownership,
                &ownership
            ));
            assert_eq!(ownership.expiration(), deadline);
            assert!(!ownership.expired().is_closed());
            assert!(h.outbound.try_recv().is_err());
        }

        #[tokio::test]
        async fn promotion_does_not_replace_shared_lease_with_stale_embedded_deadline() {
            let mut h = Harness::new();
            let deadline = extend(&mut h);
            h.driver
                .on_assign(grant("late", pb::GrantState::Allocated))
                .await
                .unwrap();
            h.driver
                .on_promote(expired_assignment(pb::GrantState::Active))
                .await
                .unwrap();
            let ownership = &h.handle.assignments.borrow()[&GrantId("late".into())].ownership;
            assert!(ownership.active().is_closed());
            assert_eq!(ownership.expiration(), deadline);
            assert!(!ownership.expired().is_closed());
        }

        #[tokio::test]
        async fn revoked_assignment_keeps_its_detached_deadline() {
            let mut h = Harness::new();
            extend(&mut h);
            h.driver
                .on_assign(expired_assignment(pb::GrantState::Revoked))
                .await
                .unwrap();
            assert!(h.handle.assignments.borrow().is_empty());
            assert!(h.outbound.try_recv().is_err());
        }

        #[tokio::test]
        async fn expired_shared_lease_does_not_create_or_release_assignment() {
            let mut h = Harness::new();
            h.driver.on_extend(client_message::Extend {
                lease: Some(system_time_to_timestamp(SystemTime::UNIX_EPOCH)),
            });
            h.driver
                .on_assign(grant("late", pb::GrantState::Active))
                .await
                .unwrap();
            assert!(h.handle.assignments.borrow().is_empty());
            assert!(h.outbound.try_recv().is_err());
        }
    }
    mod initial_join {
        use super::*;
        use std::sync::atomic::{AtomicBool, Ordering};

        struct Dropped(Arc<AtomicBool>);

        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        fn pending_connector(dropped: Arc<AtomicBool>) -> Connector {
            Arc::new(move |active| {
                assert!(
                    active.is_empty(),
                    "initial registration cannot replay old grants"
                );
                let guard = Dropped(dropped.clone());
                Box::pin(async move {
                    let _guard = guard;
                    std::future::pending().await
                })
            })
        }

        #[tokio::test(start_paused = true)]
        async fn initial_join_timeout_drops_pending_rpc_before_spawning_driver() {
            let dropped = Arc::new(AtomicBool::new(false));
            let start = tokio::time::Instant::now();
            let result = timeout(
                Duration::from_secs(10),
                start_join(
                    pending_connector(dropped.clone()),
                    ConsumerOptions::default()
                        .without_reconnect()
                        .with_join_timeout(Duration::from_secs(2)),
                ),
            )
            .await
            .expect("Join RPC must enforce its own timeout");
            assert!(matches!(result, Err(ClientError::JoinTimeout)));
            assert_eq!(start.elapsed(), Duration::from_secs(2));
            assert!(dropped.load(Ordering::SeqCst));
        }

        #[tokio::test(start_paused = true)]
        async fn fail_fast_initial_join_budget_is_thirty_seconds() {
            let start = tokio::time::Instant::now();
            let options = ConsumerOptions::default().without_reconnect();
            let result = timeout(
                Duration::from_secs(40),
                start_join(pending_connector(Arc::new(AtomicBool::new(false))), options),
            )
            .await
            .expect("default Join timeout must be bounded");
            assert!(matches!(result, Err(ClientError::JoinTimeout)));
            assert_eq!(start.elapsed(), Duration::from_secs(30));
        }

        #[tokio::test]
        async fn invalid_timeout_is_rejected_before_connector_invocation() {
            for duration in [Duration::ZERO, Duration::MAX] {
                let connector: Connector =
                    Arc::new(|_| panic!("invalid settings must not start an RPC"));
                assert!(matches!(
                    start_join(
                        connector,
                        ConsumerOptions::default()
                            .without_reconnect()
                            .with_join_timeout(duration)
                    )
                    .await,
                    Err(ClientError::InvalidMessage(_))
                ));
            }
        }

        #[tokio::test(start_paused = true)]
        async fn caller_cancellation_drops_pending_initial_rpc() {
            let dropped = Arc::new(AtomicBool::new(false));
            let result = timeout(
                Duration::from_secs(1),
                start_join(
                    pending_connector(dropped.clone()),
                    ConsumerOptions::default().without_reconnect(),
                ),
            )
            .await;
            assert!(result.is_err());
            assert!(dropped.load(Ordering::SeqCst));
        }

        #[tokio::test(start_paused = true)]
        async fn initial_transport_error_is_preserved_without_retry() {
            let connector: Connector = Arc::new(|_| {
                Box::pin(async {
                    Err(ClientError::Transport(tonic::Status::permission_denied(
                        "denied",
                    )))
                })
            });
            assert!(
                matches!(start_join(connector, ConsumerOptions::default().without_reconnect()).await, Err(ClientError::Transport(status)) if status.code() == tonic::Code::PermissionDenied)
            );
        }

        #[tokio::test(start_paused = true)]
        async fn successful_rpc_returns_handle_and_session_can_outlive_join_timeout() {
            let (incoming, receiver) = mpsc::channel(8);
            incoming.send(Ok(established())).await.unwrap();
            let (outbound, _outgoing) = mpsc::channel(8);
            let transport = Arc::new(std::sync::Mutex::new(Some(Transport {
                outbound,
                inbound: Box::pin(ReceiverStream::new(receiver)),
            })));
            let connector: Connector =
                Arc::new(move |_| ready(Ok(transport.lock().unwrap().take().unwrap())).boxed());
            let handle = start_join(
                connector,
                ConsumerOptions::default()
                    .without_reconnect()
                    .with_join_timeout(Duration::from_secs(1)),
            )
            .await
            .unwrap();
            tokio::task::yield_now().await;
            tokio::time::advance(Duration::from_secs(2)).await;
            assert!(
                !handle.closed().is_closed(),
                "Join timeout must not become a session lifetime limit"
            );
            handle.shutdown().await.unwrap();
        }

        #[tokio::test(start_paused = true)]
        async fn deadline_wins_if_rpc_response_becomes_ready_at_the_same_instant() {
            let connector: Connector = Arc::new(|_| {
                Box::pin(async {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    Err(ClientError::Cancelled)
                })
            });
            let result = start_join(
                connector,
                ConsumerOptions::default()
                    .without_reconnect()
                    .with_join_timeout(Duration::from_secs(1)),
            )
            .await;
            assert!(matches!(result, Err(ClientError::JoinTimeout)));
        }

        #[test]
        fn join_timeout_maps_to_deadline_exceeded_and_is_not_an_ownership_error() {
            assert!(!ClientError::JoinTimeout.is_ownership());
            assert_eq!(
                ClientError::JoinTimeout.into_grpc().code(),
                tonic::Code::DeadlineExceeded
            );
        }
    }
    mod live_promotions {
        use super::*;
        use crate::ids::{Endpoint, InstanceId, Location};
        use tokio::sync::oneshot;

        fn instance() -> Instance {
            Instance {
                id: InstanceId::new(),
                location: Location::new(
                    std::env::var("SPLITTER_REGION").unwrap_or_else(|_| "region".into()),
                    "test",
                ),
                name: "splitter-rs-promotion-race".into(),
                created: SystemTime::now(),
                endpoint: Endpoint::default(),
            }
        }

        async fn assignment(handle: &mut JoinHandle) -> (GrantId, Arc<Ownership>) {
            timeout(Duration::from_secs(30), async {
                loop {
                    if let Some((id, assignment)) =
                        handle.assignments.borrow_and_update().iter().next()
                    {
                        return (id.clone(), assignment.ownership.clone());
                    }
                    handle.assignments.changed().await.unwrap();
                }
            })
            .await
            .expect("server assigns the UNIT shard")
        }

        #[tokio::test]
        #[ignore = "requires an isolated live Splitter UNIT service; run tools/live_splitter.py"]
        async fn live_delayed_promotion_releases_expired_grant() {
            let endpoint = std::env::var("SPLITTER_ENDPOINT").expect("set SPLITTER_ENDPOINT");
            let service = QualifiedServiceName::parse(
                &std::env::var("SPLITTER_UNIT_SERVICE").expect("set SPLITTER_UNIT_SERVICE"),
            )
            .unwrap();
            let channel = Channel::from_shared(endpoint)
                .unwrap()
                .connect_timeout(Duration::from_secs(5))
                .connect()
                .await
                .unwrap();
            let mut first = ConsumerClient::new(channel.clone())
                .join(instance(), service.clone(), ConsumerOptions::default())
                .await
                .unwrap();
            let (_, previous) = assignment(&mut first).await;
            timeout(Duration::from_secs(30), previous.active().closed())
                .await
                .unwrap();

            let transport = open_session(
                ConsumerServiceClient::new(channel),
                instance(),
                service,
                ConsumerOptions::default(),
                Vec::new(),
            )
            .await
            .unwrap();
            let (observed, promotion) = oneshot::channel();
            let (resume, delayed) = oneshot::channel();
            // Hold one real coordinator promotion at the transport boundary. The server
            // and both session drivers otherwise use the normal Join protocol.
            let inbound = futures::stream::unfold(
                (transport.inbound, Some((observed, delayed))),
                |(mut inbound, mut gate)| async move {
                    let message = inbound.next().await?;
                    if let Ok(JoinMessage {
                        msg:
                            Some(join_message::Msg::Consumer(ConsumerMessage {
                                msg:
                                    Some(consumer_message::Msg::Client(ClientMessage {
                                        msg: Some(client_message::Msg::Promote(promote)),
                                    })),
                            })),
                    }) = &message
                        && let Some((observed, delayed)) = gate.take()
                    {
                        observed.send(promote.grants.clone()).unwrap();
                        delayed.await.unwrap();
                    }
                    Some((message, (inbound, gate)))
                },
            );
            let (driver, mut second) = Driver::new(Some(Transport {
                outbound: transport.outbound,
                inbound: Box::pin(inbound),
            }));
            second.task = Some(tokio::spawn(driver.run()));

            first.shutdown.close();
            let (id, ownership) = assignment(&mut second).await;
            assert!(!ownership.active().is_closed());
            ownership.loader().load();
            timeout(Duration::from_secs(5), async {
                while !second
                    .cluster
                    .borrow_and_update()
                    .grants()
                    .contains_key(&id)
                {
                    second.cluster.changed().await.unwrap();
                }
            })
            .await
            .expect("coordinator advertises the replacement grant");
            timeout(Duration::from_secs(30), previous.revoked().closed())
                .await
                .unwrap();
            previous.release();
            let promoted = timeout(Duration::from_secs(30), promotion)
                .await
                .expect("coordinator promotes the replacement")
                .unwrap();
            assert!(promoted.iter().any(|grant| grant.id == id.0));
            println!("Held real promotion for {id:?}");

            // Move only this local lease deadline into the past, reproducing expiry
            // before delivery without waiting for the server's full lease duration.
            ownership.set_expiration(SystemTime::now() - Duration::from_secs(1));
            timeout(Duration::from_secs(5), async {
                while second.assignments.borrow_and_update().contains_key(&id) {
                    second.assignments.changed().await.unwrap();
                }
            })
            .await
            .expect("local expired grant is removed before promotion delivery");
            assert!(ownership.expired().is_closed());
            resume.send(()).unwrap();

            // Observe the actual coordinator's routing view, not only an outbound frame.
            timeout(Duration::from_secs(5), async {
                loop {
                    if !second
                        .cluster
                        .borrow_and_update()
                        .grants()
                        .contains_key(&id)
                    {
                        break;
                    }
                    second.cluster.changed().await.unwrap();
                }
            })
            .await
            .expect("coordinator must stop routing to the rejected promotion");
            assert!(
                !second.closed().is_closed(),
                "session must remain connected"
            );
            assert!(second.cluster.borrow().id().version > 0);
            assert!(!ownership.active().is_closed());
            println!("Coordinator removed rejected promotion {id:?}");
            timeout(Duration::from_secs(5), first.shutdown())
                .await
                .unwrap()
                .unwrap();
            let mut assignments = second.assignments.clone();
            let releaser = tokio::spawn(async move {
                loop {
                    for assignment in assignments.borrow_and_update().values() {
                        assignment.ownership.release();
                    }
                    if assignments.changed().await.is_err() {
                        break;
                    }
                }
            });
            timeout(Duration::from_secs(5), second.shutdown())
                .await
                .unwrap()
                .unwrap();
            releaser.await.unwrap();
        }
    }
    mod promotion {
        use super::*;

        fn promotion(grant: pb::Grant) -> ClientMessage {
            ClientMessage {
                msg: Some(client_message::Msg::Promote(client_message::Promote {
                    grants: vec![grant],
                })),
            }
        }

        #[tokio::test]
        async fn unknown_promotion_is_released() {
            let mut h = Harness::new();
            let promoted = grant("unknown", pb::GrantState::Active);
            h.driver
                .handle_client_message(promotion(promoted.clone()))
                .await
                .unwrap();
            let client_message::Msg::Released(released) =
                client_variant(next(&mut h.outbound).await)
            else {
                panic!("expected Released for unknown promotion");
            };
            assert_eq!(released.grants, vec![promoted]);
            assert!(h.handle.assignments.borrow().is_empty());
        }

        #[tokio::test]
        async fn invalid_promotion_expires_and_releases_local_grant() {
            for reason in [
                "expired", "deadline", "revoked", "unloaded", "stale", "active",
            ] {
                let mut h = Harness::new();
                let state = match reason {
                    "revoked" => pb::GrantState::Revoked,
                    "unloaded" => pb::GrantState::RevokedUnloaded,
                    "active" => pb::GrantState::Active,
                    _ => pb::GrantState::Allocated,
                };
                h.driver.on_assign(grant("g", state)).await.unwrap();
                let id = GrantId("g".into());
                let entry = h.driver.grants.get_mut(&id).unwrap();
                let ownership = entry.ownership.clone();
                match reason {
                    "expired" => ownership.expire(),
                    "deadline" => {
                        ownership.set_expiration(SystemTime::now() - Duration::from_secs(1));
                    }
                    "stale" => entry.confirmed = false,
                    _ => {}
                }
                let promoted = grant("g", pb::GrantState::Active);
                h.driver
                    .handle_client_message(promotion(promoted.clone()))
                    .await
                    .unwrap();
                assert!(ownership.expired().is_closed(), "{reason}");
                assert!(h.handle.assignments.borrow().is_empty(), "{reason}");
                assert!(!h.driver.grants.contains_key(&id), "{reason}");
                let client_message::Msg::Released(released) =
                    client_variant(next(&mut h.outbound).await)
                else {
                    panic!("expected Released for {reason} promotion");
                };
                assert_eq!(released.grants, vec![promoted], "{reason}");
                h.driver.on_extend(client_message::Extend {
                    lease: Some(system_time_to_timestamp(
                        SystemTime::now() + Duration::from_secs(120),
                    )),
                });
                assert!(ownership.expired().is_closed(), "{reason}");
            }
        }

        #[tokio::test]
        async fn loaded_promotion_keeps_ownership_identity() {
            let mut h = Harness::new();
            h.driver
                .on_assign(grant("g", pb::GrantState::AllocatedLoaded))
                .await
                .unwrap();
            let id = GrantId("g".into());
            let ownership = h.driver.grants[&id].ownership.clone();
            h.driver
                .handle_client_message(promotion(grant("g", pb::GrantState::Active)))
                .await
                .unwrap();
            assert!(ownership.active().is_closed());
            assert!(!ownership.expired().is_closed());
            assert!(Arc::ptr_eq(
                &h.handle.assignments.borrow()[&id].ownership,
                &ownership
            ));
            assert!(h.outbound.try_recv().is_err());
        }

        #[tokio::test]
        async fn rejected_promotion_propagates_send_failure() {
            let mut h = Harness::new();
            h.outbound.close();
            assert!(
                h.driver
                    .handle_client_message(promotion(grant("g", pb::GrantState::Active)))
                    .await
                    .is_err()
            );
        }
    }
    mod reconnection {
        use super::*;
        use std::sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        };

        pub(super) fn empty_snapshot() -> JoinMessage {
            JoinMessage {
                msg: Some(join_message::Msg::Consumer(ConsumerMessage {
                    msg: Some(consumer_message::Msg::Cluster(pb::ClusterMessage {
                        version: 1,
                        msg: Some(pb::cluster_message::Msg::Snapshot(
                            pb::cluster_message::Snapshot::default(),
                        )),
                        ..Default::default()
                    })),
                })),
            }
        }

        fn ready_transport() -> (
            Transport,
            mpsc::Sender<std::result::Result<JoinMessage, tonic::Status>>,
            mpsc::Receiver<JoinMessage>,
        ) {
            let (incoming, receiver) = mpsc::channel(8);
            incoming.try_send(Ok(established())).unwrap();
            incoming.try_send(Ok(empty_snapshot())).unwrap();
            let (outbound, outgoing) = mpsc::channel(8);
            (
                Transport {
                    outbound,
                    inbound: Box::pin(ReceiverStream::new(receiver)),
                },
                incoming,
                outgoing,
            )
        }

        async fn snapshot_received(handle: &mut JoinHandle) {
            timeout(Duration::from_secs(10), async {
                loop {
                    if handle.cluster.borrow_and_update().id().version == 1 {
                        return;
                    }
                    handle.cluster.changed().await.unwrap();
                }
            })
            .await
            .expect("valid snapshot within the test deadline");
        }

        #[tokio::test(start_paused = true)]
        async fn defaults_return_before_connecting_and_retry_initial_rpc_rejections() {
            let calls = Arc::new(AtomicUsize::new(0));
            let attempts = calls.clone();
            let (transport, _incoming, _outgoing) = ready_transport();
            let transport = Mutex::new(Some(transport));
            let connector: Connector = Arc::new(move |_| {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                if attempt < 3 {
                    Box::pin(async {
                        Err(ClientError::Transport(tonic::Status::permission_denied(
                            "not yet permitted",
                        )))
                    })
                } else {
                    ready(Ok(transport.lock().unwrap().take().unwrap())).boxed()
                }
            });
            let mut handle = start_join(connector, ConsumerOptions::default())
                .await
                .unwrap();
            assert_eq!(
                calls.load(Ordering::SeqCst),
                0,
                "join returns a lifetime handle, not readiness"
            );
            snapshot_received(&mut handle).await;
            assert_eq!(calls.load(Ordering::SeqCst), 4);
            handle.shutdown().await.unwrap();
        }

        #[tokio::test(start_paused = true)]
        async fn default_retries_have_no_total_outage_limit() {
            let calls = Arc::new(AtomicUsize::new(0));
            let attempts = calls.clone();
            let connector: Connector = Arc::new(move |_| {
                attempts.fetch_add(1, Ordering::SeqCst);
                Box::pin(async {
                    Err(ClientError::Transport(tonic::Status::unavailable(
                        "offline",
                    )))
                })
            });
            let handle = start_join(connector, ConsumerOptions::default())
                .await
                .unwrap();
            tokio::task::yield_now().await;
            for _ in 0..3 {
                tokio::time::advance(Duration::from_secs(3600)).await;
                tokio::task::yield_now().await;
                assert!(!handle.closed().is_closed());
            }
            assert!(calls.load(Ordering::SeqCst) >= 4);
            handle.shutdown().await.unwrap();
        }

        struct Dropped(Arc<AtomicUsize>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        #[tokio::test(start_paused = true)]
        async fn connection_and_handshake_attempts_are_individually_bounded() {
            for silent_handshake in [false, true] {
                let dropped = Arc::new(AtomicUsize::new(0));
                let drops = dropped.clone();
                let calls = Arc::new(AtomicUsize::new(0));
                let attempts = calls.clone();
                let (transport, _incoming, _outgoing) = ready_transport();
                let transport = Mutex::new(Some(transport));
                let connector: Connector = Arc::new(move |_| {
                    if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        let guard = Dropped(drops.clone());
                        Box::pin(async move {
                            if silent_handshake {
                                let (outbound, _) = mpsc::channel(1);
                                Ok(Transport {
                                    outbound,
                                    inbound: Box::pin(futures::stream::once(async move {
                                        let _guard = guard;
                                        std::future::pending().await
                                    })),
                                })
                            } else {
                                let _guard = guard;
                                std::future::pending().await
                            }
                        })
                    } else {
                        ready(Ok(transport.lock().unwrap().take().unwrap())).boxed()
                    }
                });
                let start = tokio::time::Instant::now();
                let mut handle = start_join(
                    connector,
                    ConsumerOptions::default().with_join_timeout(Duration::from_secs(2)),
                )
                .await
                .unwrap();
                snapshot_received(&mut handle).await;
                assert_eq!(calls.load(Ordering::SeqCst), 2);
                assert_eq!(dropped.load(Ordering::SeqCst), 1);
                assert_eq!(start.elapsed(), Duration::from_secs(3));
                handle.shutdown().await.unwrap();
            }
        }

        #[tokio::test(start_paused = true)]
        async fn server_closure_bad_handshakes_and_rpc_errors_all_retry() {
            let calls = Arc::new(AtomicUsize::new(0));
            let attempts = calls.clone();
            let (transport, _incoming, _outgoing) = ready_transport();
            let transport = Mutex::new(Some(transport));
            let connector: Connector = Arc::new(move |_| {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                if attempt == 4 {
                    return ready(Ok(transport.lock().unwrap().take().unwrap())).boxed();
                }
                Box::pin(async move {
                    let (outbound, _) = mpsc::channel(1);
                    let messages = match attempt {
                        0 => vec![Ok(session::closed_frame("coordinator rejected session"))],
                        1 => vec![Ok(JoinMessage::default())],
                        2 => vec![Err(tonic::Status::invalid_argument(
                            "registration rejected",
                        ))],
                        _ => vec![],
                    };
                    Ok(Transport {
                        outbound,
                        inbound: Box::pin(futures::stream::iter(messages)),
                    })
                })
            });
            let mut handle = start_join(connector, ConsumerOptions::default())
                .await
                .unwrap();
            snapshot_received(&mut handle).await;
            assert_eq!(calls.load(Ordering::SeqCst), 5);
            handle.shutdown().await.unwrap();
        }

        #[tokio::test(start_paused = true)]
        async fn established_session_failures_all_reconnect() {
            for failure in [
                "closed",
                "rpc",
                "invalid-grant",
                "invalid-cluster",
                "eof",
                "send-dropped",
                "send-stalled",
            ] {
                let mut h = Harness::new();
                h.inbound
                    .try_send(Ok(deadline::session_frame(
                        SystemTime::now() + Duration::from_secs(600),
                        false,
                    )))
                    .unwrap();
                let (transport, _incoming, _outgoing) = ready_transport();
                let transport = Mutex::new(Some(transport));
                h.driver.reconnect = Some((
                    ReconnectOptions::default(),
                    Arc::new(move |_| {
                        ready(Ok(transport
                            .lock()
                            .unwrap()
                            .take()
                            .expect("one successful reconnect")))
                        .boxed()
                    }),
                ));
                match failure {
                    "closed" => h
                        .inbound
                        .try_send(Ok(session::closed_frame("restart")))
                        .unwrap(),
                    "rpc" => h
                        .inbound
                        .try_send(Err(tonic::Status::permission_denied("permission changed")))
                        .unwrap(),
                    "invalid-grant" => h
                        .inbound
                        .try_send(Ok(wrap_client(client_message::Msg::Assign(
                            client_message::Assign {
                                grants: vec![pb::Grant::default()],
                            },
                        ))))
                        .unwrap(),
                    "invalid-cluster" => h
                        .inbound
                        .try_send(Ok(JoinMessage {
                            msg: Some(join_message::Msg::Consumer(ConsumerMessage {
                                msg: Some(consumer_message::Msg::Cluster(
                                    pb::ClusterMessage::default(),
                                )),
                            })),
                        }))
                        .unwrap(),
                    "eof" => drop(h.inbound),
                    "send-dropped" => h.outbound.close(),
                    "send-stalled" => {
                        while h
                            .driver
                            .transport
                            .as_ref()
                            .unwrap()
                            .outbound
                            .try_send(JoinMessage::default())
                            .is_ok()
                        {}
                    }
                    _ => unreachable!(),
                }
                h.handle.task = Some(tokio::spawn(h.driver.run()));
                timeout(Duration::from_secs(45), async {
                    loop {
                        if h.handle.cluster.borrow_and_update().id().version == 1 {
                            break;
                        }
                        h.handle.cluster.changed().await.unwrap();
                    }
                })
                .await
                .unwrap_or_else(|_| panic!("did not recover from {failure}"));
                h.handle.shutdown().await.unwrap();
            }
        }

        #[tokio::test(start_paused = true)]
        async fn disconnect_limit_cancels_initial_attempt_and_reports_terminal_reason() {
            let dropped = Arc::new(AtomicUsize::new(0));
            let drops = dropped.clone();
            let connector: Connector = Arc::new(move |_| {
                let guard = Dropped(drops.clone());
                Box::pin(async move {
                    let _guard = guard;
                    std::future::pending().await
                })
            });
            let start = tokio::time::Instant::now();
            let handle = start_join(
                connector,
                ConsumerOptions::default().with_disconnect_timeout(Duration::from_secs(2)),
            )
            .await
            .unwrap();
            handle.closed().closed().await;
            assert_eq!(start.elapsed(), Duration::from_secs(2));
            assert_eq!(dropped.load(Ordering::SeqCst), 1);
            assert!(matches!(
                handle.shutdown().await,
                Err(ClientError::DisconnectTimeout)
            ));
        }

        #[tokio::test(start_paused = true)]
        async fn handshake_without_snapshot_does_not_reset_disconnect_budget() {
            let connector: Connector = Arc::new(|_| {
                Box::pin(async {
                    let (outbound, _) = mpsc::channel(1);
                    Ok(Transport {
                        outbound,
                        inbound: Box::pin(futures::stream::iter([Ok(established())])),
                    })
                })
            });
            let start = tokio::time::Instant::now();
            let handle = start_join(
                connector,
                ConsumerOptions::default().with_disconnect_timeout(Duration::from_secs(3)),
            )
            .await
            .unwrap();
            handle.closed().closed().await;
            assert_eq!(start.elapsed(), Duration::from_secs(3));
            assert!(matches!(
                handle.shutdown().await,
                Err(ClientError::DisconnectTimeout)
            ));
        }

        #[tokio::test(start_paused = true)]
        async fn shutdown_and_drop_cancel_pending_initial_connection() {
            for force in [false, true] {
                let dropped = Arc::new(AtomicUsize::new(0));
                let drops = dropped.clone();
                let connector: Connector = Arc::new(move |_| {
                    let guard = Dropped(drops.clone());
                    Box::pin(async move {
                        let _guard = guard;
                        std::future::pending().await
                    })
                });
                let handle = start_join(connector, ConsumerOptions::default())
                    .await
                    .unwrap();
                let closed = handle.closed();
                tokio::task::yield_now().await;
                if force {
                    drop(handle);
                } else {
                    handle.shutdown().await.unwrap();
                }
                closed.closed().await;
                assert_eq!(dropped.load(Ordering::SeqCst), 1);
            }
        }

        #[tokio::test(start_paused = true)]
        async fn invalid_controls_are_rejected_before_spawning() {
            for duration in [Duration::ZERO, Duration::MAX] {
                for opts in [
                    ConsumerOptions::default().with_drain_timeout(duration),
                    ConsumerOptions::default().with_disconnect_timeout(duration),
                    ConsumerOptions::default().with_reconnect(ReconnectOptions {
                        backoff: duration,
                        ..Default::default()
                    }),
                ] {
                    let connector: Connector =
                        Arc::new(|_| panic!("invalid options must not connect"));
                    assert!(matches!(
                        start_join(connector, opts).await,
                        Err(ClientError::InvalidMessage(_))
                    ));
                }
            }
        }

        #[tokio::test(start_paused = true)]
        async fn live_grants_survive_disconnect_and_local_actions_continue_without_coordinator() {
            let mut h = Harness::new();
            for id in ["active", "released", "expired", "loaded"] {
                h.driver
                    .on_assign(grant(
                        id,
                        if id == "loaded" {
                            pb::GrantState::Allocated
                        } else {
                            pb::GrantState::Active
                        },
                    ))
                    .await
                    .unwrap();
            }
            let ownership = |id: &str| {
                h.handle.assignments.borrow()[&GrantId(id.into())]
                    .ownership
                    .clone()
            };
            let active = ownership("active");
            let released = ownership("released");
            let expired = ownership("expired");
            let loaded = ownership("loaded");
            let seen = Arc::new(Mutex::new(Vec::new()));
            let replay = seen.clone();
            h.driver.reconnect = Some((
                ReconnectOptions::default(),
                Arc::new(move |grants| {
                    *replay.lock().unwrap() = grants;
                    Box::pin(async {
                        Err(ClientError::Transport(tonic::Status::unavailable(
                            "offline",
                        )))
                    })
                }),
            ));
            h.inbound
                .try_send(Ok(session::closed_frame("restart")))
                .unwrap();
            h.handle.task = Some(tokio::spawn(h.driver.run()));
            tokio::task::yield_now().await;
            assert!(!active.revoked().is_closed());
            active.wait_for_active().await.unwrap();
            released.release();
            expired.set_expiration(SystemTime::UNIX_EPOCH);
            loaded.loader().load();
            timeout(Duration::from_secs(1), async {
                while h.handle.assignments.borrow_and_update().len() != 2 {
                    h.handle.assignments.changed().await.unwrap();
                }
            })
            .await
            .unwrap();
            tokio::time::advance(Duration::from_secs(2)).await;
            tokio::task::yield_now().await;
            let replay = seen.lock().unwrap().clone();
            assert_eq!(replay.len(), 2);
            assert!(replay.iter().all(|g| g.id == "active" || g.id == "loaded"));
            assert_eq!(
                replay.iter().find(|g| g.id == "loaded").unwrap().state,
                pb::GrantState::AllocatedLoaded as i32
            );
            assert!(!active.expired().is_closed());
            assert!(!active.revoked().is_closed());
            assert!(!h.handle.closed().is_closed());
            active.release();
            loaded.release();
            h.handle.shutdown().await.unwrap();
        }

        #[tokio::test(start_paused = true)]
        async fn offline_shutdown_revokes_then_waits_for_local_release() {
            let mut h = Harness::new();
            h.driver
                .on_assign(grant("g", pb::GrantState::Active))
                .await
                .unwrap();
            let ownership = h.handle.assignments.borrow()[&GrantId("g".into())]
                .ownership
                .clone();
            h.driver.reconnect = Some((
                ReconnectOptions::default(),
                Arc::new(|_| Box::pin(std::future::pending())),
            ));
            h.inbound
                .try_send(Ok(session::closed_frame("restart")))
                .unwrap();
            h.handle.task = Some(tokio::spawn(h.driver.run()));
            tokio::task::yield_now().await;
            let closed = h.handle.closed();
            let shutdown = tokio::spawn(h.handle.shutdown());
            ownership.revoked().closed().await;
            assert!(!ownership.expired().is_closed());
            assert!(!closed.is_closed());
            ownership.release();
            shutdown.await.unwrap().unwrap();
            assert!(closed.is_closed());
        }

        #[tokio::test(start_paused = true)]
        async fn graceful_shutdown_is_bounded_even_with_a_stalled_outbound_stream() {
            let mut h = Harness::new();
            h.driver.drain_timeout = Duration::from_secs(2);
            h.driver
                .on_assign(grant("g", pb::GrantState::Active))
                .await
                .unwrap();
            let ownership = h.handle.assignments.borrow()[&GrantId("g".into())]
                .ownership
                .clone();
            while h
                .driver
                .transport
                .as_ref()
                .unwrap()
                .outbound
                .try_send(JoinMessage::default())
                .is_ok()
            {}
            h.handle.task = Some(tokio::spawn(h.driver.run()));
            let start = tokio::time::Instant::now();
            assert!(matches!(
                h.handle.shutdown().await,
                Err(ClientError::DrainTimeout)
            ));
            assert_eq!(start.elapsed(), Duration::from_secs(2));
            assert!(ownership.expired().is_closed());
        }

        #[tokio::test(start_paused = true)]
        async fn disconnect_timeout_drains_locally_before_reporting_termination() {
            let mut h = Harness::new();
            h.driver
                .on_assign(grant("g", pb::GrantState::Active))
                .await
                .unwrap();
            let ownership = h.handle.assignments.borrow()[&GrantId("g".into())]
                .ownership
                .clone();
            let calls = Arc::new(AtomicUsize::new(0));
            let attempts = calls.clone();
            h.driver.reconnect = Some((
                ReconnectOptions {
                    timeout: Some(Duration::from_secs(2)),
                    ..Default::default()
                },
                Arc::new(move |_| {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Box::pin(std::future::pending())
                }),
            ));
            h.inbound
                .try_send(Ok(session::closed_frame("restart")))
                .unwrap();
            h.handle.task = Some(tokio::spawn(h.driver.run()));
            tokio::task::yield_now().await;
            assert!(!ownership.revoked().is_closed());
            assert!(!ownership.expired().is_closed());
            ownership.revoked().closed().await;
            assert!(
                !h.handle.closed().is_closed(),
                "disconnect timeout begins drain, not immediate abort"
            );
            assert!(!ownership.expired().is_closed());
            ownership.release();
            h.handle.closed().closed().await;
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert!(matches!(
                h.handle.shutdown().await,
                Err(ClientError::DisconnectTimeout)
            ));
        }

        #[tokio::test(start_paused = true)]
        async fn disconnect_timeout_bounds_uncooperative_local_drain() {
            let mut h = Harness::new();
            h.driver
                .on_assign(grant("g", pb::GrantState::Active))
                .await
                .unwrap();
            let ownership = h.handle.assignments.borrow()[&GrantId("g".into())]
                .ownership
                .clone();
            h.driver.drain_timeout = Duration::from_secs(3);
            h.driver.reconnect = Some((
                ReconnectOptions {
                    timeout: Some(Duration::from_secs(2)),
                    ..Default::default()
                },
                Arc::new(|_| Box::pin(std::future::pending())),
            ));
            h.inbound
                .try_send(Ok(session::closed_frame("restart")))
                .unwrap();
            h.handle.task = Some(tokio::spawn(h.driver.run()));
            let start = tokio::time::Instant::now();
            h.handle.closed().closed().await;
            assert_eq!(start.elapsed(), Duration::from_secs(5));
            assert!(ownership.expired().is_closed());
            assert!(h.handle.assignments.borrow().is_empty());
            assert!(matches!(
                h.handle.shutdown().await,
                Err(ClientError::DisconnectTimeout)
            ));
        }

        #[tokio::test(start_paused = true)]
        async fn valid_snapshot_resets_disconnect_budget_for_next_outage() {
            let (transport, incoming, _outgoing) = ready_transport();
            let transport = Mutex::new(Some(transport));
            let connector: Connector = Arc::new(move |_| {
                let transport = transport.lock().unwrap().take();
                Box::pin(async move {
                    match transport {
                        Some(transport) => Ok(transport),
                        None => Err(ClientError::Transport(tonic::Status::unavailable(
                            "offline",
                        ))),
                    }
                })
            });
            let mut handle = start_join(
                connector,
                ConsumerOptions::default().with_disconnect_timeout(Duration::from_secs(2)),
            )
            .await
            .unwrap();
            snapshot_received(&mut handle).await;
            tokio::time::advance(Duration::from_secs(3)).await;
            tokio::task::yield_now().await;
            assert!(
                !handle.closed().is_closed(),
                "connected lifetime is not bounded by disconnect timeout"
            );
            let start = tokio::time::Instant::now();
            drop(incoming);
            handle.closed().closed().await;
            assert_eq!(start.elapsed(), Duration::from_secs(2));
            assert!(matches!(
                handle.shutdown().await,
                Err(ClientError::DisconnectTimeout)
            ));
        }

        #[tokio::test]
        async fn processor_releases_expired_range_while_consumer_keeps_reconnecting() {
            use crate::{
                ConnectionPool, DispatchFilter, FilterContext, InstanceId, Processor, Range,
                RangeFactory, recording_range::RecordingRange,
            };
            struct Factory;
            impl RangeFactory for Factory {
                type Range = RecordingRange;
                fn build(&self, _: GrantId, _: Shard, _: Arc<Ownership>) -> Option<RecordingRange> {
                    Some(RecordingRange::new(
                        "offline",
                        Duration::ZERO,
                        Duration::ZERO,
                        Arc::new(Mutex::new(Vec::new())),
                    ))
                }
            }
            let mut h = Harness::new();
            let id = GrantId("g".into());
            h.driver
                .on_assign(grant("g", pb::GrantState::Active))
                .await
                .unwrap();
            let assignment = h.handle.assignments.borrow()[&id].clone();
            let ownership = assignment.ownership.clone();
            let processor = Processor::new(Arc::new(Factory));
            processor.init(FilterContext {
                service: QualifiedServiceName::new("test", "lifecycle"),
                pool: ConnectionPool::new(InstanceId::new()),
                cluster: h.handle.cluster.clone(),
            });
            let worker = processor.clone();
            let handler = tokio::spawn(async move {
                worker
                    .try_handle(id, assignment.shard, assignment.ownership)
                    .await
            });
            let range = timeout(Duration::from_secs(1), async {
                loop {
                    if let Some((_, GrantState::Active, range)) =
                        processor.grants().get(&GrantId("g".into()))
                    {
                        return range;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            h.driver.reconnect = Some((
                ReconnectOptions::default(),
                Arc::new(|_| Box::pin(std::future::pending())),
            ));
            h.inbound
                .try_send(Ok(session::closed_frame("restart")))
                .unwrap();
            h.handle.task = Some(tokio::spawn(h.driver.run()));
            tokio::task::yield_now().await;
            assert!(!range.closed().is_closed());
            assert!(!ownership.revoked().is_closed());
            assert!(!ownership.expired().is_closed());
            ownership.set_expiration(SystemTime::UNIX_EPOCH);
            assert!(
                timeout(Duration::from_secs(1), handler)
                    .await
                    .unwrap()
                    .unwrap()
            );
            timeout(Duration::from_secs(1), async {
                while !h.handle.assignments.borrow_and_update().is_empty() {
                    h.handle.assignments.changed().await.unwrap();
                }
            })
            .await
            .unwrap();
            assert!(range.closed().is_closed());
            assert!(ownership.released.is_closed());
            assert!(processor.grants().is_empty());
            assert!(
                !h.handle.closed().is_closed(),
                "grant expiry must not stop the consumer"
            );
            h.handle.shutdown().await.unwrap();
        }

        #[tokio::test(start_paused = true)]
        async fn attempt_deadline_wins_over_simultaneously_ready_connection() {
            let (first, _first_incoming, _first_outgoing) = ready_transport();
            let (second, _incoming, _outgoing) = ready_transport();
            let transports = Mutex::new(std::collections::VecDeque::from([first, second]));
            let calls = Arc::new(AtomicUsize::new(0));
            let attempts = calls.clone();
            let connector: Connector = Arc::new(move |_| {
                let transport = transports.lock().unwrap().pop_front().unwrap();
                let first = attempts.fetch_add(1, Ordering::SeqCst) == 0;
                Box::pin(async move {
                    if first {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                    Ok(transport)
                })
            });
            let mut handle = start_join(
                connector,
                ConsumerOptions::default().with_join_timeout(Duration::from_secs(1)),
            )
            .await
            .unwrap();
            snapshot_received(&mut handle).await;
            assert_eq!(
                calls.load(Ordering::SeqCst),
                2,
                "deadline must discard the first attempt"
            );
            handle.shutdown().await.unwrap();
        }

        #[test]
        fn disconnect_limit_is_a_terminal_deadline_not_an_ownership_retry() {
            assert!(!ClientError::DisconnectTimeout.is_ownership());
            assert_eq!(
                ClientError::DisconnectTimeout.into_grpc().code(),
                tonic::Code::DeadlineExceeded
            );
        }
    }

    struct Harness {
        driver: Driver,
        handle: JoinHandle,
        inbound: mpsc::Sender<std::result::Result<JoinMessage, tonic::Status>>,
        outbound: mpsc::Receiver<JoinMessage>,
    }

    impl Harness {
        fn new() -> Self {
            let (inbound, receiver) = mpsc::channel(1024);
            let (outbound, outgoing) = mpsc::channel(1024);
            let (driver, handle) = Driver::new(Some(Transport {
                outbound,
                inbound: Box::pin(ReceiverStream::new(receiver)),
            }));
            Self {
                driver,
                handle,
                inbound,
                outbound: outgoing,
            }
        }
    }

    fn grant(id: &str, state: pb::GrantState) -> pb::Grant {
        pb::Grant {
            id: id.into(),
            state: state as i32,
            shard: Some(
                Shard {
                    domain: QualifiedDomainName {
                        service: QualifiedServiceName::new("test", "lifecycle"),
                        name: "ranges".into(),
                    },
                    kind: DomainType::Global,
                    region: crate::ids::Region::default(),
                    from: String::new(),
                    to: String::new(),
                }
                .into(),
            ),
            lease: Some(system_time_to_timestamp(
                SystemTime::now() + Duration::from_secs(60),
            )),
            ..Default::default()
        }
    }

    fn client_variant(message: JoinMessage) -> client_message::Msg {
        match message.msg.unwrap() {
            join_message::Msg::Consumer(ConsumerMessage {
                msg: Some(consumer_message::Msg::Client(ClientMessage { msg: Some(message) })),
            }) => message,
            _ => panic!("expected client frame"),
        }
    }

    async fn next(receiver: &mut mpsc::Receiver<JoinMessage>) -> JoinMessage {
        timeout(Duration::from_secs(2), receiver.recv())
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn initial_snapshot_survives_startup_and_broadcast_overflow() {
        let mut h = Harness::new();
        for n in 0..400 {
            h.driver
                .on_assign(grant(&n.to_string(), pb::GrantState::Active))
                .await
                .unwrap();
        }
        assert!(matches!(
            h.handle.grants.try_recv(),
            Err(broadcast::error::TryRecvError::Lagged(_))
        ));
        let late_subscriber = h.handle.assignments.clone();
        assert_eq!(late_subscriber.borrow().len(), 400);
    }

    #[tokio::test]
    async fn session_closed_cleans_up_grants_and_closes_handle() {
        let mut h = Harness::new();
        h.driver
            .on_assign(grant("g", pb::GrantState::Active))
            .await
            .unwrap();
        let ownership = h.driver.grants[&GrantId("g".into())].ownership.clone();
        h.inbound
            .send(Ok(session::closed_frame("server stopped")))
            .await
            .unwrap();
        assert!(h.driver.run().await.is_err());
        assert!(h.handle.closed().is_closed());
        assert!(ownership.expired().is_closed());
        assert!(h.handle.assignments.borrow().is_empty());
    }

    #[tokio::test]
    async fn shutdown_before_first_poll_sends_deregister_and_closed() {
        let mut h = Harness::new();
        h.handle.shutdown.close();
        h.driver.run().await.unwrap();
        assert!(matches!(
            client_variant(next(&mut h.outbound).await),
            client_message::Msg::Deregister(_)
        ));
        assert!(matches!(
            next(&mut h.outbound).await.msg,
            Some(join_message::Msg::Session(_))
        ));
        assert!(h.handle.closed().is_closed());
    }

    #[tokio::test]
    async fn shutdown_waits_for_release_and_sends_latest_grant_state() {
        let mut h = Harness::new();
        let mut g = grant("g", pb::GrantState::Active);
        h.driver.on_assign(g.clone()).await.unwrap();
        let ownership = h.driver.grants[&GrantId("g".into())].ownership.clone();
        h.handle.task = Some(tokio::spawn(h.driver.run()));
        let shutdown = tokio::spawn(h.handle.shutdown());
        assert!(matches!(
            client_variant(next(&mut h.outbound).await),
            client_message::Msg::Deregister(_)
        ));
        assert!(!shutdown.is_finished());
        g.state = pb::GrantState::Revoked as i32;
        h.inbound
            .send(Ok(wrap_client(client_message::Msg::Revoke(
                client_message::Revoke { grants: vec![g] },
            ))))
            .await
            .unwrap();
        ownership.wait_for_revoke().await.unwrap().unload();
        let client_message::Msg::Update(update) = client_variant(next(&mut h.outbound).await)
        else {
            panic!("expected unload");
        };
        assert_eq!(
            update.grant.unwrap().state,
            pb::GrantState::RevokedUnloaded as i32
        );
        ownership.release();
        let client_message::Msg::Released(released) = client_variant(next(&mut h.outbound).await)
        else {
            panic!("expected release");
        };
        assert_eq!(
            released.grants[0].state,
            pb::GrantState::RevokedUnloaded as i32
        );
        assert!(matches!(
            next(&mut h.outbound).await.msg,
            Some(join_message::Msg::Session(_))
        ));
        timeout(Duration::from_secs(2), shutdown)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn dropping_handle_aborts_driver_even_before_first_poll() {
        let mut h = Harness::new();
        h.driver
            .on_assign(grant("g", pb::GrantState::Active))
            .await
            .unwrap();
        let ownership = h.driver.grants[&GrantId("g".into())].ownership.clone();
        let closed = h.handle.closed();
        h.handle.task = Some(tokio::spawn(h.driver.run()));
        drop(h.handle);
        timeout(Duration::from_secs(2), closed.closed())
            .await
            .unwrap();
        assert!(ownership.expired().is_closed());
    }

    #[tokio::test]
    async fn cancelling_shutdown_aborts_driver_and_its_grants() {
        let mut h = Harness::new();
        h.driver
            .on_assign(grant("g", pb::GrantState::Active))
            .await
            .unwrap();
        let ownership = h.driver.grants[&GrantId("g".into())].ownership.clone();
        let closed = h.handle.closed();
        h.handle.task = Some(tokio::spawn(h.driver.run()));
        assert!(
            timeout(Duration::from_millis(10), h.handle.shutdown())
                .await
                .is_err()
        );
        timeout(Duration::from_secs(2), closed.closed())
            .await
            .unwrap();
        assert!(ownership.expired().is_closed());
    }

    #[tokio::test]
    async fn expiration_removes_grant_and_publishes_removal() {
        let mut h = Harness::new();
        h.driver
            .on_assign(grant("g", pb::GrantState::Active))
            .await
            .unwrap();
        h.driver.grants[&GrantId("g".into())]
            .ownership
            .set_expiration(SystemTime::UNIX_EPOCH);
        let action = timeout(Duration::from_secs(2), h.driver.actions.recv())
            .await
            .unwrap()
            .unwrap();
        h.driver.on_action(action).await.unwrap();
        assert!(h.driver.grants.is_empty());
        assert!(h.handle.assignments.borrow().is_empty());
        assert!(h.outbound.try_recv().is_err());
    }

    #[tokio::test]
    async fn promotion_refreshes_lease_and_active_load_does_not_regress_state() {
        let mut h = Harness::new();
        let mut g = grant("g", pb::GrantState::Allocated);
        h.driver.on_assign(g.clone()).await.unwrap();
        let o = h.driver.grants[&GrantId("g".into())].ownership.clone();
        let deadline = SystemTime::now() + Duration::from_secs(120);
        g.lease = Some(system_time_to_timestamp(deadline));
        g.state = pb::GrantState::Active as i32;
        h.driver.on_promote(g).await.unwrap();
        assert_eq!(o.expiration(), deadline);
        h.driver
            .on_action(GrantAction {
                id: GrantId("g".into()),
                ownership: o,
                action: Action::Load,
            })
            .await
            .unwrap();
        assert!(h.outbound.try_recv().is_err());
        assert_eq!(
            h.driver.grants[&GrantId("g".into())].pb_grant.state,
            pb::GrantState::Active as i32
        );
    }

    #[tokio::test]
    async fn duplicate_assignment_preserves_world_identity_and_stale_release_is_ignored() {
        let mut h = Harness::new();
        let g = grant("g", pb::GrantState::Active);
        h.driver.on_assign(g.clone()).await.unwrap();
        let old = h.driver.grants[&GrantId("g".into())].ownership.clone();
        h.driver.on_assign(g.clone()).await.unwrap();
        assert!(Arc::ptr_eq(
            &old,
            &h.driver.grants[&GrantId("g".into())].ownership
        ));
        old.expire();
        h.driver.on_assign(g).await.unwrap();
        let new = h.driver.grants[&GrantId("g".into())].ownership.clone();
        assert!(!Arc::ptr_eq(&old, &new));
        h.driver
            .on_action(GrantAction {
                id: GrantId("g".into()),
                ownership: old,
                action: Action::Release,
            })
            .await
            .unwrap();
        assert_eq!(h.driver.grants.len(), 1);
        assert!(!new.expired().is_closed());
    }

    #[tokio::test]
    async fn unconfirmed_and_revoked_grants_are_not_extended() {
        let mut h = Harness::new();
        h.driver
            .on_assign(grant("stale", pb::GrantState::Active))
            .await
            .unwrap();
        h.driver
            .on_assign(grant("revoked", pb::GrantState::Revoked))
            .await
            .unwrap();
        h.driver
            .grants
            .get_mut(&GrantId("stale".into()))
            .unwrap()
            .confirmed = false;
        let before: Vec<_> = h
            .driver
            .grants
            .values()
            .map(|entry| entry.ownership.expiration())
            .collect();
        h.driver.on_extend(client_message::Extend {
            lease: Some(system_time_to_timestamp(
                SystemTime::now() + Duration::from_secs(600),
            )),
        });
        let after: Vec<_> = h
            .driver
            .grants
            .values()
            .map(|entry| entry.ownership.expiration())
            .collect();
        assert_eq!(before, after);
    }

    #[tokio::test]
    async fn rejects_assignment_without_lease() {
        let mut h = Harness::new();
        let mut g = grant("missing", pb::GrantState::Active);
        g.lease = None;
        assert!(matches!(
            h.driver.on_assign(g).await,
            Err(ClientError::InvalidMessage(_))
        ));
        assert!(h.driver.grants.is_empty());
    }

    #[tokio::test]
    async fn reconnect_replays_unexpired_grants_and_preserves_identity() {
        let mut h = Harness::new();
        let g = grant("g", pb::GrantState::Active);
        h.driver.on_assign(g.clone()).await.unwrap();
        let ownership = h.driver.grants[&GrantId("g".into())].ownership.clone();
        let (transport_tx, transport_rx) = std::sync::mpsc::channel();
        let (incoming, receiver) = mpsc::channel(8);
        incoming.send(Ok(established())).await.unwrap();
        let (outbound, _outgoing) = mpsc::channel(8);
        assert!(
            transport_tx
                .send(Transport {
                    outbound,
                    inbound: Box::pin(ReceiverStream::new(receiver))
                })
                .is_ok()
        );
        let receiver = Arc::new(std::sync::Mutex::new(transport_rx));
        let replay = Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = replay.clone();
        h.driver.reconnect = Some((
            ReconnectOptions {
                timeout: Some(Duration::from_secs(1)),
                backoff: Duration::from_millis(1),
            },
            Arc::new(move |active| {
                *captured.lock().unwrap() = active;
                ready(Ok(receiver.lock().unwrap().try_recv().unwrap())).boxed()
            }),
        ));
        h.driver.reconnect().await.unwrap();
        assert_eq!(replay.lock().unwrap().len(), 1);
        assert!(!h.driver.grants[&GrantId("g".into())].confirmed);
        h.driver.on_assign(g).await.unwrap();
        assert!(Arc::ptr_eq(
            &ownership,
            &h.driver.grants[&GrantId("g".into())].ownership
        ));
    }

    #[tokio::test]
    async fn reconnect_is_bounded_and_shutdown_interrupts_it() {
        let mut h = Harness::new();
        h.driver.reconnect = Some((
            ReconnectOptions {
                timeout: Some(Duration::from_millis(10)),
                backoff: Duration::from_millis(1),
            },
            Arc::new(|_| Box::pin(std::future::pending())),
        ));
        assert!(matches!(
            h.driver.reconnect().await,
            Err(ClientError::DisconnectTimeout)
        ));
        h.handle.shutdown.close();
        assert!(matches!(
            h.driver.reconnect().await,
            Err(ClientError::Cancelled)
        ));
    }

    fn established() -> JoinMessage {
        use splitter_proto::session_pb;
        JoinMessage {
            msg: Some(join_message::Msg::Session(session_pb::Message {
                request: Some(session_pb::message::Request::Established(
                    session_pb::message::Established {
                        ttl: Some(system_time_to_timestamp(
                            SystemTime::now() + Duration::from_secs(60),
                        )),
                        ..Default::default()
                    },
                )),
            })),
        }
    }

    #[tokio::test]
    async fn silent_reconnected_stream_does_not_reset_recovery_deadline() {
        let mut h = Harness::new();
        h.driver.reconnect = Some((
            ReconnectOptions {
                timeout: Some(Duration::from_millis(10)),
                backoff: Duration::from_millis(1),
            },
            Arc::new(|_| {
                Box::pin(async {
                    let (outbound, _receiver) = mpsc::channel(1);
                    Ok(Transport {
                        outbound,
                        inbound: Box::pin(futures::stream::pending()),
                    })
                })
            }),
        ));
        assert!(matches!(
            h.driver.reconnect().await,
            Err(ClientError::DisconnectTimeout)
        ));
    }

    #[tokio::test]
    async fn two_consumers_complete_unload_load_and_release_handover() {
        use crate::{
            ConnectionPool, DispatchFilter, FilterContext, InstanceId, Processor, RangeFactory,
            recording_range::RecordingRange,
        };
        struct Factory {
            label: &'static str,
            log: Arc<std::sync::Mutex<Vec<String>>>,
        }
        impl RangeFactory for Factory {
            type Range = RecordingRange;
            fn build(&self, _: GrantId, _: Shard, _: Arc<Ownership>) -> Option<RecordingRange> {
                self.log
                    .lock()
                    .unwrap()
                    .push(format!("{} built", self.label));
                Some(RecordingRange::new(
                    self.label,
                    Duration::ZERO,
                    Duration::ZERO,
                    self.log.clone(),
                ))
            }
        }
        async fn active(p: &Processor<Factory>, id: &str) {
            timeout(Duration::from_secs(2), async {
                while !p
                    .grants()
                    .get(&GrantId(id.into()))
                    .is_some_and(|(_, state, _)| state == GrantState::Active)
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut old = Harness::new();
        let mut new = Harness::new();
        let old_grant = grant("old", pb::GrantState::Active);
        let new_grant = grant("new", pb::GrantState::Allocated);
        let old_processor = Processor::new(Arc::new(Factory {
            label: "old",
            log: log.clone(),
        }));
        let new_processor = Processor::new(Arc::new(Factory {
            label: "new",
            log: log.clone(),
        }));
        for (processor, h) in [(&old_processor, &old), (&new_processor, &new)] {
            processor.init(FilterContext {
                service: QualifiedServiceName::new("test", "lifecycle"),
                pool: ConnectionPool::new(InstanceId::new()),
                cluster: h.handle.cluster.clone(),
            });
        }
        old.driver.on_assign(old_grant.clone()).await.unwrap();
        new.driver.on_assign(new_grant.clone()).await.unwrap();
        let old_assignment = old.handle.assignments.borrow()[&GrantId("old".into())].clone();
        let new_assignment = new.handle.assignments.borrow()[&GrantId("new".into())].clone();
        let old_p = old_processor.clone();
        let old_task = tokio::spawn(async move {
            old_p
                .try_handle(
                    GrantId("old".into()),
                    old_assignment.shard,
                    old_assignment.ownership,
                )
                .await
        });
        let new_p = new_processor.clone();
        let new_task = tokio::spawn(async move {
            new_p
                .try_handle(
                    GrantId("new".into()),
                    new_assignment.shard,
                    new_assignment.ownership,
                )
                .await
        });
        old.handle.task = Some(tokio::spawn(old.driver.run()));
        new.handle.task = Some(tokio::spawn(new.driver.run()));
        active(&old_processor, "old").await;
        assert!(!log.lock().unwrap().iter().any(|line| line == "new built"));
        let revoked = pb::Grant {
            state: pb::GrantState::Revoked as i32,
            ..old_grant
        };
        old.inbound
            .send(Ok(wrap_client(client_message::Msg::Revoke(
                client_message::Revoke {
                    grants: vec![revoked.clone()],
                },
            ))))
            .await
            .unwrap();
        let client_message::Msg::Update(unloaded) = client_variant(next(&mut old.outbound).await)
        else {
            panic!("expected unload");
        };
        assert_eq!(
            unloaded.grant.as_ref().unwrap().state,
            pb::GrantState::RevokedUnloaded as i32
        );
        new.inbound
            .send(Ok(wrap_client(client_message::Msg::Notify(
                client_message::Notify {
                    target: Some(new_grant.clone()),
                    update: unloaded.grant,
                },
            ))))
            .await
            .unwrap();
        let client_message::Msg::Update(loaded) = client_variant(next(&mut new.outbound).await)
        else {
            panic!("expected load");
        };
        assert_eq!(
            loaded.grant.as_ref().unwrap().state,
            pb::GrantState::AllocatedLoaded as i32
        );
        old.inbound
            .send(Ok(wrap_client(client_message::Msg::Notify(
                client_message::Notify {
                    target: Some(revoked),
                    update: loaded.grant,
                },
            ))))
            .await
            .unwrap();
        let client_message::Msg::Released(released) = client_variant(next(&mut old.outbound).await)
        else {
            panic!("expected release");
        };
        assert_eq!(released.grants[0].id, "old");
        new.inbound
            .send(Ok(wrap_client(client_message::Msg::Promote(
                client_message::Promote {
                    grants: vec![pb::Grant {
                        state: pb::GrantState::Active as i32,
                        ..new_grant
                    }],
                },
            ))))
            .await
            .unwrap();
        active(&new_processor, "new").await;
        timeout(Duration::from_secs(2), old_task)
            .await
            .unwrap()
            .unwrap();
        assert!(old_processor.grants().is_empty());
        let events = log.lock().unwrap().clone();
        assert!(
            events.iter().position(|e| e == "old drain done").unwrap()
                < events.iter().position(|e| e == "new built").unwrap()
        );
        old.handle.shutdown().await.unwrap();
        drop(new.handle);
        timeout(Duration::from_secs(2), new_task)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn duplicate_revoke_preserves_and_replays_unloaded_state() {
        let mut h = Harness::new();
        let g = grant("g", pb::GrantState::Revoked);
        h.driver.on_assign(g.clone()).await.unwrap();
        let o = h.driver.grants[&GrantId("g".into())].ownership.clone();
        h.driver
            .on_action(GrantAction {
                id: GrantId("g".into()),
                ownership: o,
                action: Action::Unload,
            })
            .await
            .unwrap();
        let _ = next(&mut h.outbound).await;
        h.driver.on_revoke(g).await.unwrap();
        let client_message::Msg::Update(update) = client_variant(next(&mut h.outbound).await)
        else {
            panic!("expected unload replay");
        };
        assert_eq!(
            update.grant.unwrap().state,
            pb::GrantState::RevokedUnloaded as i32
        );
    }

    #[tokio::test]
    async fn assignment_replay_resends_local_load_and_revoke_request() {
        let mut h = Harness::new();
        let g = grant("g", pb::GrantState::Allocated);
        h.driver.on_assign(g.clone()).await.unwrap();
        let o = h.driver.grants[&GrantId("g".into())].ownership.clone();
        h.driver
            .on_action(GrantAction {
                id: GrantId("g".into()),
                ownership: o.clone(),
                action: Action::Load,
            })
            .await
            .unwrap();
        let _ = next(&mut h.outbound).await;
        o.request_revoke();
        h.driver.on_assign(g).await.unwrap();
        let client_message::Msg::Update(update) = client_variant(next(&mut h.outbound).await)
        else {
            panic!("expected load replay");
        };
        assert_eq!(
            update.grant.unwrap().state,
            pb::GrantState::AllocatedLoaded as i32
        );
        assert!(matches!(
            client_variant(next(&mut h.outbound).await),
            client_message::Msg::Revoke(_)
        ));
    }
}

#[cfg(test)]
mod shutdown_tests {
    use super::*;
    use crate::{
        ids::{DomainType, QualifiedDomainName},
        wrap::system_time_to_timestamp,
    };

    fn grant(id: &str, state: pb::GrantState) -> pb::Grant {
        pb::Grant {
            id: id.into(),
            state: state as i32,
            shard: Some(
                Shard {
                    domain: QualifiedDomainName {
                        service: QualifiedServiceName::new("test", "shutdown"),
                        name: "ranges".into(),
                    },
                    kind: DomainType::Global,
                    region: crate::ids::Region::default(),
                    from: String::new(),
                    to: String::new(),
                }
                .into(),
            ),
            lease: Some(system_time_to_timestamp(
                SystemTime::now() + Duration::from_secs(60),
            )),
            ..Default::default()
        }
    }

    fn driver() -> (Driver, JoinHandle, mpsc::Receiver<JoinMessage>) {
        let (outbound, outgoing) = mpsc::channel(16);
        let (driver, handle) = Driver::new(Some(Transport {
            outbound,
            inbound: Box::pin(futures::stream::pending()),
        }));
        (driver, handle, outgoing)
    }

    #[tokio::test]
    async fn replay_during_shutdown_does_not_release_a_running_grant() {
        let (mut driver, handle, mut outgoing) = driver();
        let assigned = grant("running", pb::GrantState::Active);
        driver.on_assign(assigned.clone()).await.unwrap();
        let id = GrantId(assigned.id.clone());
        let ownership = driver.grants[&id].ownership.clone();
        handle.shutdown.close();

        driver.on_assign(assigned).await.unwrap();
        assert!(
            outgoing.try_recv().is_err(),
            "replay must not announce release before the handler stops"
        );
        assert!(Arc::ptr_eq(&ownership, &driver.grants[&id].ownership));
        assert!(!ownership.expired().is_closed());

        ownership.release();
        let action = tokio::time::timeout(Duration::from_secs(1), driver.actions.recv())
            .await
            .unwrap()
            .unwrap();
        driver.on_action(action).await.unwrap();
        assert!(driver.grants.is_empty());
        let message = outgoing
            .try_recv()
            .expect("handler completion sends Released");
        assert!(matches!(
            message.msg,
            Some(join_message::Msg::Consumer(ConsumerMessage {
                msg: Some(consumer_message::Msg::Client(ClientMessage {
                    msg: Some(client_message::Msg::Released(_)),
                })),
            }))
        ));
    }

    #[tokio::test]
    async fn replay_during_shutdown_does_not_revive_a_revoked_grant() {
        let (mut driver, handle, mut outgoing) = driver();
        let assigned = grant("draining", pb::GrantState::Active);
        driver.on_assign(assigned.clone()).await.unwrap();
        let id = GrantId(assigned.id.clone());
        let ownership = driver.grants[&id].ownership.clone();
        let mut revoked = assigned.clone();
        revoked.state = pb::GrantState::Revoked as i32;
        driver.on_revoke(revoked).await.unwrap();
        handle.shutdown.close();

        driver.on_assign(assigned).await.unwrap();
        assert!(outgoing.try_recv().is_err());
        assert!(Arc::ptr_eq(&ownership, &driver.grants[&id].ownership));
        assert!(ownership.revoked().is_closed());
        assert!(!ownership.expired().is_closed());
        assert_eq!(
            driver.grants[&id].pb_grant.state,
            pb::GrantState::Revoked as i32
        );
    }

    #[tokio::test]
    async fn new_assignment_during_shutdown_is_released_without_starting_a_handler() {
        let (mut driver, handle, mut outgoing) = driver();
        handle.shutdown.close();
        driver
            .on_assign(grant("new", pb::GrantState::Active))
            .await
            .unwrap();
        assert!(driver.grants.is_empty());
        assert!(handle.assignments.borrow().is_empty());
        let message = outgoing
            .try_recv()
            .expect("new assignment is rejected with Released");
        assert!(matches!(
            message.msg,
            Some(join_message::Msg::Consumer(ConsumerMessage {
                msg: Some(consumer_message::Msg::Client(ClientMessage {
                    msg: Some(client_message::Msg::Released(_)),
                })),
            }))
        ));
    }
}
