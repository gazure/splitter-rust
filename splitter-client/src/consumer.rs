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
mod tests;

#[cfg(test)]
mod shutdown_tests;
