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
        let connector: Connector = Arc::new(|_| panic!("invalid settings must not start an RPC"));
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
