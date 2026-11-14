use super::*;
use crate::{
    consumer::GrantAssignment,
    ids::{DomainType, GrantState, QualifiedDomainName},
};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::SystemTime,
};
use tokio::time::timeout;

struct HoldingFilter {
    started: AtomicUsize,
    dropped: Arc<AtomicUsize>,
}
struct CountDrop(Arc<AtomicUsize>);
impl Drop for CountDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl DispatchFilter for HoldingFilter {
    fn init(&self, _: FilterContext) {}
    async fn try_handle(&self, _: GrantId, _: Shard, _: Arc<Ownership>) -> bool {
        let _drop = CountDrop(self.dropped.clone());
        self.started.fetch_add(1, Ordering::SeqCst);
        std::future::pending().await
    }
}
fn assignment() -> GrantAssignment {
    GrantAssignment {
        shard: Shard {
            domain: QualifiedDomainName {
                service: QualifiedServiceName::new("test", "service"),
                name: "domain".into(),
            },
            kind: DomainType::Global,
            region: crate::ids::Region::default(),
            from: String::new(),
            to: String::new(),
        },
        ownership: Ownership::new(
            GrantState::Active,
            SystemTime::now() + Duration::from_secs(60),
        ),
    }
}
async fn count(value: &AtomicUsize, expected: usize) {
    timeout(Duration::from_secs(2), async {
        while value.load(Ordering::SeqCst) != expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn router_reconciles_initial_burst_and_joins_cancelled_children() {
    let filter = Arc::new(HoldingFilter {
        started: AtomicUsize::new(0),
        dropped: Arc::new(AtomicUsize::new(0)),
    });
    let initial = (0..400)
        .map(|i| (GrantId(i.to_string()), assignment()))
        .collect();
    let (tx, rx) = watch::channel(Arc::new(initial));
    let force = Latch::new();
    let router = spawn_grants_router(rx, Arc::new(vec![filter.clone()]), force.reader());
    count(&filter.started, 400).await;
    tx.send_modify(|snapshot| {
        Arc::make_mut(snapshot).clear();
    });
    count(&filter.dropped, 400).await;
    drop(tx);
    timeout(Duration::from_secs(2), router)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn replacement_aborts_old_handler_and_ignores_old_completion() {
    let filter = Arc::new(HoldingFilter {
        started: AtomicUsize::new(0),
        dropped: Arc::new(AtomicUsize::new(0)),
    });
    let id = GrantId("same-id".into());
    let old = assignment();
    let ownership = old.ownership.clone();
    let (tx, rx) = watch::channel(Arc::new(HashMap::from([(id.clone(), old)])));
    let force = Latch::new();
    let router = spawn_grants_router(rx, Arc::new(vec![filter.clone()]), force.reader());
    count(&filter.started, 1).await;
    let replacement = assignment();
    let current = replacement.ownership.clone();
    tx.send_modify(|snapshot| {
        Arc::make_mut(snapshot).insert(id.clone(), replacement);
    });
    count(&filter.started, 2).await;
    count(&filter.dropped, 1).await;
    assert!(ownership.expired().is_closed());
    assert!(!current.expired().is_closed());
    force.close();
    router.await.unwrap();
    assert_eq!(filter.dropped.load(Ordering::SeqCst), 2);
    assert!(current.expired().is_closed());
}

#[tokio::test]
async fn dispatcher_timeout_joins_range_tasks_and_reports_timeout() {
    let filter = Arc::new(HoldingFilter {
        started: AtomicUsize::new(0),
        dropped: Arc::new(AtomicUsize::new(0)),
    });
    let (tx, rx) = watch::channel(Arc::new(HashMap::from([(
        GrantId("g".into()),
        assignment(),
    )])));
    let force = Latch::new();
    let chain: Arc<Vec<Arc<dyn DispatchFilter>>> = Arc::new(vec![filter.clone()]);
    let router = spawn_grants_router(rx, chain.clone(), force.reader());
    count(&filter.started, 1).await;
    let id = Instance {
        id: crate::InstanceId::new(),
        location: crate::ids::Location::default(),
        name: "test".into(),
        created: SystemTime::now(),
        endpoint: crate::ids::Endpoint::default(),
    };
    let (_, cluster) = watch::channel(Arc::new(ClusterMap::empty()));
    let dispatcher = Dispatcher {
        pool: ConnectionPool::new(id.id),
        id,
        service: QualifiedServiceName::new("test", "service"),
        cluster,
        chain,
        closed: Latch::new(),
        join: None,
        tasks: vec![router],
        force,
    };
    assert!(matches!(
        dispatcher.drain(Duration::from_millis(10)).await,
        Err(ClientError::DrainTimeout)
    ));
    assert_eq!(filter.dropped.load(Ordering::SeqCst), 1);
    drop(tx);
}
