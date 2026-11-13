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
            if let Some((id, assignment)) = handle.assignments.borrow_and_update().iter().next() {
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
    let (driver, mut second) = Driver::new(Transport {
        outbound: transport.outbound,
        inbound: Box::pin(inbound),
    });
    second.task = Some(tokio::spawn(driver.run()));

    first.shutdown.close();
    let (id, ownership) = assignment(&mut second).await;
    assert!(!ownership.active().is_closed());
    ownership.loader().load();
    timeout(Duration::from_secs(5), async {
        while !second.cluster.borrow_and_update().grants.contains_key(&id) {
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
            if !second.cluster.borrow_and_update().grants.contains_key(&id) {
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
    assert!(second.cluster.borrow().id.version > 0);
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
