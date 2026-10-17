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
                region: Default::default(),
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
    let transport = Transport {
        outbound,
        inbound: Box::pin(futures::stream::pending()),
    };
    let (driver, handle) = Driver::new(transport);
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
