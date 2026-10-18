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
