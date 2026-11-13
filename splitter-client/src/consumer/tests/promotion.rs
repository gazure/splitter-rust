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
    let client_message::Msg::Released(released) = client_variant(next(&mut h.outbound).await)
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
            "deadline" => ownership.set_expiration(SystemTime::now() - Duration::from_secs(1)),
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
        let client_message::Msg::Released(released) = client_variant(next(&mut h.outbound).await)
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
