use super::*;

fn initial() -> (ClusterMap, Instance, pb::Shard) {
    let owner = consumer("owner");
    let s = shard("0", "f");
    let map = snapshot(
        &ClusterMap::empty(),
        std::slice::from_ref(&s),
        vec![assignment(
            &owner,
            vec![grant("g", &s, pb::GrantState::Active)],
        )],
    );
    (map, owner, s)
}

fn delta(map: &ClusterMap, change: cm::Change) -> ClusterMessage {
    ClusterMessage {
        id: map.id().origin.as_ref().unwrap().id.to_string(),
        version: map.id().version + 1,
        msg: Some(cm::Msg::Change(change)),
        ..Default::default()
    }
}

fn rejected(map: &ClusterMap, message: ClusterMessage) {
    let before = format!("{map:?}");
    assert!(
        map.apply(message).is_err(),
        "invalid update must be rejected"
    );
    assert_eq!(
        format!("{map:?}"),
        before,
        "failed application must be atomic"
    );
}

#[test]
fn rejects_wrong_origin_stale_duplicate_skipped_and_overflowed_versions() {
    let (map, _, _) = initial();
    for version in [0, map.id().version, map.id().version + 2] {
        let mut message = delta(&map, cm::Change::default());
        message.version = version;
        rejected(&map, message);
    }
    let mut message = delta(&map, cm::Change::default());
    message.id = InstanceId::new().to_string();
    rejected(&map, message);
    let overflow = map
        .apply(ClusterMessage {
            version: i64::MAX,
            msg: Some(cm::Msg::Snapshot(cm::Snapshot {
                origin: map.id().origin.clone().map(Into::into),
                ..Default::default()
            })),
            ..Default::default()
        })
        .unwrap();
    let mut message = delta(&map, cm::Change::default());
    message.version = i64::MIN;
    rejected(&overflow, message);
    rejected(&ClusterMap::empty(), delta(&map, cm::Change::default()));
    assert!(map.apply(delta(&map, cm::Change::default())).is_ok());
}

#[test]
fn rejects_duplicate_shards_consumers_grants_unknown_shards_and_conflicting_snapshots() {
    let (map, owner, s) = initial();
    let other = consumer("other");
    let active = grant("g", &s, pb::GrantState::Active);
    let cases = [
        (vec![s.clone(), s.clone()], vec![]),
        (
            vec![s.clone()],
            vec![assignment(&owner, vec![]), assignment(&owner, vec![])],
        ),
        (
            vec![s.clone()],
            vec![assignment(&owner, vec![active.clone(), active.clone()])],
        ),
        (
            vec![s.clone()],
            vec![
                assignment(&owner, vec![active.clone()]),
                assignment(&other, vec![active.clone()]),
            ],
        ),
        (
            vec![s.clone()],
            vec![assignment(
                &owner,
                vec![grant("bad", &shard("a", "f"), pb::GrantState::Active)],
            )],
        ),
        (
            vec![s.clone()],
            vec![
                assignment(&owner, vec![active.clone()]),
                assignment(&other, vec![grant("other", &s, pb::GrantState::Active)]),
            ],
        ),
        (
            vec![s.clone()],
            vec![assignment(
                &owner,
                vec![
                    grant("a", &s, pb::GrantState::Allocated),
                    grant("r", &s, pb::GrantState::Revoked),
                    grant("r2", &s, pb::GrantState::Revoked),
                ],
            )],
        ),
    ];
    for (shards, assignments) in cases {
        rejected(
            &map,
            ClusterMessage {
                version: 1,
                msg: Some(cm::Msg::Snapshot(cm::Snapshot {
                    shards,
                    assignments,
                    ..Default::default()
                })),
                ..Default::default()
            },
        );
    }
}

#[test]
fn rejects_unregistered_updates_removals_and_regressions() {
    let (map, _, s) = initial();
    for g in [
        grant("unknown", &s, pb::GrantState::Revoked),
        grant("g", &shard("a", "f"), pb::GrantState::Revoked),
        grant("g", &s, pb::GrantState::AllocatedLoaded),
        grant("g", &s, pb::GrantState::Active),
    ] {
        rejected(
            &map,
            delta(
                &map,
                cm::Change {
                    update: Some(cm::Update { grants: vec![g] }),
                    ..Default::default()
                },
            ),
        );
    }
    rejected(
        &map,
        delta(
            &map,
            cm::Change {
                unassign: Some(cm::Unassign {
                    grants: vec!["unknown".into()],
                }),
                ..Default::default()
            },
        ),
    );
    for cid in [InstanceId::new().to_string(), "malformed".into()] {
        rejected(
            &map,
            delta(
                &map,
                cm::Change {
                    remove: Some(cm::Remove {
                        consumers: vec![cid],
                    }),
                    ..Default::default()
                },
            ),
        );
    }
}

#[test]
fn retained_grants_and_consumers_require_reconfirmation_before_mutation() {
    let (map, owner, s) = initial();
    let retained = snapshot(&map, std::slice::from_ref(&s), vec![]);
    let update = cm::Change {
        update: Some(cm::Update {
            grants: vec![grant("g", &s, pb::GrantState::Revoked)],
        }),
        ..Default::default()
    };
    let unassign = cm::Change {
        unassign: Some(cm::Unassign {
            grants: vec!["g".into()],
        }),
        ..Default::default()
    };
    let remove = cm::Change {
        remove: Some(cm::Remove {
            consumers: vec![owner.id.to_string()],
        }),
        ..Default::default()
    };
    for operation in [&update, &unassign, &remove] {
        rejected(&retained, delta(&retained, operation.clone()));
    }
    let confirmed = change(
        &retained,
        cm::Change {
            assign: Some(cm::Assign {
                assignments: vec![assignment(
                    &owner,
                    vec![grant("g", &s, pb::GrantState::Active)],
                )],
            }),
            ..Default::default()
        },
    );
    for operation in [update, unassign, remove] {
        assert!(confirmed.apply(delta(&confirmed, operation)).is_ok());
    }
}

#[test]
fn empty_assignment_confirms_consumer_but_not_its_retained_grants() {
    let (map, owner, s) = initial();
    let retained = snapshot(
        &map,
        std::slice::from_ref(&s),
        vec![assignment(&owner, vec![])],
    );
    rejected(
        &retained,
        delta(
            &retained,
            cm::Change {
                unassign: Some(cm::Unassign {
                    grants: vec!["g".into()],
                }),
                ..Default::default()
            },
        ),
    );
    let removed = change(
        &retained,
        cm::Change {
            remove: Some(cm::Remove {
                consumers: vec![owner.id.to_string()],
            }),
            ..Default::default()
        },
    );
    assert!(removed.grants().is_empty());
    assert!(removed.consumers().is_empty());
}

#[test]
fn rejects_conflicts_with_confirmed_grants_but_allows_atomic_handover() {
    let (map, owner, s) = initial();
    let new = consumer("new");
    for assignments in [
        vec![assignment(&owner, vec![]), assignment(&owner, vec![])],
        vec![assignment(
            &new,
            vec![grant("g", &s, pb::GrantState::Active)],
        )],
        vec![assignment(
            &new,
            vec![grant("new", &s, pb::GrantState::Active)],
        )],
    ] {
        rejected(
            &map,
            delta(
                &map,
                cm::Change {
                    assign: Some(cm::Assign { assignments }),
                    ..Default::default()
                },
            ),
        );
    }
    let moved = change(
        &map,
        cm::Change {
            update: Some(cm::Update {
                grants: vec![grant("g", &s, pb::GrantState::Revoked)],
            }),
            assign: Some(cm::Assign {
                assignments: vec![assignment(
                    &new,
                    vec![grant("new", &s, pb::GrantState::Allocated)],
                )],
            }),
            ..Default::default()
        },
    );
    assert_eq!(moved.grants().len(), 2);
    assert_owner(&moved, "a", &owner, "g");
}

#[test]
fn promotion_evicts_previously_confirmed_revoked_grant() {
    let old = consumer("old");
    let new = consumer("new");
    let s = shard("0", "f");
    let map = snapshot(
        &ClusterMap::empty(),
        std::slice::from_ref(&s),
        vec![
            assignment(&old, vec![grant("old", &s, pb::GrantState::Revoked)]),
            assignment(
                &new,
                vec![grant("new", &s, pb::GrantState::AllocatedLoaded)],
            ),
        ],
    );
    let promoted = change(
        &map,
        cm::Change {
            update: Some(cm::Update {
                grants: vec![grant("new", &s, pb::GrantState::Active)],
            }),
            ..Default::default()
        },
    );
    assert_owner(&promoted, "a", &new, "new");
    assert_eq!(promoted.grants().len(), 1);
    assert_eq!(
        promoted.consumers()[&old.id].grants,
        [] as [splitter_client::GrantId; 0]
    );
    assert_consistent(&promoted);
}
