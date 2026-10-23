use std::time::SystemTime;

use splitter_client::{
    ClusterMap, DomainKey, Endpoint, GrantId, Instance, InstanceId, Location, QualifiedDomainKey,
    QualifiedDomainName, QualifiedServiceName,
};
use splitter_proto::pb::{self, ClusterMessage, cluster_message as cm};

fn consumer(name: &str) -> Instance {
    Instance {
        id: InstanceId::new(),
        location: Location::new("region", name),
        name: name.into(),
        created: SystemTime::UNIX_EPOCH,
        endpoint: Endpoint::new(format!("{name}:8080")),
    }
}

fn domain() -> QualifiedDomainName {
    QualifiedDomainName {
        service: QualifiedServiceName::new("test", "restart"),
        name: "ranges".into(),
    }
}

fn shard(from: &str, to: &str) -> pb::Shard {
    pb::Shard {
        domain: Some(domain().into()),
        r#type: pb::DomainType::Global as i32,
        from: from.into(),
        to: to.into(),
        ..Default::default()
    }
}

fn grant(id: &str, shard: &pb::Shard, state: pb::GrantState) -> cm::GrantInfo {
    cm::GrantInfo {
        id: id.into(),
        shard: Some(shard.clone()),
        state: state as i32,
    }
}

fn assignment(consumer: &Instance, grants: Vec<cm::GrantInfo>) -> cm::Assignment {
    cm::Assignment {
        consumer: Some(consumer.clone().into()),
        grants,
    }
}

fn snapshot(
    old: &ClusterMap,
    shards: &[pb::Shard],
    assignments: Vec<cm::Assignment>,
) -> ClusterMap {
    old.apply(ClusterMessage {
        version: old.id().version + 1,
        msg: Some(cm::Msg::Snapshot(cm::Snapshot {
            shards: shards.to_vec(),
            origin: Some(consumer("coordinator").into()),
            assignments,
        })),
        ..Default::default()
    })
    .unwrap()
}

fn change(old: &ClusterMap, change: cm::Change) -> ClusterMap {
    old.apply(ClusterMessage {
        id: old.id().origin.as_ref().unwrap().id.to_string(),
        version: old.id().version + 1,
        msg: Some(cm::Msg::Change(change)),
        ..Default::default()
    })
    .unwrap()
}

fn assert_owner(map: &ClusterMap, key: &str, owner: &Instance, grant: &str) {
    let found = map
        .lookup(
            &QualifiedDomainKey {
                domain: domain(),
                key: DomainKey {
                    key: key.into(),
                    ..Default::default()
                },
            },
            &[],
        )
        .expect("range must remain routable");
    assert_eq!(found.consumer, owner.id);
    assert_eq!(found.id.as_str(), grant);
}

fn assert_consistent(map: &ClusterMap) {
    let mut seen = std::collections::HashSet::new();
    for (cid, consumer) in map.consumers() {
        for gid in &consumer.grants {
            assert!(seen.insert(gid), "duplicate consumer grant {gid}");
            assert_eq!(&map.grants()[gid].consumer, cid);
        }
    }
    assert_eq!(seen.len(), map.grants().len());
}

// Mirrors Go's restart-snapshot-retains-peers fixture.
#[test]
fn partial_restart_snapshot_retains_peer_routes_across_repeated_snapshots() {
    let peers = [consumer("c1"), consumer("c2"), consumer("c3")];
    let shards = [shard("0", "a"), shard("a", "d"), shard("d", "f")];
    let assignments: Vec<_> = peers
        .iter()
        .zip(&shards)
        .enumerate()
        .map(|(i, (c, s))| assignment(c, vec![grant(&i.to_string(), s, pb::GrantState::Active)]))
        .collect();
    let original = snapshot(&ClusterMap::empty(), &shards, assignments.clone());
    let partial = snapshot(&original, &shards, vec![assignments[0].clone()]);
    let empty = snapshot(&partial, &shards, vec![]);
    for map in [&original, &partial, &empty] {
        assert_eq!(map.consumers().len(), 3);
        assert_owner(map, "b", &peers[1], "1");
        assert_owner(map, "e", &peers[2], "2");
        assert_consistent(map);
    }
}

#[test]
fn snapshots_drop_removed_shards_and_omitted_consumers_without_grants() {
    let owner = consumer("owner");
    let idle = consumer("idle");
    let shards = [shard("0", "a"), shard("a", "f")];
    let original = snapshot(
        &ClusterMap::empty(),
        &shards,
        vec![
            assignment(
                &owner,
                vec![
                    grant("a", &shards[0], pb::GrantState::Active),
                    grant("b", &shards[1], pb::GrantState::Active),
                ],
            ),
            assignment(&idle, vec![]),
        ],
    );
    let partial = snapshot(&original, &shards[..1], vec![]);
    assert_eq!(partial.grants().len(), 1);
    assert_owner(&partial, "1", &owner, "a");
    assert!(!partial.consumers().contains_key(&idle.id));
    let empty = snapshot(&partial, &[], vec![]);
    assert!(empty.grants().is_empty());
    assert!(empty.consumers().is_empty());
    assert_eq!(original.grants().len(), 2);
}

#[test]
fn snapshot_retains_only_compatible_handover_states() {
    use pb::GrantState::{Active, Allocated, AllocatedLoaded, Revoked, RevokedUnloaded};
    let shards = [shard("0", "a")];
    let old_owner = consumer("old");
    let new_owner = consumer("new");
    for old in [Active, Allocated, AllocatedLoaded, Revoked, RevokedUnloaded] {
        for new in [Active, Allocated, AllocatedLoaded, Revoked, RevokedUnloaded] {
            let original = snapshot(
                &ClusterMap::empty(),
                &shards,
                vec![assignment(&old_owner, vec![grant("old", &shards[0], old)])],
            );
            let next = snapshot(
                &original,
                &shards,
                vec![assignment(&new_owner, vec![grant("new", &shards[0], new)])],
            );
            let compatible = (matches!(old, Allocated | AllocatedLoaded)
                && matches!(new, Revoked | RevokedUnloaded))
                || (matches!(new, Allocated | AllocatedLoaded)
                    && matches!(old, Revoked | RevokedUnloaded));
            assert_eq!(
                next.grants().contains_key(&GrantId::new("old")),
                compatible,
                "{old:?} -> {new:?}"
            );
            assert_eq!(next.consumers().contains_key(&old_owner.id), compatible);
            assert!(next.grants().contains_key(&GrantId::new("new")));
            assert_consistent(&next);
        }
    }
}

#[test]
fn snapshot_replaces_same_grant_and_refreshes_consumer_metadata() {
    let mut owner = consumer("owner");
    let shards = [shard("0", "a"), shard("a", "f")];
    let original = snapshot(
        &ClusterMap::empty(),
        &shards,
        vec![assignment(
            &owner,
            vec![
                grant("a", &shards[0], pb::GrantState::Active),
                grant("b", &shards[1], pb::GrantState::Active),
            ],
        )],
    );
    owner.endpoint = "new:8080".into();
    let next = snapshot(
        &original,
        &shards,
        vec![assignment(
            &owner,
            vec![grant("a", &shards[0], pb::GrantState::Allocated)],
        )],
    );
    assert_eq!(next.grants().len(), 2);
    assert_eq!(next.consumers()[&owner.id].consumer, owner);
    assert_owner(&next, "b", &owner, "b");
    assert_consistent(&next);
}

// Mirrors Go's activate-after-reconnect fixture.
#[test]
fn replay_then_activation_evicts_retained_revoked_grant() {
    let old = consumer("old");
    let new = consumer("new");
    let shards = [shard("0", "a")];
    let allocated = grant("new", &shards[0], pb::GrantState::Allocated);
    let original = snapshot(
        &ClusterMap::empty(),
        &shards,
        vec![
            assignment(
                &old,
                vec![grant("old", &shards[0], pb::GrantState::Revoked)],
            ),
            assignment(&new, vec![allocated.clone()]),
        ],
    );
    let retained = snapshot(&original, &shards, vec![]);
    assert_eq!(retained.grants().len(), 2);
    assert_consistent(&retained);
    let replay = change(
        &retained,
        cm::Change {
            assign: Some(cm::Assign {
                assignments: vec![assignment(&new, vec![allocated])],
            }),
            ..Default::default()
        },
    );
    assert_owner(&replay, "1", &old, "old");
    assert_consistent(&replay);
    let active = change(
        &replay,
        cm::Change {
            update: Some(cm::Update {
                grants: vec![grant("new", &shards[0], pb::GrantState::Active)],
            }),
            ..Default::default()
        },
    );
    assert_eq!(active.grants().len(), 1);
    assert_owner(&active, "1", &new, "new");
    assert_eq!(
        active.consumers()[&old.id].grants,
        [] as [splitter_client::GrantId; 0]
    );
    assert_consistent(&active);
}

#[test]
fn new_assignment_evicts_retained_owner_and_updates_endpoint() {
    let mut owner = consumer("owner");
    let shards = [shard("0", "a")];
    let original = snapshot(
        &ClusterMap::empty(),
        &shards,
        vec![assignment(
            &owner,
            vec![grant("old", &shards[0], pb::GrantState::Active)],
        )],
    );
    let retained = snapshot(&original, &shards, vec![]);
    assert_owner(&retained, "1", &owner, "old");
    owner.endpoint = "new:8080".into();
    let next = change(
        &retained,
        cm::Change {
            assign: Some(cm::Assign {
                assignments: vec![assignment(
                    &owner,
                    vec![grant("new", &shards[0], pb::GrantState::Active)],
                )],
            }),
            ..Default::default()
        },
    );
    assert_eq!(next.grants().len(), 1);
    assert_owner(&next, "1", &owner, "new");
    assert_eq!(next.consumers()[&owner.id].consumer, owner);
    assert_consistent(&next);
}

#[test]
fn changes_remove_retained_grants_with_their_consumer_or_shard() {
    let owner = consumer("owner");
    let shards = [shard("0", "a"), shard("a", "f")];
    let original = snapshot(
        &ClusterMap::empty(),
        &shards,
        vec![assignment(
            &owner,
            vec![
                grant("a", &shards[0], pb::GrantState::Active),
                grant("b", &shards[1], pb::GrantState::Active),
            ],
        )],
    );
    let partial = snapshot(
        &original,
        &shards,
        vec![assignment(
            &owner,
            vec![grant("a", &shards[0], pb::GrantState::Active)],
        )],
    );
    assert_eq!(partial.grants().len(), 2);
    let removed = change(
        &partial,
        cm::Change {
            remove: Some(cm::Remove {
                consumers: vec![owner.id.to_string()],
            }),
            ..Default::default()
        },
    );
    assert!(removed.consumers().is_empty());
    assert!(removed.grants().is_empty());
    let shrunk = change(
        &partial,
        cm::Change {
            shards: Some(cm::Shards {
                shards: shards[..1].to_vec(),
            }),
            ..Default::default()
        },
    );
    assert_eq!(shrunk.grants().len(), 1);
    assert_consistent(&shrunk);
}

#[test]
fn snapshot_replaces_handover_pair_and_moves_replayed_grant_to_reported_consumer() {
    let old = consumer("old");
    let allocated = consumer("allocated");
    let new = consumer("new");
    let shards = [shard("0", "a")];
    let original = snapshot(
        &ClusterMap::empty(),
        &shards,
        vec![
            assignment(
                &old,
                vec![grant("old", &shards[0], pb::GrantState::Revoked)],
            ),
            assignment(
                &allocated,
                vec![grant("allocated", &shards[0], pb::GrantState::Allocated)],
            ),
        ],
    );
    let next = snapshot(
        &original,
        &shards,
        vec![assignment(
            &new,
            vec![grant("allocated", &shards[0], pb::GrantState::Active)],
        )],
    );
    assert_eq!(next.grants().len(), 1);
    assert_eq!(next.consumers().len(), 1);
    assert_owner(&next, "1", &new, "allocated");
    assert_consistent(&next);
}

#[test]
fn unassign_drops_reported_grant_but_preserves_omitted_peer_and_idle_consumer() {
    let owner = consumer("owner");
    let idle = consumer("idle");
    let shards = [shard("0", "a"), shard("a", "f")];
    let original = snapshot(
        &ClusterMap::empty(),
        &shards,
        vec![assignment(
            &owner,
            vec![
                grant("a", &shards[0], pb::GrantState::Active),
                grant("b", &shards[1], pb::GrantState::Active),
            ],
        )],
    );
    let partial = snapshot(
        &original,
        &shards,
        vec![
            assignment(&owner, vec![grant("a", &shards[0], pb::GrantState::Active)]),
            assignment(&idle, vec![]),
        ],
    );
    let next = change(
        &partial,
        cm::Change {
            unassign: Some(cm::Unassign {
                grants: vec!["a".into()],
            }),
            ..Default::default()
        },
    );
    assert_eq!(next.grants().len(), 1);
    assert_owner(&next, "b", &owner, "b");
    assert_eq!(
        next.consumers()[&idle.id].grants,
        [] as [splitter_client::GrantId; 0]
    );
    assert_consistent(&next);
}

mod validation {
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
}
