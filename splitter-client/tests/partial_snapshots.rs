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
        version: old.id.version + 1,
        msg: Some(cm::Msg::Snapshot(cm::Snapshot {
            shards: shards.to_vec(),
            assignments,
            ..Default::default()
        })),
        ..Default::default()
    })
    .unwrap()
}

fn change(old: &ClusterMap, change: cm::Change) -> ClusterMap {
    old.apply(ClusterMessage {
        version: old.id.version + 1,
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
    for (cid, consumer) in &map.consumers {
        for gid in &consumer.grants {
            assert!(seen.insert(gid), "duplicate consumer grant {gid}");
            assert_eq!(&map.grants[gid].consumer, cid);
        }
    }
    assert_eq!(seen.len(), map.grants.len());
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
        assert_eq!(map.consumers.len(), 3);
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
    assert_eq!(partial.grants.len(), 1);
    assert_owner(&partial, "1", &owner, "a");
    assert!(!partial.consumers.contains_key(&idle.id));
    let empty = snapshot(&partial, &[], vec![]);
    assert!(empty.grants.is_empty());
    assert!(empty.consumers.is_empty());
    assert_eq!(original.grants.len(), 2);
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
                next.grants.contains_key(&GrantId::new("old")),
                compatible,
                "{old:?} -> {new:?}"
            );
            assert_eq!(next.consumers.contains_key(&old_owner.id), compatible);
            assert!(next.grants.contains_key(&GrantId::new("new")));
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
    assert_eq!(next.grants.len(), 2);
    assert_eq!(next.consumers[&owner.id].consumer, owner);
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
    assert_eq!(retained.grants.len(), 2);
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
    assert_eq!(active.grants.len(), 1);
    assert_owner(&active, "1", &new, "new");
    assert!(active.consumers[&old.id].grants.is_empty());
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
    assert_eq!(next.grants.len(), 1);
    assert_owner(&next, "1", &owner, "new");
    assert_eq!(next.consumers[&owner.id].consumer, owner);
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
    assert_eq!(partial.grants.len(), 2);
    let removed = change(
        &partial,
        cm::Change {
            remove: Some(cm::Remove {
                consumers: vec![owner.id.to_string()],
            }),
            ..Default::default()
        },
    );
    assert!(removed.consumers.is_empty());
    assert!(removed.grants.is_empty());
    let shrunk = change(
        &partial,
        cm::Change {
            shards: Some(cm::Shards {
                shards: shards[..1].to_vec(),
            }),
            ..Default::default()
        },
    );
    assert_eq!(shrunk.grants.len(), 1);
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
    assert_eq!(next.grants.len(), 1);
    assert_eq!(next.consumers.len(), 1);
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
    assert_eq!(next.grants.len(), 1);
    assert_owner(&next, "b", &owner, "b");
    assert!(next.consumers[&idle.id].grants.is_empty());
    assert_consistent(&next);
}
