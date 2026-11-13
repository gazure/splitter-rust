use std::time::SystemTime;

use splitter_client::{
    ClusterMap, DomainKey, Endpoint, GrantState, Instance, InstanceId, Location,
    QualifiedDomainKey, QualifiedDomainName, QualifiedServiceName,
};
use splitter_proto::pb::{self, ClusterMessage, cluster_message};

#[test]
fn unit_snapshot_routes_by_domain_without_key_bounds() {
    let domain = QualifiedDomainName {
        service: QualifiedServiceName::new("test", "routing"),
        name: "unit".into(),
    };
    let consumer = Instance {
        id: InstanceId::new(),
        location: Location::new("region", "node"),
        name: "owner".into(),
        created: SystemTime::UNIX_EPOCH,
        endpoint: Endpoint::default(),
    };
    let zero = "00000000-0000-0000-0000-000000000000";
    // Go serializes both UNIT bounds as the zero UUID, not an empty string.
    let shard = pb::Shard {
        domain: Some(domain.clone().into()),
        r#type: pb::DomainType::Unit as i32,
        from: zero.into(),
        to: zero.into(),
        ..Default::default()
    };
    let cluster = ClusterMap::empty()
        .apply(ClusterMessage {
            version: 1,
            msg: Some(cluster_message::Msg::Snapshot(cluster_message::Snapshot {
                shards: vec![shard.clone()],
                assignments: vec![cluster_message::Assignment {
                    consumer: Some(consumer.clone().into()),
                    grants: vec![cluster_message::GrantInfo {
                        id: "unit-owner".into(),
                        shard: Some(shard),
                        state: pb::GrantState::Active as i32,
                    }],
                }],
                ..Default::default()
            })),
            ..Default::default()
        })
        .unwrap();

    for key in ["", zero, "ffffffff-ffff-ffff-ffff-ffffffffffff"] {
        for region in ["", "another-region"] {
            let mut key = QualifiedDomainKey {
                domain: domain.clone(),
                key: DomainKey {
                    region: region.into(),
                    key: key.into(),
                },
            };
            let owner = cluster
                .lookup(&key, &[])
                .expect("UNIT owner covers every key");
            assert_eq!(owner.consumer, consumer.id);
            assert!(cluster.lookup(&key, &[GrantState::Revoked]).is_none());

            key.domain.name = "another-domain".into();
            assert!(cluster.lookup(&key, &[]).is_none());
            key.domain = domain.clone();
            key.domain.service = QualifiedServiceName::new("test", "another-service");
            assert!(cluster.lookup(&key, &[]).is_none());
        }
    }
}
