//! Immutable cluster snapshot.
//!
//! Phase-1 port of `pkg/model/cluster.go`. We deliberately skip the server-side
//! validation the Go code does on inbound snapshots/changes — the server is
//! authoritative and the client only needs to mirror the resulting view for
//! lookups and consumer enumeration.

use std::collections::HashMap;
use std::time::SystemTime;

use splitter_proto::pb::{cluster_message, ClusterMessage};

use crate::error::ClientError;
use crate::ids::{
    ConsumerId, GrantId, GrantState, Instance, InstanceId, QualifiedDomainKey,
    QualifiedDomainName, Shard,
};
use crate::wrap::{grant_state_from_i32, timestamp_to_system_time};

#[derive(Clone, Debug)]
pub struct ClusterId {
    pub version: i64,
    pub origin: Option<Instance>,
    pub timestamp: SystemTime,
}

#[derive(Clone, Debug)]
pub struct GrantEntry {
    pub id: GrantId,
    pub shard: Shard,
    pub state: GrantState,
    pub consumer: ConsumerId,
}

#[derive(Clone, Debug)]
pub struct ConsumerEntry {
    pub consumer: Instance,
    pub grants: Vec<GrantId>,
}

#[derive(Clone, Debug)]
pub struct ClusterMap {
    pub id: ClusterId,
    pub consumers: HashMap<ConsumerId, ConsumerEntry>,
    pub grants: HashMap<GrantId, GrantEntry>,
    pub shards: Vec<Shard>,
}

impl ClusterMap {
    pub fn empty() -> Self {
        Self {
            id: ClusterId {
                version: 0,
                origin: None,
                timestamp: SystemTime::UNIX_EPOCH,
            },
            consumers: HashMap::new(),
            grants: HashMap::new(),
            shards: Vec::new(),
        }
    }

    /// Apply a server-sent `ClusterMessage` to produce a new snapshot.
    /// Does not mutate `self`.
    pub fn apply(&self, msg: ClusterMessage) -> Result<Self, ClientError> {
        let timestamp = msg
            .timestamp
            .as_ref()
            .map(timestamp_to_system_time)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let version = msg.version;
        let cluster_origin_id = msg.id.clone();

        match msg.msg {
            Some(cluster_message::Msg::Snapshot(snap)) => {
                apply_snapshot(snap, version, timestamp, cluster_origin_id)
            }
            Some(cluster_message::Msg::Change(change)) => {
                self.apply_change(change, version, timestamp)
            }
            None => Err(ClientError::InvalidMessage(
                "ClusterMessage without a variant".into(),
            )),
        }
    }

    fn apply_change(
        &self,
        change: cluster_message::Change,
        version: i64,
        timestamp: SystemTime,
    ) -> Result<Self, ClientError> {
        let mut next = self.clone();
        next.id = ClusterId {
            version,
            origin: self.id.origin.clone(),
            timestamp,
        };

        if let Some(shards) = change.shards {
            next.shards = decode_shards(shards.shards)?;
            // Grants on shards no longer present should be dropped.
            let valid: std::collections::HashSet<&Shard> = next.shards.iter().collect();
            let drop_ids: Vec<GrantId> = next
                .grants
                .iter()
                .filter(|(_, g)| !valid.contains(&g.shard))
                .map(|(id, _)| id.clone())
                .collect();
            for id in drop_ids {
                drop_grant(&mut next, &id);
            }
        }

        if let Some(assign) = change.assign {
            for a in assign.assignments {
                apply_assignment(&mut next, a)?;
            }
        }

        if let Some(update) = change.update {
            for g in update.grants {
                update_grant(&mut next, g)?;
            }
        }

        if let Some(unassign) = change.unassign {
            for gid in unassign.grants {
                drop_grant(&mut next, &GrantId(gid));
            }
        }

        if let Some(remove) = change.remove {
            for cid in remove.consumers {
                match InstanceId::try_from(cid.as_str()) {
                    Ok(id) => remove_consumer(&mut next, &id),
                    Err(e) => {
                        tracing::warn!(%cid, error = %e, "ignoring invalid consumer id in Remove");
                    }
                }
            }
        }

        Ok(next)
    }

    pub fn lookup(
        &self,
        key: &QualifiedDomainKey,
        states: &[GrantState],
    ) -> Option<&GrantEntry> {
        let default_states = [
            GrantState::Active,
            GrantState::Revoked,
            GrantState::AllocatedLoaded,
            GrantState::RevokedUnloaded,
        ];
        let wanted: &[GrantState] = if states.is_empty() {
            &default_states
        } else {
            states
        };

        for state in wanted {
            if let Some(g) = self.grants.values().find(|g| {
                &g.state == state && shard_covers(&g.shard, key)
            }) {
                return Some(g);
            }
        }
        None
    }

    pub fn shards_for_domain(&self, domain: &QualifiedDomainName) -> Vec<&Shard> {
        self.shards.iter().filter(|s| &s.domain == domain).collect()
    }
}

fn apply_snapshot(
    snap: cluster_message::Snapshot,
    version: i64,
    timestamp: SystemTime,
    _cluster_origin_id: String,
) -> Result<ClusterMap, ClientError> {
    let shards = decode_shards(snap.shards)?;
    let origin = snap.origin.map(Instance::try_from).transpose()?;
    let mut out = ClusterMap {
        id: ClusterId {
            version,
            origin,
            timestamp,
        },
        consumers: HashMap::new(),
        grants: HashMap::new(),
        shards,
    };
    for a in snap.assignments {
        apply_assignment(&mut out, a)?;
    }
    Ok(out)
}

fn apply_assignment(
    map: &mut ClusterMap,
    assign: cluster_message::Assignment,
) -> Result<(), ClientError> {
    let consumer_pb = assign
        .consumer
        .ok_or_else(|| ClientError::InvalidMessage("Assignment without consumer".into()))?;
    let consumer = Instance::try_from(consumer_pb)?;
    let cid = consumer.id;

    let entry = map
        .consumers
        .entry(cid)
        .or_insert_with(|| ConsumerEntry {
            consumer,
            grants: Vec::new(),
        });

    for g in assign.grants {
        let gid = GrantId(g.id.clone());
        let shard = g
            .shard
            .ok_or_else(|| ClientError::InvalidMessage("GrantInfo without shard".into()))?;
        let shard = Shard::try_from(shard)?;
        let state = grant_state_from_i32(g.state);
        entry.grants.push(gid.clone());
        map.grants.insert(
            gid.clone(),
            GrantEntry {
                id: gid,
                shard,
                state,
                consumer: cid,
            },
        );
    }
    Ok(())
}

fn update_grant(map: &mut ClusterMap, g: cluster_message::GrantInfo) -> Result<(), ClientError> {
    let gid = GrantId(g.id.clone());
    if let Some(entry) = map.grants.get_mut(&gid) {
        if let Some(shard) = g.shard {
            entry.shard = Shard::try_from(shard)?;
        }
        entry.state = grant_state_from_i32(g.state);
    }
    Ok(())
}

fn drop_grant(map: &mut ClusterMap, gid: &GrantId) {
    if let Some(entry) = map.grants.remove(gid) {
        if let Some(c) = map.consumers.get_mut(&entry.consumer) {
            c.grants.retain(|g| g != gid);
        }
    }
}

fn remove_consumer(map: &mut ClusterMap, cid: &ConsumerId) {
    if let Some(entry) = map.consumers.remove(cid) {
        for gid in entry.grants {
            map.grants.remove(&gid);
        }
    }
}

fn decode_shards(raw: Vec<splitter_proto::pb::Shard>) -> Result<Vec<Shard>, ClientError> {
    raw.into_iter().map(Shard::try_from).collect()
}

fn shard_covers(shard: &Shard, key: &QualifiedDomainKey) -> bool {
    if shard.domain != key.domain {
        return false;
    }
    // UNIT shards cover the whole domain; REGIONAL must match region.
    if !shard.region.is_empty() && shard.region != key.key.region {
        return false;
    }
    // from/to are UUID strings; inclusive-from, exclusive-to lexicographic range.
    // Empty from/to bounds mean "unbounded".
    if !shard.from.is_empty() && key.key.key.as_str() < shard.from.as_str() {
        return false;
    }
    if !shard.to.is_empty() && key.key.key.as_str() >= shard.to.as_str() {
        return false;
    }
    true
}
