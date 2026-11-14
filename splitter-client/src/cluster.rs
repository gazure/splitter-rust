//! Immutable cluster snapshot.
//!
//! Reconciles partial snapshots and incremental changes using the grant retention
//! rules in Go's `pkg/model/cluster.go`. Coordinator assignments take precedence
//! over compatible grants retained from the previous view. Invalid snapshots and
//! changes are rejected without modifying the previous view.

use std::{
    collections::{HashMap, HashSet},
    time::SystemTime,
};

use splitter_proto::pb::{ClusterMessage, cluster_message};

use crate::{
    error::ClientError,
    ids::{
        ConsumerId, DomainType, GrantId, GrantState, Instance, InstanceId, QualifiedDomainKey,
        QualifiedDomainName, Shard,
    },
    wrap::{grant_state_from_i32, timestamp_to_system_time},
};

mod validation;

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
    retained_grants: HashSet<GrantId>,
    retained_consumers: HashSet<ConsumerId>,
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
            retained_grants: HashSet::new(),
            retained_consumers: HashSet::new(),
        }
    }

    /// Apply a server-sent `ClusterMessage` to produce a new snapshot.
    /// Does not mutate `self`.
    pub fn apply(&self, msg: ClusterMessage) -> Result<Self, ClientError> {
        self.validate(&msg)?;
        let timestamp = msg
            .timestamp
            .as_ref()
            .map_or(SystemTime::UNIX_EPOCH, timestamp_to_system_time);
        let version = msg.version;

        match msg.msg {
            Some(cluster_message::Msg::Snapshot(snap)) => {
                apply_snapshot(self, snap, version, timestamp)
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
        let shards = match change.shards {
            Some(shards) => decode_shards(shards.shards)?,
            None => self.shards.clone(),
        };
        let mut next = Self {
            id: ClusterId {
                version,
                origin: self.id.origin.clone(),
                timestamp,
            },
            consumers: HashMap::new(),
            grants: HashMap::new(),
            shards,
            retained_grants: HashSet::new(),
            retained_consumers: HashSet::new(),
        };

        // Coordinator assignments and updates take precedence over retained grants.
        if let Some(assign) = change.assign {
            for a in assign.assignments {
                apply_assignment(&mut next, a)?;
            }
        }
        if let Some(update) = change.update {
            for g in update.grants {
                let gid = GrantId(g.id.clone());
                if let Some(old) = self.grants.get(&gid) {
                    let mut grant = old.clone();
                    if let Some(shard) = g.shard {
                        grant.shard = Shard::try_from(shard)?;
                    }
                    grant.state = grant_state_from_i32(g.state);
                    insert_grant(&mut next, grant, &self.consumers[&old.consumer].consumer);
                }
            }
        }
        let unassigned: HashSet<_> = change
            .unassign
            .into_iter()
            .flat_map(|u| u.grants)
            .map(GrantId)
            .collect();
        let mut removed = HashSet::new();
        for cid in change.remove.into_iter().flat_map(|r| r.consumers) {
            match InstanceId::try_from(cid.as_str()) {
                Ok(id) => {
                    removed.insert(id);
                }
                Err(e) => {
                    tracing::warn!(%cid, error = %e, "ignoring invalid consumer id in Remove");
                }
            }
        }

        // Changes preserve consumers without grants; snapshots discard omitted ones.
        for (cid, entry) in &self.consumers {
            if !removed.contains(cid) && !next.consumers.contains_key(cid) {
                next.consumers.insert(
                    *cid,
                    ConsumerEntry {
                        consumer: entry.consumer.clone(),
                        grants: Vec::new(),
                    },
                );
                if self.retained_consumers.contains(cid) {
                    next.retained_consumers.insert(*cid);
                }
            }
        }
        retain_grants(&mut next, self, &unassigned, &removed, false);

        Ok(next)
    }

    pub fn lookup(&self, key: &QualifiedDomainKey, states: &[GrantState]) -> Option<&GrantEntry> {
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
            if let Some(g) = self
                .grants
                .values()
                .find(|g| &g.state == state && shard_covers(&g.shard, key))
            {
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
    previous: &ClusterMap,
    snap: cluster_message::Snapshot,
    version: i64,
    timestamp: SystemTime,
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
        retained_grants: HashSet::new(),
        retained_consumers: HashSet::new(),
    };
    for a in snap.assignments {
        apply_assignment(&mut out, a)?;
    }
    retain_grants(&mut out, previous, &HashSet::new(), &HashSet::new(), true);
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

    // Refresh endpoint metadata even when this consumer was retained from a snapshot.
    map.consumers
        .entry(cid)
        .and_modify(|entry| entry.consumer = consumer.clone())
        .or_insert_with(|| ConsumerEntry {
            consumer: consumer.clone(),
            grants: Vec::new(),
        });

    for g in assign.grants {
        let shard = g
            .shard
            .ok_or_else(|| ClientError::InvalidMessage("GrantInfo without shard".into()))?;
        insert_grant(
            map,
            GrantEntry {
                id: GrantId(g.id),
                shard: Shard::try_from(shard)?,
                state: grant_state_from_i32(g.state),
                consumer: cid,
            },
            &consumer,
        );
    }
    Ok(())
}

fn insert_grant(map: &mut ClusterMap, grant: GrantEntry, consumer: &Instance) {
    if let Some(old) = map.grants.remove(&grant.id)
        && let Some(entry) = map.consumers.get_mut(&old.consumer)
    {
        entry.grants.retain(|id| id != &grant.id);
    }
    map.consumers
        .entry(grant.consumer)
        .or_insert_with(|| ConsumerEntry {
            consumer: consumer.clone(),
            grants: Vec::new(),
        })
        .grants
        .push(grant.id.clone());
    map.grants.insert(grant.id.clone(), grant);
}

fn retain_grants(
    next: &mut ClusterMap,
    previous: &ClusterMap,
    unassigned: &HashSet<GrantId>,
    removed: &HashSet<ConsumerId>,
    snapshot: bool,
) {
    let mut shard_states: HashMap<Shard, Vec<GrantState>> = next
        .shards
        .iter()
        .cloned()
        .map(|s| (s, Vec::new()))
        .collect();
    for grant in next.grants.values() {
        shard_states
            .entry(grant.shard.clone())
            .or_default()
            .push(grant.state);
    }
    for grant in previous.grants.values() {
        if next.grants.contains_key(&grant.id)
            || unassigned.contains(&grant.id)
            || removed.contains(&grant.consumer)
        {
            continue;
        }
        let Some(states) = shard_states.get_mut(&grant.shard) else {
            continue;
        };
        // A shard has either one grant or an allocated/revoked handover pair.
        let compatible = match states.as_slice() {
            [] => true,
            [other] => grant_states_compatible(grant.state, *other),
            _ => false,
        };
        if compatible {
            states.push(grant.state);
            if snapshot || previous.retained_grants.contains(&grant.id) {
                next.retained_grants.insert(grant.id.clone());
            }
            if !next.consumers.contains_key(&grant.consumer)
                && (snapshot || previous.retained_consumers.contains(&grant.consumer))
            {
                next.retained_consumers.insert(grant.consumer);
            }
            insert_grant(
                next,
                grant.clone(),
                &previous.consumers[&grant.consumer].consumer,
            );
        }
    }
}

fn grant_states_compatible(a: GrantState, b: GrantState) -> bool {
    use GrantState::{Allocated, AllocatedLoaded, Revoked, RevokedUnloaded};
    (matches!(a, Allocated | AllocatedLoaded) && matches!(b, Revoked | RevokedUnloaded))
        || (matches!(b, Allocated | AllocatedLoaded) && matches!(a, Revoked | RevokedUnloaded))
}

fn decode_shards(raw: Vec<splitter_proto::pb::Shard>) -> Result<Vec<Shard>, ClientError> {
    raw.into_iter().map(Shard::try_from).collect()
}

fn shard_covers(shard: &Shard, key: &QualifiedDomainKey) -> bool {
    if shard.domain != key.domain {
        return false;
    }
    // UNIT shards cover the whole domain, regardless of serialized bounds or region.
    if shard.kind == DomainType::Unit {
        return true;
    }
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
