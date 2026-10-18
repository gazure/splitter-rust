use super::{
    ClientError, ClusterMap, ClusterMessage, GrantId, GrantState, HashMap, HashSet, Instance,
    InstanceId, Shard, cluster_message, grant_state_from_i32, grant_states_compatible,
};

type Grants = HashMap<GrantId, (Shard, GrantState)>;

fn invalid(message: impl Into<String>) -> ClientError {
    ClientError::InvalidMessage(message.into())
}

impl ClusterMap {
    pub(super) fn validate(&self, message: &ClusterMessage) -> Result<(), ClientError> {
        match message.msg.as_ref() {
            Some(cluster_message::Msg::Snapshot(snapshot)) => {
                let shards = unique_shards(&snapshot.shards)?;
                validate_assignments(&shards, HashMap::new(), &snapshot.assignments)
            }
            Some(cluster_message::Msg::Change(change)) => {
                let origin = InstanceId::try_from(message.id.as_str())
                    .map_err(|_| invalid("invalid cluster origin"))?;
                if self.id.origin.as_ref().map(|o| o.id) != Some(origin)
                    || self.id.version.checked_add(1) != Some(message.version)
                {
                    return Err(invalid("unexpected cluster origin or incremental version"));
                }
                self.validate_change(change)
            }
            None => Err(invalid("ClusterMessage without a variant")),
        }
    }

    fn validate_change(&self, change: &cluster_message::Change) -> Result<(), ClientError> {
        let shards = match &change.shards {
            Some(s) => unique_shards(&s.shards)?,
            None => self.shards.iter().cloned().collect(),
        };
        // Retained routes are usable for lookup but are not registered with this coordinator.
        let mut grants: Grants = self
            .grants
            .values()
            .filter(|g| shards.contains(&g.shard) && !self.retained_grants.contains(&g.id))
            .map(|g| (g.id.clone(), (g.shard.clone(), g.state)))
            .collect();
        let mut updated = HashSet::new();
        for g in change.update.iter().flat_map(|u| &u.grants) {
            let (id, shard, state) = decode_grant(g)?;
            if !updated.insert(id.clone()) {
                return Err(invalid("duplicate grant update"));
            }
            let (old_shard, old_state) = grants
                .get(&id)
                .ok_or_else(|| invalid(format!("updated unregistered grant {id}")))?;
            if old_shard != &shard || !state_can_advance(*old_state, state) {
                return Err(invalid(format!(
                    "invalid shard or state transition for grant {id}"
                )));
            }
            grants.insert(id, (shard, state));
        }
        for id in change.unassign.iter().flat_map(|u| &u.grants) {
            let id = GrantId::new(id.clone());
            if updated.contains(&id) || grants.remove(&id).is_none() {
                return Err(invalid(format!(
                    "unassigned unregistered or updated grant {id}"
                )));
            }
        }
        let mut removed = HashSet::new();
        for id in change.remove.iter().flat_map(|r| &r.consumers) {
            let id = InstanceId::try_from(id.as_str())
                .map_err(|_| invalid("invalid removed consumer id"))?;
            if self.retained_consumers.contains(&id) || !removed.insert(id) {
                return Err(invalid("removed unregistered or duplicate consumer"));
            }
            let consumer = self
                .consumers
                .get(&id)
                .ok_or_else(|| invalid("removed unknown consumer"))?;
            for gid in &consumer.grants {
                if updated.contains(gid) {
                    return Err(invalid("updated grant belongs to a removed consumer"));
                }
                grants.remove(gid);
            }
        }
        validate_assignments(
            &shards,
            grants,
            change
                .assign
                .as_ref()
                .map(|a| a.assignments.as_slice())
                .unwrap_or_default(),
        )
    }
}

fn unique_shards(raw: &[splitter_proto::pb::Shard]) -> Result<HashSet<Shard>, ClientError> {
    let mut shards = HashSet::new();
    for shard in raw {
        if !shards.insert(Shard::try_from(shard.clone())?) {
            return Err(invalid("duplicate shard"));
        }
    }
    Ok(shards)
}

fn decode_grant(
    g: &cluster_message::GrantInfo,
) -> Result<(GrantId, Shard, GrantState), ClientError> {
    let shard = g
        .shard
        .clone()
        .ok_or_else(|| invalid("GrantInfo without shard"))?
        .try_into()?;
    let state = grant_state_from_i32(g.state);
    if state == GrantState::Unknown {
        return Err(invalid("unknown grant state"));
    }
    Ok((GrantId::new(g.id.clone()), shard, state))
}

fn validate_assignments(
    shards: &HashSet<Shard>,
    mut grants: Grants,
    assignments: &[cluster_message::Assignment],
) -> Result<(), ClientError> {
    let mut states: HashMap<Shard, Vec<GrantState>> = HashMap::new();
    for (shard, state) in grants.values() {
        let current = states.entry(shard.clone()).or_default();
        // Explicit updates take precedence over unchanged grants during reconciliation.
        // A promotion can therefore conflict with an old grant that will be evicted.
        current.push(*state);
    }
    let mut consumers = HashSet::new();
    for assignment in assignments {
        let consumer = Instance::try_from(
            assignment
                .consumer
                .clone()
                .ok_or_else(|| invalid("Assignment without consumer"))?,
        )?;
        if !consumers.insert(consumer.id) {
            return Err(invalid("duplicate consumer"));
        }
        for g in &assignment.grants {
            let (id, shard, state) = decode_grant(g)?;
            if grants.contains_key(&id) || !shards.contains(&shard) {
                return Err(invalid(format!(
                    "duplicate grant or unknown shard for {id}"
                )));
            }
            let current = states.entry(shard.clone()).or_default();
            check_state(current, state)?;
            current.push(state);
            grants.insert(id, (shard, state));
        }
    }
    Ok(())
}

fn check_state(states: &[GrantState], next: GrantState) -> Result<(), ClientError> {
    match states {
        [] => Ok(()),
        [old] if grant_states_compatible(*old, next) => Ok(()),
        _ => Err(invalid("conflicting grants for shard")),
    }
}

fn state_can_advance(old: GrantState, next: GrantState) -> bool {
    use GrantState::{Active, Allocated, AllocatedLoaded, Revoked, RevokedUnloaded, Unknown};
    match old {
        Allocated => next != Allocated,
        AllocatedLoaded => !matches!(next, Allocated | AllocatedLoaded),
        Active => matches!(next, Revoked | RevokedUnloaded),
        Revoked => next == RevokedUnloaded,
        RevokedUnloaded | Unknown => false,
    }
}
