//! Conversions between generated proto types (`splitter-proto`) and the
//! Rust-native domain types in [`crate::ids`].
//!
//! Fallible conversions use `TryFrom` and surface [`ClientError::InvalidMessage`]
//! for missing required fields; infallible shape-preserving conversions use
//! `From`.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use prost_types::Timestamp;
use splitter_proto::location_pb;
use splitter_proto::pb;

use crate::error::ClientError;
use crate::ids::{
    DomainKey, DomainKeyName, DomainType, Endpoint, GrantState, Instance, InstanceId, Location,
    QualifiedDomainKey, QualifiedDomainName, QualifiedServiceName, Shard,
};

fn required<T>(value: Option<T>, field: &'static str) -> Result<T, ClientError> {
    value.ok_or_else(|| ClientError::InvalidMessage(format!("missing field: {field}")))
}

pub fn timestamp_to_system_time(ts: &Timestamp) -> SystemTime {
    let secs = ts.seconds.max(0) as u64;
    let nanos = ts.nanos.max(0) as u32;
    UNIX_EPOCH + Duration::new(secs, nanos)
}

pub fn system_time_to_timestamp(t: SystemTime) -> Timestamp {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => Timestamp {
            seconds: d.as_secs() as i64,
            nanos: d.subsec_nanos() as i32,
        },
        Err(_) => Timestamp { seconds: 0, nanos: 0 },
    }
}

pub fn now_timestamp() -> Timestamp {
    system_time_to_timestamp(SystemTime::now())
}

// ---------- Location / Instance ----------

impl From<location_pb::Location> for Location {
    fn from(l: location_pb::Location) -> Self {
        Location {
            region: l.region.into(),
            node: l.node.into(),
        }
    }
}

impl From<Location> for location_pb::Location {
    fn from(l: Location) -> Self {
        location_pb::Location {
            region: l.region.into_string(),
            node: l.node.into_string(),
        }
    }
}

impl TryFrom<location_pb::Instance> for Instance {
    type Error = ClientError;
    fn try_from(pb: location_pb::Instance) -> Result<Self, Self::Error> {
        let created = required(pb.created, "location.Instance.created")?;
        let id = InstanceId::try_from(pb.id.as_str())
            .map_err(|e| ClientError::InvalidMessage(format!("invalid instance id '{}': {e}", pb.id)))?;
        Ok(Instance {
            id,
            location: pb.location.map(Location::from).unwrap_or_default(),
            name: pb.name.into(),
            created: timestamp_to_system_time(&created),
            endpoint: Endpoint::default(),
        })
    }
}

impl TryFrom<pb::Instance> for Instance {
    type Error = ClientError;
    fn try_from(pb: pb::Instance) -> Result<Self, Self::Error> {
        let inner = required(pb.instance, "Instance.instance")?;
        let mut inst = Instance::try_from(inner)?;
        inst.endpoint = pb.endpoint.into();
        Ok(inst)
    }
}

impl From<Instance> for location_pb::Instance {
    fn from(i: Instance) -> Self {
        location_pb::Instance {
            id: i.id.to_string(),
            location: Some(i.location.into()),
            created: Some(system_time_to_timestamp(i.created)),
            name: i.name.into_string(),
        }
    }
}

impl From<Instance> for pb::Instance {
    fn from(i: Instance) -> Self {
        let endpoint = i.endpoint.clone().into_string();
        pb::Instance {
            instance: Some(i.into()),
            endpoint,
        }
    }
}

// ---------- Names / Keys ----------

impl From<pb::QualifiedServiceName> for QualifiedServiceName {
    fn from(p: pb::QualifiedServiceName) -> Self {
        QualifiedServiceName {
            tenant: p.tenant.into(),
            service: p.service.into(),
        }
    }
}

impl From<QualifiedServiceName> for pb::QualifiedServiceName {
    fn from(q: QualifiedServiceName) -> Self {
        pb::QualifiedServiceName {
            tenant: q.tenant.into_string(),
            service: q.service.into_string(),
        }
    }
}

impl TryFrom<pb::QualifiedDomainName> for QualifiedDomainName {
    type Error = ClientError;
    fn try_from(p: pb::QualifiedDomainName) -> Result<Self, Self::Error> {
        Ok(QualifiedDomainName {
            service: required(p.service, "QualifiedDomainName.service")?.into(),
            name: p.name.into(),
        })
    }
}

impl From<QualifiedDomainName> for pb::QualifiedDomainName {
    fn from(d: QualifiedDomainName) -> Self {
        pb::QualifiedDomainName {
            service: Some(d.service.into()),
            name: d.name.into_string(),
        }
    }
}

impl From<pb::DomainKey> for DomainKey {
    fn from(p: pb::DomainKey) -> Self {
        DomainKey {
            region: p.region.into(),
            key: p.key,
        }
    }
}

impl From<DomainKey> for pb::DomainKey {
    fn from(d: DomainKey) -> Self {
        pb::DomainKey {
            region: d.region.into_string(),
            key: d.key,
        }
    }
}

impl TryFrom<pb::QualifiedDomainKey> for QualifiedDomainKey {
    type Error = ClientError;
    fn try_from(p: pb::QualifiedDomainKey) -> Result<Self, Self::Error> {
        Ok(QualifiedDomainKey {
            domain: required(p.domain, "QualifiedDomainKey.domain")?.try_into()?,
            key: p.key.map(DomainKey::from).unwrap_or_default(),
        })
    }
}

impl From<QualifiedDomainKey> for pb::QualifiedDomainKey {
    fn from(k: QualifiedDomainKey) -> Self {
        pb::QualifiedDomainKey {
            domain: Some(k.domain.into()),
            key: Some(k.key.into()),
        }
    }
}

impl From<pb::DomainKeyName> for DomainKeyName {
    fn from(p: pb::DomainKeyName) -> Self {
        DomainKeyName {
            domain: p.domain.into(),
            name: p.name,
        }
    }
}

impl From<DomainKeyName> for pb::DomainKeyName {
    fn from(n: DomainKeyName) -> Self {
        pb::DomainKeyName {
            domain: n.domain.into_string(),
            name: n.name,
        }
    }
}

// ---------- Enums ----------

impl From<pb::DomainType> for DomainType {
    fn from(p: pb::DomainType) -> Self {
        match p {
            pb::DomainType::Invalid => DomainType::Invalid,
            pb::DomainType::Unit => DomainType::Unit,
            pb::DomainType::Global => DomainType::Global,
            pb::DomainType::Regional => DomainType::Regional,
        }
    }
}

impl From<DomainType> for pb::DomainType {
    fn from(d: DomainType) -> Self {
        match d {
            DomainType::Invalid => pb::DomainType::Invalid,
            DomainType::Unit => pb::DomainType::Unit,
            DomainType::Global => pb::DomainType::Global,
            DomainType::Regional => pb::DomainType::Regional,
        }
    }
}

fn domain_type_from_i32(v: i32) -> DomainType {
    pb::DomainType::try_from(v)
        .unwrap_or(pb::DomainType::Invalid)
        .into()
}

impl From<pb::GrantState> for GrantState {
    fn from(p: pb::GrantState) -> Self {
        match p {
            pb::GrantState::Unknown => GrantState::Unknown,
            pb::GrantState::Active => GrantState::Active,
            pb::GrantState::Allocated => GrantState::Allocated,
            pb::GrantState::Revoked => GrantState::Revoked,
            pb::GrantState::AllocatedLoaded => GrantState::AllocatedLoaded,
            pb::GrantState::RevokedUnloaded => GrantState::RevokedUnloaded,
        }
    }
}

impl From<GrantState> for pb::GrantState {
    fn from(s: GrantState) -> Self {
        match s {
            GrantState::Unknown => pb::GrantState::Unknown,
            GrantState::Active => pb::GrantState::Active,
            GrantState::Allocated => pb::GrantState::Allocated,
            GrantState::Revoked => pb::GrantState::Revoked,
            GrantState::AllocatedLoaded => pb::GrantState::AllocatedLoaded,
            GrantState::RevokedUnloaded => pb::GrantState::RevokedUnloaded,
        }
    }
}

pub fn grant_state_from_i32(v: i32) -> GrantState {
    pb::GrantState::try_from(v)
        .unwrap_or(pb::GrantState::Unknown)
        .into()
}

// ---------- Shard ----------

impl TryFrom<pb::Shard> for Shard {
    type Error = ClientError;
    fn try_from(p: pb::Shard) -> Result<Self, Self::Error> {
        Ok(Shard {
            domain: required(p.domain, "Shard.domain")?.try_into()?,
            kind: domain_type_from_i32(p.r#type),
            region: p.region.into(),
            from: p.from,
            to: p.to,
        })
    }
}

impl From<Shard> for pb::Shard {
    fn from(s: Shard) -> Self {
        let ty: pb::DomainType = s.kind.into();
        pb::Shard {
            domain: Some(s.domain.into()),
            r#type: ty as i32,
            region: s.region.into_string(),
            from: s.from,
            to: s.to,
        }
    }
}

