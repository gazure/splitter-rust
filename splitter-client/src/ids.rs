//! Rust-native domain types used throughout the client.
//!
//! These intentionally don't share structure with the generated `splitter-proto`
//! types — they derive `Hash`/`Eq` cleanly, use rust-native time, and keep the
//! public API free of prost dependencies. Conversions live in [`crate::wrap`].
//!
//! ## Newtype string wrappers
//!
//! Identifier-like fields are wrapped in newtypes (e.g. [`TenantName`],
//! [`ServiceName`], [`Region`]) so the type system rejects a tenant-where-
//! service-was-expected style of bug that plain `String` fields would allow.
//! Every wrapper offers the same API — `new(impl Into<String>)`, `as_str()`,
//! `From<&str>`/`From<String>`, `Display`, plus `Hash`/`Eq`/`Clone`.

use std::fmt;
use std::time::SystemTime;

/// Shared interop impls for a `String`-backed newtype: `as_str`,
/// `into_string`, `is_empty`, `Display`, `AsRef<str>`, `From<String>`,
/// `From<&str>`. The constructor (`new`) is provided separately by the
/// caller macro so that UUID-valued types can use `new()` to generate an
/// identifier while name-valued types use `new(Into<String>)`.
macro_rules! string_newtype_impls {
    ($name:ident) => {
        impl $name {
            pub fn as_str(&self) -> &str { &self.0 }
            pub fn into_string(self) -> String { self.0 }
            pub fn is_empty(&self) -> bool { self.0.is_empty() }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str { &self.0 }
        }

        impl From<String> for $name {
            fn from(s: String) -> Self { Self(s) }
        }

        impl From<&str> for $name {
            fn from(s: &str) -> Self { Self(s.to_string()) }
        }
    };
}

/// A name-valued newtype (tenant, region, etc). `new(impl Into<String>)`
/// wraps the caller's string. `Default` is the empty string.
macro_rules! name_newtype {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
        pub struct $name(pub String);

        impl $name {
            pub fn new(s: impl Into<String>) -> Self { Self(s.into()) }
        }

        string_newtype_impls!($name);
    };
}

/// A UUID-valued newtype backed by [`uuid::Uuid`] directly — 16 bytes,
/// `Copy`, no heap allocation. `new()` generates a fresh v4 UUID; use
/// [`TryFrom`] to parse a wire-format string (e.g. one deserialized from a
/// proto message).
macro_rules! uuid_newtype {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
        pub struct $name(pub uuid::Uuid);

        impl $name {
            /// Generate a fresh v4 UUID.
            pub fn new() -> Self {
                Self(uuid::Uuid::new_v4())
            }

            /// The inner UUID, by value (it's `Copy`).
            pub fn as_uuid(&self) -> uuid::Uuid { self.0 }
        }

        impl Default for $name {
            fn default() -> Self { Self::new() }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }

        impl From<uuid::Uuid> for $name {
            fn from(u: uuid::Uuid) -> Self { Self(u) }
        }

        impl TryFrom<&str> for $name {
            type Error = uuid::Error;
            fn try_from(s: &str) -> Result<Self, Self::Error> {
                uuid::Uuid::parse_str(s).map(Self)
            }
        }

        impl TryFrom<String> for $name {
            type Error = uuid::Error;
            fn try_from(s: String) -> Result<Self, Self::Error> {
                uuid::Uuid::parse_str(&s).map(Self)
            }
        }

        impl std::str::FromStr for $name {
            type Err = uuid::Error;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::try_from(s)
            }
        }
    };
}

uuid_newtype!(
    /// Globally-unique identifier for a consumer instance. `new()` produces
    /// a fresh v4 UUID; use `InstanceId::from("…")` to wrap an existing id.
    InstanceId
);

/// Consumer identity equals instance identity (see `model.Consumer` in Go).
pub type ConsumerId = InstanceId;

name_newtype!(
    /// Unique identifier of a grant. Grants are assigned by the server, so
    /// `GrantId` is constructed from the server-supplied string rather than
    /// generated locally.
    GrantId
);

name_newtype!(
    /// A deployment region (e.g. `us-east-1`, `local`).
    Region
);

name_newtype!(
    /// A node or pod name within a region.
    NodeName
);

name_newtype!(
    /// A tenant namespace.
    TenantName
);

name_newtype!(
    /// A service namespace within a tenant.
    ServiceName
);

name_newtype!(
    /// A domain namespace within a service.
    DomainName
);

name_newtype!(
    /// A dial-able peer endpoint (e.g. `host:port` or `http://host:port`).
    Endpoint
);

name_newtype!(
    /// Human-readable instance name. Informational only.
    InstanceName
);

#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct Location {
    pub region: Region,
    pub node: NodeName,
}

impl Location {
    pub fn new(region: impl Into<Region>, node: impl Into<NodeName>) -> Self {
        Self {
            region: region.into(),
            node: node.into(),
        }
    }
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.region, self.node)
    }
}

/// Addressable consumer instance (inner `location.Instance` + peer endpoint).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Instance {
    pub id: InstanceId,
    pub location: Location,
    pub name: InstanceName,
    pub created: SystemTime,
    pub endpoint: Endpoint,
}

impl fmt::Display for Instance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.id, self.location)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct QualifiedServiceName {
    pub tenant: TenantName,
    pub service: ServiceName,
}

impl QualifiedServiceName {
    pub fn new(tenant: impl Into<TenantName>, service: impl Into<ServiceName>) -> Self {
        Self {
            tenant: tenant.into(),
            service: service.into(),
        }
    }

    /// Parse `tenant/service`.
    pub fn parse(s: &str) -> Option<Self> {
        let (tenant, service) = s.split_once('/')?;
        if tenant.is_empty() || service.is_empty() {
            return None;
        }
        Some(Self::new(tenant, service))
    }
}

impl fmt::Display for QualifiedServiceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.tenant, self.service)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct QualifiedDomainName {
    pub service: QualifiedServiceName,
    pub name: DomainName,
}

impl fmt::Display for QualifiedDomainName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.service, self.name)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct DomainKey {
    pub region: Region,
    /// The key itself (usually a UUID or hashable string). Left as a plain
    /// `String` because the payload is user-defined.
    pub key: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct QualifiedDomainKey {
    pub domain: QualifiedDomainName,
    pub key: DomainKey,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum DomainType {
    Invalid,
    Unit,
    Global,
    Regional,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum GrantState {
    Unknown,
    Active,
    Allocated,
    Revoked,
    AllocatedLoaded,
    RevokedUnloaded,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Shard {
    pub domain: QualifiedDomainName,
    pub kind: DomainType,
    pub region: Region,
    /// Inclusive lower bound of the UUID range this shard covers.
    pub from: String,
    /// Exclusive upper bound of the UUID range this shard covers.
    pub to: String,
}

impl fmt::Display for Shard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}[{}..{}]", self.domain, self.from, self.to)
    }
}

/// Named key variant used by the `Register.Options.names` feature
/// (`model.proto` `DomainKeyName`). `domain` is the bare domain name (not
/// qualified); `name` is the label attached to the key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DomainKeyName {
    pub domain: DomainName,
    pub name: String,
}
