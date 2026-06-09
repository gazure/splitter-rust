//! Thread-safe grant tracker used by the consumer driver (and, in Phase 3, the
//! Processor). Port of `pkg/model/cluster.go`'s generic `GrantMap[T]`.
//!
//! Writes are state-keyed (`allocated` / `loaded` / `activate` / `revoke` /
//! `unloaded`) so the Processor can resolve local ownership by preferred state
//! without recomputing from a raw grant state enum.
//!
//! Phase 2 scope: per-id writes + lookups. The by-key (domain+shard range)
//! lookup lives in Phase 4 when the Proxy lands and needs it.

use std::collections::HashMap;
use std::sync::RwLock;

use crate::ids::{GrantId, GrantState, Shard};

#[derive(Clone)]
struct Entry<T> {
    shard: Shard,
    state: GrantState,
    value: T,
}

pub struct GrantMap<T: Clone> {
    inner: RwLock<HashMap<GrantId, Entry<T>>>,
}

impl<T: Clone> Default for GrantMap<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone> GrantMap<T> {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    pub fn write(&self, id: GrantId, shard: Shard, state: GrantState, value: T) {
        let mut m = self.inner.write().expect("grant map poisoned");
        m.insert(id, Entry { shard, state, value });
    }

    pub fn allocated(&self, id: GrantId, shard: Shard, value: T) {
        self.write(id, shard, GrantState::Allocated, value);
    }
    pub fn loaded(&self, id: GrantId, shard: Shard, value: T) {
        self.write(id, shard, GrantState::AllocatedLoaded, value);
    }
    pub fn activate(&self, id: GrantId, shard: Shard, value: T) {
        self.write(id, shard, GrantState::Active, value);
    }
    pub fn revoke(&self, id: GrantId, shard: Shard, value: T) {
        self.write(id, shard, GrantState::Revoked, value);
    }
    pub fn unloaded(&self, id: GrantId, shard: Shard, value: T) {
        self.write(id, shard, GrantState::RevokedUnloaded, value);
    }

    pub fn delete(&self, id: &GrantId) -> Option<(Shard, GrantState, T)> {
        let mut m = self.inner.write().expect("grant map poisoned");
        m.remove(id).map(|e| (e.shard, e.state, e.value))
    }

    /// Fetch a value by grant id. Returns `(shard, state, value)`.
    pub fn get(&self, id: &GrantId) -> Option<(Shard, GrantState, T)> {
        let m = self.inner.read().expect("grant map poisoned");
        m.get(id).map(|e| (e.shard.clone(), e.state, e.value.clone()))
    }

    pub fn len(&self) -> usize {
        self.inner.read().expect("grant map poisoned").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Snapshot of all grant ids currently tracked, cloned out under the read
    /// lock so the caller can iterate without holding it.
    pub fn ids(&self) -> Vec<GrantId> {
        let m = self.inner.read().expect("grant map poisoned");
        m.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{DomainType, QualifiedDomainName, QualifiedServiceName};

    fn mk_shard() -> Shard {
        Shard {
            domain: QualifiedDomainName {
                service: QualifiedServiceName::new("t", "s"),
                name: "d".into(),
            },
            kind: DomainType::Global,
            region: Default::default(),
            from: String::new(),
            to: String::new(),
        }
    }

    #[test]
    fn state_transitions_overwrite_in_place() {
        let gm = GrantMap::<u32>::new();
        let id = GrantId("g1".into());
        let shard = mk_shard();
        gm.allocated(id.clone(), shard.clone(), 1);
        assert!(matches!(gm.get(&id), Some((_, GrantState::Allocated, 1))));
        gm.loaded(id.clone(), shard.clone(), 2);
        assert!(matches!(
            gm.get(&id),
            Some((_, GrantState::AllocatedLoaded, 2))
        ));
        gm.activate(id.clone(), shard.clone(), 3);
        assert!(matches!(gm.get(&id), Some((_, GrantState::Active, 3))));
        gm.revoke(id.clone(), shard.clone(), 4);
        assert!(matches!(gm.get(&id), Some((_, GrantState::Revoked, 4))));
        gm.unloaded(id.clone(), shard, 5);
        assert!(matches!(
            gm.get(&id),
            Some((_, GrantState::RevokedUnloaded, 5))
        ));
    }

    #[test]
    fn delete_removes_entry() {
        let gm = GrantMap::<u32>::new();
        let id = GrantId("g1".into());
        gm.allocated(id.clone(), mk_shard(), 1);
        assert_eq!(gm.len(), 1);
        let (_, state, value) = gm.delete(&id).expect("present");
        assert_eq!(state, GrantState::Allocated);
        assert_eq!(value, 1);
        assert!(gm.is_empty());
        assert!(gm.delete(&id).is_none());
    }
}
