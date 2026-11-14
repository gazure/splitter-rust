//! Thread-safe grant tracker used by the consumer driver (and, in Phase 3, the
//! Processor). Port of `pkg/model/cluster.go`'s generic `GrantMap[T]`.
//!
//! Writes are state-keyed (`allocated` / `loaded` / `activate` / `revoke` /
//! `unloaded`) so the Processor can resolve local ownership by preferred state
//! without recomputing from a raw grant state enum.
//!
//! Phase 2 scope: per-id writes + lookups. The by-key (domain+shard range)
//! lookup lives in Phase 4 when the Proxy lands and needs it.

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

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

/// Read-only access to a processor's live grant registry.
///
/// Clones observe the same registry. Each method reads the current state;
/// separate calls do not form an atomic snapshot. Returned values are clones,
/// so a range handle can outlive its registry entry. The range remains
/// responsible for its own closure behavior.
#[derive(Clone)]
pub struct GrantMapReader<T: Clone> {
    grants: Arc<GrantMap<T>>,
}

impl<T: Clone> GrantMapReader<T> {
    pub(crate) fn new(grants: Arc<GrantMap<T>>) -> Self {
        Self { grants }
    }

    /// Returns the shard, state, and cloned value for `id`, if present.
    ///
    /// # Panics
    ///
    /// Panics if a previous writer poisoned the grant map lock.
    pub fn get(&self, id: &GrantId) -> Option<(Shard, GrantState, T)> {
        self.grants.get(id)
    }

    /// Returns the number of tracked grants.
    ///
    /// # Panics
    ///
    /// Panics if a previous writer poisoned the grant map lock.
    pub fn len(&self) -> usize {
        self.grants.len()
    }

    /// Returns whether the registry has no grants.
    ///
    /// # Panics
    ///
    /// Panics if a previous writer poisoned the grant map lock.
    pub fn is_empty(&self) -> bool {
        self.grants.is_empty()
    }

    /// Returns the current grant IDs without holding the registry lock.
    ///
    /// # Panics
    ///
    /// Panics if a previous writer poisoned the grant map lock.
    pub fn ids(&self) -> Vec<GrantId> {
        self.grants.ids()
    }
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

    /// Writes the shard, state, and value for a grant, replacing any previous entry.
    ///
    /// # Panics
    ///
    /// Panics if a previous writer poisoned the grant map lock.
    pub fn write(&self, id: GrantId, shard: Shard, state: GrantState, value: T) {
        self.inner.write().expect("grant map poisoned").insert(
            id,
            Entry {
                shard,
                state,
                value,
            },
        );
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

    /// Removes a grant and returns its shard, state, and value, if present.
    ///
    /// # Panics
    ///
    /// Panics if a previous writer poisoned the grant map lock.
    pub fn delete(&self, id: &GrantId) -> Option<(Shard, GrantState, T)> {
        self.inner
            .write()
            .expect("grant map poisoned")
            .remove(id)
            .map(|e| (e.shard, e.state, e.value))
    }

    /// Removes a grant only if `matches` accepts its current value.
    ///
    /// The predicate runs under the write lock. It must not access this map.
    /// Returns the removed entry, or `None` if the entry changed or is absent.
    ///
    /// # Panics
    ///
    /// Panics if a previous writer poisoned the grant map lock.
    pub fn delete_if(
        &self,
        id: &GrantId,
        matches: impl FnOnce(&T) -> bool,
    ) -> Option<(Shard, GrantState, T)> {
        let mut map = self.inner.write().expect("grant map poisoned");
        if map.get(id).is_some_and(|entry| matches(&entry.value)) {
            map.remove(id)
                .map(|entry| (entry.shard, entry.state, entry.value))
        } else {
            None
        }
    }

    /// Fetch a value by grant id. Returns `(shard, state, value)`.
    ///
    /// # Panics
    ///
    /// Panics if a previous writer poisoned the grant map lock.
    pub fn get(&self, id: &GrantId) -> Option<(Shard, GrantState, T)> {
        self.inner
            .read()
            .expect("grant map poisoned")
            .get(id)
            .map(|e| (e.shard.clone(), e.state, e.value.clone()))
    }

    /// Returns the number of tracked grants.
    ///
    /// # Panics
    ///
    /// Panics if a previous writer poisoned the grant map lock.
    pub fn len(&self) -> usize {
        self.inner.read().expect("grant map poisoned").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Snapshot of all grant ids currently tracked, cloned out under the read
    /// lock so the caller can iterate without holding it.
    ///
    /// # Panics
    ///
    /// Panics if a previous writer poisoned the grant map lock.
    pub fn ids(&self) -> Vec<GrantId> {
        self.inner
            .read()
            .expect("grant map poisoned")
            .keys()
            .cloned()
            .collect()
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
            region: crate::ids::Region::default(),
            from: String::new(),
            to: String::new(),
        }
    }

    #[test]
    fn retained_readers_observe_replacement_and_removal() {
        let grants = Arc::new(GrantMap::new());
        let reader = GrantMapReader::new(grants.clone());
        let retained = reader.clone();
        let id = GrantId::new("g1");
        assert!(reader.is_empty());

        grants.activate(id.clone(), mk_shard(), "original");
        let original = reader.get(&id).unwrap();
        grants.allocated(id.clone(), mk_shard(), "replacement");
        assert_eq!(retained.len(), 1);
        assert_eq!(retained.ids(), vec![id.clone()]);
        let (_, state, value) = retained.get(&id).unwrap();
        assert_eq!(state, GrantState::Allocated);
        assert_eq!(value, "replacement");
        assert_eq!(original.2, "original");

        grants.delete(&id);
        assert!(reader.get(&id).is_none());
        assert!(retained.is_empty());
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
