use crate::engine::Pending;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::sync::Arc;

pub(super) type Selector<I, T, J> = Rc<dyn Fn(&I, &T) -> J>;
pub(super) struct Entry<T, J> {
    pub key: J,
    pub value: Arc<T>,
}
pub(super) struct Index<I, T, J> {
    pub rows: BTreeMap<I, Entry<T, J>>,
    buckets: BTreeMap<J, BTreeSet<I>>,
    select: Selector<I, T, J>,
}
pub(super) struct Patch<I, T, J> {
    pub rows: BTreeMap<I, Option<Entry<T, J>>>,
    added: BTreeMap<J, BTreeSet<I>>,
    removed: BTreeMap<J, BTreeSet<I>>,
}
impl<I: Ord + Clone, T, J: Ord + Clone> Index<I, T, J> {
    pub fn new(rows: &BTreeMap<I, Arc<T>>, select: Selector<I, T, J>) -> Self {
        let mut index = Self {
            rows: BTreeMap::new(),
            buckets: BTreeMap::new(),
            select,
        };
        for (id, value) in rows {
            let key = (index.select)(id, value);
            index
                .buckets
                .entry(key.clone())
                .or_default()
                .insert(id.clone());
            index.rows.insert(
                id.clone(),
                Entry {
                    key,
                    value: value.clone(),
                },
            );
        }
        index
    }
    pub fn members(&self, key: &J) -> impl Iterator<Item = &I> {
        self.buckets.get(key).into_iter().flatten()
    }
    pub fn stage(&self, changes: Option<&Pending<I, T>>) -> Patch<I, T, J> {
        let mut patch = Patch {
            rows: BTreeMap::new(),
            added: BTreeMap::new(),
            removed: BTreeMap::new(),
        };
        for change in changes.into_iter().flat_map(|c| c.iter()) {
            let id = change.id();
            if let Some(old) = self.rows.get(id) {
                patch
                    .removed
                    .entry(old.key.clone())
                    .or_default()
                    .insert(id.clone());
            }
            let entry = change.after().map(|value| {
                let key = (self.select)(id, value);
                patch
                    .added
                    .entry(key.clone())
                    .or_default()
                    .insert(id.clone());
                Entry {
                    key,
                    value: value.clone(),
                }
            });
            patch.rows.insert(id.clone(), entry);
        }
        patch
    }
    pub fn apply(&mut self, patch: Patch<I, T, J>) {
        for (key, ids) in patch.removed {
            let bucket = self
                .buckets
                .get_mut(&key)
                .expect("removed index members have an old bucket");
            for id in ids {
                bucket.remove(&id);
            }
            if bucket.is_empty() {
                self.buckets.remove(&key);
            }
        }
        for (key, ids) in patch.added {
            self.buckets.entry(key).or_default().extend(ids);
        }
        for (id, entry) in patch.rows {
            if let Some(entry) = entry {
                self.rows.insert(id, entry);
            } else {
                self.rows.remove(&id);
            }
        }
    }
    pub fn overlay<'a>(&'a self, patch: &'a Patch<I, T, J>) -> Overlay<'a, I, T, J> {
        Overlay { base: self, patch }
    }
}
pub(super) struct Overlay<'a, I, T, J> {
    base: &'a Index<I, T, J>,
    patch: &'a Patch<I, T, J>,
}
impl<I: Ord + Clone, T, J: Ord + Clone> Overlay<'_, I, T, J> {
    pub fn get(&self, id: &I) -> Option<&Entry<T, J>> {
        match self.patch.rows.get(id) {
            Some(entry) => entry.as_ref(),
            None => self.base.rows.get(id),
        }
    }
    pub fn members<'a>(&'a self, key: &'a J) -> impl Iterator<Item = &'a I> {
        self.base
            .members(key)
            .filter(move |id| {
                !self
                    .patch
                    .removed
                    .get(key)
                    .is_some_and(|ids| ids.contains(*id))
            })
            .chain(self.patch.added.get(key).into_iter().flatten())
    }
    pub fn has_members(&self, key: &J) -> bool {
        let old = self.base.buckets.get(key).map_or(0, BTreeSet::len);
        let removed = self.patch.removed.get(key).map_or(0, BTreeSet::len);
        let added = self.patch.added.get(key).map_or(0, BTreeSet::len);
        old > removed || added > 0
    }
}
