use crate::engine::Pending;
use crate::{Error, budget::Meter};
use std::collections::{BTreeMap, BTreeSet, btree_map};
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
    pub fn new(
        rows: &BTreeMap<I, Arc<T>>,
        select: Selector<I, T, J>,
        meter: &mut Meter,
    ) -> Result<Self, Error> {
        let mut index = Self {
            rows: BTreeMap::new(),
            buckets: BTreeMap::new(),
            select,
        };
        for (id, value) in rows {
            meter.work()?;
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
        Ok(index)
    }
    pub fn members(&self, key: &J) -> impl Iterator<Item = &I> {
        self.buckets.get(key).into_iter().flatten()
    }
    pub fn stage(
        &self,
        changes: Option<&Pending<I, T>>,
        meter: &mut Meter,
    ) -> Result<Patch<I, T, J>, Error> {
        let mut patch = Patch {
            rows: BTreeMap::new(),
            added: BTreeMap::new(),
            removed: BTreeMap::new(),
        };
        for change in changes.into_iter().flat_map(|c| c.iter()) {
            meter.work()?;
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
        Ok(patch)
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
    fn members(&self, key: &J, meter: &mut Meter) -> Result<Vec<&I>, Error> {
        let mut members = Vec::new();
        // The caller caches this final bucket per key for the entire preparation.
        // Meter skipped old members too: a bulk retraction is still work.
        for id in self.base.members(key) {
            meter.work()?;
            if !self
                .patch
                .removed
                .get(key)
                .is_some_and(|ids| ids.contains(id))
            {
                members.push(id);
            }
        }
        for id in self.patch.added.get(key).into_iter().flatten() {
            meter.work()?;
            members.push(id);
        }
        Ok(members)
    }
    pub fn visit_members<'a>(
        &'a self,
        key: &J,
        cache: &mut BTreeMap<J, Vec<&'a I>>,
        meter: &mut Meter,
        mut visit: impl FnMut(&I),
    ) -> Result<(), Error> {
        if !self.patch.removed.contains_key(key) && !self.patch.added.contains_key(key) {
            for id in self.base.members(key) {
                meter.work()?;
                visit(id);
            }
            return Ok(());
        }
        let members = match cache.entry(key.clone()) {
            btree_map::Entry::Occupied(entry) => entry.into_mut(),
            btree_map::Entry::Vacant(entry) => entry.insert(self.members(key, meter)?),
        };
        for id in members.iter() {
            meter.work()?;
            visit(id);
        }
        Ok(())
    }
    pub fn has_members(&self, key: &J) -> bool {
        let old = self.base.buckets.get(key).map_or(0, BTreeSet::len);
        let removed = self.patch.removed.get(key).map_or(0, BTreeSet::len);
        let added = self.patch.added.get(key).map_or(0, BTreeSet::len);
        old > removed || added > 0
    }
}
