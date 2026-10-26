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
pub(super) enum Patch<I, T, J> {
    One {
        id: I,
        old_key: Option<J>,
        entry: Option<Entry<T, J>>,
    },
    Many(BatchPatch<I, T, J>),
}
pub(super) struct BatchPatch<I, T, J> {
    pub rows: BTreeMap<I, Option<Entry<T, J>>>,
    added: BTreeMap<J, BTreeSet<I>>,
    removed: BTreeMap<J, BTreeSet<I>>,
}
impl<I: Ord, T, J: Ord> Patch<I, T, J> {
    pub fn ids(&self) -> impl Iterator<Item = &I> {
        let (one, many) = match self {
            Self::One { id, .. } => (Some(id), None),
            Self::Many(patch) => (None, Some(&patch.rows)),
        };
        one.into_iter()
            .chain(many.into_iter().flat_map(|rows| rows.keys()))
    }
    fn get(&self, key: &I) -> Option<&Option<Entry<T, J>>> {
        match self {
            Self::One { id, entry, .. } => (id == key).then_some(entry),
            Self::Many(patch) => patch.rows.get(key),
        }
    }
    fn removed_count(&self, key: &J) -> usize {
        match self {
            Self::One { old_key, .. } => usize::from(old_key.as_ref() == Some(key)),
            Self::Many(patch) => patch.removed.get(key).map_or(0, BTreeSet::len),
        }
    }
    fn added_count(&self, key: &J) -> usize {
        match self {
            Self::One { entry, .. } => usize::from(entry.as_ref().is_some_and(|e| &e.key == key)),
            Self::Many(patch) => patch.added.get(key).map_or(0, BTreeSet::len),
        }
    }
    fn removed_contains(&self, key: &J, member: &I) -> bool {
        match self {
            Self::One { id, old_key, .. } => id == member && old_key.as_ref() == Some(key),
            Self::Many(patch) => patch
                .removed
                .get(key)
                .is_some_and(|ids| ids.contains(member)),
        }
    }
    fn added_members(&self, key: &J) -> impl Iterator<Item = &I> {
        let (one, many) = match self {
            Self::One { id, entry, .. } => {
                (entry.as_ref().filter(|e| &e.key == key).map(|_| id), None)
            }
            Self::Many(patch) => (None, patch.added.get(key)),
        };
        one.into_iter().chain(many.into_iter().flatten())
    }
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
        if let Some([change]) = changes.map(|changes| changes.as_ref()) {
            meter.work()?;
            let id = change.id();
            let old_key = self.rows.get(id).map(|old| old.key.clone());
            let entry = change.after().map(|value| Entry {
                key: (self.select)(id, value),
                value: value.clone(),
            });
            return Ok(Patch::One {
                id: id.clone(),
                old_key,
                entry,
            });
        }
        let mut patch = BatchPatch {
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
        Ok(Patch::Many(patch))
    }
    pub fn apply(&mut self, patch: Patch<I, T, J>) {
        let patch = match patch {
            Patch::One { id, old_key, entry } => {
                if let Some(key) = old_key {
                    let bucket = self
                        .buckets
                        .get_mut(&key)
                        .expect("removed member has an old bucket");
                    bucket.remove(&id);
                    if bucket.is_empty() {
                        self.buckets.remove(&key);
                    }
                }
                if let Some(entry) = entry {
                    self.buckets
                        .entry(entry.key.clone())
                        .or_default()
                        .insert(id.clone());
                    self.rows.insert(id, entry);
                } else {
                    self.rows.remove(&id);
                }
                return;
            }
            Patch::Many(patch) => patch,
        };
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
        match self.patch.get(id) {
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
            if !self.patch.removed_contains(key, id) {
                members.push(id);
            }
        }
        for id in self.patch.added_members(key) {
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
        mut visit: impl FnMut(&'a I),
    ) -> Result<(), Error> {
        if self.patch.removed_count(key) == 0 && self.patch.added_count(key) == 0 {
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
        let removed = self.patch.removed_count(key);
        let added = self.patch.added_count(key);
        old > removed || added > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Change, ExecutionBudget};

    #[test]
    fn single_change_overlay_and_commit_match_an_independent_row_model() -> Result<(), Error> {
        for (id, value) in [(1, Some(0)), (1, Some(1)), (1, None), (9, Some(2))] {
            let initial =
                BTreeMap::from([(1_u64, Arc::new(0_i64)), (2, Arc::new(0)), (3, Arc::new(2))]);
            let mut meter = Meter::new(ExecutionBudget::default());
            let mut index = Index::new(&initial, Rc::new(|_, v| *v), &mut meter)?;
            let after = value.map(Arc::new);
            let change = Change::between(id, initial.get(&id).cloned(), after.clone()).unwrap();
            let changes: Pending<u64, i64> = vec![change].into();
            let mut meter = Meter::new(ExecutionBudget {
                max_work: 1,
                ..Default::default()
            });
            let patch = index.stage(Some(&changes), &mut meter)?;
            assert!(matches!(patch, Patch::One { .. }));
            assert_eq!(meter.stats.work, 1);
            let mut expected = initial.clone();
            if let Some(after) = after {
                expected.insert(id, after);
            } else {
                expected.remove(&id);
            }
            let overlay = index.overlay(&patch);
            let mut meter = Meter::new(ExecutionBudget::default());
            for key in -1..=3 {
                let expected_ids: Vec<_> = expected
                    .iter()
                    .filter(|(_, v)| ***v == key)
                    .map(|(&id, _)| id)
                    .collect();
                let mut actual: Vec<_> = overlay
                    .members(&key, &mut meter)?
                    .into_iter()
                    .copied()
                    .collect();
                actual.sort_unstable();
                assert_eq!(actual, expected_ids);
                assert_eq!(overlay.has_members(&key), !expected_ids.is_empty());
            }
            assert_eq!(
                index
                    .rows
                    .iter()
                    .map(|(&id, e)| (id, *e.value))
                    .collect::<BTreeMap<_, _>>(),
                initial.iter().map(|(&id, v)| (id, **v)).collect()
            );
            index.apply(patch);
            assert_eq!(
                index
                    .rows
                    .iter()
                    .map(|(&id, e)| (id, *e.value))
                    .collect::<BTreeMap<_, _>>(),
                expected.iter().map(|(&id, v)| (id, **v)).collect()
            );
            for key in -1..=3 {
                assert_eq!(
                    index.members(&key).copied().collect::<Vec<_>>(),
                    expected
                        .iter()
                        .filter(|(_, v)| ***v == key)
                        .map(|(&id, _)| id)
                        .collect::<Vec<_>>()
                );
            }
        }
        Ok(())
    }

    #[test]
    fn discarded_single_patch_releases_its_payload() -> Result<(), Error> {
        let mut meter = Meter::new(ExecutionBudget::default());
        let index = Index::<u64, i64, i64>::new(&BTreeMap::new(), Rc::new(|_, v| *v), &mut meter)?;
        let value = Arc::new(7);
        let weak = Arc::downgrade(&value);
        let changes: Pending<u64, i64> = vec![Change::Insert { id: 1, value }].into();
        let patch = index.stage(Some(&changes), &mut meter)?;
        drop(changes);
        assert!(weak.upgrade().is_some());
        drop(patch);
        assert!(weak.upgrade().is_none());
        Ok(())
    }
}
