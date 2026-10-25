use crate::engine::{Patch, Stages};
use crate::{Collection, Database, Error};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

/// A sparse write overlay with read-your-writes semantics across typed collections.
///
/// Dropping an unfinished batch discards its writes. Owned values passed to a
/// failed operation are dropped; retain application input separately if retrying.
pub struct WriteBatch<'db> {
    db: &'db Database,
    pub(crate) patches: Stages,
    pub(crate) invalid: Cell<bool>,
}
impl fmt::Debug for WriteBatch<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteBatch")
            .field("touched_collections", &self.patches.len())
            .field("invalid", &self.invalid.get())
            .finish()
    }
}
impl<'db> WriteBatch<'db> {
    pub(crate) fn new(db: &'db Database) -> Self {
        Self {
            db,
            patches: Stages::new(),
            invalid: Cell::new(false),
        }
    }
    fn invalidate<T>(&self, result: Result<T, Error>) -> Result<T, Error> {
        if result.is_err() {
            self.invalid.set(true);
        }
        result
    }
    /// Reads the latest staged payload, falling back to committed source state.
    /// # Errors
    /// Ownership or poison errors invalidate this batch, even if ignored.
    /// # Panics
    /// A custom key comparison may panic and poison the enclosing database write.
    pub fn get<K: Ord + Clone + 'static, T: 'static>(
        &self,
        collection: &Collection<K, T>,
        key: &K,
    ) -> Result<Option<Arc<T>>, Error> {
        self.invalidate(collection.query.owner.check(&self.db.owner))?;
        if let Some(patch) = self.patches.get(&collection.query.id()) {
            let patch = patch
                .downcast_ref::<Patch<K, T>>()
                .expect("source patch has its handle's type");
            if let Some(value) = patch.get(key) {
                return Ok(value.clone());
            }
        }
        Ok(self
            .db
            .state::<K, T>(collection.query.id())
            .rows
            .get(key)
            .cloned())
    }
    /// Inserts a previously absent key, including absence caused by a staged removal.
    /// # Errors
    /// Duplicate keys, ownership or poison errors invalidate the complete batch.
    /// # Panics
    /// A custom key comparison may panic and poison the database.
    pub fn insert<K: Ord + Clone + 'static, T: 'static>(
        &mut self,
        collection: &Collection<K, T>,
        key: K,
        value: T,
    ) -> Result<(), Error> {
        if self.get(collection, &key)?.is_some() {
            return self.invalidate(Err(Error::DuplicateKey));
        }
        self.record(collection, key, Some(Arc::new(value)));
        Ok(())
    }
    /// Replaces an existing payload without requiring the payload to implement Clone.
    /// # Errors
    /// Missing keys, ownership or poison errors invalidate the complete batch.
    /// # Panics
    /// A custom key comparison may panic and poison the database.
    pub fn replace<K: Ord + Clone + 'static, T: 'static>(
        &mut self,
        collection: &Collection<K, T>,
        key: K,
        value: T,
    ) -> Result<(), Error> {
        if self.get(collection, &key)?.is_none() {
            return self.invalidate(Err(Error::MissingKey));
        }
        self.record(collection, key, Some(Arc::new(value)));
        Ok(())
    }
    /// Removes a key that exists in the current staged state.
    /// # Errors
    /// Missing keys, ownership or poison errors invalidate the complete batch.
    /// # Panics
    /// A custom key comparison may panic and poison the database.
    pub fn remove<K: Ord + Clone + 'static, T: 'static>(
        &mut self,
        collection: &Collection<K, T>,
        key: &K,
    ) -> Result<(), Error> {
        if self.get(collection, key)?.is_none() {
            return self.invalidate(Err(Error::MissingKey));
        }
        self.record(collection, key.clone(), None);
        Ok(())
    }
    fn record<K: Ord + Clone + 'static, T: 'static>(
        &mut self,
        collection: &Collection<K, T>,
        key: K,
        value: Option<Arc<T>>,
    ) {
        self.patches
            .entry(collection.query.id())
            .or_insert_with(|| Box::new(BTreeMap::<K, Option<Arc<T>>>::new()))
            .downcast_mut::<Patch<K, T>>()
            .expect("source patch has its handle's type")
            .insert(key, value);
    }
}
