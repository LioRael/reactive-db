use crate::{Database, Error, Query};
use std::fmt;
use std::sync::Arc;

/// A typed source handle; writes are admitted only through a database batch.
///
/// Keys require total ordering, but payloads need not implement any trait beyond
/// `'static`. Payloads must remain logically immutable until explicitly replaced.
/// A source remains owned by the database even after its last handle is dropped.
pub struct Collection<K, T> {
    pub(crate) query: Query<K, T>,
}
impl<K, T> Clone for Collection<K, T> {
    fn clone(&self) -> Self {
        Self {
            query: self.query.clone(),
        }
    }
}
impl<K, T> fmt::Debug for Collection<K, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Collection").finish_non_exhaustive()
    }
}
impl<K: Ord + Clone + 'static, T: 'static> Collection<K, T> {
    /// Starts a lazy query with this collection's stable source identity.
    pub fn query(&self) -> Query<K, T> {
        self.query.clone()
    }
    /// Reads one committed payload without cloning the payload itself.
    ///
    /// # Errors
    /// Returns an ownership, dropped-database, or poisoned-database error.
    /// # Panics
    /// A custom key comparison may panic; the database is then poisoned.
    pub fn get(&self, db: &Database, key: &K) -> Result<Option<Arc<T>>, Error> {
        self.query.owner.check(&db.owner)?;
        let _guard = crate::engine::PanicGuard(db.owner.clone());
        Ok(db.state::<K, T>(self.query.id()).rows.get(key).cloned())
    }
}
