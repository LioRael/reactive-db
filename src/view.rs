use crate::engine::PanicGuard;
use crate::{Database, Error, Query, Row, Snapshot, Subscription};
use std::fmt;

/// A snapshot and subscription cursor acquired at the same committed revision.
pub type SubscriptionStart<I, T> = (Snapshot<I, T>, Subscription<I, T>);

/// A lease on a materialized query, shared by clones and subscriptions.
#[must_use]
pub struct LiveQuery<I, T> {
    pub(crate) query: Query<I, T>,
}
impl<I, T> Clone for LiveQuery<I, T> {
    fn clone(&self) -> Self {
        Self {
            query: self.query.clone(),
        }
    }
}
impl<I, T> fmt::Debug for LiveQuery<I, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LiveQuery").finish_non_exhaustive()
    }
}
impl<I: Ord + Clone + 'static, T: 'static> LiveQuery<I, T> {
    /// Captures current rows in identity order without cloning payloads.
    ///
    /// This operation takes O(result size); incremental writes do not call it.
    /// # Errors
    /// Returns ownership, dropped-database or poisoned-database errors.
    /// # Panics
    /// Cloning an application identity may panic and poison the database.
    pub fn snapshot(&self, db: &Database) -> Result<Snapshot<I, T>, Error> {
        self.query.owner.check(&db.owner)?;
        let _guard = PanicGuard(db.owner.clone());
        Ok(Snapshot {
            epoch: db.epoch,
            rows: db
                .state::<I, T>(self.query.id())
                .rows
                .iter()
                .map(|(id, value)| Row {
                    id: id.clone(),
                    value: value.clone(),
                })
                .collect(),
        })
    }
    /// Acquires a snapshot and a cursor immediately after the same revision.
    ///
    /// No write can interleave with initialization. Dropping the subscription
    /// releases its query lease; it never pins retained history indefinitely.
    /// # Errors
    /// Returns ownership, dropped-database or poisoned-database errors.
    /// # Panics
    /// Cloning an application identity may panic and poison the database.
    pub fn subscribe(&self, db: &mut Database) -> Result<SubscriptionStart<I, T>, Error> {
        let snapshot = self.snapshot(db)?;
        let subscription = Subscription {
            view: self.clone(),
            cursor: snapshot.epoch,
        };
        Ok((snapshot, subscription))
    }
}
