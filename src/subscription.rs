use crate::{Change, Database, DeltaBatch, Epoch, Error, LiveQuery, Poll, Snapshot};
use std::fmt;

/// An independent cursor over bounded shared history, with explicit lag recovery.
///
/// Polling reads recorded deltas and never executes query closures. Successful
/// delivery advances this cursor immediately; external exactly-once processing
/// and asynchronous wakeups are not provided. Dropping it cancels its lease.
#[must_use]
pub struct Subscription<I, T> {
    pub(crate) view: LiveQuery<I, T>,
    pub(crate) cursor: Epoch,
}
impl<I, T> fmt::Debug for Subscription<I, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subscription")
            .field("cursor", &self.cursor)
            .finish_non_exhaustive()
    }
}
impl<I: Ord + Clone + 'static, T: 'static> Subscription<I, T> {
    /// Reads the next complete delta, or reports idle/evicted history immediately.
    ///
    /// A single oversized batch creates a history gap rather than blocking writes.
    /// Once lagged, use [`Self::resync`] before consuming further changes.
    /// # Errors
    /// Returns ownership, dropped-database or poisoned-database errors.
    pub fn poll(&mut self, db: &Database) -> Result<Poll<Change<I, T>>, Error> {
        self.view.query.owner.check(&db.owner)?;
        let journal = db
            .state::<I, T>(self.view.query.id())
            .journal
            .as_ref()
            .expect("a live subscription retains its materialized journal");
        if self.cursor < journal.floor {
            return Ok(Poll::Lagged {
                last_delivered: self.cursor,
                current: db.epoch,
            });
        }
        if let Some(record) = journal
            .records
            .iter()
            .find(|record| record.epoch > self.cursor)
        {
            let batch = DeltaBatch {
                from_epoch: self.cursor,
                to_epoch: record.epoch,
                changes: record.changes.clone(),
            };
            self.cursor = record.epoch;
            Ok(Poll::Delta(batch))
        } else {
            Ok(Poll::Idle)
        }
    }
    /// Replaces lost history with a snapshot and atomically resets this cursor.
    /// # Errors
    /// Returns ownership, dropped-database or poisoned-database errors.
    /// # Panics
    /// Cloning an application identity may panic and poison the database.
    pub fn resync(&mut self, db: &mut Database) -> Result<Snapshot<I, T>, Error> {
        let snapshot = self.view.snapshot(db)?;
        self.cursor = snapshot.epoch;
        Ok(snapshot)
    }
}
