use std::sync::Arc;

/// A committed database revision; compare only revisions from the same database.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Epoch(pub(crate) u64);
impl Epoch {
    /// Returns the database-local revision number, starting at zero.
    #[must_use]
    pub fn number(self) -> u64 {
        self.0
    }
}

/// One immutable typed row, identified independently of its payload.
#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Row<I, T> {
    /// The stable identity within this source or query.
    pub id: I,
    /// Shared immutable payload; mutation must go through a write batch.
    pub value: Arc<T>,
}
impl<I: Clone, T> Clone for Row<I, T> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            value: self.value.clone(),
        }
    }
}

/// An owned row list captured at a committed revision, in identity order.
#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Snapshot<I, T> {
    /// The database revision at snapshot acquisition.
    pub epoch: Epoch,
    /// Rows ordered by identity; building this list takes O(result size).
    pub rows: Vec<Row<I, T>>,
}
impl<I: Clone, T> Clone for Snapshot<I, T> {
    fn clone(&self) -> Self {
        Self {
            epoch: self.epoch,
            rows: self.rows.clone(),
        }
    }
}

/// The final transition of one identity within an atomic publication batch.
#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Change<I, T> {
    /// A previously absent identity became visible.
    Insert {
        /// Newly visible identity.
        id: I,
        /// Newly visible payload.
        value: Arc<T>,
    },
    /// A visible identity received a replacement payload.
    Update {
        /// Identity preserved by this update.
        id: I,
        /// Payload visible before this batch.
        before: Arc<T>,
        /// Payload visible after this batch.
        after: Arc<T>,
    },
    /// A previously visible identity disappeared.
    Remove {
        /// Removed identity.
        id: I,
        /// Payload visible before removal.
        value: Arc<T>,
    },
}
impl<I: Clone, T> Clone for Change<I, T> {
    fn clone(&self) -> Self {
        match self {
            Self::Insert { id, value } => Self::Insert {
                id: id.clone(),
                value: value.clone(),
            },
            Self::Update { id, before, after } => Self::Update {
                id: id.clone(),
                before: before.clone(),
                after: after.clone(),
            },
            Self::Remove { id, value } => Self::Remove {
                id: id.clone(),
                value: value.clone(),
            },
        }
    }
}
impl<I, T> Change<I, T> {
    /// Returns the identity affected by this transition.
    pub fn id(&self) -> &I {
        match self {
            Self::Insert { id, .. } | Self::Update { id, .. } | Self::Remove { id, .. } => id,
        }
    }
    pub(crate) fn after(&self) -> Option<&Arc<T>> {
        match self {
            Self::Insert { value, .. } => Some(value),
            Self::Update { after, .. } => Some(after),
            Self::Remove { .. } => None,
        }
    }
    pub(crate) fn between(id: I, before: Option<Arc<T>>, after: Option<Arc<T>>) -> Option<Self> {
        match (before, after) {
            (None, None) => None,
            (Some(before), Some(after)) if Arc::ptr_eq(&before, &after) => None,
            (None, Some(value)) => Some(Self::Insert { id, value }),
            (Some(value), None) => Some(Self::Remove { id, value }),
            (Some(before), Some(after)) => Some(Self::Update { id, before, after }),
        }
    }
}

/// One complete delta, delivered between two subscription watermarks.
#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct DeltaBatch<C> {
    /// This subscription's previous delivered or snapshot revision.
    pub from_epoch: Epoch,
    /// The revision that produced these changes.
    pub to_epoch: Epoch,
    /// Apply all changes before exposing the resulting state to readers.
    pub changes: Arc<[C]>,
}
impl<C> Clone for DeltaBatch<C> {
    fn clone(&self) -> Self {
        Self {
            from_epoch: self.from_epoch,
            to_epoch: self.to_epoch,
            changes: self.changes.clone(),
        }
    }
}

/// The result of reading a subscription without waiting or recomputing queries.
#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Poll<C> {
    /// No new changes are available for this view.
    Idle,
    /// A complete retained publication batch.
    Delta(DeltaBatch<C>),
    /// Required history was evicted; resync before consuming further changes.
    Lagged {
        /// Last successfully delivered or snapshotted revision.
        last_delivered: Epoch,
        /// Current database revision available through resync.
        current: Epoch,
    },
}
impl<C> Clone for Poll<C> {
    fn clone(&self) -> Self {
        match self {
            Self::Idle => Self::Idle,
            Self::Delta(batch) => Self::Delta(batch.clone()),
            Self::Lagged {
                last_delivered,
                current,
            } => Self::Lagged {
                last_delivered: *last_delivered,
                current: *current,
            },
        }
    }
}
