use super::{OrderKey, OrderedChange, OrderedQuery, OrderedRow, OrderedSnapshot};
use crate::engine::PanicGuard;
use crate::{Change, Database, DeltaBatch, Error, LiveQuery, Poll, Subscription, ViewOptions};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

/// An ordered snapshot and cursor acquired at the same revision.
pub type OrderedSubscriptionStart<I, T, S> =
    (OrderedSnapshot<I, T, S>, OrderedSubscription<I, T, S>);
/// A lease on an incrementally maintained ordered result or top-k window.
#[must_use]
pub struct OrderedLiveQuery<I, T, S> {
    view: LiveQuery<I, OrderedRow<I, T, S>>,
    limit: Option<usize>,
}
impl<I, T, S> Clone for OrderedLiveQuery<I, T, S> {
    fn clone(&self) -> Self {
        Self {
            view: self.view.clone(),
            limit: self.limit,
        }
    }
}
impl<I, T, S> fmt::Debug for OrderedLiveQuery<I, T, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrderedLiveQuery")
            .field("limit", &self.limit)
            .finish_non_exhaustive()
    }
}
/// An independent cursor over the bounded shared ordered history.
#[must_use]
pub struct OrderedSubscription<I, T, S> {
    sub: Subscription<I, OrderedRow<I, T, S>>,
    view: OrderedLiveQuery<I, T, S>,
}
impl<I, T, S> fmt::Debug for OrderedSubscription<I, T, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrderedSubscription")
            .finish_non_exhaustive()
    }
}
impl Database {
    /// Materializes an ordered query with finite default journal capacities.
    /// # Errors
    /// Returns ownership, budget, arithmetic, poison or configuration errors.
    /// # Panics
    /// Application closures or key operations may panic and poison the database.
    pub fn materialize_ordered<I: Ord + Clone + 'static, T: 'static, S: Ord + Clone + 'static>(
        &mut self,
        query: OrderedQuery<I, T, S>,
    ) -> Result<OrderedLiveQuery<I, T, S>, Error> {
        self.materialize_ordered_with_options(query, ViewOptions::default())
    }
    /// Materializes or reuses an ordered plan with explicit bounded shared history.
    /// # Errors
    /// Returns ownership, budget, arithmetic, poison or shared-options errors.
    /// # Panics
    /// Application closures or key operations may panic and poison the database.
    pub fn materialize_ordered_with_options<
        I: Ord + Clone + 'static,
        T: 'static,
        S: Ord + Clone + 'static,
    >(
        &mut self,
        query: OrderedQuery<I, T, S>,
        options: ViewOptions,
    ) -> Result<OrderedLiveQuery<I, T, S>, Error> {
        let (query, limit) = query.finish();
        Ok(OrderedLiveQuery {
            view: self.materialize_with_options(query, options)?,
            limit,
        })
    }
}
impl<I: Ord + Clone + 'static, T: 'static, S: Ord + Clone + 'static> OrderedLiveQuery<I, T, S> {
    /// Captures selected rows in token order in O(result size), without sorting again.
    /// # Errors
    /// Returns ownership or poison errors.
    /// # Panics
    /// Application key cloning may panic and poison the database.
    pub fn snapshot(&self, db: &Database) -> Result<OrderedSnapshot<I, T, S>, Error> {
        self.view.query.owner.check(&db.owner)?;
        let _guard = PanicGuard(db.owner.clone());
        let index = db
            .nodes
            .get(&self.view.query.id())
            .expect("ordered lease retains node")
            .ordered_index()
            .expect("ordered node has index")
            .downcast_ref::<BTreeMap<OrderKey<S, I>, Arc<T>>>()
            .expect("ordered index type");
        Ok(OrderedSnapshot {
            epoch: db.epoch,
            rows: index
                .iter()
                .take(self.limit.unwrap_or(usize::MAX))
                .map(|(order, value)| OrderedRow {
                    id: order.id.clone(),
                    value: value.clone(),
                    order: order.clone(),
                })
                .collect(),
        })
    }
    /// Acquires an ordered snapshot and its following cursor atomically.
    /// # Errors
    /// Returns ownership or poison errors.
    /// # Panics
    /// Application key cloning may panic and poison the database.
    pub fn subscribe(&self, db: &mut Database) -> Result<OrderedSubscriptionStart<I, T, S>, Error> {
        let snapshot = self.snapshot(db)?;
        let sub = Subscription {
            view: self.view.clone(),
            cursor: snapshot.epoch,
        };
        Ok((
            snapshot,
            OrderedSubscription {
                sub,
                view: self.clone(),
            },
        ))
    }
}
impl<I: Ord + Clone + 'static, T: 'static, S: Ord + Clone + 'static> OrderedSubscription<I, T, S> {
    /// Reads one complete ordered batch without invoking selectors or scanning inputs.
    /// Clone costs scale with delivered changes; apply the complete batch atomically.
    /// # Errors
    /// Returns ownership or poison errors.
    /// # Panics
    /// Cloning application keys may panic and poison the database.
    pub fn poll(&mut self, db: &Database) -> Result<Poll<OrderedChange<I, T, S>>, Error> {
        self.view.view.query.owner.check(&db.owner)?;
        let _guard = PanicGuard(db.owner.clone());
        Ok(match self.sub.poll(db)? {
            Poll::Idle => Poll::Idle,
            Poll::Lagged {
                last_delivered,
                current,
            } => Poll::Lagged {
                last_delivered,
                current,
            },
            Poll::Delta(batch) => Poll::Delta(DeltaBatch {
                from_epoch: batch.from_epoch,
                to_epoch: batch.to_epoch,
                changes: batch
                    .changes
                    .iter()
                    .map(|change| match change {
                        Change::Insert { value, .. } => OrderedChange::Insert {
                            row: (**value).clone(),
                        },
                        Change::Update { before, after, .. } => OrderedChange::Update {
                            before: (**before).clone(),
                            after: (**after).clone(),
                        },
                        Change::Remove { value, .. } => OrderedChange::Remove {
                            row: (**value).clone(),
                        },
                    })
                    .collect::<Vec<_>>()
                    .into(),
            }),
        })
    }
    /// Replaces lost ordered history with a current snapshot and resets the cursor.
    /// # Errors
    /// Returns ownership or poison errors.
    /// # Panics
    /// Application key cloning may panic and poison the database.
    pub fn resync(&mut self, db: &mut Database) -> Result<OrderedSnapshot<I, T, S>, Error> {
        let snapshot = self.view.snapshot(db)?;
        self.sub.cursor = snapshot.epoch;
        Ok(snapshot)
    }
}
