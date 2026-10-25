use crate::Epoch;
use std::sync::Arc;

/// A stable ordering token: application sort key followed by unique row identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OrderKey<S, I> {
    pub(crate) sort: S,
    pub(crate) id: I,
}
impl<S, I> OrderKey<S, I> {
    /// Borrows the application sort key; use Reverse to request descending order.
    pub fn sort_key(&self) -> &S {
        &self.sort
    }
    /// Borrows the stable tie-break identity.
    pub fn identity(&self) -> &I {
        &self.id
    }
}
/// An immutable row and its ordering token at one committed revision.
#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct OrderedRow<I, T, S> {
    /// Logical identity, stable across sort-key changes.
    pub id: I,
    /// Immutable source payload, shared without requiring Clone.
    pub value: Arc<T>,
    /// Token used to order this row, without positional indices.
    pub order: OrderKey<S, I>,
}
impl<I: Clone, T, S: Clone> Clone for OrderedRow<I, T, S> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            value: self.value.clone(),
            order: self.order.clone(),
        }
    }
}
/// An owned snapshot in token order, restricted to the selected window if any.
#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct OrderedSnapshot<I, T, S> {
    /// Database revision at acquisition.
    pub epoch: Epoch,
    /// Rows already sorted by their tokens.
    pub rows: Vec<OrderedRow<I, T, S>>,
}
impl<I: Clone, T, S: Clone> Clone for OrderedSnapshot<I, T, S> {
    fn clone(&self) -> Self {
        Self {
            epoch: self.epoch,
            rows: self.rows.clone(),
        }
    }
}
/// One final ordered transition; apply a complete batch atomically using tokens.
#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum OrderedChange<I, T, S> {
    /// A row entered the selected result.
    Insert {
        /// The inserted row and token.
        row: OrderedRow<I, T, S>,
    },
    /// A selected identity changed payload, token, or both.
    Update {
        /// Previously visible row and token.
        before: OrderedRow<I, T, S>,
        /// Finally visible row and token.
        after: OrderedRow<I, T, S>,
    },
    /// A row left the selected result.
    Remove {
        /// The removed row and old token.
        row: OrderedRow<I, T, S>,
    },
}
impl<I: Clone, T, S: Clone> Clone for OrderedChange<I, T, S> {
    fn clone(&self) -> Self {
        match self {
            Self::Insert { row } => Self::Insert { row: row.clone() },
            Self::Update { before, after } => Self::Update {
                before: before.clone(),
                after: after.clone(),
            },
            Self::Remove { row } => Self::Remove { row: row.clone() },
        }
    }
}
