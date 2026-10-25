use crate::{Change, Epoch, Error};
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::Arc;

/// Finite shared-history capacities, measured in complete batches and row changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JournalLimits {
    batches: NonZeroUsize,
    changes: NonZeroUsize,
}
impl JournalLimits {
    /// Validates nonzero batch and row-change capacities.
    ///
    /// # Errors
    /// Returns [`Error::InvalidJournalLimits`] if either capacity is zero.
    pub fn new(max_batches: usize, max_changes: usize) -> Result<Self, Error> {
        Ok(Self {
            batches: NonZeroUsize::new(max_batches).ok_or(Error::InvalidJournalLimits)?,
            changes: NonZeroUsize::new(max_changes).ok_or(Error::InvalidJournalLimits)?,
        })
    }
    /// Returns the maximum number of retained complete batches.
    #[must_use]
    pub fn max_batches(self) -> usize {
        self.batches.get()
    }
    /// Returns the maximum number of retained row changes, not payload bytes.
    #[must_use]
    pub fn max_changes(self) -> usize {
        self.changes.get()
    }
}
impl Default for JournalLimits {
    fn default() -> Self {
        Self {
            batches: NonZeroUsize::new(128).expect("default batch capacity is nonzero"),
            changes: NonZeroUsize::new(16_384).expect("default change capacity is nonzero"),
        }
    }
}

/// Configuration shared by all materializations of the same explicit query node.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ViewOptions {
    /// Bounded history; slow subscribers must resync after eviction.
    pub journal: JournalLimits,
}

pub(crate) struct Record<K, T> {
    pub epoch: Epoch,
    pub changes: Arc<[Change<K, T>]>,
}
pub(crate) struct Journal<K, T> {
    pub limits: JournalLimits,
    pub floor: Epoch,
    pub records: VecDeque<Record<K, T>>,
    count: usize,
}
impl<K, T> Journal<K, T> {
    pub fn new(limits: JournalLimits, epoch: Epoch) -> Self {
        Self {
            limits,
            floor: epoch,
            records: VecDeque::new(),
            count: 0,
        }
    }
    pub fn push(&mut self, epoch: Epoch, changes: Arc<[Change<K, T>]>) {
        if changes.is_empty() {
            return;
        }
        if changes.len() > self.limits.max_changes() {
            self.records.clear();
            self.count = 0;
            self.floor = epoch;
            return;
        }
        while self.records.len() >= self.limits.max_batches()
            || self.count > self.limits.max_changes() - changes.len()
        {
            let removed = self
                .records
                .pop_front()
                .expect("retained history exceeds capacity");
            self.count -= removed.changes.len();
            self.floor = removed.epoch;
        }
        self.count += changes.len();
        self.records.push_back(Record { epoch, changes });
    }
}
