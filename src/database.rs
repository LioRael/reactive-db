use crate::budget::Meter;
use crate::engine::{self, Lease, NodeId, Nodes, Owner, PanicGuard, Runtime, Stages, State};
use crate::journal::Journal;
use crate::{Collection, Epoch, Error, LiveQuery, Query, ViewOptions, WriteBatch};
use crate::{ExecutionBudget, ExecutionStats};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::rc::Rc;

/// The result of one successful synchronous write batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct Commit {
    /// The committed revision, unchanged for a net-empty batch.
    pub epoch: Epoch,
    /// Whether this batch changed any source identity or payload.
    pub changed: bool,
    /// Logical work consumed by this successful batch.
    pub stats: ExecutionStats,
}

/// A single-owner database with synchronous incremental publication and no runtime dependency.
///
/// All normal write errors preserve committed sources, views, journals and epoch.
/// Panics from application code propagate and permanently poison subsequent access;
/// `panic=abort` and allocator aborts cannot be recovered. This type is !Send/!Sync.
/// There is no background work and no global scheduler.
pub struct Database {
    pub(crate) owner: Rc<Owner>,
    pub(crate) nodes: Nodes,
    scheduler: crate::scheduler::Scheduler,
    pub(crate) budget: ExecutionBudget,
    names: BTreeSet<String>,
    pub(crate) epoch: Epoch,
}
impl fmt::Debug for Database {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Database")
            .field("epoch", &self.epoch)
            .field("collections", &self.names.len())
            .field("nodes", &self.nodes.len())
            .field("poisoned", &self.owner.poisoned.get())
            .finish()
    }
}
impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}
impl Database {
    /// Creates an empty database without starting threads or performing I/O.
    #[must_use]
    pub fn new() -> Self {
        Self {
            owner: Rc::new(Owner::new()),
            nodes: BTreeMap::new(),
            scheduler: crate::scheduler::Scheduler::default(),
            budget: ExecutionBudget::default(),
            names: BTreeSet::new(),
            epoch: Epoch::default(),
        }
    }
    /// Creates a database with explicit per-operation execution limits.
    #[must_use]
    pub fn with_budget(budget: ExecutionBudget) -> Self {
        {
            let mut db = Self::new();
            db.budget = budget;
            db
        }
    }
    /// Changes limits for subsequent operations; rejected writes may be retried.
    /// # Errors
    /// Returns an error if the database is poisoned.
    pub fn set_execution_budget(&mut self, budget: ExecutionBudget) -> Result<(), Error> {
        self.owner.check(&self.owner)?;
        self.budget = budget;
        Ok(())
    }
    /// Reads the current committed revision.
    /// # Errors
    /// Returns [`Error::Poisoned`] after an application panic.
    pub fn epoch(&self) -> Result<Epoch, Error> {
        self.owner.check(&self.owner)?;
        Ok(self.epoch)
    }
    /// Creates a typed source with a unique database-local diagnostic name.
    /// # Errors
    /// Returns [`Error::DuplicateCollection`] or [`Error::Poisoned`].
    /// # Panics
    /// A destructor for retired query state may panic and poison the database.
    pub fn create_collection<K: Ord + Clone + 'static, T: 'static>(
        &mut self,
        name: &str,
    ) -> Result<Collection<K, T>, Error> {
        self.owner.check(&self.owner)?;
        let _guard = PanicGuard(self.owner.clone());
        self.retire();
        if self.names.contains(name) {
            return Err(Error::DuplicateCollection);
        }
        self.scheduler.reserve(1)?;
        let token = engine::Token::new(&self.owner);
        let id = engine::node_id(&token);
        let query = Query::source(self.owner.clone(), token.clone());
        self.nodes.insert(
            id,
            Box::new(Runtime::<K, T> {
                state: State::new(BTreeMap::new()),
                lease: Lease::Source(token),
                evaluator: None,
            }),
        );
        self.nodes[&id].register();
        self.scheduler.register(id, Vec::new());
        self.names.insert(name.to_owned());
        Ok(Collection { query })
    }
    /// Materializes a query using finite default journal capacities.
    /// # Errors
    /// Returns ownership, budget, arithmetic, poison or shared-view configuration errors.
    /// # Panics
    /// Query closures or key comparisons may panic; subsequent database access fails.
    pub fn materialize<I: Ord + Clone + 'static, T: 'static>(
        &mut self,
        query: Query<I, T>,
    ) -> Result<LiveQuery<I, T>, Error> {
        self.materialize_with_options(query, ViewOptions::default())
    }
    /// Materializes or reuses an explicit plan with shared journal options.
    ///
    /// Initial construction scans its input once. Clones share nodes; independently
    /// constructed closures are never compared for semantic equivalence.
    /// # Errors
    /// Returns ownership, budget, arithmetic, poison or [`Error::ViewOptionsConflict`] errors.
    /// # Panics
    /// Query closures, comparisons or retired destructors may panic and poison access.
    pub fn materialize_with_options<I: Ord + Clone + 'static, T: 'static>(
        &mut self,
        query: Query<I, T>,
        options: ViewOptions,
    ) -> Result<LiveQuery<I, T>, Error> {
        let mut meter = Meter::new(self.budget);
        query.validate(&self.owner, &mut meter)?;
        let _guard = PanicGuard(self.owner.clone());
        self.retire();
        if let Some(node) = self.nodes.get(&query.id())
            && let Some(journal) = &engine::state::<I, T>(node.as_ref()).journal
        {
            if journal.limits != options.journal {
                return Err(Error::ViewOptionsConflict);
            }
            return Ok(LiveQuery { query });
        }
        let mut new = Nodes::new();
        let mut order = Vec::new();
        query.plan.build(self, &mut new, &mut order, &mut meter)?;
        self.scheduler.reserve(order.len())?;
        let node = new
            .get_mut(&query.id())
            .or_else(|| self.nodes.get_mut(&query.id()))
            .expect("materialized root was built or previously registered");
        engine::state_mut::<I, T>(node.as_mut()).journal =
            Some(Journal::new(options.journal, self.epoch));
        for id in order {
            let node = &new[&id];
            self.scheduler.register(id, node.parents());
            node.register();
        }
        self.nodes.extend(new);
        Ok(LiveQuery { query })
    }
    /// Stages a cross-collection batch and publishes only its final committed state.
    ///
    /// Reads through the batch observe prior staged writes. Normal errors discard
    /// the sparse overlay, without cloning entire tables. Even a swallowed batch
    /// method error invalidates the whole batch. No subscriber callback runs here.
    /// # Errors
    /// Returns validation, batch-operation, ownership, budget, poison or arithmetic errors.
    /// # Panics
    /// Application closures, key comparisons or destructors may panic. Unwinding
    /// propagates and permanently poisons the database instead of resuming it.
    pub fn write(
        &mut self,
        f: impl FnOnce(&mut WriteBatch<'_>) -> Result<(), Error>,
    ) -> Result<Commit, Error> {
        self.owner.check(&self.owner)?;
        let _guard = PanicGuard(self.owner.clone());
        self.retire();
        let (patches, mut meter) = {
            let mut batch = WriteBatch::new(self);
            f(&mut batch)?;
            if batch.invalid.get() {
                return Err(Error::InvalidBatch);
            }
            (batch.patches, batch.meter)
        };
        let mut stages = Stages::new();
        let mut commits = Vec::new();
        for (id, patch) in patches {
            let node = self
                .nodes
                .get(&id)
                .expect("batch source remains registered");
            let changes = node.source_changes(patch, &mut meter)?;
            if node.has_changes(changes.as_ref()) {
                stages.insert(id, changes);
                commits.push(id);
            }
        }
        if commits.is_empty() {
            return Ok(Commit {
                epoch: self.epoch,
                changed: false,
                stats: meter.stats,
            });
        }
        let epoch = Epoch(
            self.epoch
                .0
                .checked_add(1)
                .ok_or(Error::ArithmeticOverflow)?,
        );
        let mut maintenance = Stages::new();
        let mut ready = crate::scheduler::Queue::new();
        for &id in &commits {
            meter.work()?;
            self.scheduler.propagate(id, &mut ready, &mut meter)?;
        }
        while let Some((_, id)) = ready.pop_first() {
            meter.work()?;
            let node = self
                .nodes
                .get(&id)
                .expect("scheduled node remains registered");
            if let Some(pending) = node.prepare(&stages, &mut meter)? {
                let changed = node.has_changes(pending.changes.as_ref());
                stages.insert(id, pending.changes);
                if let Some(update) = pending.maintenance {
                    maintenance.insert(id, update);
                }
                commits.push(id);
                if changed {
                    self.scheduler.propagate(id, &mut ready, &mut meter)?;
                }
            }
        }
        // No fallible operation remains. Maintenance-only stages must commit too.
        for id in commits {
            let pending = stages.remove(&id).expect("commit has a prepared stage");
            self.nodes
                .get_mut(&id)
                .expect("prepared node remains registered")
                .commit(pending, maintenance.remove(&id), epoch);
        }
        self.epoch = epoch;
        Ok(Commit {
            epoch,
            changed: true,
            stats: meter.stats,
        })
    }
    pub(crate) fn state<K: 'static, T: 'static>(&self, id: NodeId) -> &State<K, T> {
        engine::state(
            self.nodes
                .get(&id)
                .expect("live typed handle retains its node")
                .as_ref(),
        )
    }
    fn retire(&mut self) {
        loop {
            // Release the queue borrow before dropping user state: destructors may
            // release other plan leases and enqueue further retirement events.
            let retired = std::mem::take(&mut *self.owner.retired.borrow_mut());
            if retired.is_empty() {
                self.scheduler.reclaim();
                break;
            }
            for id in retired {
                self.scheduler.remove(id);
                self.nodes.remove(&id);
            }
        }
    }
}
impl Drop for Database {
    fn drop(&mut self) {
        self.owner.alive.set(false);
        self.owner.retired.take();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_drop_frees_pending_retirement_storage_with_surviving_handles() -> Result<(), Error>
    {
        let mut db = Database::new();
        let rows = db.create_collection::<u64, i64>("rows")?;
        let view = db.materialize(rows.query().map(|v| *v))?;
        let owner = db.owner.clone();
        drop(view);
        assert_eq!(owner.retired.borrow().len(), 1);
        drop(db);
        assert_eq!(owner.retired.borrow().capacity(), 0);
        assert_eq!(rows.get(&Database::new(), &0), Err(Error::DatabaseDropped));
        Ok(())
    }

    #[test]
    fn exhausted_epoch_rejects_before_any_source_or_view_commit() -> Result<(), Error> {
        let mut db = Database::new();
        let rows = db.create_collection::<u64, i64>("rows")?;
        let view = db.materialize(rows.query().map(|v| v * 2))?;
        db.epoch = Epoch(u64::MAX);
        assert_eq!(
            db.write(|tx| tx.insert(&rows, 1, 10)),
            Err(Error::ArithmeticOverflow)
        );
        assert!(rows.get(&db, &1)?.is_none());
        assert!(view.snapshot(&db)?.rows.is_empty());
        assert_eq!(db.epoch()?.number(), u64::MAX);
        assert!(!db.write(|_| Ok(()))?.changed);
        Ok(())
    }
}
