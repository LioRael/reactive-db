//! Typed incremental relational queries, ordered windows, atomic writes and bounded delta subscriptions.
//!
//! Payloads need not implement `Clone`, `Eq`, `Ord`, or serialization. Keys require
//! `Ord + Clone`. Payloads and query closures must remain logically immutable and
//! deterministic; changes through interior mutability are not tracked.
//!
//! ```
//! use reactive_db::{Database, Poll};
//! # fn main() -> Result<(), reactive_db::Error> {
//! struct Task { title: String, score: i64 }
//! let mut db = Database::new();
//! let tasks = db.create_collection::<u64, Task>("tasks")?;
//! let view = db.materialize(tasks.query()
//!     .filter(|task| task.score >= 10)
//!     .map(|task| task.title.clone()))?;
//! let (snapshot, mut subscription) = view.subscribe(&mut db)?;
//! assert!(snapshot.rows.is_empty());
//! db.write(|tx| tx.insert(&tasks, 1, Task { title: "typed core".into(), score: 20 }))?;
//! assert!(matches!(subscription.poll(&db)?, Poll::Delta(_)));
//! # Ok(())
//! # }
//! ```
//!
//! A database deliberately stays on its owner thread:
//! ```compile_fail
//! fn assert_send<T: Send>() {}
//! assert_send::<reactive_db::Database>();
//! ```
//! A query's fields remain statically checked:
//! ```compile_fail
//! # fn main() -> Result<(), reactive_db::Error> {
//! let mut db = reactive_db::Database::new();
//! let rows = db.create_collection::<u64, String>("rows")?;
//! let query = rows.query().filter(|row| row.missing_field);
//! # Ok(())
//! # }
//! ```
//! A collection cannot accept the wrong payload:
//! ```compile_fail
//! # fn main() -> Result<(), reactive_db::Error> {
//! let mut db = reactive_db::Database::new();
//! let rows = db.create_collection::<u64, String>("rows")?;
//! db.write(|tx| tx.insert(&rows, 1, 42))?;
//! # Ok(())
//! # }
//! ```
//! Writes cannot reenter the same owner:
//! ```compile_fail
//! # fn main() -> Result<(), reactive_db::Error> {
//! let mut db = reactive_db::Database::new();
//! db.write(|_| { db.write(|_| Ok(()))?; Ok(()) })?;
//! # Ok(())
//! # }
//! ```

//! Live query and subscription handles also stay on their owner thread:
//! ```compile_fail
//! fn assert_send<T: Send>() {}
//! assert_send::<reactive_db::LiveQuery<u64, String>>();
//! ```
//! ```compile_fail
//! fn assert_send<T: Send>() {}
//! assert_send::<reactive_db::Subscription<u64, String>>();
//! ```

//! Group statistics and ordered windows compose without an async runtime:
//! ```
//! # fn main() -> Result<(), reactive_db::Error> {
//! use reactive_db::Database;
//! use std::cmp::Reverse;
//! let mut db = Database::new();
//! let rows = db.create_collection::<u64, (u64, i64)>("scores")?;
//! let stats = rows.query().group_by(|v| v.0).stats_i64(|v| v.1);
//! let top = db.materialize_ordered(stats.sort_by_key(|s| Reverse(s.sum)).take(3))?;
//! db.write(|tx| { tx.insert(&rows, 1, (7, 10))?; tx.insert(&rows, 2, (7, 20)) })?;
//! assert_eq!(top.snapshot(&db)?.rows[0].value.sum, 30);
//! # Ok(())
//! # }
//! ```
//! Floating sort keys require an explicit total-order policy:
//! ```compile_fail
//! # fn main() -> Result<(), reactive_db::Error> {
//! let mut db = reactive_db::Database::new();
//! let rows = db.create_collection::<u64, f64>("rows")?;
//! let ordered = rows.query().sort_by_key(|v| *v);
//! # Ok(())
//! # }
//! ```
//! Ordered plans do not expose unordered transformations:
//! ```compile_fail
//! # fn main() -> Result<(), reactive_db::Error> {
//! let mut db = reactive_db::Database::new();
//! let rows = db.create_collection::<u64, i64>("rows")?;
//! let query = rows.query().sort_by_key(|v| *v).group_by(|v| *v);
//! # Ok(())
//! # }
//! ```
//! Statistics require i64 contributions:
//! ```compile_fail
//! # fn main() -> Result<(), reactive_db::Error> {
//! let mut db = reactive_db::Database::new();
//! let rows = db.create_collection::<u64, String>("rows")?;
//! let query = rows.query().group_by(|_| ()).stats_i64(|v| v.clone());
//! # Ok(())
//! # }
//! ```

mod aggregate;
mod budget;
mod change;
mod collection;
mod database;
mod engine;
mod error;
mod join;
mod journal;
mod ordered;
mod query;
mod subscription;
mod view;
mod write_batch;

pub use aggregate::{GroupedQuery, StatsI64};
pub use budget::{ExecutionBudget, ExecutionStats};
pub use change::{Change, DeltaBatch, Epoch, Poll, Row, Snapshot};
pub use collection::Collection;
pub use database::{Commit, Database};
pub use error::Error;
pub use join::{Joined, LeftJoined};
pub use journal::{JournalLimits, ViewOptions};
pub use ordered::{
    OrderKey, OrderedChange, OrderedLiveQuery, OrderedQuery, OrderedRow, OrderedSnapshot,
    OrderedSubscription, OrderedSubscriptionStart,
};
pub use query::Query;
pub use subscription::Subscription;
pub use view::{LiveQuery, SubscriptionStart};
pub use write_batch::WriteBatch;
