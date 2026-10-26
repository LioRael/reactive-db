# reactive-db

Experimental native Rust reactive collections. M0–M3 are implemented: typed
collections, atomic in-memory write batches, incremental filter/map, inner/left joins, grouped count/stats, distinct values,
sort/top-k, ordered deltas and bounded subscriptions. Execution budgets reject
excessive batches before publication. Writes propagate only through affected query
dependencies, with empty deltas stopping downstream work. The planned relational core MVP is implemented.

Zero runtime dependencies. Safe Rust. No JS, FFI, serde requirement, background thread,
async runtime or connection to either original project. Rust 1.88+; development
checks use the pinned 1.99.0 toolchain.

```rust
use reactive_db::{Database, Poll};

fn main() -> Result<(), reactive_db::Error> {
    struct Task { title: String, score: i64 }
    let mut db = Database::new();
    let tasks = db.create_collection::<u64, Task>("tasks")?;
    let view = db.materialize(tasks.query()
        .filter(|task| task.score >= 10)
        .map(|task| task.title.clone()))?;
    let (initial, mut updates) = view.subscribe(&mut db)?;

    db.write(|tx| {
        tx.insert(&tasks, 1, Task { title: "typed core".into(), score: 20 })?;
        tx.insert(&tasks, 2, Task { title: "later".into(), score: 5 })
    })?;
    assert!(initial.rows.is_empty());
    while let Poll::Delta(batch) = updates.poll(&db)? {
        println!("epoch {}: {:?}", batch.to_epoch.number(), batch.changes);
    }
    Ok(())
}
```

Run `cargo run --example basic` or `cargo run --example joins`; `cargo run --example analytics` demonstrates
grouped statistics and an ordered leaderboard. Read [semantics](docs/semantics.md),
[implementation architecture](docs/architecture.md), and [delivery status](docs/status.md).

## Contracts

- Keys are `Ord + Clone`; payloads only require `'static`. `Arc<T>` preserves
  immutable snapshots without requiring payload Clone, Eq, Ord or Serialize.
- Payloads must be logically immutable, even when they contain interior-mutable
  fields. Replace through a write batch to publish a change. Closures must be pure
  and deterministic.
- One synchronous owner thread. Database/handles are !Send/!Sync. Snapshots and
  deltas may cross threads when their identities and payloads permit Send/Sync.
- A successful write publishes one epoch after all incremental work completes.
  Normal errors discard the entire sparse overlay, even if a method error was
  swallowed. Panics propagate and permanently poison access; there is no recovery
  API. No full-table clone is used to stage a write.
- Query clones share explicit nodes. Separately constructed closures do not get
  automatic semantic equivalence or deduplication.
- A subscription starts with snapshot+cursor. Poll reads recorded deltas without
  re-evaluating queries. Slow consumers receive Lagged and must resync. Delivery
  advances the cursor immediately, without external acknowledgements.
- Shared journals default to 128 batches / 16,384 changes. Capacity is not an
  arbitrary-payload byte limit. An oversized batch commits normally but creates a
  history gap. Application-owned snapshots can retain payloads independently.

## Validation

```sh
cargo fmt --all --check
cargo check --all-targets --all-features --locked
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --locked
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --locked
cargo +1.88.0 test --locked
```

The integration suite includes deterministic generated histories compared against
an independent full-recomputation model and a mirror rebuilt exclusively from
deltas; work-count tests check single-row updates at different collection sizes.
Rustdoc includes compile-fail contracts. No external services or oracle runtime
are required. Criterion is a development-only benchmark dependency.

Optimistic writes, persistence and UI adapters remain future milestones. Nothing has been published.

## Typed joins

```rust
let query = tasks.query().left_join_on(
    projects.query(),
    |_, task| task.project,
    |project_id, _| *project_id,
).map(|pair| (pair.left.title.clone(), pair.right.as_deref().cloned()));
```

`join_on` returns identities `(left_id, right_id)` and `Joined<L, R>`.
`left_join_on` returns `(left_id, Option<right_id>)` and `LeftJoined<L, R>`.
Duplicate keys produce all matching pairs; unmatched left rows have exactly one
`None` placeholder. Both sides may change in the same write, including self-joins.
Only final pair transitions are published. See the complete `joins` example.

## Aggregates and ordered views

```rust
let stats = tasks.query().group_by(|task| task.project).stats_i64(|task| task.score);
let top = db.materialize_ordered(stats.sort_by_key(|s| std::cmp::Reverse(s.sum)).take(10))?;
```

Groups expose `count()` or `stats_i64()` (count, checked i128 sum, retractable
min/max). Empty groups disappear. `distinct_values(|row| value)` emits each complete
projected value once, not a representative input row. `StatsI64::average()` returns
an approximate `Option<f64>`; None protects manually constructed empty statistics.

An `OrderedQuery` supports `take` and ordered materialization. Ordered snapshots
are in `(sort_key, id)` order. `OrderedChange` carries old/new tokens and payloads;
consumers apply each complete batch atomically without positional Move storms.
Top-k maintains the full index but compares only old/new selected windows.

`Database::with_budget(ExecutionBudget { .. })` configures per-operation limits;
`set_execution_budget` can raise limits before retrying a rejected batch. Defaults:
100,000 staged mutations, 1,000,000 emitted source/derived transitions and
10,000,000 logical work units. Zero forbids the corresponding work. `Commit.stats`
reports consumed counts. These limits do not bound payload bytes, elapsed time or
arbitrary user closure execution. See [execution contracts](docs/semantics.md).

## Audit and performance

Run `cargo bench --bench core --locked` for Criterion workloads across input size,
change count, join fanout, window size, snapshots and graph/subscription size.
`cargo bench --bench memory --locked` checks payload lifetimes over repeated full
graph construction/drop; on systems with `ps` it also prints RSS in KiB.
See [audit and measurement results](docs/audit-performance.md) for methodology,
regressions fixed, measured results and remaining limits. Benchmark dependencies
are not linked into applications that depend on the library.

Run `cargo bench --bench scheduler --locked` to compare unrelated graph width,
empty writes, pruned propagation and fully affected fanout. See the
[scheduler follow-up](docs/scheduler-performance.md) for before/after results.

Dense scheduling uses a merging heap over borrowed dependency sequences. See
[dense scheduler measurements](docs/dense-scheduler-performance.md) for comparisons
with the first affected-node scheduler and the original full-graph scheduler.

The [scheduler hot-path follow-up](docs/scheduler-path-performance.md) measures
inline queue/stage storage, including 10,000-wide layered and converging graphs.

The [layered graph investigation](docs/layered-scheduler-performance.md) includes
CPU sampling and separate measurements of queue compaction and node lookup.
