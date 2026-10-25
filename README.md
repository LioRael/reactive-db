# reactive-db

Experimental native Rust reactive collections. M0/M1 are implemented: typed
collections, atomic in-memory write batches, incremental filter/map views, and
bounded delta subscriptions. This is not yet the complete relational MVP.

Zero dependencies. Safe Rust. No JS, FFI, serde requirement, background thread,
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

Run `cargo run --example basic`. Read [semantics](docs/semantics.md),
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
are required.

Join, group/aggregate, sort/top-k, optimistic writes, persistence and UI adapters
are deliberately not implemented in M1. Nothing has been published.
