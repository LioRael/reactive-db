# Changelog

## 0.1.0-alpha.1 — 2026-10-27

Initial experimental release of the native Rust relational core.

- Typed collections and atomic heterogeneous write batches.
- Incremental filter/map, inner/left joins, grouped count/stats, distinct, sort/top-k.
- Snapshot plus delta subscriptions, ordered deltas, bounded journals and lag recovery.
- Sparse dependency propagation, execution budgets and rollback on normal errors.
- Stable-key single-left-row left joins reuse existing matches without repeated input lookups.
- Independent recomputation models, lifecycle and budget regressions, and performance harnesses.

Zero runtime dependencies; safe Rust; minimum Rust 1.88. The database and handles
are single-thread-owned and synchronous. Public APIs are experimental. Persistence,
SQLite, optimistic mutations, UI adapters, general reducers and recursive queries
are not included. See README.md and docs/semantics.md for execution contracts.
