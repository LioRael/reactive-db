# M0/M1 delivery status

Date: 2026-10-26. Local implementation only; not published.

## Delivered

- Independent, initialized Git project; one dependency-free library crate, Rust 2024.
- Rust 1.88 minimum verified locally; development toolchain pinned to 1.99.0.
- Typed collections with non-Clone/non-Ord/non-Serialize payloads.
- Heterogeneous write batches, read-your-writes, key validation, sticky failure,
  net-change coalescing, one atomic publication epoch and fail-stop panic policy.
- Lazy explicit query plans, shared-node materialization, incremental filter/map,
  cached removals and lease-based retirement.
- Immutable snapshots, typed Insert/Update/Remove batches, independent cursors,
  finite shared history, oversized-batch gaps and atomic resync.
- Public docs, runnable example, compile-pass/fail consumers, independent model
  tests and operation-count regression tests.

## Validation scope

The suite contains 32 tests: 25 unit/integration tests plus 7 rustdoc tests
(including 6 compile-fail contracts). Model equivalence covers 12 deterministic
seeds × 500 batches, comparing both materialized snapshots and delta-replayed
mirrors with independent full recomputation. These are generated regression cases,
not an exhaustive proof or a coverage percentage.

Native Cargo check/test/clippy, rustfmt, warning-clean rustdoc and the basic
example passed on local macOS arm64 with Rust 1.99.0. Cargo check/test also passed
with Rust 1.88.0. No additional toolchain installation was needed. No Linux/Windows
execution or broad throughput/latency benchmark is claimed.

The work-count test compares single-row updates at 100 and 10,000 rows: one map
invocation and fewer than 20 identity clones in either case. It deliberately
avoids hardware-dependent timing assertions. Another test proves that filter/map
closures do not execute for cached removals.

## Implementation decisions versus the larger design

M1 rewrites the source/scheduler/subscription representation instead of retaining
old Ord-on-payload bags or callback delivery. No old source or compatibility module
was copied. Algorithm/test lessons are recorded in THIRD_PARTY.md. For unary
M1 operators, keyed before/after changes suffice; general weighted join FactId
versioning is deferred until M2 rather than represented by an unused abstraction.

Arc payloads and !Send owner handles follow the accepted design intentionally.
SubscriptionStart is a public alias for the existing `(Snapshot, Subscription)`
return shape. There is no API stub that pretends join, sorting or persistence is
implemented.

## Remaining milestones

M2: transactional incremental inner/left join, including same-batch cross terms,
join-key changes and self-joins. M3: group/aggregate, correct distinct, ordered
views/top-k, ordered deltas and explicit execution budgets. Together those reach
the complete requested relational core MVP. Optional optimistic, SQLite and UI /
livequery diagnostic adapters follow afterward.

Current limits: eager single-threaded unary graph; O(active nodes) scheduling;
O(result size) snapshot construction; O(retained history) cursor lookup; row-change
counts rather than byte budgets. Source tables and retained query plans intentionally
hold data until their owning lifetimes end. Payload interior mutation is untracked.

This implementation performed no writes to the original livequery or tanstack-db-rs
directories. The tanstack-db-rs source/config/document fingerprint comparison was
unchanged. The livequery comparison detected concurrent additions and edits from
outside this implementation; those changes were not reverted or incorporated.
Therefore no claim is made that the livequery directory stayed globally unchanged.
