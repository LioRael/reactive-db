# M0–M3 delivery status

Date: 2026-10-26. Local implementation only; not published.

## Delivered

The planned native relational core MVP is implemented in a runtime-dependency-free,
safe Rust 2024 library (minimum Rust 1.88):

- Typed collections, atomic heterogeneous writes, read-your-writes, sticky errors,
  net-change coalescing and explicit fail-stop panic behavior.
- Shared explicit plans, incremental filter/map, inner/left joins, key migration,
  duplicate matches, self-joins and chained/diamond query graphs.
- Grouped count and i64 stats (u64 count, checked i128 sum, retractable min/max),
  cached member contributions and distinct projected values with multiplicities.
- Incremental ordered indexes, stable identity ties, top-k, typed order tokens,
  token-based ordered deltas and ordered snapshots.
- Bounded shared journals, independent subscriptions, lag/resync and lease retirement.
- Per-operation input/output/work budgets, rollback on rejection and Commit.stats.
- Three runnable examples, public API docs, independent oracle models and work tests.

## Validation scope

88 tests pass: 75 unit/integration and 13 rustdoc tests, including 10 compile-fail
contracts. All M1/M2 regression tests remain enabled. Independent model coverage:

- Filter/map: 12 seeds × 500 batches, full snapshots plus delta-only mirrors.
- Joins: 16 seeds × 400 batches, inner/left and asymmetric self-inner/self-left.
- M3: 16 seeds × 400 batches, count, stats, distinct, full ordering, top-5 and
  take(0), each checked against independent full recomputation and delta replay.

The M3 oracle groups source model rows with Vecs and scans min/max/sum independently;
it sorts the full expected input with ordinary Vec sorting. It reuses no production
operator, index or selector implementation. Inputs cover duplicate values, i64
extremes, key moves, repeated writes, deletion, batch rejection and empty groups.
This is regression evidence, not exhaustive proof or a coverage percentage.

Directed tests cover join→stats→ordered composition, token changes without rank
storms, window replacement, deterministic ties, repeated take, bounded history,
oversized gaps, panic poisoning, failed initial materialization and lease cleanup.
Budget tests reject input/output/work exhaustion, ignored method failures, join
fanout and already-prepared aggregate/window changes, then retry successfully.

Work-count tests at 100 and 10,000 rows show one changed member invokes only its
group/value/sort selectors and consumes fewer than 100 logical units for stats plus
a top-5. Retraction invokes no selector. Full-sort replacement of one row among
10,000 emits one ordered Update and consumes fewer than 20 units. Existing clone
count tests for sparse writes/joins remain passing. The subsequent full audit adds
Criterion timing measurements and adversarial graph/bucket/lifetime regressions;
see [audit and performance](audit-performance.md).

Native Cargo check/test/clippy, rustfmt, warning-clean rustdoc and all three examples
pass on macOS arm64 with pinned Rust 1.99.0. Native check/test also pass with Rust
1.88.0. Rustc wrappers were bypassed invocation-locally; no toolchain installation,
global configuration change or cross-platform execution occurred. Criterion is now
a development-only benchmark dependency; runtime dependencies remain empty.

## Design choices and remaining scope

No old source, compatibility layer or vendor code was copied. Source/scheduler/
subscription state and keyed join maintenance remain independent implementations;
algorithm provenance is recorded in THIRD_PARTY.md. Group stats use sparse value
multiplicity patches, not cloned whole groups. Ordered snapshots iterate maintained
indexes rather than sorting on read.

The planned average API is made fallible as Option<f64> only to handle manually
constructed empty StatsI64 values; actual query groups are always nonempty. Exact
sum/count fields remain available. A public generic Reducer trait stays deferred.

Owner handles are !Send/!Sync, execution is synchronous, scheduling follows affected
dependency edges, and ordered tree/window comparisons add logarithmic overhead. Cloned ordered
plans share indexes; separately created windows currently own separate indexes.
Top-k retains a full input index. Budget limits count work/transitions rather than
bytes, elapsed time or cumulative retained database memory. Arbitrary user code
cannot be preempted. Payload interior mutation is untracked.

Optional persistence/SQLite, optimistic mutations, UI/diagnostic adapters and async
integration remain separate work. Offset/rank/dynamic windows, general optimizer,
recursive queries and public arbitrary reducers are not implemented.

No write operation from this implementation targeted livequery or tanstack-db-rs.
Those projects were not merged, reset or deployed. Other concurrent tasks may still
change them independently; this task does not claim global directory immutability.

The scheduler follow-up replaces full-node scheduling, publication and retirement
scans with reverse edges, a topological ready queue and lease retirement events.
See [scheduler performance](scheduler-performance.md) for regressions and measurements.

Dense propagation now merges borrowed ordered dependency runs instead of inserting
each edge into a ready tree. See [dense scheduling](dense-scheduler-performance.md)
for three-version benchmarks and retained sparse behavior.

The hot-path follow-up adds inline single-run scheduling and inline-first stage
lookup, preserving ordered source patches and independent commit order. See
[hot-path measurements](scheduler-path-performance.md).
