# M0–M2 delivery status

Date: 2026-10-26. Local implementation only; not published.

## Delivered

- Independent Git project; dependency-free Rust 2024 library, minimum Rust 1.88.
- Typed collections; payloads require neither Clone, Ord nor Serialize.
- Heterogeneous atomic write batches, read-your-writes, sticky failure, net-change
  coalescing and a fail-stop panic policy.
- Explicit shared query plans, incremental filter/map, cached removals, lease retirement.
- Incremental inner/left joins with typed selectors, duplicate-key multiplicity,
  composite pair identities, key migration, self-joins and chained/diamond graphs.
- Sparse staged side indexes, including updates with no visible output. Both sides
  reconcile against final batch state before atomic cross-collection publication.
- Immutable snapshots, typed delta batches, independent bounded cursors, lag/resync.
- Public docs, two runnable examples, compile-fail contracts and independent model tests.

## Validation scope

48 tests pass: 39 unit/integration tests and 9 rustdoc tests (7 compile-fail).
The original filter/map oracle covers 12 seeds × 500 batches. The join oracle
covers 16 seeds × 400 batches, checking four views after every batch: inner,
left, asymmetric self-inner and asymmetric self-left. Each compares both the
materialized snapshot and a delta-only replay mirror with independent nested-loop
full recomputation. Histories include repeated writes, rekeys, removals, duplicate
keys, dual-side mutations and rejected whole batches. This is regression evidence,
not exhaustive proof or a coverage percentage.

Ten directed join tests cover atomic source/join epochs, final before/after
payloads, first/last-match placeholders, simultaneous key moves, same-batch match
replacement, unmatched index maintenance, ownership rejection, self-joins and
nested/shared graph propagation. Additional tests check panic poisoning and lease
retirement of both derived parents and captured selectors.

A join work-count regression compares 100 and 10,000 right rows under one matching
key. Replacing one right row calls only its selector and one downstream projection,
with fewer than 60 identity clones in either case. Removal reruns neither selector
nor projection. This protects incremental work bounds without hardware-sensitive
timing assertions. Large required output fanout still costs proportional work.

Native Cargo check/test/clippy, rustfmt, warning-clean rustdoc and both examples
pass on macOS arm64 using pinned Rust 1.99.0. Check/test also pass on minimum Rust
1.88.0. No toolchain installation, external runtime or new dependency is required.
No cross-platform execution or broad throughput/latency benchmark is claimed.

## Implementation decisions

M1 rewrote source/scheduler/subscription representations instead of copying the
old Ord-on-payload bags or callback delivery. M2 likewise implements sparse keyed
join reconciliation independently; no original source or compatibility modules
were copied. Algorithm/test provenance remains in THIRD_PARTY.md.

Keyed before/after transitions and final side overlays cover join cross terms
without introducing weighted FactId/version infrastructure. Pair identities are
deduplicated before comparison against final operands. This is a deliberate
simplification for keyed relational inputs, not full-query recomputation.

Arc payloads and !Send owner handles follow the accepted design. No JS, TS, FFI,
serde, async runtime, SQLite or diagnostic transport is introduced.

## Remaining milestones and limits

M3: group/aggregate, correct distinct, ordered views/top-k, ordered deltas and
explicit execution budgets. These complete the requested relational core MVP.
Optimistic writes, SQLite persistence and UI/livequery adapters follow separately.

Current limits: eager single-thread graph; O(active nodes) scheduling; O(result
size) snapshot construction; O(retained history) cursor lookup; count-based rather
than byte-based journal budgets. Many-to-many output growth and batch preparation
are unbounded in M2. Sources and retained plans hold data for their lifetimes;
interior payload mutation is untracked.

No write operation from this implementation targeted livequery or tanstack-db-rs.
The tanstack-db-rs fingerprint remained unchanged. The livequery fingerprint
shows concurrent workspace restructuring outside this task; it was neither
incorporated nor reverted, so global immutability of that directory is not claimed.
