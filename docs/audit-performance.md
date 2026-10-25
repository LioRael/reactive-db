# M0–M3 audit and performance measurements

Local macOS arm64 audit of all 23 library source files, public contracts and test
suites, against baseline `49c56b33d07fd086e091cd45a9e6095a6621a557`.
This is a code/behavior/performance audit, not a formal proof or security certification.

## Findings and repairs

### P1: repeated final-bucket filtering escaped the join work budget

`join/index.rs` filtered removed identities inside an iterator while the caller
charged only yielded matches. When many left rows entered a key as almost all
right members were removed, each left row rescanned the same removed members.
The old preparation cost could grow with changed-left × removed-right counts even
when final output was small; the hidden visits also escaped max_work.

Final memberships of changed buckets are now cached once per key and preparation,
using borrowed identity references. Every scanned old member is charged, including
removed ones. Unchanged buckets keep direct iteration without allocating a cache.
State/index publication remains delayed until all preparation succeeds.

A 200-right/100-left adversarial case previously performed 211,429 counted key
comparisons while reporting 801 work units. The repaired case performs 28,576
comparisons and reports 1,001 units (comparisons themselves are intentionally not
work units). A separate zero-match case that previously succeeded under a 700-unit
limit now correctly rejects without changing sources, views or epoch.

### P2: ownership validation revisited shared DAG paths exponentially

Recursive validation did not remember visited plan identities. A depth-12 diamond
with only 25 unique source/join/group nodes exhausted a 1,000-unit budget before
materialization. Validation now memoizes identities within the current operation;
every handle still checks its owner before consulting the memo. The same query
materializes within that budget. No cross-operation ownership cache is retained.

### Performance cleanup: skip redundant execution-order filtering

Retirement filtered the execution-order vector through BTreeMap lookups on every
mutable operation, even when it removed no nodes. It now filters only after a real
retirement. The existing lifecycle tests and a new repeated all-operator probe
verify actual retirement still releases state. Active-node scanning itself remains.

Three new regressions fail on the baseline and pass after repair. A fourth new
regression verifies 30 cycles of rejected preparation, all operator types, journal/
subscription leases and deliberately retained external snapshots. All 69 tests pass
(56 unit/integration, 13 rustdoc including 10 compile-fail), with the previous
18,800 generated model batches retained.

## Audit coverage

| Area | Checks and outcome |
|---|---|
| Collection / WriteBatch | Staged reads, key errors, sticky invalidation, repeated-key coalescing and ownership checked; no additional reproducible defect found. |
| Engine / Database | All source/operator preparation precedes commit; budget/arithmetic failures preserve rows, indexes, journals and epoch. Panic behavior remains fail-stop, not recovery. |
| Filter / map | Cached retractions; unchanged source rows are not reevaluated. |
| Inner / left join | Same-batch cross terms, self-joins, rekeys, placeholders, empty-output index updates and fanout checked; hidden overlay scans repaired. |
| Group / distinct | Count/sum overflow checks, duplicate extremes, final empty groups, sparse membership/value patches and unchanged output suppression checked. |
| Sort / top-k | Stable identity ties, cached old tokens, sparse index changes, bounded window merge, token delta replay and take(0) checked. |
| Journal / subscriptions | Whole-batch eviction, oversized gaps, watermark semantics, independent cursors, resync, and ordered delta conversion checked. |
| Leases / snapshots | Database drop, final derived lease, retained user snapshots and failed preparation checked with live-payload counters. |
| Dependencies / unsafe | No library runtime dependencies and unsafe remains forbidden. Criterion is development-only; no JS compatibility, native bridge or original-project code introduced. |

## Reproducible benchmark method

- Apple M2 Pro, 16 GiB RAM, macOS 27.0.1 (26A434).
- Rust 1.99.0 `b940084d7`, aarch64-apple-darwin; native Cargo, release/bench profiles.
- Criterion 0.5.1 with default features disabled. Fifty main cases, 20 samples,
  100 ms warmup, 400 ms measurement target, 10,000 bootstrap resamples per run.
- Identical harness and lockfile on an exported baseline source copy and fixed
  source. Sequential order: baseline, fixed, fixed, baseline. No compilation or
  other audit command was deliberately run concurrently with measured loops.
- Table values are the arithmetic mean of the two per-run median time estimates,
  in microseconds per operation. They are not individual-request p50/p99 latencies.
- Setup is excluded. Mutation workloads use a hot identity and alternate values;
  journals retain/evict at their normal bounds. Batch delta, join fanout, window,
  graph size, ordered delivery, snapshots and full-recompute examples are separate.
- This was a shared local computer, without CPU pinning/frequency control or a
  quiet-machine guarantee. Small differences and cross-workload comparisons need
  care. Full Vec recomputation examples omit library identity/journal costs and
  are illustrative baselines, not like-for-like external library comparisons.

| Workload | Before µs | After µs | Before / after |
|---|---:|---:|---:|
| Shared DAG, depth 12 | 23.813 | 0.677 | 35.20× |
| Shared DAG, depth 4 | 0.137 | 0.209 | 0.65× |
| Retract right bucket 2,000; 100 changing left rows | 4880.106 | 867.669 | 5.62× |
| Write with 1,000 unaffected nodes | 55.161 | 21.007 | 2.63× |
| Single filter/map update, N=100,000 | 0.825 | 0.805 | 1.02× |
| Single stats update, N=100,000 | 0.885 | 0.879 | 1.01× |
| Single ordered update, N=100,000 | 0.883 | 0.875 | 1.01× |
| Single top-10 update, N=100,000 | 1.353 | 1.345 | 1.01× |
| Snapshot, N=100,000, full suite | 386.107 | 450.952 | 0.86× |

Small graph validation is slower because bounded deduplication has a fixed cost;
there is no claim of universal speedup. The full-suite large snapshot result was
slower, despite its implementation being unchanged. An additional fresh-process
snapshot-only ABBA run (40 samples, 500 ms warmup, 2 s measurement) measured
436.682 µs before and 405.739 µs after. The direction reversal does not establish
a cause or a reliable snapshot speedup/regression; allocator/workload history,
layout and machine noise remain possible influences. Both results are retained.

The [full 50-case CSV](../benches/results/macos-m2-pro.csv) includes both individual
runs, not just the summary. [Metadata](../benches/results/metadata.json) records
source hashes and environment. Per-run bootstrap estimates are included in the
external audit deliverable JSON.

Run:

```sh
RUSTC_WRAPPER= RUSTC_WORKSPACE_WRAPPER= CARGO_PROFILE_BENCH_STRIP=none \
  cargo bench --bench core --locked -- --noplot
REACTIVE_DB_BENCH_SUITE=snapshot RUSTC_WRAPPER= RUSTC_WORKSPACE_WRAPPER= \
  CARGO_PROFILE_BENCH_STRIP=none cargo bench --bench core --locked -- --noplot
RUSTC_WRAPPER= RUSTC_WORKSPACE_WRAPPER= CARGO_PROFILE_BENCH_STRIP=none \
  cargo bench --bench memory --locked
```

The installed rust-objcopy could not locate libLLVM during an initial stripping
attempt. Final measurements set profile strip=none only for those invocations;
no persistent toolchain/configuration changes were made. Standard fmt/check/test/
clippy/doc and Rust 1.88 check/test passed; the final release test suite also passed.

## Complexity and memory interpretation

Single updates from N=100 to N=100,000 remain close to logarithmic-index/hot-key
costs, not full input scans. Join work scales with changed inputs plus affected
matching fanout and cached changed-bucket scans; removed members are now metered.
Many-to-many output size remains inherently large. Group min/max scans only
invalidated extremes; full sort changes only affected identities. Top-k scans its
selected window, so W=1,000 is much more expensive than W=10 even for one update.
Snapshots are O(result size), and UI adapters should consume deltas when appropriate.

The scheduler still visits active nodes, with ordered-map lookup overhead. Reducing
the retirement pass does not make scheduling proportional only to affected nodes.
Execution limits count logical work/transitions; they do not bound bytes, elapsed
time, arbitrary user code or cleanup costs. Graph construction/ownership/drop paths
remain recursive and are not designed for adversarial unbounded nesting.

The isolated memory probe creates 10,000 source and 10,000 projected 256-byte
payloads per cycle, with group/distinct/order/join state and subscriptions. Across
10 cycles, live payload count returned to zero after Database plus external snapshot
release. RSS after drop ranged from 32,528 to 33,008 KiB and settled near 32 MiB;
it did not return to process-start RSS. This is compatible with allocator caching,
not proof that every byte was returned to the OS. Payload Drop counts are the
release oracle, while RSS is diagnostic. Probe cycle times include `ps` overhead
and are not engine-latency measurements. [Memory CSV](../benches/results/memory.csv).

## Remaining limits

No Linux/Windows execution, long-duration soak, allocation-profiler trace,
multithread throughput, cold-cache workload, arbitrary closure preemption or
production latency guarantee is claimed. Owner-thread execution and logically
immutable/pure user data/closures remain requirements. Source tables intentionally
live until database drop; user-owned snapshots/deltas can intentionally retain
payloads independently of bounded journals.
