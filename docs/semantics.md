# M0–M3 semantics

## Identity and rows

Collection keys are unique and totally ordered. Names are unique diagnostic labels
within a Database. Map/filter preserve keys; equal projected values from distinct
keys remain distinct rows. Payloads are shared immutable Arc values. The core
never compares whole payloads and does not serialize them. Replacing a payload by
an equal value can still produce Update.

Arc does not prevent interior mutation: consumers must not mutate query-relevant
state behind Cell, Mutex, or nested shared references without a subsequent
replacement. Query closures and key comparisons must obey their Rust contracts.

## Batches

WriteBatch reads staged values before committed ones. Insert requires absence;
replace/remove require presence. Every failed batch method invalidates the batch,
including reads using a foreign handle. An explicit closure Err or invalid batch
leaves all committed rows, derived state, history and epoch unchanged.

Per source key, insert then remove is a net no-op if the key was initially absent.
Remove then insert an existing key is an Update. Repeated replacements are folded
to the final value. A net-empty batch does not advance the epoch. Heterogeneous
collections share one epoch; source and derived snapshots after write returns are
mutually committed. No application subscriber callbacks run during commit.

Filter/map use the committed old output and compute only a new/replaced payload.
Removing a cached row does not rerun its closures. Whole-table cloning is absent;
staging and output allocation are proportional to touched identities through the
active graph. The scheduler still visits active graph nodes, so per-write cost
includes O(active nodes), even when few nodes have affected input.

## Joins

Join selectors take `(&identity, &payload)` and return the same typed `Ord + Clone`
key. Both input graphs must belong to this database; recursive validation happens
before any materialization callback. A dropped or foreign right input is rejected,
even when hidden under a map or nested join.

Inner joins preserve all matched identity pairs, including equal payloads under
different identities. Left joins preserve matched pairs and one `(left_id, None)`
row only when that left row has no matches. Both sides may refer to the same plan;
self-pairs and both orientations are distinct identities. Selectors on the two
sides may differ. Option-valued join keys use Rust equality: None matches None.
This is not SQL NULL behavior.

A join-key change retracts old pairs and adds new pairs. A pair that remains valid
keeps its identity and receives Update if either operand was replaced. One batch
contains at most one final transition per pair identity. Same-batch dual updates
and cross-collection writes never expose intermediate mixed versions. Chained
joins consume the same final batch. No output changes means no journal record,
but side indexes still advance. Large fanout uses the normal bounded-journal lag
contract. Execution budgets limit per-operation work and transitions; retained
output tables are not governed by a global memory limit.

## Panics

A drop guard marks the owning Database poisoned during unwinding; the library
does not catch and resume application panics. Guards cover writes, initial query
materialization, source key lookup and snapshot key cloning. Subsequent operations
return Poisoned. Already acquired immutable data remains owned by its caller.
An allocator abort or panic=abort ends the process. A user destructor can panic
while cleanup is running; no guarantee can override Rust's double-panic abort.
Normal error atomicity is not a claim of panic-safe recovery or disk durability.

## Subscriptions

Subscribe returns a snapshot at E and a cursor after E in one noninterleavable
owner operation. Poll reads the next complete retained view batch, with the
subscription's previous watermark and the batch epoch. Gaps in database epoch
numbers are valid when other collections changed or a filter suppressed output.
Only eviction of required view history causes Lagged.

A shared per-view journal is bounded by batches and changes. Whole oldest batches
are evicted; a single oversized batch is not partially delivered. It clears
retained history and advances the loss watermark. A subscriber that has not yet
crossed that watermark must resync. A new subscriber can always snapshot the
current committed state. Poll advances its cursor at delivery, not at user ack.

Limits are chosen by materialize_with_options. A subsequent materialization of
the same explicit root with different limits returns ViewOptionsConflict.
Independent plans have independent journals. Poll is O(retained batch count) in
this initial implementation, bounded by configured capacity, not source row count.
No byte budget, asynchronous wakeup, disk history or exactly-once external
processing is promised.

## Ownership

Source runtime state belongs to Database until its drop. Handles cannot resurrect
it and report DatabaseDropped when used afterward. Derived state is kept by the
query plan, live-view or subscription leases (including downstream parent plans).
Dropping the final lease retires it at the next create/materialize/write entry;
Database drop releases all runtime state. Keeping a lazy Query clone intentionally
keeps its already-materialized nodes alive. Old snapshots may outlive the database.

A live view is eager: mutations maintain it even when no subscriber is currently
polling. There are no implicit timers or workers.

## Aggregates and distinct values

`group_by` selects an ordered group key. `count` includes duplicate contributions;
`stats_i64` maintains checked u64 count, i128 sum and i64 min/max. Groups exist only
while nonempty. Removing one duplicate extreme keeps the remaining multiplicity;
removing the last extreme finds the next indexed value. New/replaced members run
selectors once; removed members use cached contributions. Equal final aggregate
values do not emit Update even if group membership changed. Exact mean is sum/count;
`average()` uses rounded f64 casts and division, returning None for manually
constructed count-zero values. Monetary arithmetic should use the exact fields.

`distinct_values` uses the complete projected value as both identity and output.
Multiplicity changes from positive to positive emit no delta. Only first presence
and last absence emit Insert/Remove. It never picks an arbitrary source row.

## Ordered results

`sort_by_key` requires Ord keys and uses identity as a stable tie-breaker. Reverse
provides descending order. Payloads remain free of Clone/Eq/Ord bounds. `take(0)`
returns no rows; repeated takes choose the smallest limit. Ordered snapshots are
already token-sorted. Ordered batches are ordered by identity, not application
sort key: consumers must use tokens and apply each whole batch atomically.

An Update carries both previous and final OrderedRow, including tokens. A changed
sort key does not change logical identity. Window exits/entries are Remove/Insert;
unchanged retained rows receive no delta just because their rank shifted. Windows
retain a full input index. Arbitrary offset, rank, dynamic windows and automatic
order propagation through unordered transformations are not provided.

The ordered subscription has the same finite shared history, epoch, lag/resync and
lease rules as an unordered subscription. Oversized batches create explicit gaps.
Polling converts only the retained batch and never runs query selectors.

## Budgets

ExecutionBudget applies independently to each write/materialization. Defaults are
100,000 staged write operations, 1,000,000 source/derived output changes and
10,000,000 logical work units. Repeated operations count even if the final source
change coalesces away; reads are not mutation operations. Output counts include
internal graph nodes, not just subscribed roots. Work meters plan/node visits,
rows/candidates (including removed members skipped by join overlays) and bounded
window/extrema scans, excluding tree comparisons. Exact work counts are diagnostic
and may change with implementation; the configured limit is always enforced.
Initial scans use the work limit; input-change limits apply only to writes.

A zero limit forbids that work. BudgetExceeded is a normal pre-commit error;
catching a write-method budget error inside the closure still invalidates the
batch. No epoch, journal, source row or operator state publishes on rejection.
Application closure side effects cannot be undone; selectors must remain pure.
`set_execution_budget` changes subsequent limits and `Commit.stats` exposes usage.
These are not hard byte/time limits, callback preemption, cumulative database
memory limits or asynchronous yielding. Slow subscribers use independent journal
retention limits and never force a write rollback.
