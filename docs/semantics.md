# M0/M1 semantics

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
