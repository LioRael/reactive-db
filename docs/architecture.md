# M0–M3 implementation architecture

This file describes the implemented relational core and its synchronous publication contract.

- `database.rs`: unique owner, instance checks, graph registration, lease retirement,
  synchronous write preparation and publication.
- `write_batch.rs`: heterogeneous sparse source patches with read-your-writes and
  sticky invalidation. Source payload types are checked at the public interface.
- `query.rs`: typed lazy source/linear plans and recursive owner validation. Clones share a plan token; parent-first
  construction creates a topological order. Initial construction runs once per
  explicitly shared node.
- `engine.rs`: private erased node storage with checked Any downcasts, typed row
  state, and cached-output incremental filter/map evaluators. No unsafe cast or
  dynamic JSON value model is used.
- `join/`: typed two-parent plans, sparse side indexes, pair reconciliation and
  inner/left output types.
- `aggregate.rs`: cached member contributions, sparse per-group count/sum/value
  multiplicities, count/stats/distinct projections.
- `ordered/`: typed order tokens, ordered index, selected windows and ordered
  snapshot/subscription facade.
- `budget.rs`: shared per-operation logical work, input and output meters.
- `view.rs`, `subscription.rs`, `journal.rs`: immutable snapshots, bounded shared
  delta history, cursor initialization, lag detection and resync.
- `change.rs`: typed row transitions and database-local epochs.

## Publication

1. Validate source operations into sparse key→optional Arc payload patches.
2. Convert patches into source before/after transitions, removing net no-ops.
3. Walk the registered parent-first graph. Nodes with incoming changes compute
   typed pending output without changing committed rows or operator state.
4. Once preparation succeeds, apply all pending state/journal changes and advance
   epoch. Ordinary Result failures cannot occur during this final phase.
5. Consumers read snapshots/deltas after the exclusive owner call returns.

BTreeMap mutations and destructors in the final phase may still panic. A panic
poisons the Database; there is no recovery claim. Allocation exhaustion may abort.

## Indexed joins

Each side stores identity → (cached join key, Arc payload), plus join key → ordered
identity set. A batch stages sparse index removals/additions and final row entries;
it never clones a complete side, bucket or output table. Removals reuse cached
keys. Only inserted/replaced rows invoke selectors.

For each changed left identity, collect pairs from its old right bucket and its
final right bucket. For each changed right identity, do the symmetric lookup.
Deduplicate candidate pair identities, then compare committed output against the
final overlays on both sides. This includes simultaneous insert/update cross terms
exactly once, including self-joins. It is equivalent to keyed differential join
maintenance without a public weighted bag or FactId/version layer. Unchanged
operand Arc identities reuse cached output and do not publish redundant updates.

Left joins additionally consider a placeholder for every affected left identity.
Its final right bucket cardinality decides whether it exists. Cardinality checks
use old/removed/added set sizes rather than scanning the whole bucket. A last-match
removal or first-match insertion publishes the placeholder transition together
with matched-pair transitions. Same-batch replacement of a match never exposes a
transient placeholder.

Prepared output and operator-maintenance patches are separate. Even when a join
produces no output delta, changed unmatched rows must commit their indexes for a
later matching write. Downstream nodes consume final pending output in topological
order; all source, index, output and journal changes commit under one epoch.

Initial construction costs input indexing plus full result construction. Updates
visit changed inputs and their matching old/final fanout, with ordered-map/set
lookup and deduplication overhead. Many-to-many fanout can be quadratic in input
size; work/output limits stop oversized preparation before publication. Scheduler overhead remains O(active nodes).

## Ownership and implementation choices

Runtime nodes are owned by Database. A source owns a strong allocation token;
derived runtime nodes retain only a weak token while plans/downstream plans own
strong tokens. Node lookup uses the private allocation address while the allocation
is retained by strong or weak references, so old IDs cannot alias live allocations.
IDs never escape the public interface, and no slot is reused through an unchecked
cast. Retirement removes dead derived nodes before the next mutable operation.

The node store may erase types internally to own heterogeneous collections; only
typed constructors can register and resolve them. An internal downcast failure is
an invariant panic, not a runtime query type-conversion error.

Arc<T> in the accepted data interface is deliberate shared snapshot ownership;
Database and handles use Rc internally to enforce owner-thread execution. These
are explicit design choices, not accidental Clone/Send requirements on user rows.

Future work should extend operator-specific staged state rather than reverting to
whole-table clone-on-write. It should not introduce JS opcode dispatch, upstream
runtime identity or a public generic backend plugin framework.

## Groups and distinct

The member cache stores only identity → (group key, i64 contribution). A group
stores count, checked i128 sum and an ordered value → multiplicity map. Preparation
accumulates sparse signed count/sum/value differences for touched groups. It then
resolves final statistics against the committed state, skipping only retracted
extreme values when finding min/max. Untouched groups and member payloads are not
cloned or rescanned. A count/distinct query uses zero-valued contributions.
Distinct projects the group identity itself and suppresses unchanged multiplicity
updates; statistics/count suppress equal output values. Group membership patches
still commit when visible output is unchanged.

## Ordered state and top-k

An identity → old token cache and `(sort_key, identity)` → Arc payload index allow
sparse retractions and additions. Full-sort writes compare only touched identities.
Top-k merges the committed sorted index (excluding staged removals) with sorted
staged additions, stopping at W rows. Only old/new window identities are compared.
There is no full-input snapshot or sorted Vec rebuild during writes. Tree lookup
and window-map/dedup comparisons add logarithmic factors; logical visited rows are
bounded by changed inputs plus W, excluding unavoidable graph scheduling.

Full snapshots iterate the existing index; top-k snapshots stop at W. Snapshot
construction is O(result size). Ordered journals use typed cached OrderedRow
transitions internally; poll converts just the delivered batch into OrderedChange,
cloning keys and Arc references without evaluating selectors. Identity tie-breaks
make equal sort keys deterministic. `take` creates a new plan identity; clones of
the same ordered plan share one index, while separately derived windows currently
own separate indexes. Ordering is terminal: no implicit unordered join/group API
is exposed on OrderedQuery.

## Execution budgets and failure atomicity

Preparation and plan construction now return Result. One meter spans a write or
materialization, including nested plans. Inputs count successful staged mutation
operations before coalescing. Outputs count source/derived transitions (or initial
constructed result rows). Work counts logical plan/node/row visits and join
candidates, plus aggregate extrema and window work; it does not count BTree
comparisons, cleanup, arbitrary callbacks or payload bytes.

Limits are checked incrementally while producing candidates/results. Failed
materialization discards temporary nodes without registering a partial graph.
Failed writes discard every source/operator patch, retain journals/epoch and can
be retried. Raising a budget does not execute or replay a previously failed batch.
Existing materialized view reuse does not reconstruct or charge output rows.
Panic poisoning remains separate from normal budget/arithmetic rejection.
