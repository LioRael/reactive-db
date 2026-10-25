# M0/M1 implementation architecture

The accepted full design includes future join/aggregate/order operators. This file
describes the implemented slice only, not those planned capabilities.

- `database.rs`: unique owner, instance checks, graph registration, lease retirement,
  synchronous write preparation and publication.
- `write_batch.rs`: heterogeneous sparse source patches with read-your-writes and
  sticky invalidation. Source payload types are checked at the public interface.
- `query.rs`: typed lazy source/linear plans. Clones share a plan token; parent-first
  construction creates a topological order. Initial construction runs once per
  explicitly shared node.
- `engine.rs`: private erased node storage with checked Any downcasts, typed row
  state, and cached-output incremental filter/map evaluators. No unsafe cast or
  dynamic JSON value model is used.
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

The M1 graph is unary and keyed, so it does not yet need the general weighted
FactId/version representation proposed for joins. Source before/after values and
cached output identities suffice without requiring T: Ord. M2 must implement
join staging and the cross term; this release does not claim that work is done.

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

Future work should add operator-specific staged state rather than reverting to
whole-table clone-on-write. It should not introduce JS opcode dispatch, upstream
runtime identity or a public generic backend plugin framework.
