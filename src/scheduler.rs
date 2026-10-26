use crate::Error;
use crate::budget::Meter;
use crate::engine::NodeId;
use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeSet, BinaryHeap, HashMap, btree_set};

type Children = BTreeSet<(u64, NodeId)>;

#[derive(Default)]
pub(crate) struct Queue<'a> {
    first: Option<Run<'a>>,
    runs: BinaryHeap<Reverse<Run<'a>>>,
    singles: BinaryHeap<Reverse<(u64, NodeId)>>,
    last: Option<u64>,
}
struct Run<'a> {
    next: (u64, NodeId),
    rest: btree_set::Iter<'a, (u64, NodeId)>,
}
impl PartialEq for Run<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.next == other.next
    }
}
impl Eq for Run<'_> {}
impl PartialOrd for Run<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Run<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.next.cmp(&other.next)
    }
}
impl<'a> Queue<'a> {
    pub fn new() -> Self {
        Self::default()
    }
    fn push(&mut self, children: &'a Children) {
        let mut rest = children.iter();
        if let Some(&next) = rest.next() {
            let mut incoming = Run { next, rest };
            if let Some(first) = &mut self.first {
                if incoming.next < first.next {
                    std::mem::swap(first, &mut incoming);
                }
                if incoming.rest.len() == 0 {
                    self.singles.push(Reverse(incoming.next));
                } else {
                    self.runs.push(Reverse(incoming));
                }
            } else {
                self.first = Some(incoming);
            }
        }
    }
    #[inline]
    pub fn pop_first(&mut self) -> Option<NodeId> {
        if let Some(first) = &self.first {
            if first.rest.len() == 0 && self.runs.is_empty() && self.singles.is_empty() {
                let (rank, id) = first.next;
                self.first = None;
                if self.last == Some(rank) {
                    return None;
                }
                self.last = Some(rank);
                return Some(id);
            }
        } else if self.singles.is_empty() {
            return None;
        }
        self.pop_merged()
    }
    fn pop_merged(&mut self) -> Option<NodeId> {
        loop {
            let next = if self.singles.peek().is_some_and(|single| {
                self.first
                    .as_ref()
                    .is_none_or(|first| single.0 < first.next)
            }) {
                self.singles.pop().expect("a singleton head was present").0
            } else {
                let first = self.first.as_mut()?;
                let next = first.next;
                if let Some(&following) = first.rest.next() {
                    first.next = following;
                    if let Some(mut head) = self.runs.peek_mut()
                        && head.0.next < first.next
                    {
                        std::mem::swap(first, &mut head.0);
                    }
                } else {
                    self.first = self.runs.pop().map(|run| run.0);
                }
                next
            };
            // Every new edge points forward. Equal ranks from converging runs
            // arrive together, so no persistent visited set is needed.
            if self.last != Some(next.0) {
                self.last = Some(next.0);
                return Some(next.1);
            }
        }
    }
}

#[derive(Default)]
pub(crate) struct Scheduler {
    entries: HashMap<NodeId, Entry>,
    next_rank: u64,
}
struct Entry {
    rank: u64,
    parents: Vec<NodeId>,
    children: Children,
}
impl Scheduler {
    pub fn reserve(&self, count: usize) -> Result<(), Error> {
        self.next_rank
            .checked_add(u64::try_from(count).map_err(|_| Error::ArithmeticOverflow)?)
            .ok_or(Error::ArithmeticOverflow)?;
        Ok(())
    }
    // Callers preflight capacity and register in parent-first build order.
    pub fn register(&mut self, id: NodeId, parents: Vec<NodeId>) {
        let rank = self.next_rank;
        self.next_rank += 1;
        for parent in &parents {
            self.entries
                .get_mut(parent)
                .expect("parent precedes child")
                .children
                .insert((rank, id));
        }
        self.entries.insert(
            id,
            Entry {
                rank,
                parents,
                children: Children::new(),
            },
        );
    }
    pub fn propagate<'a>(
        &'a self,
        id: NodeId,
        ready: &mut Queue<'a>,
        meter: &mut Meter,
    ) -> Result<(), Error> {
        let children = &self
            .entries
            .get(&id)
            .expect("registered node has dependents")
            .children;
        // Charge every edge before borrowing a run into the queue. No per-edge
        // allocation or reinsertion is needed: children are already sorted.
        meter.work_many(children.len())?;
        ready.push(children);
        Ok(())
    }
    pub fn reclaim(&mut self) {
        // Lookup order is irrelevant; ranks and child sets define execution order.
        // Shrink only after substantial retirement to avoid resize churn while
        // releasing peak metadata capacity when a large graph is dropped.
        if self.entries.capacity() > self.entries.len().saturating_mul(4).max(16) {
            self.entries.shrink_to_fit();
        }
    }
    pub fn remove(&mut self, id: NodeId) {
        if let Some(entry) = self.entries.remove(&id) {
            for parent in entry.parents {
                if let Some(parent) = self.entries.get_mut(&parent) {
                    parent.children.remove(&(entry.rank, id));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_ids_are_returned_without_truncation_or_rank_confusion() {
        let source = Children::from([(u64::MAX - 2, usize::MAX), (u64::MAX - 1, 0), (u64::MAX, 7)]);
        let duplicate = Children::from([(u64::MAX - 1, 0), (u64::MAX, 7)]);
        let mut ready = Queue::new();
        ready.push(&source);
        ready.push(&duplicate);
        assert_eq!(ready.pop_first(), Some(usize::MAX));
        assert_eq!(ready.pop_first(), Some(0));
        assert_eq!(ready.pop_first(), Some(7));
        assert_eq!(ready.pop_first(), None);
    }

    #[test]
    fn converging_runs_can_exhaust_at_different_duplicate_heads() {
        let a = Children::from([(1, 101), (2, 102)]);
        let b = Children::from([(1, 101), (2, 102), (3, 103)]);
        let c = Children::from([(1, 101), (3, 103)]);
        let mut ready = Queue::new();
        for children in [&a, &b, &c] {
            ready.push(children);
        }
        for id in [101, 102, 103] {
            assert_eq!(ready.pop_first(), Some(id));
        }
        assert_eq!(ready.pop_first(), None);
        // A subsequent forward run must work after both the merged and fast
        // single-item paths have consumed their tails.
        let tail = Children::from([(4, 104)]);
        ready.push(&tail);
        assert_eq!(ready.pop_first(), Some(104));
        assert_eq!(ready.pop_first(), None);
    }

    #[test]
    fn inline_run_stays_allocation_free_and_merges_new_earlier_children() {
        let source = Children::from([(1, 1), (100, 100)]);
        let branch = Children::from([(2, 2), (3, 3), (100, 100)]);
        let mut ready = Queue::new();
        ready.push(&source);
        assert_eq!(ready.runs.capacity(), 0);
        assert_eq!(ready.pop_first(), Some(1));
        assert_eq!(ready.runs.capacity(), 0);
        ready.push(&branch);
        assert_eq!(ready.pop_first(), Some(2));
        assert_eq!(ready.pop_first(), Some(3));
        assert_eq!(ready.pop_first(), Some(100));
        assert!(ready.pop_first().is_none());
    }

    #[test]
    fn singleton_frontier_keeps_only_compact_heads_and_merges_with_runs() {
        let source = Children::from([(1, 1), (10, 10)]);
        let a = Children::from([(20, 20)]);
        let duplicate = Children::from([(10, 10)]);
        let branch = Children::from([(3, 3), (11, 11)]);
        let mut ready = Queue::new();
        ready.push(&source);
        assert_eq!(ready.pop_first(), Some(1));
        ready.push(&a);
        ready.push(&duplicate);
        ready.push(&branch);
        assert!(
            ready.runs.is_empty(),
            "exhausted tails need no tree iterator in the heap"
        );
        assert_eq!(ready.singles.len(), 3);
        let mut actual = Vec::new();
        while let Some(value) = ready.pop_first() {
            actual.push(value);
        }
        assert_eq!(actual, vec![3, 10, 11, 20]);
    }

    #[test]
    fn a_wide_singleton_layer_never_allocates_a_heap_of_tree_iterators() {
        let roots: Children = (1..=1000).map(|rank| (rank, rank as usize)).collect();
        let leaves: Vec<Children> = (1001..=2000)
            .map(|rank| Children::from([(rank, rank as usize)]))
            .collect();
        let mut ready = Queue::new();
        ready.push(&roots);
        for rank in 1..=1000 {
            assert_eq!(ready.pop_first(), Some(rank as usize));
            ready.push(&leaves[rank as usize - 1]);
        }
        assert_eq!(ready.runs.capacity(), 0);
        assert_eq!(ready.singles.len(), 999);
        for rank in 1001..=2000 {
            assert_eq!(ready.pop_first(), Some(rank as usize));
        }
        assert!(ready.pop_first().is_none());
    }

    #[test]
    fn wide_fanout_borrows_one_run_and_deduplicates_shared_runs() {
        let children: Children = (1..=10_000).map(|rank| (rank, rank as usize)).collect();
        let mut ready = Queue::new();
        ready.push(&children);
        ready.push(&children);
        assert_eq!(ready.runs.len(), 1, "queue storage follows runs, not edges");
        for rank in 1..=10_000 {
            assert_eq!(ready.pop_first(), Some(rank as usize));
        }
        assert!(ready.pop_first().is_none());
    }

    #[test]
    fn dynamically_merged_runs_match_independent_topological_reachability() {
        for seed in 1..=32_u64 {
            let mut rng = seed;
            let mut scheduler = Scheduler::default();
            let mut parents = vec![Vec::new(); 200];
            scheduler.reserve(parents.len()).unwrap();
            for (rank, incoming) in parents.iter_mut().enumerate() {
                if rank >= 3 {
                    rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                    incoming.push((rng as usize) % rank);
                    incoming.push(rank - 1);
                }
                scheduler.register(1000 - rank, incoming.iter().map(|p| 1000 - p).collect());
            }
            let mut active = [false; 200];
            let mut expected = Vec::new();
            for rank in 0..200 {
                let reached = rank < 3 || parents[rank].iter().any(|p| active[*p]);
                active[rank] = reached && (rank < 3 || rank % 7 != 0);
                if reached && rank >= 3 {
                    expected.push((rank as u64, 1000 - rank));
                }
            }
            let mut ready = Queue::new();
            let mut meter = Meter::new(crate::ExecutionBudget::default());
            for source in 0..3 {
                scheduler
                    .propagate(1000 - source, &mut ready, &mut meter)
                    .unwrap();
            }
            let mut actual = Vec::new();
            while let Some(id) = ready.pop_first() {
                let rank = (1000 - id) as u64;
                actual.push((rank, id));
                if !rank.is_multiple_of(7) {
                    scheduler.propagate(id, &mut ready, &mut meter).unwrap();
                }
            }
            assert_eq!(actual, expected, "seed {seed}");
        }
    }

    #[test]
    fn rejected_run_is_not_queued_and_a_new_queue_can_retry() {
        let mut scheduler = Scheduler::default();
        scheduler.reserve(101).unwrap();
        scheduler.register(0, Vec::new());
        for id in 1..=100 {
            scheduler.register(id, vec![0]);
        }
        let mut ready = Queue::new();
        let mut meter = Meter::new(crate::ExecutionBudget {
            max_work: 99,
            ..Default::default()
        });
        assert_eq!(
            scheduler.propagate(0, &mut ready, &mut meter),
            Err(Error::BudgetExceeded("work"))
        );
        assert!(ready.pop_first().is_none());
        let mut meter = Meter::new(crate::ExecutionBudget {
            max_work: 100,
            ..Default::default()
        });
        scheduler.propagate(0, &mut ready, &mut meter).unwrap();
        assert_eq!(meter.stats.work, 100);
        assert_eq!(ready.pop_first(), Some(1));
        drop(ready);
        scheduler.remove(100);
        let mut ready = Queue::new();
        let mut meter = Meter::new(crate::ExecutionBudget::default());
        scheduler.propagate(0, &mut ready, &mut meter).unwrap();
        let mut count = 0;
        while ready.pop_first().is_some() {
            count += 1;
        }
        assert_eq!(count, 99);
    }

    #[test]
    fn retired_graph_releases_peak_lookup_capacity_and_reverse_edges() {
        let mut scheduler = Scheduler::default();
        scheduler.reserve(10_001).unwrap();
        scheduler.register(0, Vec::new());
        for id in 1..=10_000 {
            scheduler.register(id, vec![0]);
        }
        assert!(scheduler.entries.capacity() >= 10_001);
        for id in 1..=10_000 {
            scheduler.remove(id);
        }
        scheduler.reclaim();
        assert!(scheduler.entries.capacity() <= 16);
        assert!(scheduler.entries[&0].children.is_empty());
        // Repeated registration after reclamation still uses fresh ranks.
        scheduler.reserve(1).unwrap();
        scheduler.register(1, vec![0]);
        let mut ready = Queue::new();
        let mut meter = Meter::new(crate::ExecutionBudget::default());
        scheduler.propagate(0, &mut ready, &mut meter).unwrap();
        assert_eq!(ready.pop_first(), Some(1));
    }

    #[test]
    fn exhausted_rank_reservation_leaves_graph_unchanged() {
        let mut scheduler = Scheduler {
            next_rank: u64::MAX - 1,
            ..Default::default()
        };
        scheduler.reserve(1).unwrap();
        scheduler.register(7, Vec::new());
        assert_eq!(scheduler.reserve(1), Err(Error::ArithmeticOverflow));
        assert!(scheduler.reserve(0).is_ok());
        assert_eq!(scheduler.entries[&7].rank, u64::MAX - 1);
        scheduler.remove(7);
        assert!(scheduler.entries.is_empty());
        assert_eq!(scheduler.reserve(1), Err(Error::ArithmeticOverflow));
    }
}
