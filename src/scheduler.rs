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
                self.runs.push(Reverse(incoming));
            } else {
                self.first = Some(incoming);
            }
        }
    }
    pub fn pop_first(&mut self) -> Option<(u64, NodeId)> {
        loop {
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
            // Every new edge points forward. Equal ranks from converging runs
            // arrive together, so no persistent visited set is needed.
            if self.last != Some(next.0) {
                self.last = Some(next.0);
                return Some(next);
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
    fn inline_run_stays_allocation_free_and_merges_new_earlier_children() {
        let source = Children::from([(1, 1), (100, 100)]);
        let branch = Children::from([(2, 2), (3, 3), (100, 100)]);
        let mut ready = Queue::new();
        ready.push(&source);
        assert_eq!(ready.runs.capacity(), 0);
        assert_eq!(ready.pop_first(), Some((1, 1)));
        assert_eq!(ready.runs.capacity(), 0);
        ready.push(&branch);
        assert_eq!(ready.pop_first(), Some((2, 2)));
        assert_eq!(ready.pop_first(), Some((3, 3)));
        assert_eq!(ready.pop_first(), Some((100, 100)));
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
            assert_eq!(ready.pop_first(), Some((rank, rank as usize)));
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
            while let Some((rank, id)) = ready.pop_first() {
                actual.push((rank, id));
                if rank % 7 != 0 {
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
        assert_eq!(ready.pop_first(), Some((1, 1)));
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
        assert_eq!(ready.pop_first(), Some((10_001, 1)));
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
