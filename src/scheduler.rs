use crate::Error;
use crate::budget::Meter;
use crate::engine::NodeId;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) type Queue = BTreeSet<(u64, NodeId)>;

#[derive(Default)]
pub(crate) struct Scheduler {
    entries: BTreeMap<NodeId, Entry>,
    next_rank: u64,
}
struct Entry {
    rank: u64,
    parents: Vec<NodeId>,
    children: Queue,
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
                children: Queue::new(),
            },
        );
    }
    pub fn propagate(&self, id: NodeId, ready: &mut Queue, meter: &mut Meter) -> Result<(), Error> {
        for child in &self
            .entries
            .get(&id)
            .expect("registered node has dependents")
            .children
        {
            // Bound queue growth as well as node evaluation, including converging edges.
            meter.work()?;
            ready.insert(*child);
        }
        Ok(())
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
