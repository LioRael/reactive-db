use crate::engine::NodeId;
use std::any::Any;
use std::collections::HashMap;

/// Per-write lookup only; execution and publication order live in the scheduler.
/// The first stage is commonly the single source shared by a wide fanout.
#[derive(Default)]
pub(crate) struct Stages {
    first: Option<(NodeId, Box<dyn Any>)>,
    rest: HashMap<NodeId, Box<dyn Any>>,
}
impl Stages {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn get(&self, id: &NodeId) -> Option<&dyn Any> {
        if let Some((key, value)) = &self.first
            && key == id
        {
            return Some(value.as_ref());
        }
        self.rest.get(id).map(Box::as_ref)
    }
    pub fn insert(&mut self, id: NodeId, value: Box<dyn Any>) {
        if let Some((key, first)) = &mut self.first {
            if *key == id {
                *first = value;
            } else {
                self.rest.insert(id, value);
            }
        } else if let Some(existing) = self.rest.get_mut(&id) {
            *existing = value;
        } else {
            self.first = Some((id, value));
        }
    }
    pub fn remove(&mut self, id: &NodeId) -> Option<Box<dyn Any>> {
        if self.first.as_ref().is_some_and(|(key, _)| key == id) {
            return self.first.take().map(|(_, value)| value);
        }
        self.rest.remove(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::rc::Rc;

    #[test]
    fn sparse_stage_avoids_table_allocation_and_releases_replacements() {
        let mut stages = Stages::new();
        let value = Rc::new(String::from("retained"));
        let weak = Rc::downgrade(&value);
        stages.insert(1, Box::new(value));
        assert_eq!(stages.rest.capacity(), 0);
        stages.insert(1, Box::new(17_u64));
        assert!(weak.upgrade().is_none());
        assert_eq!(stages.get(&1).unwrap().downcast_ref::<u64>(), Some(&17));
        assert_eq!(stages.rest.capacity(), 0);
        let other = Rc::new(());
        let weak = Rc::downgrade(&other);
        stages.insert(2, Box::new(other));
        drop(stages);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn mixed_removal_reinsertion_and_replacement_match_independent_map() {
        for seed in 1..=16_u64 {
            let mut rng = seed;
            let mut actual = Stages::new();
            let mut model = BTreeMap::new();
            for step in 0..500_u64 {
                rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                let key = (rng >> 32) as usize % 20;
                if rng % 3 == 0 {
                    let removed = actual.remove(&key).map(|v| *v.downcast::<u64>().unwrap());
                    assert_eq!(removed, model.remove(&key));
                } else {
                    actual.insert(key, Box::new(step));
                    model.insert(key, step);
                }
                for key in 0..20 {
                    assert_eq!(
                        actual.get(&key).map(|v| *v.downcast_ref::<u64>().unwrap()),
                        model.get(&key).copied(),
                        "seed={seed}, step={step}, key={key}"
                    );
                }
            }
        }
    }
}
