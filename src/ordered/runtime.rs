use super::plan::Selector;
use super::types::{OrderKey, OrderedRow};
use crate::budget::Meter;
use crate::engine::Token;
use crate::engine::{Node, NodeId, Pending, Prepared, Stages, State};
use crate::{Change, Epoch, Error};
use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Weak;
use std::sync::Arc;

type Selected<I, T, S> = BTreeMap<I, Arc<OrderedRow<I, T, S>>>;
type Index<I, T, S> = BTreeMap<OrderKey<S, I>, Arc<T>>;
struct Patch<I, T, S> {
    keys: BTreeMap<I, Option<OrderKey<S, I>>>,
    removed: BTreeSet<OrderKey<S, I>>,
    added: Index<I, T, S>,
}
pub(super) struct OrderNode<I, T, S> {
    state: State<I, OrderedRow<I, T, S>>,
    token: Weak<Token>,
    parent: NodeId,
    selector: Selector<T, S>,
    limit: Option<usize>,
    keys: BTreeMap<I, OrderKey<S, I>>,
    index: Index<I, T, S>,
}
fn row<I: Clone, T, S: Clone>(key: &OrderKey<S, I>, value: &Arc<T>) -> Arc<OrderedRow<I, T, S>> {
    Arc::new(OrderedRow {
        id: key.id.clone(),
        value: value.clone(),
        order: key.clone(),
    })
}
impl<I: Ord + Clone + 'static, T: 'static, S: Ord + Clone + 'static> OrderNode<I, T, S> {
    pub fn new(
        token: Weak<Token>,
        parent: NodeId,
        selector: Selector<T, S>,
        limit: Option<usize>,
        keys: BTreeMap<I, OrderKey<S, I>>,
        index: Index<I, T, S>,
        meter: &mut Meter,
    ) -> Result<Self, Error> {
        let mut rows = BTreeMap::new();
        for (key, value) in index.iter().take(limit.unwrap_or(usize::MAX)) {
            meter.work()?;
            meter.output()?;
            rows.insert(key.id.clone(), row(key, value));
        }
        Ok(Self {
            state: State::new(rows),
            token,
            parent,
            selector,
            limit,
            keys,
            index,
        })
    }
    fn selected(
        &self,
        patch: &Patch<I, T, S>,
        limit: usize,
        meter: &mut Meter,
    ) -> Result<Selected<I, T, S>, Error> {
        let mut old = self.index.iter().peekable();
        let mut added = patch.added.iter().peekable();
        let mut selected = BTreeMap::new();
        while selected.len() < limit {
            while old.peek().is_some_and(|(k, _)| patch.removed.contains(*k)) {
                meter.work()?;
                old.next();
            }
            let next = match (old.peek(), added.peek()) {
                (Some((a, _)), Some((b, _))) if a < b => old.next(),
                (_, Some(_)) => added.next(),
                (Some(_), None) => old.next(),
                (None, None) => break,
            };
            let (key, value) = next.expect("peeked ordered entry exists");
            meter.work()?;
            selected.insert(key.id.clone(), self.reuse(key, value));
        }
        Ok(selected)
    }
    fn reuse(&self, key: &OrderKey<S, I>, value: &Arc<T>) -> Arc<OrderedRow<I, T, S>> {
        if let Some(old) = self.state.rows.get(&key.id)
            && old.order == *key
            && Arc::ptr_eq(&old.value, value)
        {
            old.clone()
        } else {
            row(key, value)
        }
    }
}
impl<I: Ord + Clone + 'static, T: 'static, S: Ord + Clone + 'static> Node for OrderNode<I, T, S> {
    fn state(&self) -> &dyn Any {
        &self.state
    }
    fn state_mut(&mut self) -> &mut dyn Any {
        &mut self.state
    }
    fn ordered_index(&self) -> Option<&dyn Any> {
        Some(&self.index)
    }
    fn register(&self) {
        Token::register(&self.token);
    }
    fn parents(&self) -> Vec<NodeId> {
        vec![self.parent]
    }
    fn source_changes(&self, _: Box<dyn Any>, _: &mut Meter) -> Result<Box<dyn Any>, Error> {
        unreachable!("ordered query is not writable")
    }
    fn has_changes(&self, p: &dyn Any) -> bool {
        !p.downcast_ref::<Pending<I, OrderedRow<I, T, S>>>()
            .expect("ordered delta type")
            .is_empty()
    }
    fn prepare(&self, stages: &Stages, meter: &mut Meter) -> Result<Option<Prepared>, Error> {
        let Some(input) = stages.get(&self.parent) else {
            return Ok(None);
        };
        let input = input
            .downcast_ref::<Pending<I, T>>()
            .expect("ordered input type");
        if input.is_empty() {
            return Ok(None);
        }
        let mut patch = Patch {
            keys: BTreeMap::new(),
            removed: BTreeSet::new(),
            added: BTreeMap::new(),
        };
        for change in input.iter() {
            meter.work()?;
            if let Some(key) = self.keys.get(change.id()) {
                patch.removed.insert(key.clone());
            }
            let key = change.after().map(|value| {
                let key = OrderKey {
                    sort: (self.selector)(value),
                    id: change.id().clone(),
                };
                patch.added.insert(key.clone(), value.clone());
                key
            });
            patch.keys.insert(change.id().clone(), key);
        }
        let mut changes = Vec::new();
        if let Some(limit) = self.limit {
            let selected = self.selected(&patch, limit, meter)?;
            // Only old/new window identities are compared; unchanged shifts have no delta.
            for id in self
                .state
                .rows
                .keys()
                .chain(selected.keys())
                .collect::<BTreeSet<_>>()
            {
                meter.work()?;
                if let Some(change) = Change::between(
                    id.clone(),
                    self.state.rows.get(id).cloned(),
                    selected.get(id).cloned(),
                ) {
                    meter.output()?;
                    changes.push(change);
                }
            }
        } else {
            for (id, key) in &patch.keys {
                meter.work()?;
                let after = key
                    .as_ref()
                    .map(|key| self.reuse(key, patch.added.get(key).expect("new key has payload")));
                if let Some(change) =
                    Change::between(id.clone(), self.state.rows.get(id).cloned(), after)
                {
                    meter.output()?;
                    changes.push(change);
                }
            }
        }
        let changes: Pending<I, OrderedRow<I, T, S>> = changes.into();
        Ok(Some(Prepared {
            changes: Box::new(changes),
            maintenance: Some(Box::new(patch)),
        }))
    }
    fn commit(&mut self, p: Box<dyn Any>, m: Option<Box<dyn Any>>, epoch: Epoch) {
        let patch = *m
            .expect("ordered staged indexes")
            .downcast::<Patch<I, T, S>>()
            .expect("ordered patch type");
        for key in patch.removed {
            self.index.remove(&key);
        }
        self.index.extend(patch.added);
        for (id, key) in patch.keys {
            if let Some(key) = key {
                self.keys.insert(id, key);
            } else {
                self.keys.remove(&id);
            }
        }
        self.state.apply(
            *p.downcast::<Pending<I, OrderedRow<I, T, S>>>()
                .expect("ordered delta type"),
            epoch,
        );
    }
}
