use super::runtime::OrderNode;
use super::types::{OrderKey, OrderedRow};
use crate::budget::Meter;
use crate::engine::{self, NodeId, Nodes, Owner};
use crate::query::Plan;
use crate::{Database, Error, Query};
use std::collections::BTreeMap;
use std::fmt;
use std::marker::PhantomData;
use std::rc::Rc;

type Finished<I, T, S> = (Query<I, OrderedRow<I, T, S>>, Option<usize>);
pub(super) type Selector<T, S> = Rc<dyn Fn(&T) -> S>;
/// A lazy sorted plan; only take and ordered materialization preserve its contract.
#[must_use]
pub struct OrderedQuery<I, T, S> {
    parent: Query<I, T>,
    selector: Selector<T, S>,
    limit: Option<usize>,
    token: Rc<()>,
}
impl<I, T, S> Clone for OrderedQuery<I, T, S> {
    fn clone(&self) -> Self {
        Self {
            parent: self.parent.clone(),
            selector: self.selector.clone(),
            limit: self.limit,
            token: self.token.clone(),
        }
    }
}
impl<I, T, S> fmt::Debug for OrderedQuery<I, T, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrderedQuery")
            .field("limit", &self.limit)
            .finish_non_exhaustive()
    }
}
impl<I: Ord + Clone + 'static, T: 'static> Query<I, T> {
    /// Orders incrementally by a pure key, breaking ties by stable row identity.
    /// Descending order uses `std::cmp::Reverse`; float keys require explicit total ordering.
    pub fn sort_by_key<S: Ord + Clone + 'static>(
        self,
        key: impl Fn(&T) -> S + 'static,
    ) -> OrderedQuery<I, T, S> {
        OrderedQuery {
            parent: self,
            selector: Rc::new(key),
            limit: None,
            token: Rc::new(()),
        }
    }
}
impl<I: Ord + Clone + 'static, T: 'static, S: Ord + Clone + 'static> OrderedQuery<I, T, S> {
    /// Selects the first k rows; repeated takes keep the smaller limit, including zero.
    /// Updates cost changed-index work plus the selected window, not a full-input sort.
    pub fn take(mut self, k: usize) -> Self {
        self.limit = Some(self.limit.map_or(k, |old| old.min(k)));
        self.token = Rc::new(());
        self
    }
    pub(super) fn finish(self) -> Finished<I, T, S> {
        let limit = self.limit;
        (
            Query {
                owner: self.parent.owner.clone(),
                plan: Rc::new(self),
                marker: PhantomData,
            },
            limit,
        )
    }
}
impl<I: Ord + Clone + 'static, T: 'static, S: Ord + Clone + 'static> Plan
    for OrderedQuery<I, T, S>
{
    fn token(&self) -> &Rc<()> {
        &self.token
    }
    fn validate(&self, owner: &Rc<Owner>, meter: &mut Meter) -> Result<(), Error> {
        self.parent.validate(owner, meter)
    }
    fn build(
        &self,
        db: &Database,
        new: &mut Nodes,
        order: &mut Vec<NodeId>,
        meter: &mut Meter,
    ) -> Result<(), Error> {
        meter.work()?;
        let id = engine::node_id(&self.token);
        if new.contains_key(&id) || db.nodes.contains_key(&id) {
            return Ok(());
        }
        self.parent.plan.build(db, new, order, meter)?;
        let parent = new
            .get(&self.parent.id())
            .or_else(|| db.nodes.get(&self.parent.id()))
            .expect("ordered parent built");
        let mut keys = BTreeMap::new();
        let mut index = BTreeMap::new();
        for (id, value) in &engine::state::<I, T>(parent.as_ref()).rows {
            meter.work()?;
            let key = OrderKey {
                sort: (self.selector)(value),
                id: id.clone(),
            };
            keys.insert(id.clone(), key.clone());
            index.insert(key, value.clone());
        }
        let node = OrderNode::new(
            Rc::downgrade(&self.token),
            self.parent.id(),
            self.selector.clone(),
            self.limit,
            keys,
            index,
            meter,
        )?;
        new.insert(id, Box::new(node));
        order.push(id);
        Ok(())
    }
}
