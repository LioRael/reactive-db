use crate::journal::Journal;
use crate::{Change, Epoch, Error};
use std::any::Any;
use std::cell::Cell;
use std::collections::BTreeMap;
use std::rc::{Rc, Weak};
use std::sync::Arc;

pub(crate) type NodeId = usize;
pub(crate) type Stages = BTreeMap<NodeId, Box<dyn Any>>;
pub(crate) type Nodes = BTreeMap<NodeId, Box<dyn Node>>;
pub(crate) type Patch<K, T> = BTreeMap<K, Option<Arc<T>>>;
pub(crate) type Pending<K, T> = Arc<[Change<K, T>]>;
pub(crate) type Transform<A, B> = Rc<dyn Fn(&Arc<A>) -> Option<Arc<B>>>;

#[derive(Debug)]
pub(crate) struct Owner {
    pub alive: Cell<bool>,
    pub poisoned: Cell<bool>,
}
impl Owner {
    pub fn new() -> Self {
        Self {
            alive: Cell::new(true),
            poisoned: Cell::new(false),
        }
    }
    pub fn check(self: &Rc<Self>, target: &Rc<Self>) -> Result<(), Error> {
        if !self.alive.get() {
            return Err(Error::DatabaseDropped);
        }
        if !Rc::ptr_eq(self, target) {
            return Err(Error::WrongDatabase);
        }
        if self.poisoned.get() {
            return Err(Error::Poisoned);
        }
        Ok(())
    }
}
pub(crate) struct PanicGuard(pub Rc<Owner>);
impl Drop for PanicGuard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.poisoned.set(true);
        }
    }
}
pub(crate) fn node_id(token: &Rc<()>) -> NodeId {
    Rc::as_ptr(token) as usize
}

pub(crate) struct State<K, T> {
    pub rows: BTreeMap<K, Arc<T>>,
    pub journal: Option<Journal<K, T>>,
}
impl<K: Ord + Clone, T> State<K, T> {
    pub fn new(rows: BTreeMap<K, Arc<T>>) -> Self {
        Self {
            rows,
            journal: None,
        }
    }
    fn changes(&self, patch: Patch<K, T>) -> Pending<K, T> {
        patch
            .into_iter()
            .filter_map(|(key, after)| {
                let before = self.rows.get(&key).cloned();
                Change::between(key, before, after)
            })
            .collect::<Vec<_>>()
            .into()
    }
    fn apply(&mut self, changes: Pending<K, T>, epoch: Epoch) {
        for change in changes.iter() {
            if let Some(value) = change.after() {
                self.rows.insert(change.id().clone(), value.clone());
            } else {
                self.rows.remove(change.id());
            }
        }
        if let Some(journal) = &mut self.journal {
            journal.push(epoch, changes);
        }
    }
}

pub(crate) trait Node {
    fn state(&self) -> &dyn Any;
    fn state_mut(&mut self) -> &mut dyn Any;
    fn alive(&self) -> bool;
    fn source_changes(&self, patch: Box<dyn Any>) -> Box<dyn Any>;
    fn prepare(&self, stages: &Stages) -> Option<Box<dyn Any>>;
    fn has_changes(&self, pending: &dyn Any) -> bool;
    fn commit(&mut self, pending: Box<dyn Any>, epoch: Epoch);
}

pub(crate) enum Lease {
    Source(Rc<()>),
    Derived(Weak<()>),
}
impl Lease {
    fn alive(&self) -> bool {
        match self {
            Self::Source(token) => Rc::strong_count(token) > 0,
            Self::Derived(token) => token.strong_count() > 0,
        }
    }
}
pub(crate) trait Evaluate<K, T> {
    fn prepare(&self, current: &State<K, T>, stages: &Stages) -> Option<Pending<K, T>>;
}
pub(crate) struct Runtime<K, T> {
    pub state: State<K, T>,
    pub lease: Lease,
    pub evaluator: Option<Box<dyn Evaluate<K, T>>>,
}
impl<K: Ord + Clone + 'static, T: 'static> Node for Runtime<K, T> {
    fn state(&self) -> &dyn Any {
        &self.state
    }
    fn state_mut(&mut self) -> &mut dyn Any {
        &mut self.state
    }
    fn alive(&self) -> bool {
        self.lease.alive()
    }
    fn source_changes(&self, patch: Box<dyn Any>) -> Box<dyn Any> {
        let patch = *patch
            .downcast::<Patch<K, T>>()
            .expect("typed source patch matches its registered node");
        Box::new(self.state.changes(patch))
    }
    fn prepare(&self, stages: &Stages) -> Option<Box<dyn Any>> {
        self.evaluator
            .as_ref()?
            .prepare(&self.state, stages)
            .map(|pending| Box::new(pending) as Box<dyn Any>)
    }
    fn has_changes(&self, pending: &dyn Any) -> bool {
        !pending
            .downcast_ref::<Pending<K, T>>()
            .expect("typed pending batch matches its node")
            .is_empty()
    }
    fn commit(&mut self, pending: Box<dyn Any>, epoch: Epoch) {
        let pending = *pending
            .downcast::<Pending<K, T>>()
            .expect("typed pending batch matches its node");
        self.state.apply(pending, epoch);
    }
}

pub(crate) struct Linear<K, A, B> {
    pub parent: NodeId,
    pub transform: Transform<A, B>,
    pub marker: std::marker::PhantomData<fn() -> K>,
}
impl<K: Ord + Clone + 'static, A: 'static, B: 'static> Evaluate<K, B> for Linear<K, A, B> {
    fn prepare(&self, current: &State<K, B>, stages: &Stages) -> Option<Pending<K, B>> {
        let input = stages
            .get(&self.parent)?
            .downcast_ref::<Pending<K, A>>()
            .expect("linear operator input has its declared type");
        let changes: Vec<_> = input
            .iter()
            .filter_map(|change| {
                let before = current.rows.get(change.id()).cloned();
                let after = change.after().and_then(|row| (self.transform)(row));
                Change::between(change.id().clone(), before, after)
            })
            .collect();
        (!changes.is_empty()).then(|| changes.into())
    }
}

pub(crate) fn state<K: 'static, T: 'static>(node: &dyn Node) -> &State<K, T> {
    node.state()
        .downcast_ref()
        .expect("typed handle matches its registered state")
}
pub(crate) fn state_mut<K: 'static, T: 'static>(node: &mut dyn Node) -> &mut State<K, T> {
    node.state_mut()
        .downcast_mut()
        .expect("typed handle matches its registered state")
}
