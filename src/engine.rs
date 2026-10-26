use crate::budget::Meter;
use crate::journal::Journal;
use crate::{Change, Epoch, Error};
use std::any::Any;
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::rc::{Rc, Weak};
use std::sync::Arc;

pub(crate) type NodeId = usize;
pub(crate) use crate::stages::Stages;
pub(crate) type Nodes = BTreeMap<NodeId, Box<dyn Node>>;
pub(crate) type Patch<K, T> = BTreeMap<K, Option<Arc<T>>>;
pub(crate) type Pending<K, T> = Arc<[Change<K, T>]>;

// Keep the one-row allocation separate from the filter rejection path.
#[inline(never)]
fn single_pending<K, T>(change: Change<K, T>) -> Pending<K, T> {
    Arc::from([change])
}

pub(crate) type Transform<A, B> = Rc<dyn Fn(&Arc<A>) -> Option<Arc<B>>>;

#[derive(Debug)]
pub(crate) struct Owner {
    pub alive: Cell<bool>,
    pub poisoned: Cell<bool>,
    pub retired: RefCell<Vec<NodeId>>,
}
impl Owner {
    pub fn new() -> Self {
        Self {
            alive: Cell::new(true),
            poisoned: Cell::new(false),
            retired: RefCell::new(Vec::new()),
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
pub(crate) struct Token {
    owner: Weak<Owner>,
    registered: Cell<bool>,
}
impl Token {
    pub fn new(owner: &Rc<Owner>) -> Rc<Self> {
        Rc::new(Self {
            owner: Rc::downgrade(owner),
            registered: Cell::new(false),
        })
    }
    pub fn register(token: &Weak<Self>) {
        token
            .upgrade()
            .expect("new node retains its plan")
            .registered
            .set(true);
    }
}
impl Drop for Token {
    fn drop(&mut self) {
        if self.registered.get()
            && let Some(owner) = self.owner.upgrade()
            && owner.alive.get()
        {
            owner
                .retired
                .borrow_mut()
                .push(self as *const Self as NodeId);
        }
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
pub(crate) fn node_id(token: &Rc<Token>) -> NodeId {
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
    fn changes(&self, patch: Patch<K, T>, meter: &mut Meter) -> Result<Pending<K, T>, Error> {
        let mut patch = patch.into_iter();
        if patch.len() == 1 {
            let (key, after) = patch.next().expect("single source patch has one entry");
            meter.work()?;
            let before = self.rows.get(&key).cloned();
            return match Change::between(key, before, after) {
                Some(change) => {
                    meter.output()?;
                    // Construct the shared publication directly, without a
                    // temporary Vec or cloning the already-owned identity.
                    Ok(Arc::from([change]))
                }
                None => Ok(Arc::from([])),
            };
        }
        let mut changes = Vec::new();
        for (key, after) in patch {
            meter.work()?;
            if let Some(change) = Change::between(key.clone(), self.rows.get(&key).cloned(), after)
            {
                meter.output()?;
                changes.push(change);
            }
        }
        Ok(changes.into())
    }
    pub(crate) fn apply(&mut self, changes: Pending<K, T>, epoch: Epoch) {
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

pub(crate) struct Prepared {
    pub changes: Box<dyn Any>,
    pub maintenance: Option<Box<dyn Any>>,
}

pub(crate) trait Node {
    fn state(&self) -> &dyn Any;
    fn state_mut(&mut self) -> &mut dyn Any;
    fn register(&self);
    fn parents(&self) -> Vec<NodeId>;
    fn ordered_index(&self) -> Option<&dyn Any> {
        None
    }
    fn source_changes(&self, patch: Box<dyn Any>, meter: &mut Meter)
    -> Result<Box<dyn Any>, Error>;
    fn prepare(&self, stages: &Stages, meter: &mut Meter) -> Result<Option<Prepared>, Error>;
    fn has_changes(&self, pending: &dyn Any) -> bool;
    fn commit(&mut self, pending: Box<dyn Any>, maintenance: Option<Box<dyn Any>>, epoch: Epoch);
}

pub(crate) enum Lease {
    Source(Rc<Token>),
    Derived(Weak<Token>),
}
pub(crate) trait Evaluate<K, T> {
    fn parent(&self) -> NodeId;
    fn prepare(
        &self,
        current: &State<K, T>,
        stages: &Stages,
        meter: &mut Meter,
    ) -> Result<Option<Pending<K, T>>, Error>;
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
    fn register(&self) {
        match &self.lease {
            Lease::Source(token) => token.registered.set(true),
            Lease::Derived(token) => Token::register(token),
        }
    }
    fn parents(&self) -> Vec<NodeId> {
        self.evaluator
            .as_ref()
            .map_or_else(Vec::new, |e| vec![e.parent()])
    }
    fn source_changes(
        &self,
        patch: Box<dyn Any>,
        meter: &mut Meter,
    ) -> Result<Box<dyn Any>, Error> {
        let patch = *patch
            .downcast::<Patch<K, T>>()
            .expect("typed source patch matches its registered node");
        Ok(Box::new(self.state.changes(patch, meter)?))
    }
    fn prepare(&self, stages: &Stages, meter: &mut Meter) -> Result<Option<Prepared>, Error> {
        let Some(evaluator) = &self.evaluator else {
            return Ok(None);
        };
        Ok(evaluator
            .prepare(&self.state, stages, meter)?
            .map(|pending| Prepared {
                changes: Box::new(pending),
                maintenance: None,
            }))
    }
    fn has_changes(&self, pending: &dyn Any) -> bool {
        !pending
            .downcast_ref::<Pending<K, T>>()
            .expect("typed pending batch matches its node")
            .is_empty()
    }
    fn commit(&mut self, pending: Box<dyn Any>, maintenance: Option<Box<dyn Any>>, epoch: Epoch) {
        assert!(
            maintenance.is_none(),
            "linear/source nodes have no staged operator state"
        );
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
    fn parent(&self) -> NodeId {
        self.parent
    }
    fn prepare(
        &self,
        current: &State<K, B>,
        stages: &Stages,
        meter: &mut Meter,
    ) -> Result<Option<Pending<K, B>>, Error> {
        let Some(input) = stages.get(&self.parent) else {
            return Ok(None);
        };
        let input = input
            .downcast_ref::<Pending<K, A>>()
            .expect("linear input has declared type");
        let mut changes = Vec::new();
        for change in input.iter() {
            meter.work()?;
            let before = current.rows.get(change.id()).cloned();
            let after = change.after().and_then(|row| (self.transform)(row));
            if let Some(change) = Change::between(change.id().clone(), before, after) {
                meter.output()?;
                if input.len() == 1 {
                    return Ok(Some(single_pending(change)));
                }
                changes.push(change);
            }
        }
        Ok((!changes.is_empty()).then(|| changes.into()))
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

#[cfg(test)]
mod token_tests {
    use super::*;

    #[test]
    fn only_registered_live_database_tokens_enqueue_retirement() {
        let owner = Rc::new(Owner::new());
        for _ in 0..1000 {
            drop(Token::new(&owner));
        }
        assert!(owner.retired.borrow().is_empty());
        let token = Token::new(&owner);
        let id = node_id(&token);
        let weak = Rc::downgrade(&token);
        Token::register(&weak);
        drop(token);
        assert_eq!(*owner.retired.borrow(), vec![id]);
        owner.retired.borrow_mut().clear();
        let token = Token::new(&owner);
        Token::register(&Rc::downgrade(&token));
        owner.alive.set(false);
        drop(token);
        assert!(owner.retired.borrow().is_empty());
    }
}

#[cfg(test)]
mod source_patch_tests {
    use super::*;
    use crate::ExecutionBudget;

    #[test]
    fn single_and_multi_entry_source_patches_match_an_independent_row_model() -> Result<(), Error> {
        let initial = BTreeMap::from([(1_u64, Arc::new(7_i64)), (2, Arc::new(9))]);
        for (id, after, changed) in [
            (1, None, true),
            (1, Some(Arc::new(8)), true),
            (1, Some(Arc::new(7)), true),
            (3, Some(Arc::new(10)), true),
            (3, None, false),
            (1, initial.get(&1).cloned(), false),
        ] {
            for extra_noop in [false, true] {
                let mut state = State::new(initial.clone());
                let mut patch = BTreeMap::from([(id, after.clone())]);
                if extra_noop {
                    patch.insert(2, initial.get(&2).cloned());
                }
                let mut meter = Meter::new(ExecutionBudget::default());
                let pending = state.changes(patch, &mut meter)?;
                assert_eq!(meter.stats.work, 1 + usize::from(extra_noop));
                assert_eq!(meter.stats.output_changes, usize::from(changed));
                assert_eq!(pending.len(), usize::from(changed));
                if let Some(change) = pending.first() {
                    assert_eq!(*change.id(), id);
                    let before = match change {
                        Change::Insert { .. } => None,
                        Change::Update { before, .. } => Some(before),
                        Change::Remove { value, .. } => Some(value),
                    };
                    assert_eq!(before, initial.get(&id));
                    assert_eq!(change.after(), after.as_ref());
                }
                assert_eq!(state.rows, initial, "preparation must not publish");
                let mut expected = initial.clone();
                if let Some(value) = &after {
                    expected.insert(id, value.clone());
                } else {
                    expected.remove(&id);
                }
                state.apply(pending, Epoch(1));
                assert_eq!(state.rows, expected);
            }
        }
        Ok(())
    }

    #[test]
    fn rejected_single_source_publication_releases_the_owned_payload() {
        for limits in [
            ExecutionBudget {
                max_work: 0,
                ..Default::default()
            },
            ExecutionBudget {
                max_output_changes: 0,
                ..Default::default()
            },
        ] {
            let state = State::<u64, String>::new(BTreeMap::new());
            let value = Arc::new("staged payload".to_owned());
            let weak = Arc::downgrade(&value);
            let mut meter = Meter::new(limits);
            assert!(matches!(
                state.changes(BTreeMap::from([(1, Some(value))]), &mut meter),
                Err(Error::BudgetExceeded(_))
            ));
            assert!(weak.upgrade().is_none());
            assert!(state.rows.is_empty());
        }
    }

    #[test]
    fn single_source_publication_moves_its_owned_identity() -> Result<(), Error> {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static CLONES: AtomicUsize = AtomicUsize::new(0);
        #[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
        struct Id(u64);
        impl Clone for Id {
            fn clone(&self) -> Self {
                CLONES.fetch_add(1, Ordering::Relaxed);
                Self(self.0)
            }
        }
        let state = State::<Id, u64>::new(BTreeMap::new());
        let mut meter = Meter::new(ExecutionBudget::default());
        let pending = state.changes(BTreeMap::from([(Id(7), Some(Arc::new(11)))]), &mut meter)?;
        assert_eq!(pending[0].id().0, 7);
        assert_eq!(
            CLONES.load(Ordering::Relaxed),
            0,
            "owned source ID must not be cloned for publication"
        );
        Ok(())
    }
}

#[cfg(test)]
mod linear_patch_tests {
    use super::*;
    use crate::ExecutionBudget;

    #[test]
    fn single_and_multi_input_filter_map_match_an_independent_model() -> Result<(), Error> {
        for old in [None, Some(-2_i64), Some(2)] {
            for new in [None, Some(-3_i64), Some(3)] {
                if old.is_none() && new.is_none() {
                    continue;
                }
                for extra_filtered_row in [false, true] {
                    let mut rows = BTreeMap::new();
                    if let Some(value) = old.filter(|v| *v >= 0) {
                        rows.insert(1_u64, Arc::new(value * 2));
                    }
                    let mut current = State::new(rows.clone());
                    let change = Change::between(1, old.map(Arc::new), new.map(Arc::new)).unwrap();
                    let mut input = vec![change];
                    if extra_filtered_row {
                        input.push(Change::Update {
                            id: 9,
                            before: Arc::new(-1),
                            after: Arc::new(-2),
                        });
                    }
                    let calls = Rc::new(Cell::new(0));
                    let counter = calls.clone();
                    let linear = Linear {
                        parent: 7,
                        transform: Rc::new(move |v: &Arc<i64>| {
                            counter.set(counter.get() + 1);
                            (**v >= 0).then(|| Arc::new(**v * 2))
                        }),
                        marker: std::marker::PhantomData::<fn() -> u64>,
                    };
                    let mut stages = Stages::new();
                    let input: Pending<u64, i64> = input.into();
                    stages.insert(7, Box::new(input));
                    let mut meter = Meter::new(ExecutionBudget::default());
                    let pending = linear.prepare(&current, &stages, &mut meter)?;
                    assert_eq!(current.rows, rows, "preparation must not publish");
                    assert_eq!(
                        calls.get(),
                        usize::from(new.is_some()) + usize::from(extra_filtered_row)
                    );
                    assert_eq!(meter.stats.work, 1 + usize::from(extra_filtered_row));
                    let changed = old.is_some_and(|v| v >= 0) || new.is_some_and(|v| v >= 0);
                    assert_eq!(meter.stats.output_changes, usize::from(changed));
                    assert_eq!(pending.is_some(), changed);
                    let expected = new
                        .filter(|v| *v >= 0)
                        .map(|v| BTreeMap::from([(1, Arc::new(v * 2))]))
                        .unwrap_or_default();
                    if let Some(pending) = pending {
                        assert_eq!(pending.len(), 1);
                        assert_eq!(*pending[0].id(), 1);
                        current.apply(pending, Epoch(1));
                    }
                    assert_eq!(current.rows, expected);
                }
            }
        }
        Ok(())
    }

    #[test]
    fn single_map_budget_failure_releases_derived_payload_without_publication() {
        for output_limit in [0, 1] {
            let weak = Rc::new(RefCell::new(None));
            let captured = weak.clone();
            let linear = Linear {
                parent: 7,
                transform: Rc::new(move |v: &Arc<i64>| {
                    let value = Arc::new(**v * 2);
                    *captured.borrow_mut() = Some(Arc::downgrade(&value));
                    Some(value)
                }),
                marker: std::marker::PhantomData::<fn() -> u64>,
            };
            let current = State::<u64, i64>::new(BTreeMap::new());
            let input: Pending<u64, i64> = Arc::from([Change::Insert {
                id: 1,
                value: Arc::new(3),
            }]);
            let mut stages = Stages::new();
            stages.insert(7, Box::new(input));
            let mut meter = Meter::new(ExecutionBudget {
                max_output_changes: output_limit,
                ..Default::default()
            });
            let result = linear.prepare(&current, &stages, &mut meter);
            if output_limit == 0 {
                assert!(matches!(result, Err(Error::BudgetExceeded(_))));
            } else {
                assert!(result.as_ref().unwrap().is_some());
            }
            drop(result);
            assert!(weak.borrow().as_ref().unwrap().upgrade().is_none());
            assert!(current.rows.is_empty());
        }
    }
}
