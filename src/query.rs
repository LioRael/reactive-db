use crate::Error;
use crate::database::Database;
use crate::engine::{self, Lease, Linear, NodeId, Nodes, Owner, Runtime, State, Transform};
use std::collections::BTreeMap;
use std::fmt;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;

pub(crate) trait Plan {
    fn token(&self) -> &Rc<()>;
    fn validate(&self, _owner: &Rc<Owner>) -> Result<(), Error> {
        Ok(())
    }
    fn build(&self, db: &Database, new: &mut Nodes, order: &mut Vec<NodeId>);
}

/// A lazy typed query plan; cloning shares its explicit node identity.
///
/// Closures must be deterministic and must not observe silently mutable payloads
/// or external state. Materialization evaluates all initial rows; later writes
/// evaluate only affected rows. Dropping all plan/view/subscription leases retires
/// derived runtime state at the next mutable database operation.
#[must_use]
pub struct Query<I, T> {
    pub(crate) owner: Rc<Owner>,
    pub(crate) plan: Rc<dyn Plan>,
    pub(crate) marker: PhantomData<fn() -> (I, T)>,
}
impl<I, T> Clone for Query<I, T> {
    fn clone(&self) -> Self {
        Self {
            owner: self.owner.clone(),
            plan: self.plan.clone(),
            marker: PhantomData,
        }
    }
}
impl<I, T> fmt::Debug for Query<I, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Query").finish_non_exhaustive()
    }
}
impl<I, T> Query<I, T> {
    pub(crate) fn validate(&self, owner: &Rc<Owner>) -> Result<(), Error> {
        self.owner.check(owner)?;
        self.plan.validate(owner)
    }
    pub(crate) fn id(&self) -> NodeId {
        engine::node_id(self.plan.token())
    }
    pub(crate) fn source(owner: Rc<Owner>, token: Rc<()>) -> Self {
        Self {
            owner,
            plan: Rc::new(SourcePlan(token)),
            marker: PhantomData,
        }
    }
}
impl<I: Ord + Clone + 'static, T: 'static> Query<I, T> {
    /// Keeps matching rows without changing their identities.
    ///
    /// The predicate runs for initial rows and new/replaced rows, never for a
    /// cached removal. Its panic poisons the database during execution.
    pub fn filter(self, predicate: impl Fn(&T) -> bool + 'static) -> Self {
        self.transform(Rc::new(move |value| {
            predicate(value).then(|| value.clone())
        }))
    }
    /// Projects payloads while preserving identities, including duplicate values.
    ///
    /// Cached output is used for removals; projection runs only on insertion or
    /// replacement. No `Clone`, `Ord`, or `Eq` bound is imposed on either payload.
    pub fn map<U: 'static>(self, project: impl Fn(&T) -> U + 'static) -> Query<I, U> {
        self.transform(Rc::new(move |value| Some(Arc::new(project(value)))))
    }
    fn transform<U: 'static>(self, transform: Transform<T, U>) -> Query<I, U> {
        Query {
            owner: self.owner.clone(),
            plan: Rc::new(LinearPlan {
                token: Rc::new(()),
                parent: self,
                transform,
            }),
            marker: PhantomData,
        }
    }
}
struct SourcePlan(Rc<()>);
impl Plan for SourcePlan {
    fn token(&self) -> &Rc<()> {
        &self.0
    }
    fn build(&self, db: &Database, _new: &mut Nodes, _order: &mut Vec<NodeId>) {
        assert!(
            db.nodes.contains_key(&engine::node_id(&self.0)),
            "source plan must belong to a registered collection"
        );
    }
}
struct LinearPlan<K, A, B> {
    token: Rc<()>,
    parent: Query<K, A>,
    transform: Transform<A, B>,
}
impl<K: Ord + Clone + 'static, A: 'static, B: 'static> Plan for LinearPlan<K, A, B> {
    fn validate(&self, owner: &Rc<Owner>) -> Result<(), Error> {
        self.parent.validate(owner)
    }
    fn token(&self) -> &Rc<()> {
        &self.token
    }
    fn build(&self, db: &Database, new: &mut Nodes, order: &mut Vec<NodeId>) {
        let id = engine::node_id(&self.token);
        if db.nodes.contains_key(&id) || new.contains_key(&id) {
            return;
        }
        self.parent.plan.build(db, new, order);
        let parent = new
            .get(&self.parent.id())
            .or_else(|| db.nodes.get(&self.parent.id()))
            .expect("parent is built before its dependent node");
        let rows: BTreeMap<_, _> = engine::state::<K, A>(parent.as_ref())
            .rows
            .iter()
            .filter_map(|(key, value)| (self.transform)(value).map(|output| (key.clone(), output)))
            .collect();
        new.insert(
            id,
            Box::new(Runtime {
                state: State::new(rows),
                lease: Lease::Derived(Rc::downgrade(&self.token)),
                evaluator: Some(Box::new(Linear::<K, A, B> {
                    parent: self.parent.id(),
                    transform: self.transform.clone(),
                    marker: PhantomData,
                })),
            }),
        );
        order.push(id);
    }
}
