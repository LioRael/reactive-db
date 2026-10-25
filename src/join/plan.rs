use super::index::{Index, Selector};
use super::output::{Inner, Left, Mode};
use super::runtime::JoinNode;
use crate::budget::Meter;
use crate::engine::Token;
use crate::engine::{self, NodeId, Nodes, Owner};
use crate::query::Plan;
use crate::{Database, Error, Joined, LeftJoined, Query};
use std::marker::PhantomData;
use std::rc::Rc;

impl<LI: Ord + Clone + 'static, L: 'static> Query<LI, L> {
    /// Matches rows by equal typed keys, with pair identities `(left_id, right_id)`.
    ///
    /// Both selectors must be pure and return the same `Ord + Clone` key type.
    /// Duplicate keys produce every matching pair; payloads need not implement
    /// Clone, Eq or Ord. Rust equality applies, including `None == None` keys.
    /// Self-joins and same-batch updates use both final inputs atomically.
    /// Database ownership of both plans is checked before materialization.
    ///
    /// ```
    /// # fn main() -> Result<(), reactive_db::Error> {
    /// let mut db = reactive_db::Database::new();
    /// let tasks = db.create_collection::<u64, u64>("tasks")?;
    /// let projects = db.create_collection::<u64, String>("projects")?;
    /// let query = tasks.query().join_on(projects.query(), |_, project| *project, |id, _| *id);
    /// let view = db.materialize(query.map(|pair| pair.right.to_string()))?;
    /// db.write(|tx| { tx.insert(&tasks, 1, 7)?; tx.insert(&projects, 7, "Research".into()) })?;
    /// assert_eq!(view.snapshot(&db)?.rows[0].id, (1, 7));
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Selectors cannot silently compare different key types:
    /// ```compile_fail
    /// # fn main() -> Result<(), reactive_db::Error> {
    /// let mut db = reactive_db::Database::new();
    /// let left = db.create_collection::<u64, u64>("left")?;
    /// let right = db.create_collection::<u64, String>("right")?;
    /// let query = left.query().join_on(right.query(), |_, v| *v, |_, v| v.clone());
    /// # Ok(())
    /// # }
    /// ```
    pub fn join_on<RI: Ord + Clone + 'static, R: 'static, J: Ord + Clone + 'static>(
        self,
        right: Query<RI, R>,
        left_key: impl Fn(&LI, &L) -> J + 'static,
        right_key: impl Fn(&RI, &R) -> J + 'static,
    ) -> Query<(LI, RI), Joined<L, R>> {
        self.join::<RI, R, J, Inner>(right, Rc::new(left_key), Rc::new(right_key))
    }
    /// Retains unmatched left rows as `(left_id, None)` with no right payload.
    ///
    /// Matches use `(left_id, Some(right_id))`. The last-match removal and first
    /// match insertion retract/add the placeholder in one atomic delta batch.
    /// Selectors follow [`Self::join_on`] contracts; `Option` keys use ordinary
    /// Rust equality, not SQL NULL semantics. Only changed rows rerun selectors.
    pub fn left_join_on<RI: Ord + Clone + 'static, R: 'static, J: Ord + Clone + 'static>(
        self,
        right: Query<RI, R>,
        left_key: impl Fn(&LI, &L) -> J + 'static,
        right_key: impl Fn(&RI, &R) -> J + 'static,
    ) -> Query<(LI, Option<RI>), LeftJoined<L, R>> {
        self.join::<RI, R, J, Left>(right, Rc::new(left_key), Rc::new(right_key))
    }
    fn join<
        RI: Ord + Clone + 'static,
        R: 'static,
        J: Ord + Clone + 'static,
        M: Mode<LI, RI, L, R>,
    >(
        self,
        right: Query<RI, R>,
        left_key: Selector<LI, L, J>,
        right_key: Selector<RI, R, J>,
    ) -> Query<M::Id, M::Value> {
        Query {
            owner: self.owner.clone(),
            plan: Rc::new(JoinPlan::<LI, L, RI, R, J, M> {
                token: Token::new(&self.owner),
                left: self,
                right,
                left_key,
                right_key,
                marker: PhantomData,
            }),
            marker: PhantomData,
        }
    }
}
struct JoinPlan<LI, L, RI, R, J, M> {
    token: Rc<Token>,
    left: Query<LI, L>,
    right: Query<RI, R>,
    left_key: Selector<LI, L, J>,
    right_key: Selector<RI, R, J>,
    marker: PhantomData<fn() -> M>,
}
impl<
    LI: Ord + Clone + 'static,
    L: 'static,
    RI: Ord + Clone + 'static,
    R: 'static,
    J: Ord + Clone + 'static,
    M: Mode<LI, RI, L, R>,
> Plan for JoinPlan<LI, L, RI, R, J, M>
{
    fn token(&self) -> &Rc<Token> {
        &self.token
    }
    fn validate(&self, owner: &Rc<Owner>, meter: &mut Meter) -> Result<(), Error> {
        self.left.validate(owner, meter)?;
        self.right.validate(owner, meter)
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
        if db.nodes.contains_key(&id) || new.contains_key(&id) {
            return Ok(());
        }
        self.left.plan.build(db, new, order, meter)?;
        self.right.plan.build(db, new, order, meter)?;
        let left_node = new
            .get(&self.left.id())
            .or_else(|| db.nodes.get(&self.left.id()))
            .expect("left parent is built before join");
        let right_node = new
            .get(&self.right.id())
            .or_else(|| db.nodes.get(&self.right.id()))
            .expect("right parent is built before join");
        let left = Index::new(
            &engine::state::<LI, L>(left_node.as_ref()).rows,
            self.left_key.clone(),
            meter,
        )?;
        let right = Index::new(
            &engine::state::<RI, R>(right_node.as_ref()).rows,
            self.right_key.clone(),
            meter,
        )?;
        let node = JoinNode::<LI, L, RI, R, J, M>::new(
            Rc::downgrade(&self.token),
            self.left.id(),
            self.right.id(),
            left,
            right,
            meter,
        )?;
        new.insert(id, Box::new(node));
        order.push(id);
        Ok(())
    }
}
