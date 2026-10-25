use crate::budget::Meter;
use crate::engine::Token;
use crate::engine::{self, Node, NodeId, Nodes, Owner, Pending, Prepared, Stages, State};
use crate::query::Plan;
use crate::{Change, Database, Epoch, Error, Query};
use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::marker::PhantomData;
use std::rc::{Rc, Weak};
use std::sync::Arc;

/// Exact integer statistics of a nonempty group, supporting member retractions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatsI64 {
    /// Number of members, including duplicate values.
    pub count: u64,
    /// Checked sum; wider than the i64 input domain.
    pub sum: i128,
    /// Smallest member value.
    pub min: i64,
    /// Largest member value.
    pub max: i64,
}
impl StatsI64 {
    /// Divides rounded f64 conversions of sum and count; not an exact decimal mean.
    /// Empty manually constructed statistics return None instead of dividing by zero.
    #[must_use]
    pub fn average(&self) -> Option<f64> {
        (self.count != 0).then(|| self.sum as f64 / self.count as f64)
    }
}
type GroupKey<T, G> = Rc<dyn Fn(&T) -> G>;
type Value<T> = Rc<dyn Fn(&T) -> i64>;
type Project<G, O> = Rc<dyn Fn(&G, StatsI64) -> O>;

/// A typed grouping plan; choose count or integer statistics before materializing.
#[must_use]
pub struct GroupedQuery<I, T, G> {
    parent: Query<I, T>,
    key: GroupKey<T, G>,
}
impl<I, T, G> Clone for GroupedQuery<I, T, G> {
    fn clone(&self) -> Self {
        Self {
            parent: self.parent.clone(),
            key: self.key.clone(),
        }
    }
}
impl<I, T, G> fmt::Debug for GroupedQuery<I, T, G> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GroupedQuery").finish_non_exhaustive()
    }
}
impl<I: Ord + Clone + 'static, T: 'static> Query<I, T> {
    /// Groups by a pure typed key; empty groups disappear after their last removal.
    pub fn group_by<G: Ord + Clone + 'static>(
        self,
        key: impl Fn(&T) -> G + 'static,
    ) -> GroupedQuery<I, T, G> {
        GroupedQuery {
            parent: self,
            key: Rc::new(key),
        }
    }
    /// Counts complete projected values and emits each value once, as identity and payload.
    /// No arbitrary representative input row is selected. Equal keys must mean equal values.
    pub fn distinct_values<V: Ord + Clone + 'static>(
        self,
        value: impl Fn(&T) -> V + 'static,
    ) -> Query<V, V> {
        self.group_by(value)
            .finish(Rc::new(|_| 0), Rc::new(|g, _| g.clone()))
    }
}
impl<I: Ord + Clone + 'static, T: 'static, G: Ord + Clone + 'static> GroupedQuery<I, T, G> {
    /// Maintains checked multiplicity for each nonempty group.
    pub fn count(self) -> Query<G, u64> {
        self.finish(Rc::new(|_| 0), Rc::new(|_, s| s.count))
    }
    /// Maintains count, i128 sum and retractable min/max for i64 contributions.
    /// Selectors run only on initial/new/replaced rows; removals reuse cached contributions.
    pub fn stats_i64(self, value: impl Fn(&T) -> i64 + 'static) -> Query<G, StatsI64> {
        self.finish(Rc::new(value), Rc::new(|_, s| s))
    }
    fn finish<O: Eq + 'static>(self, value: Value<T>, project: Project<G, O>) -> Query<G, O> {
        Query {
            owner: self.parent.owner.clone(),
            plan: Rc::new(GroupPlan {
                token: Token::new(&self.parent.owner),
                parent: self.parent,
                key: self.key,
                value,
                project,
            }),
            marker: PhantomData,
        }
    }
}
struct GroupPlan<I, T, G, O> {
    token: Rc<Token>,
    parent: Query<I, T>,
    key: GroupKey<T, G>,
    value: Value<T>,
    project: Project<G, O>,
}
struct Group {
    stats: StatsI64,
    values: BTreeMap<i64, u64>,
}
#[derive(Default)]
struct Difference {
    count: i128,
    sum: i128,
    values: BTreeMap<i64, i128>,
}
struct GroupPatch {
    stats: Option<StatsI64>,
    values: BTreeMap<i64, u64>,
}
struct Updates<I, G> {
    members: BTreeMap<I, Option<(G, i64)>>,
    groups: BTreeMap<G, GroupPatch>,
}
struct GroupNode<I, T, G, O> {
    state: State<G, O>,
    token: Weak<Token>,
    parent: NodeId,
    members: BTreeMap<I, (G, i64)>,
    groups: BTreeMap<G, Group>,
    key: GroupKey<T, G>,
    value: Value<T>,
    project: Project<G, O>,
}
fn add(diff: &mut Difference, value: i64, sign: i128) -> Result<(), Error> {
    diff.count = diff
        .count
        .checked_add(sign)
        .ok_or(Error::ArithmeticOverflow)?;
    diff.sum = diff
        .sum
        .checked_add(i128::from(value) * sign)
        .ok_or(Error::ArithmeticOverflow)?;
    let count = diff.values.entry(value).or_default();
    *count = count.checked_add(sign).ok_or(Error::ArithmeticOverflow)?;
    Ok(())
}
fn count_after(old: u64, delta: i128) -> Result<u64, Error> {
    i128::from(old)
        .checked_add(delta)
        .and_then(|v| u64::try_from(v).ok())
        .ok_or(Error::ArithmeticOverflow)
}
fn stats_after(
    old: Option<&Group>,
    diff: Difference,
    meter: &mut Meter,
) -> Result<GroupPatch, Error> {
    let count = count_after(old.map_or(0, |g| g.stats.count), diff.count)?;
    let sum = old
        .map_or(0, |g| g.stats.sum)
        .checked_add(diff.sum)
        .ok_or(Error::ArithmeticOverflow)?;
    let mut values = BTreeMap::new();
    for (value, delta) in diff.values {
        meter.work()?;
        values.insert(
            value,
            count_after(
                old.and_then(|g| g.values.get(&value)).copied().unwrap_or(0),
                delta,
            )?,
        );
    }
    if count == 0 {
        return Ok(GroupPatch {
            stats: None,
            values,
        });
    }
    let mut old_min = None;
    let mut old_max = None;
    if let Some(group) = old {
        for (&v, &n) in &group.values {
            meter.work()?;
            if values.get(&v).copied().unwrap_or(n) > 0 {
                old_min = Some(v);
                break;
            }
        }
        for (&v, &n) in group.values.iter().rev() {
            meter.work()?;
            if values.get(&v).copied().unwrap_or(n) > 0 {
                old_max = Some(v);
                break;
            }
        }
    }
    let added_min = values.iter().find(|(_, n)| **n > 0).map(|(&v, _)| v);
    let added_max = values.iter().rev().find(|(_, n)| **n > 0).map(|(&v, _)| v);
    let min = old_min
        .into_iter()
        .chain(added_min)
        .min()
        .expect("nonempty group has a minimum");
    let max = old_max
        .into_iter()
        .chain(added_max)
        .max()
        .expect("nonempty group has a maximum");
    Ok(GroupPatch {
        stats: Some(StatsI64 {
            count,
            sum,
            min,
            max,
        }),
        values,
    })
}
impl<I: Ord + Clone + 'static, T: 'static, G: Ord + Clone + 'static, O: Eq + 'static> Node
    for GroupNode<I, T, G, O>
{
    fn state(&self) -> &dyn Any {
        &self.state
    }
    fn state_mut(&mut self) -> &mut dyn Any {
        &mut self.state
    }
    fn register(&self) {
        Token::register(&self.token);
    }
    fn parents(&self) -> Vec<NodeId> {
        vec![self.parent]
    }
    fn source_changes(&self, _: Box<dyn Any>, _: &mut Meter) -> Result<Box<dyn Any>, Error> {
        unreachable!("group is not writable")
    }
    fn has_changes(&self, p: &dyn Any) -> bool {
        !p.downcast_ref::<Pending<G, O>>()
            .expect("group output type")
            .is_empty()
    }
    fn prepare(&self, stages: &Stages, meter: &mut Meter) -> Result<Option<Prepared>, Error> {
        let Some(input) = stages.get(&self.parent) else {
            return Ok(None);
        };
        let input = input
            .downcast_ref::<Pending<I, T>>()
            .expect("group input type");
        if input.is_empty() {
            return Ok(None);
        }
        let mut members = BTreeMap::new();
        let mut differences = BTreeMap::<G, Difference>::new();
        for change in input.iter() {
            meter.work()?;
            if let Some((g, v)) = self.members.get(change.id()) {
                add(differences.entry(g.clone()).or_default(), *v, -1)?;
            }
            let after = change.after().map(|v| ((self.key)(v), (self.value)(v)));
            if let Some((g, v)) = &after {
                add(differences.entry(g.clone()).or_default(), *v, 1)?;
            }
            members.insert(change.id().clone(), after);
        }
        let mut groups = BTreeMap::new();
        let mut changes = Vec::new();
        for (g, diff) in differences {
            meter.work()?;
            let patch = stats_after(self.groups.get(&g), diff, meter)?;
            let before = self.state.rows.get(&g).cloned();
            let after = patch.stats.map(|stats| {
                let output = (self.project)(&g, stats);
                if let Some(old) = &before
                    && **old == output
                {
                    old.clone()
                } else {
                    Arc::new(output)
                }
            });
            if let Some(change) = Change::between(g.clone(), before, after) {
                meter.output()?;
                changes.push(change);
            }
            groups.insert(g, patch);
        }
        let pending: Pending<G, O> = changes.into();
        Ok(Some(Prepared {
            changes: Box::new(pending),
            maintenance: Some(Box::new(Updates { members, groups })),
        }))
    }
    fn commit(&mut self, p: Box<dyn Any>, m: Option<Box<dyn Any>>, epoch: Epoch) {
        let updates = *m
            .expect("group staged state")
            .downcast::<Updates<I, G>>()
            .expect("group patch type");
        for (id, value) in updates.members {
            if let Some(value) = value {
                self.members.insert(id, value);
            } else {
                self.members.remove(&id);
            }
        }
        for (g, patch) in updates.groups {
            if let Some(stats) = patch.stats {
                let group = self.groups.entry(g).or_insert_with(|| Group {
                    stats,
                    values: BTreeMap::new(),
                });
                group.stats = stats;
                for (v, n) in patch.values {
                    if n == 0 {
                        group.values.remove(&v);
                    } else {
                        group.values.insert(v, n);
                    }
                }
            } else {
                self.groups.remove(&g);
            }
        }
        self.state.apply(
            *p.downcast::<Pending<G, O>>().expect("group delta type"),
            epoch,
        );
    }
}
impl<I: Ord + Clone + 'static, T: 'static, G: Ord + Clone + 'static, O: Eq + 'static> Plan
    for GroupPlan<I, T, G, O>
{
    fn token(&self) -> &Rc<Token> {
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
        let id = engine::node_id(&self.token);
        meter.work()?;
        if db.nodes.contains_key(&id) || new.contains_key(&id) {
            return Ok(());
        }
        self.parent.plan.build(db, new, order, meter)?;
        let parent = new
            .get(&self.parent.id())
            .or_else(|| db.nodes.get(&self.parent.id()))
            .expect("group parent built");
        let mut input = Vec::new();
        for (id, value) in &engine::state::<I, T>(parent.as_ref()).rows {
            meter.work()?;
            input.push(Change::Insert {
                id: id.clone(),
                value: value.clone(),
            });
        }
        let input: Pending<I, T> = input.into();
        let mut stages = Stages::new();
        stages.insert(self.parent.id(), Box::new(input));
        let mut node = GroupNode {
            state: State::new(BTreeMap::new()),
            token: Rc::downgrade(&self.token),
            parent: self.parent.id(),
            members: BTreeMap::<I, _>::new(),
            groups: BTreeMap::new(),
            key: self.key.clone(),
            value: self.value.clone(),
            project: self.project.clone(),
        };
        if let Some(p) = node.prepare(&stages, meter)? {
            node.commit(p.changes, p.maintenance, db.epoch);
        }
        new.insert(id, Box::new(node));
        order.push(id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn checked_group_arithmetic() {
        assert_eq!(count_after(u64::MAX, 1), Err(Error::ArithmeticOverflow));
        assert_eq!(count_after(0, -1), Err(Error::ArithmeticOverflow));
        let old = Group {
            stats: StatsI64 {
                count: 1,
                sum: i128::MAX,
                min: 0,
                max: 0,
            },
            values: BTreeMap::from([(0, 1)]),
        };
        let diff = Difference {
            sum: 1,
            ..Difference::default()
        };
        assert!(matches!(
            stats_after(Some(&old), diff, &mut Meter::new(Default::default())),
            Err(Error::ArithmeticOverflow)
        ));
    }
}
