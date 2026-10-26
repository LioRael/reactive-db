use super::index::{Index, Patch};
use super::output::Mode;
use crate::engine::Token;
use crate::engine::{Node, NodeId, Pending, Prepared, Stages, State};
use crate::{Change, Epoch};
use crate::{Error, budget::Meter};
use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Weak;
use std::sync::Arc;

type OutputState<LI, RI, L, R, M> =
    State<<M as Mode<LI, RI, L, R>>::Id, <M as Mode<LI, RI, L, R>>::Value>;
type OutputPending<LI, RI, L, R, M> =
    Pending<<M as Mode<LI, RI, L, R>>::Id, <M as Mode<LI, RI, L, R>>::Value>;

struct IndexUpdates<LI, L, RI, R, J> {
    left: Patch<LI, L, J>,
    right: Patch<RI, R, J>,
}

pub(super) struct JoinNode<LI, L, RI, R, J, M: Mode<LI, RI, L, R>> {
    state: OutputState<LI, RI, L, R, M>,
    token: Weak<Token>,
    left_parent: NodeId,
    right_parent: NodeId,
    left: Index<LI, L, J>,
    right: Index<RI, R, J>,
}
impl<
    LI: Ord + Clone + 'static,
    L: 'static,
    RI: Ord + Clone + 'static,
    R: 'static,
    J: Ord + Clone + 'static,
    M: Mode<LI, RI, L, R>,
> JoinNode<LI, L, RI, R, J, M>
{
    pub fn new(
        token: Weak<Token>,
        left_parent: NodeId,
        right_parent: NodeId,
        left: Index<LI, L, J>,
        right: Index<RI, R, J>,
        meter: &mut Meter,
    ) -> Result<Self, Error> {
        let mut rows = BTreeMap::new();
        for (li, l) in &left.rows {
            meter.work()?;
            let mut matches = right.members(&l.key).peekable();
            if matches.peek().is_none()
                && let Some(id) = M::id(li, None)
            {
                meter.output()?;
                rows.insert(id, Arc::new(M::value(l.value.clone(), None)));
            }
            for ri in matches {
                meter.work()?;
                meter.output()?;
                let r = right
                    .rows
                    .get(ri)
                    .expect("indexed right identity has a payload");
                let id = M::id(li, Some(ri)).expect("matched pair has an output identity");
                rows.insert(
                    id,
                    Arc::new(M::value(l.value.clone(), Some(r.value.clone()))),
                );
            }
        }
        Ok(Self {
            state: State::new(rows),
            token,
            left_parent,
            right_parent,
            left,
            right,
        })
    }
}
fn input<I: 'static, T: 'static>(stages: &Stages, id: NodeId) -> Option<&Pending<I, T>> {
    stages
        .get(&id)
        .map(|v| v.downcast_ref().expect("join input has its declared type"))
}
impl<
    LI: Ord + Clone + 'static,
    L: 'static,
    RI: Ord + Clone + 'static,
    R: 'static,
    J: Ord + Clone + 'static,
    M: Mode<LI, RI, L, R>,
> Node for JoinNode<LI, L, RI, R, J, M>
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
        vec![self.left_parent, self.right_parent]
    }
    fn source_changes(
        &self,
        _patch: Box<dyn Any>,
        _meter: &mut Meter,
    ) -> Result<Box<dyn Any>, Error> {
        unreachable!("a join cannot be a writable source")
    }
    fn has_changes(&self, pending: &dyn Any) -> bool {
        !pending
            .downcast_ref::<OutputPending<LI, RI, L, R, M>>()
            .expect("join pending output has its declared type")
            .is_empty()
    }
    fn prepare(&self, stages: &Stages, meter: &mut Meter) -> Result<Option<Prepared>, Error> {
        let left_changes = input::<LI, L>(stages, self.left_parent);
        let right_changes = input::<RI, R>(stages, self.right_parent);
        if left_changes.is_none_or(|c| c.is_empty()) && right_changes.is_none_or(|c| c.is_empty()) {
            return Ok(None);
        }
        let left_patch = self.left.stage(left_changes, meter)?;
        let right_patch = self.right.stage(right_changes, meter)?;
        let left = self.left.overlay(&left_patch);
        let right = self.right.overlay(&right_patch);
        let mut candidates = BTreeSet::new();
        let mut left_buckets = BTreeMap::new();
        let mut right_buckets = BTreeMap::new();
        for li in left_patch.ids() {
            if let Some(old) = self.left.rows.get(li) {
                for ri in self.right.members(&old.key) {
                    meter.work()?;
                    candidates.insert((li.clone(), Some(ri.clone())));
                }
            }
            if let Some(new) = left.get(li) {
                right.visit_members(&new.key, &mut right_buckets, meter, |ri| {
                    candidates.insert((li.clone(), Some(ri.clone())));
                })?;
            }
            if M::OUTER {
                candidates.insert((li.clone(), None));
            }
        }
        for ri in right_patch.ids() {
            let old_left = self
                .right
                .rows
                .get(ri)
                .into_iter()
                .flat_map(|old| self.left.members(&old.key));
            for li in old_left {
                meter.work()?;
                candidates.insert((li.clone(), Some(ri.clone())));
                if M::OUTER {
                    candidates.insert((li.clone(), None));
                }
            }
            if let Some(new) = right.get(ri) {
                left.visit_members(&new.key, &mut left_buckets, meter, |li| {
                    candidates.insert((li.clone(), Some(ri.clone())));
                    if M::OUTER {
                        candidates.insert((li.clone(), None));
                    }
                })?;
            }
        }
        // Pair identities deduplicate the two sides, including the simultaneous
        // delta cross term. Final overlays prevent transient outer placeholders.
        let mut changes = Vec::new();
        for (li, ri) in candidates {
            meter.work()?;
            let Some(id) = M::id(&li, ri.as_ref()) else {
                continue;
            };
            let before = self.state.rows.get(&id).cloned();
            let desired = left.get(&li).and_then(|l| match &ri {
                Some(ri) => right
                    .get(ri)
                    .filter(|r| l.key == r.key)
                    .map(|r| (&l.value, Some(&r.value))),
                None => (!right.has_members(&l.key)).then_some((&l.value, None)),
            });
            let after = desired.map(|(l, r)| {
                if let Some(existing) = &before
                    && M::unchanged(existing, l, r)
                {
                    return existing.clone();
                }
                Arc::new(M::value(l.clone(), r.cloned()))
            });
            if let Some(change) = Change::between(id, before, after) {
                meter.output()?;
                changes.push(change);
            }
        }
        let pending: OutputPending<LI, RI, L, R, M> = changes.into();
        // Unmatched input updates must commit their indexes even with no output.
        Ok(Some(Prepared {
            changes: Box::new(pending),
            maintenance: Some(Box::new(IndexUpdates {
                left: left_patch,
                right: right_patch,
            })),
        }))
    }
    fn commit(&mut self, pending: Box<dyn Any>, maintenance: Option<Box<dyn Any>>, epoch: Epoch) {
        let updates = *maintenance
            .expect("join preparation includes staged indexes")
            .downcast::<IndexUpdates<LI, L, RI, R, J>>()
            .expect("join index patch has its declared type");
        let changes = *pending
            .downcast::<OutputPending<LI, RI, L, R, M>>()
            .expect("join pending output has its declared type");
        self.left.apply(updates.left);
        self.right.apply(updates.right);
        self.state.apply(changes, epoch);
    }
}
