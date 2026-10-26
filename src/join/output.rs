use std::sync::Arc;

/// A matched pair, retaining typed immutable payloads without cloning either row.
#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Joined<L, R> {
    /// The left input payload.
    pub left: Arc<L>,
    /// The matching right input payload.
    pub right: Arc<R>,
}
impl<L, R> Clone for Joined<L, R> {
    fn clone(&self) -> Self {
        Self {
            left: self.left.clone(),
            right: self.right.clone(),
        }
    }
}

/// A left row with a matching right payload, or one explicit unmatched result.
#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct LeftJoined<L, R> {
    /// The left input payload, present for every result.
    pub left: Arc<L>,
    /// A matched right payload; absent only for the unmatched placeholder.
    pub right: Option<Arc<R>>,
}
impl<L, R> Clone for LeftJoined<L, R> {
    fn clone(&self) -> Self {
        Self {
            left: self.left.clone(),
            right: self.right.clone(),
        }
    }
}

pub(super) trait Mode<LI, RI, L, R>: 'static {
    type Id: Ord + Clone + 'static;
    type Value: 'static;
    const OUTER: bool;
    fn id(left: &LI, right: Option<&RI>) -> Option<Self::Id>;
    fn value(left: Arc<L>, right: Option<Arc<R>>) -> Self::Value;
    fn right(value: &Self::Value) -> Option<&Arc<R>>;
    fn unchanged(value: &Self::Value, left: &Arc<L>, right: Option<&Arc<R>>) -> bool;
}
pub(super) struct Inner;
pub(super) struct Left;
impl<LI: Ord + Clone + 'static, RI: Ord + Clone + 'static, L: 'static, R: 'static>
    Mode<LI, RI, L, R> for Inner
{
    type Id = (LI, RI);
    type Value = Joined<L, R>;
    const OUTER: bool = false;
    fn id(left: &LI, right: Option<&RI>) -> Option<Self::Id> {
        right.map(|r| (left.clone(), r.clone()))
    }
    fn value(left: Arc<L>, right: Option<Arc<R>>) -> Self::Value {
        Joined {
            left,
            right: right.expect("inner output is created only for matched pairs"),
        }
    }
    fn right(value: &Self::Value) -> Option<&Arc<R>> {
        Some(&value.right)
    }
    fn unchanged(value: &Self::Value, left: &Arc<L>, right: Option<&Arc<R>>) -> bool {
        Arc::ptr_eq(&value.left, left) && right.is_some_and(|r| Arc::ptr_eq(&value.right, r))
    }
}
impl<LI: Ord + Clone + 'static, RI: Ord + Clone + 'static, L: 'static, R: 'static>
    Mode<LI, RI, L, R> for Left
{
    type Id = (LI, Option<RI>);
    type Value = LeftJoined<L, R>;
    const OUTER: bool = true;
    fn id(left: &LI, right: Option<&RI>) -> Option<Self::Id> {
        Some((left.clone(), right.cloned()))
    }
    fn value(left: Arc<L>, right: Option<Arc<R>>) -> Self::Value {
        LeftJoined { left, right }
    }
    fn right(value: &Self::Value) -> Option<&Arc<R>> {
        value.right.as_ref()
    }
    fn unchanged(value: &Self::Value, left: &Arc<L>, right: Option<&Arc<R>>) -> bool {
        Arc::ptr_eq(&value.left, left)
            && match (&value.right, right) {
                (None, None) => true,
                (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                _ => false,
            }
    }
}
