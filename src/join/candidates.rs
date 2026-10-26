type Pair<'a, L, R> = (&'a L, Option<&'a R>);

/// Candidate IDs borrow their input indexes/patches until preparation completes.
/// A converging join commonly visits the same single pair from both parents.
pub(super) enum Candidates<'a, L, R> {
    Empty,
    One(Pair<'a, L, R>),
    Many(Vec<Pair<'a, L, R>>),
}
impl<'a, L: Ord, R: Ord> Candidates<'a, L, R> {
    pub fn push(&mut self, pair: Pair<'a, L, R>) {
        match self {
            Self::Empty => *self = Self::One(pair),
            Self::One(first) => {
                if *first != pair {
                    *self = Self::Many(vec![*first, pair]);
                }
            }
            Self::Many(pairs) => pairs.push(pair),
        }
    }
    pub fn into_pairs(self) -> impl Iterator<Item = Pair<'a, L, R>> {
        let (one, many) = match self {
            Self::Empty => (None, Vec::new()),
            Self::One(pair) => (Some(pair), Vec::new()),
            Self::Many(mut pairs) => {
                // Preserve the first ID representation when Ord treats distinct
                // representations as equal, matching BTreeSet insertion behavior.
                pairs.sort();
                pairs.dedup();
                (None, pairs)
            }
        };
        one.into_iter().chain(many)
    }
}
