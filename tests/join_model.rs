//! Independent nested-loop oracle: no production index or join helper is reused.
use reactive_db::{Change, Database, Epoch, Error, LiveQuery, Poll, Subscription};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;

type Input = BTreeMap<u32, (u32, u64)>;
type Value = ((u32, u64), Option<(u32, u64)>);
type Output = BTreeMap<(u32, Option<u32>), Value>;

fn recompute(left: &Input, right: &Input, offset: u32) -> (Output, Output) {
    let mut inner = Output::new();
    let mut outer = Output::new();
    for (&li, &l) in left {
        let mut matched = false;
        for (&ri, &r) in right {
            if (l.0 + offset) % 5 == r.0 {
                inner.insert((li, Some(ri)), (l, Some(r)));
                outer.insert((li, Some(ri)), (l, Some(r)));
                matched = true;
            }
        }
        if !matched {
            outer.insert((li, None), (l, None));
        }
    }
    (inner, outer)
}
fn verify<I: Ord + Clone + Debug + 'static>(
    db: &Database,
    view: &LiveQuery<I, Value>,
    sub: &mut Subscription<I, Value>,
    mirror: &mut BTreeMap<I, Value>,
    expected: BTreeMap<I, Value>,
    epoch: Epoch,
) -> Result<(), Error> {
    match sub.poll(db)? {
        Poll::Delta(batch) => {
            assert_eq!(batch.to_epoch, epoch);
            assert!(batch.from_epoch < batch.to_epoch);
            let mut seen = BTreeSet::new();
            for change in batch.changes.iter() {
                assert!(
                    seen.insert(change.id()),
                    "duplicate identity in one publication"
                );
                match change {
                    Change::Insert { id, value } => {
                        assert!(mirror.insert(id.clone(), **value).is_none());
                    }
                    Change::Update { id, before, after } => {
                        assert_eq!(mirror.insert(id.clone(), **after), Some(**before));
                    }
                    Change::Remove { id, value } => {
                        assert_eq!(mirror.remove(id), Some(**value));
                    }
                    _ => unreachable!("new Change variant requires model update"),
                }
            }
            assert!(
                matches!(sub.poll(db)?, Poll::Idle),
                "one publication per commit"
            );
        }
        Poll::Idle => {}
        _ => panic!("small promptly consumed batches must never lag"),
    }
    assert_eq!(
        *mirror, expected,
        "delta replay diverged from full recomputation"
    );
    let snapshot = view.snapshot(db)?;
    assert_eq!(snapshot.epoch, epoch);
    assert_eq!(
        snapshot
            .rows
            .into_iter()
            .map(|r| (r.id, *r.value))
            .collect::<BTreeMap<_, _>>(),
        expected
    );
    Ok(())
}
struct Random(u64);
impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

#[test]
fn random_cross_collection_and_self_joins_match_full_recomputation() -> Result<(), Error> {
    for seed in 1..=16 {
        let mut random = Random(seed * 0x9e3779b9);
        let mut db = Database::new();
        let l = db.create_collection::<u32, (u32, u64)>("left")?;
        let r = db.create_collection::<u32, (u32, u64)>("right")?;
        let inner = db.materialize(
            l.query()
                .join_on(r.query(), |_, v| v.0, |_, v| v.0)
                .map(|p| (*p.left, Some(*p.right))),
        )?;
        let outer = db.materialize(
            l.query()
                .left_join_on(r.query(), |_, v| v.0, |_, v| v.0)
                .map(|p| (*p.left, p.right.as_deref().copied())),
        )?;
        let self_inner = db.materialize(
            l.query()
                .join_on(l.query(), |_, v| (v.0 + 1) % 5, |_, v| v.0)
                .map(|p| (*p.left, Some(*p.right))),
        )?;
        let self_outer = db.materialize(
            l.query()
                .left_join_on(l.query(), |_, v| (v.0 + 1) % 5, |_, v| v.0)
                .map(|p| (*p.left, p.right.as_deref().copied())),
        )?;
        let (_, mut si) = inner.subscribe(&mut db)?;
        let (_, mut so) = outer.subscribe(&mut db)?;
        let (_, mut ssi) = self_inner.subscribe(&mut db)?;
        let (_, mut sso) = self_outer.subscribe(&mut db)?;
        let (mut mi, mut mo, mut msi, mut mso) = (
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
        );
        let (mut left, mut right) = (Input::new(), Input::new());
        for step in 0..400 {
            let (mut next_left, mut next_right) = (left.clone(), right.clone());
            let reject = random.next().is_multiple_of(17);
            let operations = 1 + random.next() % 8;
            let before_epoch = db.epoch()?;
            let result = db.write(|tx| {
                for operation in 0..operations {
                    let (source, model) = if random.next().is_multiple_of(2) {
                        (&l, &mut next_left)
                    } else {
                        (&r, &mut next_right)
                    };
                    let id = (random.next() % 12) as u32;
                    if random.next().is_multiple_of(4) {
                        if model.remove(&id).is_some() {
                            tx.remove(source, &id)?;
                        }
                    } else {
                        let value = (
                            (random.next() % 5) as u32,
                            seed * 100_000 + step * 10 + operation,
                        );
                        if model.insert(id, value).is_some() {
                            tx.replace(source, id, value)?;
                        } else {
                            tx.insert(source, id, value)?;
                        }
                    }
                }
                if reject {
                    Err(Error::Validation("oracle rollback".into()))
                } else {
                    Ok(())
                }
            });
            if reject {
                assert!(matches!(result, Err(Error::Validation(_))));
                assert_eq!(db.epoch()?, before_epoch);
            } else {
                result?;
                left = next_left;
                right = next_right;
            }
            let epoch = db.epoch()?;
            let (i, o) = recompute(&left, &right, 0);
            let (s_i, s_o) = recompute(&left, &left, 1);
            let unoption = |map: Output| {
                map.into_iter()
                    .map(|((l, r), v)| ((l, r.expect("inner always matched")), v))
                    .collect()
            };
            verify(&db, &inner, &mut si, &mut mi, unoption(i), epoch)?;
            verify(&db, &outer, &mut so, &mut mo, o, epoch)?;
            verify(&db, &self_inner, &mut ssi, &mut msi, unoption(s_i), epoch)?;
            verify(&db, &self_outer, &mut sso, &mut mso, s_o, epoch)?;
        }
    }
    Ok(())
}
