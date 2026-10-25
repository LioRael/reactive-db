//! Independent full-scan oracles and delta-only replay for all M3 operators.
use reactive_db::{
    Change, Database, Epoch, Error, LiveQuery, OrderKey, OrderedChange, OrderedLiveQuery,
    OrderedSubscription, Poll, StatsI64, Subscription,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Item {
    group: u32,
    value: i64,
}
fn relational<I: Ord + Clone + Debug + 'static, T: Eq + Clone + Debug + 'static>(
    db: &Database,
    view: &LiveQuery<I, T>,
    sub: &mut Subscription<I, T>,
    mirror: &mut BTreeMap<I, T>,
    expected: BTreeMap<I, T>,
    epoch: Epoch,
) -> Result<(), Error> {
    match sub.poll(db)? {
        Poll::Delta(batch) => {
            assert_eq!(batch.to_epoch, epoch);
            assert!(batch.from_epoch < epoch);
            let mut seen = BTreeSet::new();
            for c in batch.changes.iter() {
                assert!(seen.insert(c.id()));
                match c {
                    Change::Insert { id, value } => {
                        assert!(mirror.insert(id.clone(), (**value).clone()).is_none())
                    }
                    Change::Remove { id, value } => {
                        assert_eq!(mirror.remove(id), Some((**value).clone()))
                    }
                    Change::Update { id, before, after } => assert_eq!(
                        mirror.insert(id.clone(), (**after).clone()),
                        Some((**before).clone())
                    ),
                    _ => unreachable!(),
                }
            }
            assert!(matches!(sub.poll(db)?, Poll::Idle));
        }
        Poll::Idle => {}
        _ => panic!("prompt consumption must not lag"),
    }
    assert_eq!(*mirror, expected);
    assert_eq!(
        view.snapshot(db)?
            .rows
            .into_iter()
            .map(|r| (r.id, (*r.value).clone()))
            .collect::<BTreeMap<_, _>>(),
        expected
    );
    Ok(())
}
type OrderedMirror = BTreeMap<OrderKey<i64, u32>, (u32, Item)>;
fn ordered(
    db: &Database,
    view: &OrderedLiveQuery<u32, Item, i64>,
    sub: &mut OrderedSubscription<u32, Item, i64>,
    mirror: &mut OrderedMirror,
    expected: &[(u32, Item)],
    epoch: Epoch,
) -> Result<(), Error> {
    match sub.poll(db)? {
        Poll::Delta(batch) => {
            assert_eq!(batch.to_epoch, epoch);
            assert!(batch.from_epoch < epoch);
            let mut seen = BTreeSet::new();
            for c in batch.changes.iter() {
                match c {
                    OrderedChange::Insert { row } => {
                        assert!(seen.insert(row.id));
                        assert_eq!(*row.order.sort_key(), row.value.value);
                        assert!(
                            mirror
                                .insert(row.order.clone(), (row.id, *row.value))
                                .is_none()
                        );
                    }
                    OrderedChange::Remove { row } => {
                        assert!(seen.insert(row.id));
                        assert_eq!(mirror.remove(&row.order), Some((row.id, *row.value)));
                    }
                    OrderedChange::Update { before, after } => {
                        assert_eq!(before.id, after.id);
                        assert!(seen.insert(after.id));
                        assert_eq!(
                            mirror.remove(&before.order),
                            Some((before.id, *before.value))
                        );
                        assert!(
                            mirror
                                .insert(after.order.clone(), (after.id, *after.value))
                                .is_none()
                        );
                    }
                    _ => unreachable!(),
                }
            }
            assert!(matches!(sub.poll(db)?, Poll::Idle));
        }
        Poll::Idle => {}
        _ => panic!("prompt consumption must not lag"),
    }
    assert_eq!(mirror.values().copied().collect::<Vec<_>>(), expected);
    let snapshot = view.snapshot(db)?;
    assert_eq!(snapshot.epoch, epoch);
    assert_eq!(
        snapshot
            .rows
            .iter()
            .map(|r| (r.id, *r.value))
            .collect::<Vec<_>>(),
        expected
    );
    assert!(snapshot.rows.windows(2).all(|p| p[0].order < p[1].order));
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
fn generated_histories_match_independent_full_recomputation() -> Result<(), Error> {
    for seed in 1..=16 {
        let mut rng = Random(seed * 0x9e3779b9);
        let mut db = Database::new();
        let rows = db.create_collection::<u32, Item>("rows")?;
        let counts = db.materialize(rows.query().group_by(|v| v.group).count())?;
        let stats = db.materialize(rows.query().group_by(|v| v.group).stats_i64(|v| v.value))?;
        let distinct = db.materialize(rows.query().distinct_values(|v| v.value))?;
        let sorted = db.materialize_ordered(rows.query().sort_by_key(|v| v.value))?;
        let top = db.materialize_ordered(rows.query().sort_by_key(|v| v.value).take(5))?;
        let zero = db.materialize_ordered(rows.query().sort_by_key(|v| v.value).take(0))?;
        let (_, mut cs) = counts.subscribe(&mut db)?;
        let (_, mut ss) = stats.subscribe(&mut db)?;
        let (_, mut ds) = distinct.subscribe(&mut db)?;
        let (_, mut os) = sorted.subscribe(&mut db)?;
        let (_, mut ts) = top.subscribe(&mut db)?;
        let (_, mut zs) = zero.subscribe(&mut db)?;
        let (mut cm, mut sm, mut dm) = (BTreeMap::new(), BTreeMap::new(), BTreeMap::new());
        let (mut om, mut tm, mut zm) = (
            OrderedMirror::new(),
            OrderedMirror::new(),
            OrderedMirror::new(),
        );
        let mut model = BTreeMap::new();
        for _ in 0..400 {
            let mut next = model.clone();
            let reject = rng.next().is_multiple_of(19);
            let result = db.write(|tx| {
                for _ in 0..1 + rng.next() % 7 {
                    let id = (rng.next() % 24) as u32;
                    if rng.next().is_multiple_of(4) {
                        if next.remove(&id).is_some() {
                            tx.remove(&rows, &id)?;
                        }
                    } else {
                        let value = match rng.next() % 20 {
                            0 => i64::MIN,
                            1 => i64::MAX,
                            n => n as i64 - 10,
                        };
                        let item = Item {
                            group: (rng.next() % 5) as u32,
                            value,
                        };
                        if next.insert(id, item).is_some() {
                            tx.replace(&rows, id, item)?;
                        } else {
                            tx.insert(&rows, id, item)?;
                        }
                    }
                }
                if reject {
                    Err(Error::Validation("oracle rejects whole batch".into()))
                } else {
                    Ok(())
                }
            });
            if reject {
                assert!(matches!(result, Err(Error::Validation(_))))
            } else {
                result?;
                model = next;
            }
            let mut grouped = BTreeMap::<u32, Vec<i64>>::new();
            for item in model.values() {
                grouped.entry(item.group).or_default().push(item.value);
            }
            let expected_counts = grouped.iter().map(|(&g, v)| (g, v.len() as u64)).collect();
            let expected_stats = grouped
                .iter()
                .map(|(&g, v)| {
                    (
                        g,
                        StatsI64 {
                            count: v.len() as u64,
                            sum: v.iter().map(|&v| i128::from(v)).sum(),
                            min: *v.iter().min().unwrap(),
                            max: *v.iter().max().unwrap(),
                        },
                    )
                })
                .collect();
            let expected_distinct = model.values().map(|v| (v.value, v.value)).collect();
            let mut expected_order = model.iter().map(|(&id, &v)| (id, v)).collect::<Vec<_>>();
            expected_order.sort_by_key(|&(id, v)| (v.value, id));
            let epoch = db.epoch()?;
            relational(&db, &counts, &mut cs, &mut cm, expected_counts, epoch)?;
            relational(&db, &stats, &mut ss, &mut sm, expected_stats, epoch)?;
            relational(&db, &distinct, &mut ds, &mut dm, expected_distinct, epoch)?;
            ordered(&db, &sorted, &mut os, &mut om, &expected_order, epoch)?;
            ordered(
                &db,
                &top,
                &mut ts,
                &mut tm,
                &expected_order[..expected_order.len().min(5)],
                epoch,
            )?;
            ordered(&db, &zero, &mut zs, &mut zm, &[], epoch)?;
        }
    }
    Ok(())
}
