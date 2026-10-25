//! Work-count and lifecycle regressions for aggregation and ordered indexes.
use reactive_db::{Database, Error, Poll};
use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;

#[test]
fn group_and_order_updates_are_bounded_by_changes_and_window() -> Result<(), Error> {
    for size in [100, 10_000] {
        let mut db = Database::new();
        let rows = db.create_collection::<u32, i64>("rows")?;
        db.write(|tx| {
            for id in 0..size {
                tx.insert(&rows, id, i64::from(id))?;
            }
            Ok(())
        })?;
        let group_calls = Rc::new(Cell::new(0));
        let value_calls = Rc::new(Cell::new(0));
        let sort_calls = Rc::new(Cell::new(0));
        let (g, v, s) = (group_calls.clone(), value_calls.clone(), sort_calls.clone());
        let stats = db.materialize(
            rows.query()
                .group_by(move |_| {
                    g.set(g.get() + 1);
                })
                .stats_i64(move |value| {
                    v.set(v.get() + 1);
                    *value
                }),
        )?;
        let plan = rows
            .query()
            .sort_by_key(move |value| {
                s.set(s.get() + 1);
                *value
            })
            .take(5);
        let top = db.materialize_ordered(plan.clone())?;
        let _shared = db.materialize_ordered(plan)?;
        assert_eq!(sort_calls.get(), size, "explicit clones reuse the index");
        let (_, mut sub) = top.subscribe(&mut db)?;
        group_calls.set(0);
        value_calls.set(0);
        sort_calls.set(0);
        let commit = db.write(|tx| tx.replace(&rows, size - 1, -1))?;
        assert_eq!(
            (group_calls.get(), value_calls.get(), sort_calls.get()),
            (1, 1, 1)
        );
        assert!(commit.stats.work < 100, "size {size}: {:?}", commit.stats);
        let Poll::Delta(batch) = sub.poll(&db)? else {
            panic!("window transition missing")
        };
        assert_eq!(batch.changes.len(), 2);
        assert_eq!(stats.snapshot(&db)?.rows[0].value.min, -1);
        group_calls.set(0);
        value_calls.set(0);
        sort_calls.set(0);
        let commit = db.write(|tx| tx.remove(&rows, &(size - 1)))?;
        assert_eq!(
            (group_calls.get(), value_calls.get(), sort_calls.get()),
            (0, 0, 0)
        );
        assert!(commit.stats.work < 100);
    }
    Ok(())
}
#[test]
fn full_sort_does_not_recompute_unchanged_keys_or_emit_shift_moves() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u32, i64>("rows")?;
    db.write(|tx| {
        for id in 0..10_000 {
            tx.insert(&rows, id, i64::from(id))?;
        }
        Ok(())
    })?;
    let calls = Rc::new(Cell::new(0));
    let count = calls.clone();
    let view = db.materialize_ordered(rows.query().sort_by_key(move |v| {
        count.set(count.get() + 1);
        *v
    }))?;
    let (_, mut sub) = view.subscribe(&mut db)?;
    calls.set(0);
    let commit = db.write(|tx| tx.replace(&rows, 9_999, -1))?;
    assert_eq!(calls.get(), 1);
    assert!(commit.stats.work < 20);
    let Poll::Delta(batch) = sub.poll(&db)? else {
        panic!("ordered move missing")
    };
    assert_eq!(batch.changes.len(), 1);
    Ok(())
}
#[test]
fn final_ordered_lease_releases_index_parent_and_selectors() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u32, i64>("rows")?;
    db.write(|tx| tx.insert(&rows, 1, 7))?;
    let capture = Rc::new(());
    let weak_capture = Rc::downgrade(&capture);
    let view = db.materialize_ordered(
        rows.query()
            .map(|v| v.to_string())
            .sort_by_key(move |v| {
                let _ = &capture;
                v.len()
            })
            .take(1),
    )?;
    let (snapshot, sub) = view.subscribe(&mut db)?;
    let weak = Arc::downgrade(&snapshot.rows[0].value);
    drop(snapshot);
    drop(view);
    db.write(|_| Ok(()))?;
    assert!(weak.upgrade().is_some());
    drop(sub);
    db.write(|_| Ok(()))?;
    assert!(weak.upgrade().is_none());
    assert!(weak_capture.upgrade().is_none());
    Ok(())
}
