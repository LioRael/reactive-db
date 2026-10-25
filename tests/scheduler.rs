//! Affected-cone scheduling, atomic rejection, and event-driven lease retirement.
use reactive_db::{Database, Error, ExecutionBudget, Poll};
use std::cell::Cell;
use std::rc::Rc;

#[test]
fn unrelated_graph_width_does_not_consume_write_budget() -> Result<(), Error> {
    let mut expected = None;
    for width in [0, 100, 1000, 10_000] {
        let mut db = Database::new();
        let idle = db.create_collection::<u64, i64>("idle")?;
        let hot = db.create_collection::<u64, i64>("hot")?;
        let views = (0..width)
            .map(|_| db.materialize(idle.query().map(|v| v + 1)))
            .collect::<Result<Vec<_>, _>>()?;
        let live = db.materialize(hot.query().map(|v| v * 2))?;
        db.set_execution_budget(ExecutionBudget {
            max_work: 20,
            ..Default::default()
        })?;
        let commit = db.write(|tx| tx.insert(&hot, 0, 7))?;
        assert_eq!(*live.snapshot(&db)?.rows[0].value, 14);
        assert_eq!(
            commit.stats.work,
            *expected.get_or_insert(commit.stats.work),
            "width={width}"
        );
        assert_eq!(db.write(|_| Ok(()))?.stats.work, 0);
        drop(views);
    }
    Ok(())
}

#[test]
fn empty_delta_prunes_wide_descendants_but_commits_group_maintenance() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u64, i64>("rows")?;
    db.write(|tx| tx.insert(&rows, 1, 10))?;
    let count = rows.query().group_by(|_| ()).count();
    let distinct = rows.query().distinct_values(|_| ());
    let filter = rows.query().filter(|v| *v < 0);
    let mut views = Vec::new();
    for _ in 0..1000 {
        views.push(db.materialize(count.clone().map(|v| *v))?);
    }
    let distinct_view = db.materialize(distinct)?;
    let _filtered = db.materialize(filter.map(|v| *v))?;
    db.set_execution_budget(ExecutionBudget {
        max_work: 40,
        ..Default::default()
    })?;
    // Identity replacement preserves count/distinct output but changes cached membership.
    db.write(|tx| {
        tx.remove(&rows, &1)?;
        tx.insert(&rows, 2, 20)
    })?;
    assert_eq!(*views[0].snapshot(&db)?.rows[0].value, 1);
    assert_eq!(distinct_view.snapshot(&db)?.rows.len(), 1);
    db.set_execution_budget(ExecutionBudget::default())?;
    db.write(|tx| tx.remove(&rows, &2))?;
    assert!(views[0].snapshot(&db)?.rows.is_empty());
    assert!(distinct_view.snapshot(&db)?.rows.is_empty());
    Ok(())
}

#[test]
fn converging_paths_and_self_join_evaluate_once_after_both_parents() -> Result<(), Error> {
    let mut db = Database::new();
    let left = db.create_collection::<u64, i64>("left")?;
    let right = db.create_collection::<u64, i64>("right")?;
    let long = left.query().map(|v| v + 1).map(|v| v + 1);
    let joined = long.join_on(right.query(), |_, v| *v, |_, v| *v);
    let calls = Rc::new(Cell::new(0));
    let c = calls.clone();
    let view = db.materialize(joined.map(move |v| {
        c.set(c.get() + 1);
        *v.left + *v.right
    }))?;
    let self_calls = Rc::new(Cell::new(0));
    let c = self_calls.clone();
    let self_view = db.materialize(
        left.query()
            .join_on(left.query(), |_, v| *v, |_, v| *v)
            .map(move |v| {
                c.set(c.get() + 1);
                *v.left + *v.right
            }),
    )?;
    let (_, mut sub) = view.subscribe(&mut db)?;
    let (_, mut self_sub) = self_view.subscribe(&mut db)?;
    for value in 0..20 {
        let commit = db.write(|tx| {
            if value == 0 {
                tx.insert(&left, 0, value)?;
                tx.insert(&right, 0, value + 2)
            } else {
                tx.replace(&left, 0, value)?;
                tx.replace(&right, 0, value + 2)
            }
        })?;
        assert_eq!(*view.snapshot(&db)?.rows[0].value, 2 * (value + 2));
        assert_eq!(*self_view.snapshot(&db)?.rows[0].value, 2 * value);
        for poll in [sub.poll(&db)?, self_sub.poll(&db)?] {
            let Poll::Delta(delta) = poll else {
                panic!("missing atomic delta")
            };
            assert_eq!(delta.to_epoch, commit.epoch);
            assert_eq!(delta.changes.len(), 1);
        }
    }
    assert_eq!(calls.get(), 20);
    assert_eq!(self_calls.get(), 20);
    Ok(())
}

#[test]
fn scheduling_budget_failure_publishes_nothing_and_retry_is_complete() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u64, i64>("rows")?;
    let calls = Rc::new(Cell::new(0));
    let views = (0..100)
        .map(|_| {
            let c = calls.clone();
            db.materialize(rows.query().map(move |v| {
                c.set(c.get() + 1);
                *v
            }))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let (_, mut sub) = views[0].subscribe(&mut db)?;
    let before = db.epoch()?;
    db.set_execution_budget(ExecutionBudget {
        max_work: 30,
        ..Default::default()
    })?;
    assert!(matches!(
        db.write(|tx| tx.insert(&rows, 1, 7)),
        Err(Error::BudgetExceeded("work"))
    ));
    assert_eq!(
        calls.get(),
        0,
        "fanout is charged before evaluating the queue"
    );
    assert_eq!(db.epoch()?, before);
    assert!(rows.get(&db, &1)?.is_none());
    assert!(matches!(sub.poll(&db)?, Poll::Idle));
    db.set_execution_budget(ExecutionBudget::default())?;
    db.write(|tx| tx.insert(&rows, 1, 7))?;
    assert_eq!(calls.get(), 100);
    assert!(
        views
            .iter()
            .all(|v| v.snapshot(&db).unwrap().rows.len() == 1)
    );
    Ok(())
}

#[test]
fn retirement_drains_destructor_cascades_and_churn_removes_edges() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u64, i64>("rows")?;
    db.write(|tx| tx.insert(&rows, 0, 1))?;
    for _ in 0..30 {
        let marker = Rc::new(());
        let weak = Rc::downgrade(&marker);
        let captured = db.materialize(rows.query().map(move |v| {
            let _ = &marker;
            *v
        }))?;
        let capture_view = db.materialize(rows.query().map(move |v| {
            let _ = &captured;
            *v
        }))?;
        let (_, sub) = capture_view.subscribe(&mut db)?;
        drop(capture_view);
        db.write(|_| Ok(()))?;
        assert!(weak.upgrade().is_some(), "subscription retains plan");
        drop(sub);
        db.write(|_| Ok(()))?;
        assert!(
            weak.upgrade().is_none(),
            "retiring closure releases another query"
        );
        db.set_execution_budget(ExecutionBudget {
            max_work: 10,
            ..Default::default()
        })?;
        db.write(|tx| tx.replace(&rows, 0, 1))?;
        db.set_execution_budget(ExecutionBudget::default())?;
    }
    Ok(())
}

#[test]
fn failed_materialization_does_not_register_partial_dependents() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u64, i64>("rows")?;
    db.write(|tx| tx.insert(&rows, 0, 1))?;
    let query = rows.query().map(|v| v + 1).map(|v| v * 2);
    db.set_execution_budget(ExecutionBudget {
        max_output_changes: 1,
        ..Default::default()
    })?;
    assert!(matches!(
        db.materialize(query.clone()),
        Err(Error::BudgetExceeded(_))
    ));
    db.set_execution_budget(ExecutionBudget {
        max_work: 10,
        ..Default::default()
    })?;
    db.write(|tx| tx.replace(&rows, 0, 2))?;
    db.set_execution_budget(ExecutionBudget::default())?;
    let view = db.materialize(query)?;
    assert_eq!(*view.snapshot(&db)?.rows[0].value, 6);
    db.write(|tx| tx.replace(&rows, 0, 3))?;
    assert_eq!(*view.snapshot(&db)?.rows[0].value, 8);
    Ok(())
}

#[test]
fn failure_after_consuming_part_of_dense_queue_can_retry_and_retire() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u64, i64>("rows")?;
    let calls = Rc::new(Cell::new(0));
    let views = (0..128)
        .map(|_| {
            let calls = calls.clone();
            db.materialize(rows.query().map(move |v| {
                calls.set(calls.get() + 1);
                *v
            }))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let (_, mut subscription) = views[0].subscribe(&mut db)?;
    let before = db.epoch()?;
    db.set_execution_budget(ExecutionBudget {
        max_work: 200,
        ..Default::default()
    })?;
    assert!(matches!(
        db.write(|tx| tx.insert(&rows, 0, 3)),
        Err(Error::BudgetExceeded("work"))
    ));
    assert!(calls.get() > 0 && calls.get() < 128);
    assert_eq!(db.epoch()?, before);
    assert!(rows.get(&db, &0)?.is_none());
    for view in &views {
        assert!(view.snapshot(&db)?.rows.is_empty());
    }
    assert!(matches!(subscription.poll(&db)?, Poll::Idle));
    calls.set(0);
    db.set_execution_budget(ExecutionBudget::default())?;
    db.write(|tx| tx.insert(&rows, 0, 3))?;
    assert_eq!(calls.get(), 128);
    drop(views);
    drop(subscription);
    db.write(|_| Ok(()))?;
    calls.set(0);
    db.write(|tx| tx.replace(&rows, 0, 4))?;
    assert_eq!(calls.get(), 0);
    Ok(())
}

#[test]
fn dense_shared_parent_joins_publish_once_per_node_after_both_inputs() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u64, i64>("rows")?;
    let left = rows.query().map(|v| v + 1);
    let right = rows.query().map(|v| v + 2).map(|v| v - 1);
    let calls = Rc::new(Cell::new(0));
    let views = (0..128)
        .map(|_| {
            let calls = calls.clone();
            db.materialize(
                left.clone()
                    .join_on(right.clone(), |_, v| *v, |_, v| *v)
                    .map(move |v| {
                        calls.set(calls.get() + 1);
                        *v.left + *v.right
                    }),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    for value in 0..10 {
        calls.set(0);
        db.write(|tx| {
            if value == 0 {
                tx.insert(&rows, 0, value)
            } else {
                tx.replace(&rows, 0, value)
            }
        })?;
        assert_eq!(calls.get(), 128);
        for view in &views {
            assert_eq!(*view.snapshot(&db)?.rows[0].value, 2 * (value + 1));
        }
    }
    Ok(())
}

#[test]
fn failed_dense_stages_release_payloads_before_retry_and_after_retirement() -> Result<(), Error> {
    struct Tracked(Rc<Cell<usize>>);
    impl Drop for Tracked {
        fn drop(&mut self) {
            self.0.set(self.0.get() - 1);
        }
    }
    let mut db = Database::new();
    let source = db.create_collection::<u64, i64>("source")?;
    let live = Rc::new(Cell::new(0));
    let views = (0..1000)
        .map(|_| {
            let live = live.clone();
            db.materialize(source.query().map(move |_| {
                live.set(live.get() + 1);
                Tracked(live.clone())
            }))
        })
        .collect::<Result<Vec<_>, _>>()?;
    db.set_execution_budget(ExecutionBudget {
        max_output_changes: 500,
        ..Default::default()
    })?;
    assert!(matches!(
        db.write(|tx| tx.insert(&source, 0, 1)),
        Err(Error::BudgetExceeded("output changes"))
    ));
    assert_eq!(
        live.get(),
        0,
        "failed sparse/hash stages release all prepared payloads"
    );
    assert!(source.get(&db, &0)?.is_none());
    db.set_execution_budget(ExecutionBudget::default())?;
    db.write(|tx| tx.insert(&source, 0, 1))?;
    assert_eq!(live.get(), 1000);
    drop(views);
    db.write(|_| Ok(()))?;
    assert_eq!(
        live.get(),
        0,
        "runtime rows and journals release on retirement"
    );
    Ok(())
}
