//! Boundary contracts, atomic execution limits and ordered history.
use reactive_db::{
    Database, Error, ExecutionBudget, JournalLimits, OrderedChange, Poll, StatsI64, ViewOptions,
};
use std::cmp::Reverse;

#[test]
fn initial_stats_duplicates_extremes_and_last_removal() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u32, i64>("rows")?;
    db.write(|tx| {
        tx.insert(&rows, 1, i64::MIN)?;
        tx.insert(&rows, 2, i64::MIN)?;
        tx.insert(&rows, 3, i64::MAX)
    })?;
    let stats = db.materialize(rows.query().group_by(|_| ()).stats_i64(|v| *v))?;
    let distinct = db.materialize(rows.query().distinct_values(|v| *v))?;
    let (_, mut sub) = distinct.subscribe(&mut db)?;
    assert_eq!(
        *stats.snapshot(&db)?.rows[0].value,
        StatsI64 {
            count: 3,
            sum: 2 * i128::from(i64::MIN) + i128::from(i64::MAX),
            min: i64::MIN,
            max: i64::MAX
        }
    );
    db.write(|tx| tx.remove(&rows, &1))?;
    assert!(matches!(sub.poll(&db)?, Poll::Idle));
    db.write(|tx| tx.remove(&rows, &2))?;
    assert_eq!(stats.snapshot(&db)?.rows[0].value.min, i64::MAX);
    db.write(|tx| tx.remove(&rows, &3))?;
    assert!(stats.snapshot(&db)?.rows.is_empty());
    Ok(())
}
#[test]
fn join_aggregate_distinct_and_topk_publish_one_final_epoch() -> Result<(), Error> {
    let mut db = Database::new();
    let a = db.create_collection::<u32, i64>("a")?;
    let b = db.create_collection::<u32, i64>("b")?;
    let joined = a.query().join_on(b.query(), |_, _| (), |_, _| ());
    let groups = joined.group_by(|p| *p.left).stats_i64(|p| *p.right);
    let stats = db.materialize(groups.clone())?;
    let ordered = db.materialize_ordered(groups.sort_by_key(|s| Reverse(s.sum)).take(1))?;
    let (_, mut sub) = ordered.subscribe(&mut db)?;
    let commit = db.write(|tx| {
        tx.insert(&a, 1, 7)?;
        tx.insert(&a, 2, 8)?;
        tx.insert(&b, 1, 10)?;
        tx.insert(&b, 2, 20)
    })?;
    let Poll::Delta(batch) = sub.poll(&db)? else {
        panic!("expected ordered publication")
    };
    assert_eq!(batch.to_epoch, commit.epoch);
    assert_eq!(stats.snapshot(&db)?.rows.len(), 2);
    assert_eq!(ordered.snapshot(&db)?.rows[0].id, 7);
    db.write(|tx| {
        tx.replace(&a, 1, 8)?;
        tx.replace(&b, 2, 30)
    })?;
    let row = &ordered.snapshot(&db)?.rows[0];
    assert_eq!(row.id, 8);
    assert_eq!(row.value.count, 4);
    assert_eq!(row.value.sum, 80);
    Ok(())
}
#[test]
fn ordered_ties_moves_take_composition_and_lag_resync() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u32, i64>("rows")?;
    let q = rows.query().sort_by_key(|v| *v).take(3).take(2);
    let options = ViewOptions {
        journal: JournalLimits::new(1, 2)?,
    };
    let view = db.materialize_ordered_with_options(q.clone(), options)?;
    assert!(matches!(
        db.materialize_ordered(q),
        Err(Error::ViewOptionsConflict)
    ));
    let (_, mut slow) = view.subscribe(&mut db)?;
    db.write(|tx| {
        tx.insert(&rows, 3, 5)?;
        tx.insert(&rows, 2, 5)?;
        tx.insert(&rows, 1, 5)
    })?;
    assert_eq!(
        view.snapshot(&db)?
            .rows
            .iter()
            .map(|r| r.id)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    let (_, mut fast) = view.subscribe(&mut db)?;
    db.write(|tx| tx.replace(&rows, 1, 4))?;
    let Poll::Delta(batch) = fast.poll(&db)? else {
        panic!("move missing")
    };
    assert_eq!(batch.changes.len(), 1);
    assert!(
        matches!(&batch.changes[0],OrderedChange::Update {before,after} if *before.order.sort_key()==5 && *after.order.sort_key()==4)
    );
    assert!(matches!(slow.poll(&db)?, Poll::Lagged { .. }));
    assert_eq!(slow.resync(&mut db)?.rows.len(), 2);
    assert!(matches!(slow.poll(&db)?, Poll::Idle));
    db.write(|tx| tx.replace(&rows, 3, 0))?;
    let Poll::Delta(batch) = fast.poll(&db)? else {
        panic!("window boundary missing")
    };
    assert_eq!(
        batch.changes.len(),
        2,
        "one entry, one exit; retained row does not move"
    );
    Ok(())
}
#[test]
fn budgets_reject_before_atomic_publication_and_database_can_retry() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u32, i64>("rows")?;
    let stats = db.materialize(rows.query().group_by(|_| ()).stats_i64(|v| *v))?;
    let top = db.materialize_ordered(rows.query().sort_by_key(|v| *v).take(2))?;
    let (_, mut sub) = top.subscribe(&mut db)?;
    for budget in [
        ExecutionBudget {
            max_input_changes: 1,
            ..Default::default()
        },
        ExecutionBudget {
            max_output_changes: 2,
            ..Default::default()
        },
        ExecutionBudget {
            max_work: 2,
            ..Default::default()
        },
    ] {
        db.set_execution_budget(budget)?;
        let epoch = db.epoch()?;
        assert!(matches!(
            db.write(|tx| {
                tx.insert(&rows, 1, 3)?;
                tx.insert(&rows, 2, 4)
            }),
            Err(Error::BudgetExceeded(_))
        ));
        assert_eq!(db.epoch()?, epoch);
        assert!(rows.get(&db, &1)?.is_none());
        assert!(stats.snapshot(&db)?.rows.is_empty());
        assert!(top.snapshot(&db)?.rows.is_empty());
        assert!(matches!(sub.poll(&db)?, Poll::Idle));
    }
    db.set_execution_budget(Default::default())?;
    db.write(|tx| {
        tx.insert(&rows, 1, 3)?;
        tx.insert(&rows, 2, 4)
    })?;
    assert_eq!(stats.snapshot(&db)?.rows[0].value.sum, 7);
    assert_eq!(top.snapshot(&db)?.rows.len(), 2);
    Ok(())
}
#[test]
fn ignored_budget_failure_is_sticky_and_failed_initial_build_is_retryable() -> Result<(), Error> {
    let mut db = Database::with_budget(ExecutionBudget {
        max_input_changes: 0,
        ..Default::default()
    });
    let rows = db.create_collection::<u32, i64>("rows")?;
    assert_eq!(
        db.write(|tx| {
            let _ = tx.insert(&rows, 1, 1);
            Ok(())
        }),
        Err(Error::InvalidBatch)
    );
    db.set_execution_budget(Default::default())?;
    db.write(|tx| {
        for id in 0..10 {
            tx.insert(&rows, id, i64::from(id))?;
        }
        Ok(())
    })?;
    let q = rows.query().group_by(|v| *v).count();
    let sorted = rows.query().sort_by_key(|v| *v);
    db.set_execution_budget(ExecutionBudget {
        max_output_changes: 2,
        ..Default::default()
    })?;
    assert!(matches!(
        db.materialize(q.clone()),
        Err(Error::BudgetExceeded(_))
    ));
    assert!(matches!(
        db.materialize_ordered(sorted.clone()),
        Err(Error::BudgetExceeded(_))
    ));
    db.set_execution_budget(Default::default())?;
    assert_eq!(db.materialize(q)?.snapshot(&db)?.rows.len(), 10);
    assert_eq!(
        db.materialize_ordered(sorted)?.snapshot(&db)?.rows.len(),
        10
    );
    Ok(())
}
#[test]
fn join_fanout_is_budgeted_and_no_partial_indexes_survive() -> Result<(), Error> {
    let mut db = Database::new();
    let a = db.create_collection::<u32, i64>("a")?;
    let b = db.create_collection::<u32, i64>("b")?;
    let join = db.materialize(a.query().join_on(b.query(), |_, _| (), |_, _| ()))?;
    db.write(|tx| {
        for i in 0..100 {
            tx.insert(&a, i, 1)?;
        }
        Ok(())
    })?;
    db.set_execution_budget(ExecutionBudget {
        max_work: 30,
        ..Default::default()
    })?;
    assert!(matches!(
        db.write(|tx| tx.insert(&b, 1, 1)),
        Err(Error::BudgetExceeded("work"))
    ));
    assert!(join.snapshot(&db)?.rows.is_empty());
    assert!(b.get(&db, &1)?.is_none());
    db.set_execution_budget(Default::default())?;
    db.write(|tx| tx.insert(&b, 1, 1))?;
    assert_eq!(join.snapshot(&db)?.rows.len(), 100);
    Ok(())
}

#[test]
fn budget_rejection_preserves_existing_aggregate_and_order_indexes() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u32, i64>("rows")?;
    let stats = db.materialize(rows.query().group_by(|_| ()).stats_i64(|v| *v))?;
    let top = db.materialize_ordered(rows.query().sort_by_key(|v| *v).take(2))?;
    db.write(|tx| {
        tx.insert(&rows, 1, 1)?;
        tx.insert(&rows, 2, 2)
    })?;
    let (old, mut sub) = top.subscribe(&mut db)?;
    let epoch = db.epoch()?;
    db.set_execution_budget(ExecutionBudget {
        max_output_changes: 3,
        ..Default::default()
    })?;
    assert!(matches!(
        db.write(|tx| {
            tx.replace(&rows, 1, 20)?;
            tx.replace(&rows, 2, 10)
        }),
        Err(Error::BudgetExceeded("output changes"))
    ));
    assert_eq!(db.epoch()?, epoch);
    assert_eq!(stats.snapshot(&db)?.rows[0].value.sum, 3);
    assert_eq!(top.snapshot(&db)?, old);
    assert!(matches!(sub.poll(&db)?, Poll::Idle));
    db.set_execution_budget(Default::default())?;
    db.write(|tx| {
        tx.replace(&rows, 1, 20)?;
        tx.replace(&rows, 2, 10)
    })?;
    assert_eq!(stats.snapshot(&db)?.rows[0].value.min, 10);
    assert_eq!(
        top.snapshot(&db)?
            .rows
            .iter()
            .map(|r| r.id)
            .collect::<Vec<_>>(),
        [2, 1]
    );
    db.write(|tx| tx.remove(&rows, &2))?;
    assert_eq!(stats.snapshot(&db)?.rows[0].value.min, 20);
    assert_eq!(top.snapshot(&db)?.rows[0].id, 1);
    Ok(())
}

#[test]
fn ordered_oversized_batch_is_a_gap_and_selector_panics_are_fail_stop() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u32, i64>("rows")?;
    let view = db.materialize_ordered_with_options(
        rows.query().sort_by_key(|v| {
            assert_ne!(*v, 99, "invalid application sort key");
            *v
        }),
        ViewOptions {
            journal: JournalLimits::new(4, 1)?,
        },
    )?;
    let (_, mut sub) = view.subscribe(&mut db)?;
    db.write(|tx| {
        tx.insert(&rows, 1, 1)?;
        tx.insert(&rows, 2, 2)
    })?;
    assert!(matches!(sub.poll(&db)?, Poll::Lagged { .. }));
    let snapshot = sub.resync(&mut db)?;
    assert_eq!(snapshot.rows.len(), 2);
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || db.write(|tx| tx.replace(&rows, 1, 99))
        ))
        .is_err()
    );
    assert_eq!(*snapshot.rows[0].value, 1);
    assert!(matches!(sub.poll(&db), Err(Error::Poisoned)));
    Ok(())
}
