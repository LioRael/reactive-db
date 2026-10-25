//! Cursor initialization, retention and resynchronization contracts.

use reactive_db::{Change, Database, Error, JournalLimits, Poll, ViewOptions};

#[test]
fn initial_snapshot_cursor_and_independent_subscribers() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u64, i64>("rows")?;
    db.write(|tx| tx.insert(&rows, 1, 10))?;
    let view = db.materialize(rows.query())?;
    let (initial, mut a) = view.subscribe(&mut db)?;
    let (_, mut b) = view.subscribe(&mut db)?;
    assert_eq!(initial.epoch.number(), 1);
    assert_eq!(*initial.rows[0].value, 10);
    assert!(matches!(a.poll(&db)?, Poll::Idle));
    db.write(|tx| tx.replace(&rows, 1, 20))?;
    db.write(|tx| tx.replace(&rows, 1, 30))?;
    let Poll::Delta(first) = a.poll(&db)? else {
        panic!("expected first batch")
    };
    assert_eq!(first.from_epoch, initial.epoch);
    assert_eq!(first.to_epoch.number(), 2);
    let Poll::Delta(second) = a.poll(&db)? else {
        panic!("expected second batch")
    };
    assert_eq!(second.from_epoch, first.to_epoch);
    assert!(
        matches!(&second.changes[0], Change::Update { before, after, .. } if **before==20 && **after==30)
    );
    let Poll::Delta(independent) = b.poll(&db)? else {
        panic!("expected independent first batch")
    };
    assert_eq!(independent.to_epoch, first.to_epoch);
    assert!(std::sync::Arc::ptr_eq(&independent.changes, &first.changes));
    Ok(())
}

#[test]
fn lag_is_sticky_until_resync_and_writes_continue() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u64, i64>("rows")?;
    let view = db.materialize_with_options(
        rows.query(),
        ViewOptions {
            journal: JournalLimits::new(1, 10)?,
        },
    )?;
    let (_, mut fast) = view.subscribe(&mut db)?;
    let (_, mut slow) = view.subscribe(&mut db)?;
    db.write(|tx| tx.insert(&rows, 1, 10))?;
    assert!(matches!(fast.poll(&db)?, Poll::Delta(_)));
    db.write(|tx| tx.replace(&rows, 1, 20))?;
    assert!(matches!(slow.poll(&db)?, Poll::Lagged { .. }));
    assert!(matches!(slow.poll(&db)?, Poll::Lagged { .. }));
    assert!(matches!(fast.poll(&db)?, Poll::Delta(_)));
    let replacement = slow.resync(&mut db)?;
    assert_eq!(*replacement.rows[0].value, 20);
    assert!(matches!(slow.poll(&db)?, Poll::Idle));
    db.write(|tx| tx.replace(&rows, 1, 30))?;
    let Poll::Delta(next) = slow.poll(&db)? else {
        panic!("resynced cursor should resume")
    };
    assert_eq!(next.from_epoch, replacement.epoch);
    Ok(())
}

#[test]
fn oversized_batch_creates_explicit_gap_not_partial_delivery() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u64, i64>("rows")?;
    let view = db.materialize_with_options(
        rows.query(),
        ViewOptions {
            journal: JournalLimits::new(10, 2)?,
        },
    )?;
    let (_, mut sub) = view.subscribe(&mut db)?;
    let commit = db.write(|tx| {
        for i in 0..3 {
            tx.insert(&rows, i, i as i64)?;
        }
        Ok(())
    })?;
    assert!(matches!(sub.poll(&db)?, Poll::Lagged { current, .. } if current==commit.epoch));
    let (snapshot, mut fresh) = view.subscribe(&mut db)?;
    assert_eq!(snapshot.rows.len(), 3);
    assert!(matches!(fresh.poll(&db)?, Poll::Idle));
    assert_eq!(sub.resync(&mut db)?.rows.len(), 3);
    db.write(|tx| tx.remove(&rows, &0))?;
    assert!(matches!(sub.poll(&db)?, Poll::Delta(_)));
    Ok(())
}

#[test]
fn change_capacity_evicts_whole_batches() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u64, i64>("rows")?;
    let view = db.materialize_with_options(
        rows.query(),
        ViewOptions {
            journal: JournalLimits::new(10, 3)?,
        },
    )?;
    let (_, mut sub) = view.subscribe(&mut db)?;
    db.write(|tx| {
        tx.insert(&rows, 1, 1)?;
        tx.insert(&rows, 2, 2)
    })?;
    db.write(|tx| {
        tx.insert(&rows, 3, 3)?;
        tx.insert(&rows, 4, 4)
    })?;
    assert!(matches!(sub.poll(&db)?, Poll::Lagged { .. }));
    assert_eq!(view.snapshot(&db)?.rows.len(), 4);
    Ok(())
}

#[test]
fn unrelated_and_filtered_epochs_are_not_mistaken_for_lost_events() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u64, i64>("rows")?;
    let other = db.create_collection::<u64, i64>("other")?;
    let view = db.materialize(rows.query().filter(|v| *v > 10))?;
    let (_, mut sub) = view.subscribe(&mut db)?;
    db.write(|tx| tx.insert(&rows, 1, 0))?;
    db.write(|tx| tx.insert(&other, 1, 42))?;
    assert!(matches!(sub.poll(&db)?, Poll::Idle));
    db.write(|tx| tx.replace(&rows, 1, 20))?;
    let Poll::Delta(batch) = sub.poll(&db)? else {
        panic!("gap in database epochs is not lost view history")
    };
    assert_eq!(batch.from_epoch.number(), 0);
    assert_eq!(batch.to_epoch.number(), 3);
    Ok(())
}

#[test]
fn shared_view_options_cannot_silently_change() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u64, i64>("rows")?;
    let plan = rows.query().map(|v| v * 2);
    let _first = db.materialize(plan.clone())?;
    assert!(matches!(
        db.materialize_with_options(
            plan.clone(),
            ViewOptions {
                journal: JournalLimits::new(1, 1)?
            }
        ),
        Err(Error::ViewOptionsConflict)
    ));
    assert!(db.materialize(plan).is_ok());
    assert_eq!(JournalLimits::new(0, 1), Err(Error::InvalidJournalLimits));
    assert_eq!(JournalLimits::new(1, 0), Err(Error::InvalidJournalLimits));
    Ok(())
}

#[test]
fn eviction_releases_old_payloads_without_a_consumer_pin() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u64, String>("rows")?;
    let view = db.materialize_with_options(
        rows.query(),
        ViewOptions {
            journal: JournalLimits::new(1, 1)?,
        },
    )?;
    let (_, mut slow) = view.subscribe(&mut db)?;
    db.write(|tx| tx.insert(&rows, 1, "first".into()))?;
    let first = rows.get(&db, &1)?.unwrap();
    let weak = std::sync::Arc::downgrade(&first);
    drop(first);
    db.write(|tx| tx.replace(&rows, 1, "second".into()))?;
    assert!(
        weak.upgrade().is_some(),
        "the retained update carries its before payload"
    );
    db.write(|tx| tx.replace(&rows, 1, "third".into()))?;
    assert!(
        weak.upgrade().is_none(),
        "a slow cursor must not pin evicted values"
    );
    assert!(matches!(slow.poll(&db)?, Poll::Lagged { .. }));
    Ok(())
}
