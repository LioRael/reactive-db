//! Ownership, sharing and fail-stop panic contracts.

use reactive_db::{Database, Error, Poll};
use std::cell::Cell;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::rc::Rc;
use std::sync::Arc;

#[test]
fn shared_parent_executes_once_and_subscription_keeps_query_alive() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u64, i64>("rows")?;
    let calls = Rc::new(Cell::new(0));
    let count = calls.clone();
    let shared = rows.query().map(move |v| {
        count.set(count.get() + 1);
        v * 2
    });
    let left = db.materialize(shared.clone().map(|v| v + 1))?;
    let right = db.materialize(shared.clone().map(|v| v + 2))?;
    let (_, mut subscription) = left.subscribe(&mut db)?;
    drop(left);
    drop(shared);
    db.write(|tx| tx.insert(&rows, 1, 10))?;
    assert_eq!(calls.get(), 1, "explicit shared parent runs once");
    assert_eq!(*right.snapshot(&db)?.rows[0].value, 22);
    assert!(matches!(subscription.poll(&db)?, Poll::Delta(_)));
    Ok(())
}

#[test]
fn final_lease_drop_releases_state_and_callbacks_on_next_mutable_operation() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u64, i64>("rows")?;
    db.write(|tx| tx.insert(&rows, 1, 10))?;
    let closure_token = Arc::new(());
    let weak_closure = Arc::downgrade(&closure_token);
    let captured = closure_token.clone();
    let view = db.materialize(rows.query().map(move |v| {
        let _ = &captured;
        format!("value {v}")
    }))?;
    drop(closure_token);
    let (snapshot, sub) = view.subscribe(&mut db)?;
    let weak_payload = Arc::downgrade(&snapshot.rows[0].value);
    drop(snapshot);
    drop(view);
    db.write(|_| Ok(()))?;
    assert!(
        weak_payload.upgrade().is_some(),
        "subscription retains the view"
    );
    drop(sub);
    db.write(|_| Ok(()))?;
    assert!(
        weak_payload.upgrade().is_none(),
        "retired state must release cached payloads"
    );
    assert!(
        weak_closure.upgrade().is_none(),
        "retired evaluator must release captures"
    );
    assert_eq!(
        *rows.get(&db, &1)?.unwrap(),
        10,
        "source stays owned by the database"
    );
    Ok(())
}

#[test]
fn database_drop_releases_source_even_when_handles_survive() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u64, String>("rows")?;
    db.write(|tx| tx.insert(&rows, 1, "owned".into()))?;
    let payload = rows.get(&db, &1)?.unwrap();
    let weak = Arc::downgrade(&payload);
    drop(payload);
    drop(db);
    assert!(weak.upgrade().is_none());
    let another = Database::new();
    assert_eq!(rows.get(&another, &1), Err(Error::DatabaseDropped));
    Ok(())
}

#[test]
fn application_write_panic_propagates_and_poison_is_per_database() -> Result<(), Error> {
    let mut db = Database::new();
    let mut healthy = Database::new();
    let rows = db.create_collection::<u64, i64>("rows")?;
    let view = db.materialize(rows.query())?;
    let (_, mut sub) = view.subscribe(&mut db)?;
    let panic = catch_unwind(AssertUnwindSafe(|| {
        let _ = db.write(|tx| {
            tx.insert(&rows, 1, 5)?;
            panic!("application failure")
        });
    }));
    assert!(panic.is_err());
    assert_eq!(db.epoch(), Err(Error::Poisoned));
    assert_eq!(rows.get(&db, &1), Err(Error::Poisoned));
    assert!(matches!(view.snapshot(&db), Err(Error::Poisoned)));
    assert!(matches!(sub.poll(&db), Err(Error::Poisoned)));
    assert_eq!(db.write(|_| Ok(())), Err(Error::Poisoned));
    assert!(healthy.write(|_| Ok(())).is_ok());
    Ok(())
}

#[test]
fn execution_panic_never_resumes_database() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u64, i64>("rows")?;
    let view = db.materialize(rows.query().map(|v| {
        assert_ne!(*v, 99, "projection rejected 99");
        v * 2
    }))?;
    db.write(|tx| tx.insert(&rows, 1, 10))?;
    let before = view.snapshot(&db)?;
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let _ = db.write(|tx| tx.replace(&rows, 1, 99));
        }))
        .is_err()
    );
    assert_eq!(
        *before.rows[0].value, 20,
        "already acquired snapshot survives poison"
    );
    assert!(matches!(view.snapshot(&db), Err(Error::Poisoned)));
    Ok(())
}

#[test]
fn initial_materialization_panic_poison_is_explicit() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u64, i64>("rows")?;
    db.write(|tx| tx.insert(&rows, 1, 10))?;
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let _ = db.materialize(
                rows.query()
                    .map(|_| -> i64 { panic!("initial projection failure") }),
            );
        }))
        .is_err()
    );
    assert_eq!(db.epoch(), Err(Error::Poisoned));
    Ok(())
}
