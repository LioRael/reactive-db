//! Public typed collection and publication contracts.

use reactive_db::{Change, Database, Error, Poll};
use std::cell::Cell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

struct Payload {
    score: f64,
    fields: HashMap<String, i64>,
}

#[test]
fn non_clone_non_ord_payload_and_snapshots_work_end_to_end() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u64, Payload>("rows")?;
    let source = db.materialize(rows.query())?;
    let filtered = db.materialize(
        rows.query()
            .filter(|v| v.score > 0.5)
            .map(|v| v.fields["value"]),
    )?;
    let (_, mut subscription) = filtered.subscribe(&mut db)?;
    db.write(|tx| {
        tx.insert(
            &rows,
            1,
            Payload {
                score: 0.75,
                fields: HashMap::from([("value".into(), 7)]),
            },
        )
    })?;
    let snapshot = source.snapshot(&db)?;
    let cloned = snapshot.clone();
    assert!(Arc::ptr_eq(&snapshot.rows[0].value, &cloned.rows[0].value));
    let Poll::Delta(batch) = subscription.poll(&db)? else {
        panic!("expected initial delta")
    };
    let batch_clone = batch.clone();
    assert!(Arc::ptr_eq(&batch.changes, &batch_clone.changes));
    assert!(matches!(&batch.changes[0], Change::Insert { id: 1, value } if **value == 7));
    db.write(|tx| {
        tx.replace(
            &rows,
            1,
            Payload {
                score: 0.25,
                fields: HashMap::new(),
            },
        )
    })?;
    assert_eq!(
        snapshot.rows[0].value.score, 0.75,
        "old snapshot must remain immutable"
    );
    assert!(filtered.snapshot(&db)?.rows.is_empty());
    let Poll::Delta(batch) = subscription.poll(&db)? else {
        panic!("expected removal delta")
    };
    assert!(matches!(&batch.changes[0], Change::Remove { id: 1, value } if **value == 7));
    Ok(())
}

#[test]
fn map_preserves_equal_values_as_distinct_identities() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u64, i64>("rows")?;
    let view = db.materialize(rows.query().map(|_| 0))?;
    db.write(|tx| {
        tx.insert(&rows, 1, 10)?;
        tx.insert(&rows, 2, 20)
    })?;
    assert_eq!(
        view.snapshot(&db)?
            .rows
            .iter()
            .map(|r| r.id)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    db.write(|tx| tx.remove(&rows, &1))?;
    assert_eq!(view.snapshot(&db)?.rows[0].id, 2);
    Ok(())
}

#[test]
fn write_overlay_is_ordered_and_coalesces_each_key() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u64, String>("rows")?;
    let view = db.materialize(rows.query())?;
    let (_, mut sub) = view.subscribe(&mut db)?;
    let empty = db.write(|tx| {
        tx.insert(&rows, 1, "temporary".into())?;
        tx.remove(&rows, &1)
    })?;
    assert!(!empty.changed);
    assert_eq!(empty.epoch.number(), 0);
    assert!(matches!(sub.poll(&db)?, Poll::Idle));
    db.write(|tx| {
        tx.insert(&rows, 1, "first".into())?;
        assert_eq!(
            tx.get(&rows, &1)?.as_deref().map(String::as_str),
            Some("first")
        );
        tx.replace(&rows, 1, "middle".into())?;
        tx.remove(&rows, &1)?;
        assert!(tx.get(&rows, &1)?.is_none());
        tx.insert(&rows, 1, "final".into())
    })?;
    let Poll::Delta(batch) = sub.poll(&db)? else {
        panic!("expected one final insertion")
    };
    assert_eq!(batch.changes.len(), 1);
    assert!(matches!(&batch.changes[0], Change::Insert { value, .. } if value.as_str() == "final"));
    db.write(|tx| {
        tx.remove(&rows, &1)?;
        tx.insert(&rows, 1, "replaced".into())
    })?;
    let Poll::Delta(batch) = sub.poll(&db)? else {
        panic!("expected replacement")
    };
    assert!(
        matches!(&batch.changes[0], Change::Update { before, after, .. } if before.as_str() == "final" && after.as_str() == "replaced")
    );
    Ok(())
}

#[test]
fn normal_errors_and_swallowed_errors_preserve_all_collections_and_journals() -> Result<(), Error> {
    let mut db = Database::new();
    let a = db.create_collection::<u64, i64>("a")?;
    let b = db.create_collection::<String, String>("b")?;
    let av = db.materialize(a.query().map(|v| v * 2))?;
    let bv = db.materialize(b.query())?;
    let (_, mut sa) = av.subscribe(&mut db)?;
    let (_, mut sb) = bv.subscribe(&mut db)?;
    let epoch = db.epoch()?;
    let error = db
        .write(|tx| {
            tx.insert(&a, 1, 8)?;
            tx.insert(&b, "k".into(), "v".into())?;
            Err(Error::Validation("reject both".into()))
        })
        .unwrap_err();
    assert!(matches!(error, Error::Validation(_)));
    assert_eq!(db.epoch()?, epoch);
    assert!(a.get(&db, &1)?.is_none());
    assert!(b.get(&db, &"k".into())?.is_none());
    assert!(av.snapshot(&db)?.rows.is_empty());
    assert!(bv.snapshot(&db)?.rows.is_empty());
    assert!(matches!(sa.poll(&db)?, Poll::Idle));
    assert!(matches!(sb.poll(&db)?, Poll::Idle));
    assert_eq!(
        db.write(|tx| {
            tx.insert(&a, 1, 8)?;
            assert_eq!(tx.insert(&a, 1, 9), Err(Error::DuplicateKey));
            Ok(())
        }),
        Err(Error::InvalidBatch)
    );
    assert_eq!(db.epoch()?, epoch);
    assert!(a.get(&db, &1)?.is_none());
    let commit = db.write(|tx| {
        tx.insert(&a, 1, 8)?;
        tx.insert(&b, "k".into(), "v".into())
    })?;
    for poll in [sa.poll(&db)?.map_epoch(), sb.poll(&db)?.map_epoch()] {
        assert_eq!(poll, Some(commit.epoch));
    }
    Ok(())
}
trait PollEpoch {
    fn map_epoch(self) -> Option<reactive_db::Epoch>;
}
impl<C> PollEpoch for Poll<C> {
    fn map_epoch(self) -> Option<reactive_db::Epoch> {
        if let Poll::Delta(batch) = self {
            Some(batch.to_epoch)
        } else {
            None
        }
    }
}

#[test]
fn wrong_database_and_dropped_owner_are_reported_without_aliasing() -> Result<(), Error> {
    let mut a = Database::new();
    let mut b = Database::new();
    let rows = a.create_collection::<u64, i64>("rows")?;
    let view = a.materialize(rows.query())?;
    let (_, mut sub) = view.subscribe(&mut a)?;
    assert!(matches!(
        b.materialize(rows.query()),
        Err(Error::WrongDatabase)
    ));
    assert_eq!(rows.get(&b, &0), Err(Error::WrongDatabase));
    assert!(matches!(sub.poll(&b), Err(Error::WrongDatabase)));
    assert_eq!(
        b.write(|tx| {
            let _ = tx.get(&rows, &0);
            Ok(())
        }),
        Err(Error::InvalidBatch)
    );
    assert_eq!(b.epoch()?.number(), 0);
    drop(a);
    assert_eq!(rows.get(&b, &0), Err(Error::DatabaseDropped));
    assert!(matches!(sub.poll(&b), Err(Error::DatabaseDropped)));
    Ok(())
}

#[test]
fn changed_rows_only_and_cached_removals() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u64, i64>("rows")?;
    db.write(|tx| {
        for i in 0..10_000 {
            tx.insert(&rows, i, i as i64)?;
        }
        Ok(())
    })?;
    let predicates = Rc::new(Cell::new(0));
    let projects = Rc::new(Cell::new(0));
    let p = predicates.clone();
    let m = projects.clone();
    let view = db.materialize(
        rows.query()
            .filter(move |v| {
                p.set(p.get() + 1);
                v % 2 == 0
            })
            .map(move |v| {
                m.set(m.get() + 1);
                v * 10
            }),
    )?;
    assert_eq!(predicates.get(), 10_000);
    assert_eq!(projects.get(), 5_000);
    predicates.set(0);
    projects.set(0);
    db.write(|tx| tx.replace(&rows, 0, 12))?;
    assert_eq!(predicates.get(), 1);
    assert_eq!(projects.get(), 1);
    predicates.set(0);
    projects.set(0);
    db.write(|tx| tx.remove(&rows, &0))?;
    assert_eq!(predicates.get(), 0, "removals use cached matching state");
    assert_eq!(projects.get(), 0, "removals use cached projection");
    assert_eq!(view.snapshot(&db)?.rows.len(), 4_999);
    Ok(())
}

#[test]
fn immutable_snapshots_and_deltas_are_send_sync_when_payloads_are() {
    fn check<T: Send + Sync>() {}
    check::<reactive_db::Snapshot<u64, String>>();
    check::<reactive_db::DeltaBatch<Change<u64, String>>>();
    check::<Error>();
}

#[test]
fn missing_and_duplicate_operations_are_errors_not_upserts() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u64, i64>("rows")?;
    assert!(matches!(
        db.create_collection::<String, String>("rows"),
        Err(Error::DuplicateCollection)
    ));
    assert_eq!(
        db.write(|tx| tx.replace(&rows, 1, 10)),
        Err(Error::MissingKey)
    );
    assert_eq!(db.write(|tx| tx.remove(&rows, &1)), Err(Error::MissingKey));
    assert_eq!(db.epoch()?.number(), 0);
    db.write(|tx| tx.insert(&rows, 1, 10))?;
    assert_eq!(
        db.write(|tx| tx.insert(&rows, 1, 20)),
        Err(Error::DuplicateKey)
    );
    assert_eq!(*rows.get(&db, &1)?.unwrap(), 10);
    Ok(())
}
