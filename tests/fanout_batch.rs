//! Large fanout publication remains atomic when a single-source batch is rejected.
use reactive_db::{Database, Error, ExecutionBudget, Poll};
use std::sync::Arc;

#[test]
fn fanout_key_moves_retry_against_a_full_model_and_release_payloads() -> Result<(), Error> {
    let mut db = Database::new();
    let left = db.create_collection::<u64, (u64, Arc<()>)>("left")?;
    let right = db.create_collection::<u64, u64>("right")?;
    let token = Arc::new(());
    let mut weak = vec![Arc::downgrade(&token)];
    db.write(|tx| {
        tx.insert(&left, 0, (0, token))?;
        for id in 0..1024 {
            tx.insert(&right, id, 0)?;
        }
        Ok(())
    })?;
    let view = db.materialize(left.query().left_join_on(
        right.query(),
        |_, row| row.0,
        |_, row| *row,
    ))?;
    let (_, mut sub) = view.subscribe(&mut db)?;
    for key in [1, 0, 1, 0] {
        let old = view.snapshot(&db)?;
        db.set_execution_budget(ExecutionBudget {
            max_output_changes: 1024,
            ..Default::default()
        })?;
        let rejected = Arc::new(());
        let rejected_weak = Arc::downgrade(&rejected);
        assert!(matches!(
            db.write(|tx| tx.replace(&left, 0, (key, rejected))),
            Err(Error::BudgetExceeded(_))
        ));
        assert!(
            rejected_weak.upgrade().is_none(),
            "prepared joins must not retain rejected payloads"
        );
        let still = view.snapshot(&db)?;
        assert_eq!(still.epoch, old.epoch);
        assert_eq!(still.rows.len(), old.rows.len());
        assert!(matches!(sub.poll(&db)?, Poll::Idle));
        db.set_execution_budget(ExecutionBudget::default())?;
        let token = Arc::new(());
        weak.push(Arc::downgrade(&token));
        let commit = db.write(|tx| tx.replace(&left, 0, (key, token)))?;
        let snapshot = view.snapshot(&db)?;
        let mut expected: Vec<_> = (0..1024).filter(|_| key == 0).map(Some).collect();
        if expected.is_empty() {
            expected.push(None);
        }
        let actual: Vec<_> = snapshot
            .rows
            .iter()
            .map(|row| {
                assert_eq!(row.value.left.0, key);
                row.id.1
            })
            .collect();
        assert_eq!(actual, expected);
        let Poll::Delta(delta) = sub.poll(&db)? else {
            panic!("expected atomic join delta")
        };
        assert_eq!(delta.to_epoch, commit.epoch);
        assert_eq!(delta.changes.len(), 1025);
        assert!(matches!(sub.poll(&db)?, Poll::Idle));
    }
    drop((db, view, sub, left, right));
    assert!(weak.iter().all(|value| value.upgrade().is_none()));
    Ok(())
}
