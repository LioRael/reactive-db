//! Existing matches must retain final input values across payload-only updates and fallback paths.
use reactive_db::{Change, Database, Error, ExecutionBudget, Poll};
use std::collections::BTreeMap;
use std::sync::Arc;

#[test]
fn payload_updates_respect_the_exact_work_boundary() -> Result<(), Error> {
    let mut db = Database::new();
    let left = db.create_collection::<u64, i64>("left")?;
    let right = db.create_collection::<u64, i64>("right")?;
    db.write(|tx| {
        tx.insert(&left, 0, 0)?;
        for id in 0..64 {
            tx.insert(&right, id, 0)?;
        }
        Ok(())
    })?;
    let view = db.materialize(
        left.query()
            .left_join_on(right.query(), |_, _| (), |_, _| ()),
    )?;
    let first = db.write(|tx| tx.replace(&left, 0, 1))?;
    let (_, mut sub) = view.subscribe(&mut db)?;
    db.set_execution_budget(ExecutionBudget {
        max_work: first.stats.work - 1,
        ..Default::default()
    })?;
    assert!(matches!(
        db.write(|tx| tx.replace(&left, 0, 2)),
        Err(Error::BudgetExceeded(_))
    ));
    assert_eq!(db.epoch()?, first.epoch);
    assert!(view.snapshot(&db)?.rows.iter().all(|r| *r.value.left == 1));
    assert!(matches!(sub.poll(&db)?, Poll::Idle));
    db.set_execution_budget(ExecutionBudget {
        max_work: first.stats.work,
        ..Default::default()
    })?;
    let second = db.write(|tx| tx.replace(&left, 0, 2))?;
    assert_eq!(second.stats, first.stats);
    assert!(view.snapshot(&db)?.rows.iter().all(|r| *r.value.left == 2));
    let Poll::Delta(delta) = sub.poll(&db)? else {
        panic!("missing budget-boundary delta")
    };
    assert_eq!(delta.to_epoch, second.epoch);
    assert_eq!(delta.changes.len(), 64);
    Ok(())
}

#[test]
fn reused_matches_follow_full_recomputation_and_release_rejected_values() -> Result<(), Error> {
    let mut db = Database::new();
    let left = db.create_collection::<u64, (i64, i64, Arc<()>)>("left")?;
    let right = db.create_collection::<u64, (i64, i64)>("right")?;
    let mut tokens = Vec::new();
    let mut row = |key, value| {
        let token = Arc::new(());
        tokens.push(Arc::downgrade(&token));
        (key, value, token)
    };
    let mut lm = BTreeMap::from([(0, (0, 0)), (1, (9, 0))]);
    let mut rm: BTreeMap<_, _> = (0..128).map(|id| (id, (0, id as i64))).collect();
    db.write(|tx| {
        for (&id, &(key, value)) in &lm {
            tx.insert(&left, id, row(key, value))?;
        }
        for (&id, &value) in &rm {
            tx.insert(&right, id, value)?;
        }
        Ok(())
    })?;
    let outer = db.materialize(
        left.query()
            .left_join_on(right.query(), |_, v| v.0, |_, v| v.0),
    )?;
    let inner = db.materialize(left.query().join_on(right.query(), |_, v| v.0, |_, v| v.0))?;
    let (_, mut sub) = outer.subscribe(&mut db)?;
    for step in 1..=12 {
        let id = u64::from(step == 3);
        let key = match step {
            3 => 9,     // Existing unmatched placeholder.
            4 | 5 => 1, // Key move, then payload-only update with no matches.
            _ => 0,
        };
        let old = outer.snapshot(&db)?;
        if step == 2 {
            db.set_execution_budget(ExecutionBudget {
                max_output_changes: 64,
                ..Default::default()
            })?;
            let rejected = Arc::new(());
            let weak = Arc::downgrade(&rejected);
            assert!(matches!(
                db.write(|tx| tx.replace(&left, id, (key, step, rejected))),
                Err(Error::BudgetExceeded(_))
            ));
            assert!(weak.upgrade().is_none());
            assert_eq!(db.epoch()?, old.epoch);
            assert!(matches!(sub.poll(&db)?, Poll::Idle));
            db.set_execution_budget(ExecutionBudget::default())?;
        }
        let commit = db.write(|tx| {
            tx.replace(&left, id, row(key, step))?;
            if step == 7 {
                tx.replace(&right, 0, (0, 777))?;
                tx.remove(&right, &1)?;
            }
            if step == 9 {
                tx.replace(&right, 2, (1, 999))?;
            }
            Ok(())
        })?;
        lm.insert(id, (key, step));
        if step == 7 {
            rm.insert(0, (0, 777));
            rm.remove(&1);
        }
        if step == 9 {
            rm.insert(2, (1, 999));
        }
        let mut expected = BTreeMap::new();
        for (&li, &(lk, lv)) in &lm {
            let mut matched = false;
            for (&ri, &(rk, rv)) in &rm {
                if lk == rk {
                    matched = true;
                    expected.insert((li, Some(ri)), (lv, Some(rv)));
                }
            }
            if !matched {
                expected.insert((li, None), (lv, None));
            }
        }
        let snapshot = outer.snapshot(&db)?;
        let actual: BTreeMap<_, _> = snapshot
            .rows
            .iter()
            .map(|r| (r.id, (r.value.left.1, r.value.right.as_ref().map(|v| v.1))))
            .collect();
        assert_eq!(actual, expected, "outer step {step}");
        let actual_inner: BTreeMap<_, _> = inner
            .snapshot(&db)?
            .rows
            .iter()
            .map(|r| {
                (
                    (r.id.0, Some(r.id.1)),
                    (r.value.left.1, Some(r.value.right.1)),
                )
            })
            .collect();
        expected.retain(|(_, ri), _| ri.is_some());
        assert_eq!(actual_inner, expected, "inner step {step}");
        let Poll::Delta(delta) = sub.poll(&db)? else {
            panic!("missing atomic delta at {step}")
        };
        assert_eq!(delta.to_epoch, commit.epoch);
        if step == 2 {
            assert_eq!(delta.changes.len(), 128);
            for change in delta.changes.iter() {
                let Change::Update { before, after, .. } = change else {
                    panic!("expected payload update")
                };
                assert!(Arc::ptr_eq(
                    before.right.as_ref().unwrap(),
                    after.right.as_ref().unwrap()
                ));
                assert_eq!(before.left.1, 1);
                assert_eq!(after.left.1, 2);
            }
        }
        assert!(matches!(sub.poll(&db)?, Poll::Idle));
    }
    drop((sub, outer, inner, left, right, db));
    assert!(tokens.iter().all(|token| token.upgrade().is_none()));
    Ok(())
}
