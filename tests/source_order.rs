//! Source staging order must not change atomic join results or budget accounting.
use reactive_db::{Database, Error, ExecutionBudget, Poll};
use std::sync::Arc;

#[test]
fn source_permutations_and_repeated_writes_match_a_full_join_model() -> Result<(), Error> {
    let mut stats = [None; 12];
    for order in [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ] {
        let mut db = Database::new();
        let sources = [
            db.create_collection::<u64, i64>("left")?,
            db.create_collection::<u64, i64>("right")?,
            db.create_collection::<u64, i64>("cancelled")?,
        ];
        let view = db.materialize(sources[0].query().map(|v| *v).join_on(
            sources[1].query().map(|v| *v).map(|v| *v),
            |_, v| v % 3,
            |_, v| v % 3,
        ))?;
        let (_, mut sub) = view.subscribe(&mut db)?;
        for tick in 0..12 {
            let values = [tick, tick + if tick % 2 == 0 { 3 } else { 1 }];
            let commit = db.write(|tx| {
                for i in order {
                    if i == 2 {
                        tx.insert(&sources[i], 0, 99)?;
                    } else if tick == 0 {
                        tx.insert(&sources[i], 0, -1)?;
                    } else {
                        tx.replace(&sources[i], 0, -1)?;
                    }
                }
                // Revisit every source in reverse order before publishing the batch.
                for i in order.into_iter().rev() {
                    if i == 2 {
                        tx.remove(&sources[i], &0)?;
                        assert!(tx.get(&sources[i], &0)?.is_none());
                    } else {
                        tx.replace(&sources[i], 0, values[i])?;
                        assert_eq!(*tx.get(&sources[i], &0)?.unwrap(), values[i]);
                    }
                }
                Ok(())
            })?;
            assert_eq!(
                commit.stats,
                *stats[tick as usize].get_or_insert(commit.stats),
                "{order:?}, tick={tick}"
            );
            let expected: Vec<_> = [values[0]]
                .into_iter()
                .flat_map(|l| {
                    [values[1]]
                        .into_iter()
                        .filter(move |r| l % 3 == r % 3)
                        .map(move |r| (l, r))
                })
                .collect();
            let actual: Vec<_> = view
                .snapshot(&db)?
                .rows
                .iter()
                .map(|r| (*r.value.left, *r.value.right))
                .collect();
            assert_eq!(actual, expected, "order={order:?}, tick={tick}");
            assert!(sources[2].get(&db, &0)?.is_none());
            let Poll::Delta(delta) = sub.poll(&db)? else {
                panic!("expected join transition")
            };
            assert_eq!(delta.to_epoch, commit.epoch);
            assert_eq!(delta.changes.len(), 1);
            assert!(matches!(sub.poll(&db)?, Poll::Idle));
        }
    }
    Ok(())
}

#[test]
fn rejected_multi_source_batches_release_values_and_retry_atomically() -> Result<(), Error> {
    for limit in [0, 1, 2, 3, 4, 5] {
        let mut db = Database::new();
        let a = db.create_collection::<u64, Arc<()>>("a")?;
        let b = db.create_collection::<u64, Arc<()>>("b")?;
        let view = db.materialize(a.query().join_on(b.query(), |_, _| (), |_, _| ()))?;
        let (_, mut sub) = view.subscribe(&mut db)?;
        db.set_execution_budget(ExecutionBudget {
            max_work: limit,
            ..Default::default()
        })?;
        let mut weak = Vec::new();
        let result = db.write(|tx| {
            for source in [&b, &a, &b] {
                let value = Arc::new(());
                weak.push(Arc::downgrade(&value));
                if tx.get(source, &0)?.is_some() {
                    tx.replace(source, 0, value)?;
                } else {
                    tx.insert(source, 0, value)?;
                }
            }
            Ok(())
        });
        assert!(
            matches!(result, Err(Error::BudgetExceeded(_))),
            "limit={limit}"
        );
        assert!(weak.iter().all(|v| v.upgrade().is_none()));
        assert!(a.get(&db, &0)?.is_none());
        assert!(b.get(&db, &0)?.is_none());
        assert!(view.snapshot(&db)?.rows.is_empty());
        assert!(matches!(sub.poll(&db)?, Poll::Idle));
        db.set_execution_budget(ExecutionBudget::default())?;
        let commit = db.write(|tx| {
            tx.insert(&b, 0, Arc::new(()))?;
            tx.insert(&a, 0, Arc::new(()))
        })?;
        assert_eq!(view.snapshot(&db)?.rows.len(), 1);
        let Poll::Delta(delta) = sub.poll(&db)? else {
            panic!("expected retry delta")
        };
        assert_eq!(delta.to_epoch, commit.epoch);
    }
    Ok(())
}
