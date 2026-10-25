//! Independent full recomputation and delta replay model.

use reactive_db::{Change, Database, Error, Poll};
use std::collections::BTreeMap;

#[test]
fn incremental_snapshots_and_delta_mirrors_match_independent_full_model() -> Result<(), Error> {
    for seed in 1..=12_u64 {
        let mut rng = seed;
        let mut random = || {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            rng
        };
        let mut db = Database::new();
        let rows = db.create_collection::<u64, i64>("rows")?;
        let view = db.materialize(
            rows.query()
                .filter(|v| v.rem_euclid(3) != 0)
                .map(|v| v * 2)
                .filter(|v| *v > 40),
        )?;
        let (_, mut sub) = view.subscribe(&mut db)?;
        let mut model = BTreeMap::new();
        let mut mirror = BTreeMap::new();
        for step in 0..500 {
            let operations: Vec<_> = (0..1 + (random() % 6))
                .map(|_| (random() % 31, (random() % 100) as i64, random() % 5 == 0))
                .collect();
            db.write(|tx| {
                for (key, value, remove) in operations {
                    if remove {
                        if model.remove(&key).is_some() {
                            tx.remove(&rows, &key)?;
                        }
                    } else if model.insert(key, value).is_some() {
                        tx.replace(&rows, key, value)?;
                    } else {
                        tx.insert(&rows, key, value)?;
                    }
                }
                Ok(())
            })?;
            while let Poll::Delta(batch) = sub.poll(&db)? {
                let mut seen = std::collections::BTreeSet::new();
                for change in batch.changes.iter() {
                    assert!(
                        seen.insert(*change.id()),
                        "one transition per identity seed={seed} step={step}"
                    );
                    match change {
                        Change::Insert { id, value } => {
                            assert!(mirror.insert(*id, **value).is_none());
                        }
                        Change::Update { id, before, after } => {
                            assert_eq!(mirror.insert(*id, **after), Some(**before));
                        }
                        Change::Remove { id, value } => {
                            assert_eq!(mirror.remove(id), Some(**value));
                        }
                        _ => panic!("unexpected change variant"),
                    }
                }
            }
            let expected: BTreeMap<_, _> = model
                .iter()
                .filter(|(_, v)| **v % 3 != 0 && **v * 2 > 40)
                .map(|(k, v)| (*k, *v * 2))
                .collect();
            let snapshot: BTreeMap<_, _> = view
                .snapshot(&db)?
                .rows
                .into_iter()
                .map(|row| (row.id, *row.value))
                .collect();
            assert_eq!(snapshot, expected, "snapshot seed={seed} step={step}");
            assert_eq!(mirror, expected, "delta mirror seed={seed} step={step}");
        }
    }
    Ok(())
}
