//! Incremental keyed joins and atomic cross-collection publication.

use reactive_db::{Change, Database, Error, Poll};

struct Item {
    group: i32,
    value: i32,
}

#[test]
fn both_sides_update_once_with_final_values_and_one_epoch() -> Result<(), Error> {
    let mut db = Database::new();
    let left = db.create_collection::<u64, Item>("left")?;
    let right = db.create_collection::<u64, Item>("right")?;
    let view = db.materialize(left.query().join_on(
        right.query(),
        |_, v| v.group,
        |_, v| v.group,
    ))?;
    let (_, mut sub) = view.subscribe(&mut db)?;
    db.write(|tx| {
        tx.insert(
            &left,
            1,
            Item {
                group: 7,
                value: 10,
            },
        )?;
        tx.insert(
            &right,
            2,
            Item {
                group: 7,
                value: 20,
            },
        )
    })?;
    let Poll::Delta(first) = sub.poll(&db)? else {
        panic!("expected inserted pair")
    };
    assert_eq!(first.changes.len(), 1);
    let old = view.snapshot(&db)?;
    let commit = db.write(|tx| {
        tx.replace(
            &left,
            1,
            Item {
                group: 7,
                value: 30,
            },
        )?;
        tx.replace(
            &right,
            2,
            Item {
                group: 7,
                value: 40,
            },
        )
    })?;
    let Poll::Delta(batch) = sub.poll(&db)? else {
        panic!("expected updated pair")
    };
    assert_eq!(batch.to_epoch, commit.epoch);
    assert_eq!(batch.changes.len(), 1);
    assert!(
        matches!(&batch.changes[0], Change::Update { id: (1,2), before, after } if (before.left.value,before.right.value)==(10,20) && (after.left.value,after.right.value)==(30,40))
    );
    assert_eq!(old.rows[0].value.left.value, 10);
    assert!(matches!(sub.poll(&db)?, Poll::Idle));
    Ok(())
}

#[test]
fn left_placeholder_transitions_and_key_moves_are_atomic() -> Result<(), Error> {
    let mut db = Database::new();
    let left = db.create_collection::<u64, Item>("left")?;
    let right = db.create_collection::<u64, Item>("right")?;
    let view = db.materialize(left.query().left_join_on(
        right.query(),
        |_, v| v.group,
        |_, v| v.group,
    ))?;
    db.write(|tx| {
        tx.insert(
            &left,
            1,
            Item {
                group: 7,
                value: 10,
            },
        )
    })?;
    let (initial, mut sub) = view.subscribe(&mut db)?;
    assert_eq!(initial.rows[0].id, (1, None));
    db.write(|tx| {
        tx.insert(
            &right,
            2,
            Item {
                group: 7,
                value: 20,
            },
        )
    })?;
    let Poll::Delta(batch) = sub.poll(&db)? else {
        panic!("expected placeholder replacement")
    };
    assert_eq!(batch.changes.len(), 2);
    assert_eq!(view.snapshot(&db)?.rows[0].id, (1, Some(2)));
    db.write(|tx| {
        tx.replace(
            &left,
            1,
            Item {
                group: 8,
                value: 30,
            },
        )?;
        tx.replace(
            &right,
            2,
            Item {
                group: 8,
                value: 40,
            },
        )
    })?;
    let Poll::Delta(batch) = sub.poll(&db)? else {
        panic!("expected same-identity update")
    };
    assert_eq!(batch.changes.len(), 1);
    assert!(matches!(
        &batch.changes[0],
        Change::Update {
            id: (1, Some(2)),
            ..
        }
    ));
    db.write(|tx| {
        tx.replace(
            &right,
            2,
            Item {
                group: 9,
                value: 50,
            },
        )
    })?;
    assert_eq!(view.snapshot(&db)?.rows[0].id, (1, None));
    Ok(())
}

#[test]
fn self_join_keeps_cross_pairs_and_never_duplicates_an_identity() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u64, Item>("rows")?;
    let q = rows.query();
    let view = db.materialize(q.clone().join_on(q, |_, v| v.group, |_, v| v.group))?;
    let (_, mut sub) = view.subscribe(&mut db)?;
    db.write(|tx| {
        tx.insert(
            &rows,
            1,
            Item {
                group: 7,
                value: 10,
            },
        )?;
        tx.insert(
            &rows,
            2,
            Item {
                group: 7,
                value: 20,
            },
        )
    })?;
    let Poll::Delta(batch) = sub.poll(&db)? else {
        panic!("expected four pairs")
    };
    assert_eq!(batch.changes.len(), 4);
    db.write(|tx| {
        tx.replace(
            &rows,
            1,
            Item {
                group: 8,
                value: 30,
            },
        )?;
        tx.replace(
            &rows,
            2,
            Item {
                group: 8,
                value: 40,
            },
        )
    })?;
    let Poll::Delta(batch) = sub.poll(&db)? else {
        panic!("expected four updates")
    };
    assert_eq!(batch.changes.len(), 4);
    assert!(
        batch
            .changes
            .iter()
            .all(|v| matches!(v, Change::Update { .. }))
    );
    db.write(|tx| tx.remove(&rows, &1))?;
    assert_eq!(
        view.snapshot(&db)?
            .rows
            .iter()
            .map(|r| r.id)
            .collect::<Vec<_>>(),
        [(2, 2)]
    );
    Ok(())
}

#[test]
fn unmatched_index_changes_survive_empty_output_batches() -> Result<(), Error> {
    let mut db = Database::new();
    let l = db.create_collection::<u64, Item>("l")?;
    let r = db.create_collection::<u64, Item>("r")?;
    let view = db.materialize(l.query().join_on(r.query(), |_, v| v.group, |_, v| v.group))?;
    db.write(|tx| {
        tx.insert(
            &l,
            1,
            Item {
                group: 1,
                value: 10,
            },
        )
    })?;
    db.write(|tx| {
        tx.replace(
            &l,
            1,
            Item {
                group: 2,
                value: 20,
            },
        )
    })?;
    assert!(view.snapshot(&db)?.rows.is_empty());
    db.write(|tx| {
        tx.insert(
            &r,
            3,
            Item {
                group: 1,
                value: 30,
            },
        )
    })?;
    assert!(
        view.snapshot(&db)?.rows.is_empty(),
        "old join key must have been removed even without output"
    );
    db.write(|tx| {
        tx.replace(
            &r,
            3,
            Item {
                group: 2,
                value: 40,
            },
        )
    })?;
    assert_eq!(view.snapshot(&db)?.rows[0].value.left.value, 20);
    db.write(|tx| tx.remove(&r, &3))?;
    db.write(|tx| tx.remove(&l, &1))?;
    db.write(|tx| {
        tx.insert(
            &r,
            3,
            Item {
                group: 2,
                value: 50,
            },
        )
    })?;
    assert!(
        view.snapshot(&db)?.rows.is_empty(),
        "deleted unmatched row must not remain indexed"
    );
    Ok(())
}

#[test]
fn duplicate_keys_produce_all_pairs_and_last_match_controls_placeholder() -> Result<(), Error> {
    let mut db = Database::new();
    let l = db.create_collection::<u64, Item>("l")?;
    let r = db.create_collection::<String, Item>("r")?;
    db.write(|tx| {
        tx.insert(
            &l,
            1,
            Item {
                group: 1,
                value: 10,
            },
        )?;
        tx.insert(
            &l,
            2,
            Item {
                group: 1,
                value: 10,
            },
        )?;
        tx.insert(
            &r,
            "a".into(),
            Item {
                group: 1,
                value: 20,
            },
        )?;
        tx.insert(
            &r,
            "b".into(),
            Item {
                group: 1,
                value: 20,
            },
        )
    })?;
    let view = db.materialize(
        l.query()
            .left_join_on(r.query(), |_, v| v.group, |_, v| v.group),
    )?;
    assert_eq!(view.snapshot(&db)?.rows.len(), 4);
    let (_, mut sub) = view.subscribe(&mut db)?;
    db.write(|tx| tx.remove(&r, &"a".to_owned()))?;
    let Poll::Delta(batch) = sub.poll(&db)? else {
        panic!("expected pair removals")
    };
    assert_eq!(batch.changes.len(), 2);
    assert!(
        view.snapshot(&db)?
            .rows
            .iter()
            .all(|row| row.id.1.as_deref() == Some("b"))
    );
    db.write(|tx| tx.remove(&r, &"b".to_owned()))?;
    let Poll::Delta(batch) = sub.poll(&db)? else {
        panic!("expected final-match transitions")
    };
    assert_eq!(batch.changes.len(), 4);
    assert!(
        view.snapshot(&db)?
            .rows
            .iter()
            .all(|row| row.id.1.is_none() && row.value.right.is_none())
    );
    Ok(())
}

#[test]
fn remove_and_insert_right_in_one_batch_never_publishes_placeholder() -> Result<(), Error> {
    let mut db = Database::new();
    let l = db.create_collection::<u64, Item>("l")?;
    let r = db.create_collection::<u64, Item>("r")?;
    let view = db.materialize(
        l.query()
            .left_join_on(r.query(), |_, v| v.group, |_, v| v.group),
    )?;
    db.write(|tx| {
        tx.insert(
            &l,
            1,
            Item {
                group: 1,
                value: 10,
            },
        )?;
        tx.insert(
            &r,
            2,
            Item {
                group: 1,
                value: 20,
            },
        )
    })?;
    let (_, mut sub) = view.subscribe(&mut db)?;
    db.write(|tx| {
        tx.remove(&r, &2)?;
        tx.insert(
            &r,
            3,
            Item {
                group: 1,
                value: 30,
            },
        )
    })?;
    let Poll::Delta(batch) = sub.poll(&db)? else {
        panic!("expected pair replacement")
    };
    assert_eq!(batch.changes.len(), 2);
    assert!(batch.changes.iter().all(|c| c.id().1.is_some()));
    Ok(())
}

#[test]
fn rejected_cross_collection_batch_changes_neither_indexes_nor_publications() -> Result<(), Error> {
    let mut db = Database::new();
    let l = db.create_collection::<u64, Item>("l")?;
    let r = db.create_collection::<u64, Item>("r")?;
    let lv = db.materialize(l.query())?;
    let rv = db.materialize(r.query())?;
    let join = db.materialize(
        l.query()
            .join_on(r.query(), |_, v| v.group, |_, v| v.group)
            .map(|p| (p.left.value, p.right.value)),
    )?;
    db.write(|tx| {
        tx.insert(
            &l,
            1,
            Item {
                group: 1,
                value: 10,
            },
        )?;
        tx.insert(
            &r,
            2,
            Item {
                group: 1,
                value: 20,
            },
        )
    })?;
    let (_, mut ls) = lv.subscribe(&mut db)?;
    let (_, mut rs) = rv.subscribe(&mut db)?;
    let (_, mut js) = join.subscribe(&mut db)?;
    let epoch = db.epoch()?;
    assert!(
        db.write(|tx| {
            tx.replace(
                &l,
                1,
                Item {
                    group: 2,
                    value: 30,
                },
            )?;
            tx.replace(
                &r,
                2,
                Item {
                    group: 3,
                    value: 40,
                },
            )?;
            Err(Error::Validation("reject".into()))
        })
        .is_err()
    );
    assert_eq!(db.epoch()?, epoch);
    assert!(matches!(ls.poll(&db)?, Poll::Idle));
    assert!(matches!(rs.poll(&db)?, Poll::Idle));
    assert!(matches!(js.poll(&db)?, Poll::Idle));
    assert_eq!(*join.snapshot(&db)?.rows[0].value, (10, 20));
    let commit = db.write(|tx| {
        tx.replace(
            &l,
            1,
            Item {
                group: 2,
                value: 50,
            },
        )?;
        tx.replace(
            &r,
            2,
            Item {
                group: 2,
                value: 60,
            },
        )
    })?;
    let Poll::Delta(lb) = ls.poll(&db)? else {
        panic!("left publication missing")
    };
    let Poll::Delta(rb) = rs.poll(&db)? else {
        panic!("right publication missing")
    };
    let Poll::Delta(jb) = js.poll(&db)? else {
        panic!("join publication missing")
    };
    assert_eq!([lb.to_epoch, rb.to_epoch, jb.to_epoch], [commit.epoch; 3]);
    assert_eq!(jb.changes.len(), 1);
    assert_eq!(*join.snapshot(&db)?.rows[0].value, (50, 60));
    Ok(())
}

#[test]
fn nested_foreign_plan_is_rejected_before_any_selector_runs() -> Result<(), Error> {
    use std::cell::Cell;
    use std::rc::Rc;
    let mut a = Database::new();
    let mut b = Database::new();
    let left = a.create_collection::<u64, i64>("l")?;
    let right = b.create_collection::<u64, i64>("r")?;
    a.write(|tx| tx.insert(&left, 1, 1))?;
    b.write(|tx| tx.insert(&right, 1, 1))?;
    let calls = Rc::new(Cell::new(0));
    let count = calls.clone();
    let bad = left
        .query()
        .join_on(
            right.query(),
            move |_, v| {
                count.set(count.get() + 1);
                *v
            },
            |_, v| *v,
        )
        .map(|p| *p.left);
    assert!(matches!(a.materialize(bad), Err(Error::WrongDatabase)));
    assert_eq!(calls.get(), 0);
    assert!(
        a.write(|_| Ok(())).is_ok(),
        "validation errors must not poison the owner"
    );
    let bad = left
        .query()
        .left_join_on(right.query(), |_, v| *v, |_, v| *v);
    drop(b);
    assert!(matches!(a.materialize(bad), Err(Error::DatabaseDropped)));
    Ok(())
}

#[test]
fn self_left_join_supports_different_selectors_and_unmatched_diagonal() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u64, Item>("rows")?;
    let q = rows.query();
    let view = db.materialize(
        q.clone()
            .left_join_on(q, |_, v| v.group + 1, |_, v| v.group),
    )?;
    db.write(|tx| {
        tx.insert(
            &rows,
            1,
            Item {
                group: 1,
                value: 10,
            },
        )?;
        tx.insert(
            &rows,
            2,
            Item {
                group: 2,
                value: 20,
            },
        )
    })?;
    assert_eq!(
        view.snapshot(&db)?
            .rows
            .iter()
            .map(|r| r.id)
            .collect::<Vec<_>>(),
        [(1, Some(2)), (2, None)]
    );
    db.write(|tx| {
        tx.replace(
            &rows,
            1,
            Item {
                group: 4,
                value: 30,
            },
        )?;
        tx.replace(
            &rows,
            2,
            Item {
                group: 5,
                value: 40,
            },
        )
    })?;
    assert_eq!(
        view.snapshot(&db)?
            .rows
            .iter()
            .map(|r| r.id)
            .collect::<Vec<_>>(),
        [(1, Some(2)), (2, None)]
    );
    db.write(|tx| tx.remove(&rows, &2))?;
    assert_eq!(view.snapshot(&db)?.rows[0].id, (1, None));
    Ok(())
}

#[test]
fn chained_and_diamond_joins_use_final_upstream_outputs() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u64, Item>("rows")?;
    let labels = db.create_collection::<i32, String>("labels")?;
    let shared = rows.query().map(|v| (v.group, v.value));
    let pairs = shared.clone().filter(|v| v.1 >= 10).join_on(
        shared.filter(|v| v.1 >= 20),
        |_, v| v.0,
        |_, v| v.0,
    );
    let view = db.materialize(
        pairs
            .join_on(labels.query(), |_, p| p.left.0, |id, _| *id)
            .map(|p| (p.left.left.1, p.left.right.1, p.right.to_string())),
    )?;
    let (_, mut sub) = view.subscribe(&mut db)?;
    db.write(|tx| {
        tx.insert(
            &rows,
            1,
            Item {
                group: 7,
                value: 10,
            },
        )?;
        tx.insert(
            &rows,
            2,
            Item {
                group: 7,
                value: 20,
            },
        )?;
        tx.insert(&labels, 7, "old".into())
    })?;
    assert_eq!(view.snapshot(&db)?.rows.len(), 2);
    let _ = sub.poll(&db)?;
    db.write(|tx| {
        tx.replace(
            &rows,
            1,
            Item {
                group: 8,
                value: 30,
            },
        )?;
        tx.replace(
            &rows,
            2,
            Item {
                group: 8,
                value: 40,
            },
        )?;
        tx.insert(&labels, 8, "new".into())
    })?;
    let snapshot = view.snapshot(&db)?;
    assert_eq!(snapshot.rows.len(), 4);
    assert!(snapshot.rows.iter().all(|r| r.value.2 == "new"));
    let Poll::Delta(batch) = sub.poll(&db)? else {
        panic!("missing chained publication")
    };
    assert_eq!(
        batch.changes.len(),
        6,
        "two old triples removed, four final triples inserted"
    );
    Ok(())
}
