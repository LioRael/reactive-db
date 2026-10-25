//! Work bounds and fail-stop behavior of indexed joins.
use reactive_db::{Database, Error, Poll};
use std::cell::Cell;
use std::rc::Rc;

thread_local! { static CLONES: Cell<usize> = const { Cell::new(0) }; }
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Id(u32);
impl Clone for Id {
    fn clone(&self) -> Self {
        CLONES.with(|c| c.set(c.get() + 1));
        Self(self.0)
    }
}

#[test]
fn one_changed_pair_does_not_clone_or_reproject_a_large_matching_bucket() -> Result<(), Error> {
    for size in [100, 10_000] {
        let mut db = Database::new();
        let l = db.create_collection::<Id, u32>("left")?;
        let r = db.create_collection::<Id, u32>("right")?;
        db.write(|tx| {
            tx.insert(&l, Id(0), 0)?;
            for id in 0..size {
                tx.insert(&r, Id(id), id)?;
            }
            Ok(())
        })?;
        let lc = Rc::new(Cell::new(0));
        let rc = Rc::new(Cell::new(0));
        let mc = Rc::new(Cell::new(0));
        let (a, b, c) = (lc.clone(), rc.clone(), mc.clone());
        let query = l
            .query()
            .left_join_on(
                r.query(),
                move |_, _| {
                    a.set(a.get() + 1);
                    0
                },
                move |_, _| {
                    b.set(b.get() + 1);
                    0
                },
            )
            .map(move |p| {
                c.set(c.get() + 1);
                p.right.as_deref().copied()
            });
        let view = db.materialize(query.clone())?;
        let _shared = db.materialize(query)?;
        assert_eq!(
            (lc.get(), rc.get(), mc.get()),
            (1, size, size),
            "clones share the materialized graph"
        );
        let (_, mut sub) = view.subscribe(&mut db)?;
        lc.set(0);
        rc.set(0);
        mc.set(0);
        CLONES.with(|c| c.set(0));
        db.write(|tx| tx.replace(&r, Id(0), 999_999))?;
        let clones = CLONES.with(Cell::get);
        assert!(
            clones < 60,
            "size {size}: cloned {clones} identities for one pair"
        );
        assert_eq!((lc.get(), rc.get(), mc.get()), (0, 1, 1));
        let Poll::Delta(batch) = sub.poll(&db)? else {
            panic!("missing one pair delta")
        };
        assert_eq!(batch.changes.len(), 1);
        lc.set(0);
        rc.set(0);
        mc.set(0);
        db.write(|tx| tx.remove(&r, &Id(0)))?;
        assert_eq!(
            (lc.get(), rc.get(), mc.get()),
            (0, 0, 0),
            "retraction uses cached keys and output"
        );
    }
    Ok(())
}

#[test]
fn selector_panic_poisons_without_mutating_previously_acquired_snapshots() -> Result<(), Error> {
    let mut db = Database::new();
    let l = db.create_collection::<u32, u32>("l")?;
    let r = db.create_collection::<u32, u32>("r")?;
    db.write(|tx| {
        tx.insert(&l, 1, 1)?;
        tx.insert(&r, 2, 1)
    })?;
    let view = db.materialize(l.query().join_on(
        r.query(),
        |_, v| *v,
        |_, v| {
            assert_ne!(*v, 99, "application selector failed");
            *v
        },
    ))?;
    let old = view.snapshot(&db)?;
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        db.write(|tx| {
            tx.replace(&l, 1, 99)?;
            tx.replace(&r, 2, 99)
        })
    }));
    assert!(failure.is_err());
    assert_eq!(*old.rows[0].value.left, 1);
    assert_eq!(*old.rows[0].value.right, 1);
    assert!(matches!(db.epoch(), Err(Error::Poisoned)));
    assert!(matches!(view.snapshot(&db), Err(Error::Poisoned)));
    Ok(())
}

#[test]
fn final_join_lease_releases_both_derived_parents_and_index_captures() -> Result<(), Error> {
    use std::sync::Arc;
    let mut db = Database::new();
    let source = db.create_collection::<u32, u32>("source")?;
    db.write(|tx| tx.insert(&source, 1, 7))?;
    let capture = Rc::new(());
    let weak_capture = Rc::downgrade(&capture);
    let l = source.query().map(|v| format!("left {v}"));
    let r = source.query().map(|v| format!("right {v}"));
    let view = db.materialize(l.join_on(
        r,
        move |id, _| {
            let _ = &capture;
            *id
        },
        |id, _| *id,
    ))?;
    let (snapshot, sub) = view.subscribe(&mut db)?;
    let weak_left = Arc::downgrade(&snapshot.rows[0].value.left);
    let weak_right = Arc::downgrade(&snapshot.rows[0].value.right);
    drop(snapshot);
    drop(view);
    db.write(|_| Ok(()))?;
    assert!(weak_left.upgrade().is_some());
    assert!(weak_right.upgrade().is_some());
    drop(sub);
    db.write(|_| Ok(()))?;
    assert!(weak_left.upgrade().is_none());
    assert!(weak_right.upgrade().is_none());
    assert!(weak_capture.upgrade().is_none());
    Ok(())
}
