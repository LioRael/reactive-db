//! Updating an existing row preserves its stored key representation and atomic deltas.
use reactive_db::{Database, Error, Poll};
use std::cmp::Ordering;
use std::sync::Arc;

#[derive(Debug, Clone)]
struct Key {
    id: u64,
    label: &'static str,
}
impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for Key {}
impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

#[test]
fn updates_preserve_canonical_keys_and_release_replaced_values() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<Key, Arc<()>>("rows")?;
    let key = |label| Key { id: 7, label };
    let old = Arc::new(());
    let old_weak = Arc::downgrade(&old);
    db.write(|tx| tx.insert(&rows, key("stored"), old))?;
    let view = db.materialize(rows.query())?;
    let (_, mut sub) = view.subscribe(&mut db)?;
    let new = Arc::new(());
    let new_weak = Arc::downgrade(&new);
    let commit = db.write(|tx| tx.replace(&rows, key("update"), new))?;
    let snapshot = view.snapshot(&db)?;
    assert_eq!(snapshot.rows[0].id.label, "stored");
    let Poll::Delta(delta) = sub.poll(&db)? else {
        panic!("missing update delta")
    };
    assert_eq!(delta.to_epoch, commit.epoch);
    assert_eq!(delta.changes[0].id().label, "update");
    assert_eq!(delta.changes.len(), 1);
    drop((snapshot, delta, sub, view, rows, db));
    assert!(old_weak.upgrade().is_none());
    assert!(new_weak.upgrade().is_none());
    Ok(())
}
