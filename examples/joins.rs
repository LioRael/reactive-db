//! Atomic task/project joins without an async runtime.
use reactive_db::{Database, Error, Poll};

struct Task {
    project: u64,
    title: String,
}
fn main() -> Result<(), Error> {
    let mut db = Database::new();
    let tasks = db.create_collection::<u64, Task>("tasks")?;
    let projects = db.create_collection::<u64, String>("projects")?;
    let view = db.materialize(
        tasks
            .query()
            .left_join_on(projects.query(), |_, task| task.project, |id, _| *id)
            .map(|pair| (pair.left.title.clone(), pair.right.as_deref().cloned())),
    )?;
    let (_, mut subscription) = view.subscribe(&mut db)?;
    db.write(|tx| {
        tx.insert(
            &tasks,
            1,
            Task {
                project: 7,
                title: "Implement joins".into(),
            },
        )?;
        tx.insert(&projects, 7, "Reactive DB".into())
    })?;
    if let Poll::Delta(batch) = subscription.poll(&db)? {
        assert_eq!(batch.changes.len(), 1);
        println!("epoch {}: {:?}", batch.to_epoch.number(), batch.changes);
    }
    db.write(|tx| tx.remove(&projects, &7))?;
    let snapshot = view.snapshot(&db)?;
    assert_eq!(snapshot.rows[0].id, (1, None));
    assert_eq!(snapshot.rows[0].value.1, None);
    println!("unmatched task: {:?}", snapshot.rows[0]);
    Ok(())
}
