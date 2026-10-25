//! A synchronous typed collection and delta subscription example.

use reactive_db::{Database, Error, Poll};

struct Task {
    title: String,
    score: i64,
}

fn main() -> Result<(), Error> {
    let mut db = Database::new();
    let tasks = db.create_collection::<u64, Task>("tasks")?;
    let view = db.materialize(
        tasks
            .query()
            .filter(|t| t.score >= 10)
            .map(|t| t.title.clone()),
    )?;
    let (initial, mut subscription) = view.subscribe(&mut db)?;
    println!(
        "initial epoch={} rows={}",
        initial.epoch.number(),
        initial.rows.len()
    );
    db.write(|tx| {
        tx.insert(
            &tasks,
            1,
            Task {
                title: "typed collections".into(),
                score: 20,
            },
        )?;
        tx.insert(
            &tasks,
            2,
            Task {
                title: "later".into(),
                score: 5,
            },
        )
    })?;
    db.write(|tx| {
        tx.replace(
            &tasks,
            2,
            Task {
                title: "delta subscriptions".into(),
                score: 30,
            },
        )
    })?;
    while let Poll::Delta(batch) = subscription.poll(&db)? {
        println!(
            "epoch {} -> {}: {:?}",
            batch.from_epoch.number(),
            batch.to_epoch.number(),
            batch.changes
        );
    }
    Ok(())
}
