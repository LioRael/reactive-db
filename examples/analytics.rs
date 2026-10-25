//! Incremental grouped statistics and an ordered leaderboard.
use reactive_db::{Database, Error, ExecutionBudget, OrderedChange, Poll};
use std::cmp::Reverse;
struct Task {
    project: u64,
    score: i64,
}
fn main() -> Result<(), Error> {
    let mut db = Database::with_budget(ExecutionBudget {
        max_input_changes: 100,
        max_output_changes: 1_000,
        max_work: 10_000,
    });
    let tasks = db.create_collection::<u64, Task>("tasks")?;
    let stats = tasks.query().group_by(|t| t.project).stats_i64(|t| t.score);
    let leaderboard = db.materialize_ordered(stats.sort_by_key(|s| Reverse(s.sum)).take(3))?;
    let (_, mut changes) = leaderboard.subscribe(&mut db)?;
    let commit = db.write(|tx| {
        tx.insert(
            &tasks,
            1,
            Task {
                project: 7,
                score: 10,
            },
        )?;
        tx.insert(
            &tasks,
            2,
            Task {
                project: 7,
                score: 20,
            },
        )?;
        tx.insert(
            &tasks,
            3,
            Task {
                project: 8,
                score: 40,
            },
        )
    })?;
    let snapshot = leaderboard.snapshot(&db)?;
    assert_eq!(snapshot.rows[0].id, 8);
    assert_eq!(snapshot.rows[1].value.sum, 30);
    if let Poll::Delta(batch) = changes.poll(&db)? {
        for change in batch.changes.iter() {
            if let OrderedChange::Insert { row } = change {
                println!(
                    "project {}: sum {}, token {:?}",
                    row.id, row.value.sum, row.order
                );
            }
        }
    }
    println!("epoch {}: {:?}", commit.epoch.number(), commit.stats);
    Ok(())
}
