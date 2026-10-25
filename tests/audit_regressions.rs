//! Adversarial graph and batch shapes found during the M0–M3 audit.
use reactive_db::{Database, Error, ExecutionBudget};
use std::cell::Cell;
use std::cmp::Ordering;
thread_local! {static COMPARISONS:Cell<usize>=const {Cell::new(0)};}
#[derive(Clone, Debug, Eq, PartialEq)]
struct CountedId(u64);
impl Ord for CountedId {
    fn cmp(&self, other: &Self) -> Ordering {
        COMPARISONS.with(|c| c.set(c.get() + 1));
        self.0.cmp(&other.0)
    }
}
impl PartialOrd for CountedId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[test]
fn shared_diamond_validation_is_proportional_to_unique_nodes() -> Result<(), Error> {
    let mut db = Database::with_budget(ExecutionBudget {
        max_work: 1000,
        ..Default::default()
    });
    let source = db.create_collection::<u64, u64>("source")?;
    let mut query = source.query();
    for _ in 0..12 {
        query = query
            .clone()
            .join_on(query, |_, _| (), |_, _| ())
            .group_by(|_| 0_u64)
            .count();
    }
    let view = db.materialize(query)?;
    assert!(view.snapshot(&db)?.rows.is_empty());
    Ok(())
}

#[test]
fn join_final_bucket_work_is_shared_across_changed_rows() -> Result<(), Error> {
    let mut db = Database::new();
    let left = db.create_collection::<u64, u64>("left")?;
    let right = db.create_collection::<CountedId, u64>("right")?;
    db.write(|tx| {
        for id in 0..200 {
            tx.insert(&right, CountedId(id), 0)?;
        }
        Ok(())
    })?;
    let view = db.materialize(left.query().join_on(right.query(), |_, v| *v, |_, v| *v))?;
    db.set_execution_budget(ExecutionBudget {
        max_work: 3000,
        ..Default::default()
    })?;
    COMPARISONS.with(|c| c.set(0));
    let commit = db.write(|tx| {
        for id in 0..199 {
            tx.remove(&right, &CountedId(id))?;
        }
        for id in 0..100 {
            tx.insert(&left, id, 0)?;
        }
        Ok(())
    })?;
    assert_eq!(view.snapshot(&db)?.rows.len(), 100);
    // Besides output assertions, the implementation must meter every visited input
    // candidate, including rows rejected by a staged membership overlay.
    assert!(
        commit.stats.work >= 700,
        "hidden candidate scans are not accounted for: {:?}",
        commit.stats
    );
    assert!(commit.stats.work < 3000);
    let comparisons = COMPARISONS.with(Cell::get);
    eprintln!(
        "join retraction: {comparisons} comparisons, {} work units",
        commit.stats.work
    );
    assert!(
        comparisons < 40_000,
        "one final bucket was rescanned per left row: {comparisons}"
    );
    Ok(())
}

#[test]
fn budget_counts_members_filtered_out_of_final_join_buckets() -> Result<(), Error> {
    let mut db = Database::new();
    let l = db.create_collection::<u64, u64>("l")?;
    let r = db.create_collection::<u64, u64>("r")?;
    db.write(|tx| {
        for id in 0..200 {
            tx.insert(&r, id, 0)?;
        }
        Ok(())
    })?;
    let view = db.materialize(l.query().join_on(r.query(), |_, v| *v, |_, v| *v))?;
    let epoch = db.epoch()?;
    db.set_execution_budget(ExecutionBudget {
        max_work: 700,
        ..Default::default()
    })?;
    assert!(matches!(
        db.write(|tx| {
            for id in 0..200 {
                tx.remove(&r, &id)?;
            }
            for id in 0..100 {
                tx.insert(&l, id, 0)?;
            }
            Ok(())
        }),
        Err(Error::BudgetExceeded("work"))
    ));
    assert_eq!(db.epoch()?, epoch);
    assert!(r.get(&db, &0)?.is_some());
    assert!(l.get(&db, &0)?.is_none());
    assert!(view.snapshot(&db)?.rows.is_empty());
    Ok(())
}

struct Probe {
    value: i64,
    live: std::rc::Rc<Cell<usize>>,
}
impl Probe {
    fn new(value: i64, live: &std::rc::Rc<Cell<usize>>) -> Self {
        live.set(live.get() + 1);
        Self {
            value,
            live: live.clone(),
        }
    }
}
impl Drop for Probe {
    fn drop(&mut self) {
        self.live.set(self.live.get() - 1);
    }
}
#[test]
fn rejected_preparation_and_all_operator_leases_release_payloads() -> Result<(), Error> {
    use std::rc::Rc;
    let live = Rc::new(Cell::new(0));
    let mut db = Database::new();
    let source = db.create_collection::<u64, Probe>("source")?;
    for cycle in 0..30 {
        let counter = live.clone();
        let mapped = source.query().map(move |p| Probe::new(p.value, &counter));
        let stats = db.materialize(
            mapped
                .clone()
                .group_by(|p| p.value % 4)
                .stats_i64(|p| p.value),
        )?;
        let distinct = db.materialize(mapped.clone().distinct_values(|p| p.value))?;
        let ordered = db.materialize_ordered(mapped.clone().sort_by_key(|p| p.value).take(5))?;
        let joined = db.materialize(mapped.join_on(source.query(), |id, _| *id, |id, _| *id))?;
        let (_, subscription) = joined.subscribe(&mut db)?;
        db.set_execution_budget(ExecutionBudget {
            max_output_changes: 101,
            ..Default::default()
        })?;
        assert!(matches!(
            db.write(|tx| {
                for id in 0..100 {
                    tx.insert(&source, id, Probe::new(id as i64, &live))?;
                }
                Ok(())
            }),
            Err(Error::BudgetExceeded(_))
        ));
        assert_eq!(
            live.get(),
            0,
            "cycle {cycle}: rejected source/map allocations leaked"
        );
        db.set_execution_budget(Default::default())?;
        db.write(|tx| {
            for id in 0..100 {
                tx.insert(&source, id, Probe::new(id as i64, &live))?;
            }
            Ok(())
        })?;
        let retained = joined.snapshot(&db)?;
        assert_eq!(live.get(), 200);
        db.write(|tx| {
            for id in 0..100 {
                tx.remove(&source, &id)?;
            }
            Ok(())
        })?;
        drop(stats);
        drop(distinct);
        drop(ordered);
        drop(joined);
        drop(subscription);
        db.write(|_| Ok(()))?;
        assert_eq!(
            live.get(),
            200,
            "caller snapshot deliberately retains both operands"
        );
        drop(retained);
        assert_eq!(
            live.get(),
            0,
            "cycle {cycle}: retired indexes or journals retained payloads"
        );
    }
    Ok(())
}
