//! Work-count checks for incremental updates without wall-clock timing assumptions.

use reactive_db::{Database, Error};
use std::cell::Cell;
use std::rc::Rc;

thread_local! { static KEY_CLONES: Cell<usize> = const { Cell::new(0) }; }
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Key(u64);
impl Clone for Key {
    fn clone(&self) -> Self {
        KEY_CLONES.with(|counter| counter.set(counter.get() + 1));
        Self(self.0)
    }
}

#[test]
fn sparse_single_write_does_not_clone_table_keys_or_revisit_untouched_payloads() -> Result<(), Error>
{
    for size in [100, 10_000] {
        let mut db = Database::new();
        let rows = db.create_collection::<Key, u64>("rows")?;
        db.write(|tx| {
            for id in 0..size {
                tx.insert(&rows, Key(id), id)?;
            }
            Ok(())
        })?;
        let invocations = Rc::new(Cell::new(0));
        let count = invocations.clone();
        let _view = db.materialize(rows.query().map(move |v| {
            count.set(count.get() + 1);
            v * 2
        }))?;
        invocations.set(0);
        KEY_CLONES.with(|counter| counter.set(0));
        db.write(|tx| tx.replace(&rows, Key(0), 42))?;
        let clones = KEY_CLONES.with(Cell::get);
        assert_eq!(invocations.get(), 1, "size={size}");
        assert!(
            clones < 20,
            "sparse update cloned {clones} keys for size={size}"
        );
    }
    Ok(())
}

#[test]
fn rematerializing_plan_clones_shares_runtime_but_distinct_closures_do_not() -> Result<(), Error> {
    let mut db = Database::new();
    let rows = db.create_collection::<u64, u64>("rows")?;
    db.write(|tx| tx.insert(&rows, 1, 10))?;
    let invocations = Rc::new(Cell::new(0));
    let count = invocations.clone();
    let plan = rows.query().map(move |v| {
        count.set(count.get() + 1);
        v * 2
    });
    let a = db.materialize(plan.clone())?;
    let b = db.materialize(plan)?;
    assert_eq!(
        invocations.get(),
        1,
        "second materialization must reuse existing initialization"
    );
    db.write(|tx| tx.replace(&rows, 1, 20))?;
    assert_eq!(invocations.get(), 2);
    assert_eq!(
        *a.snapshot(&db)?.rows[0].value,
        *b.snapshot(&db)?.rows[0].value
    );
    let other_count = invocations.clone();
    let _independent = db.materialize(rows.query().map(move |v| {
        other_count.set(other_count.get() + 1);
        v * 2
    }))?;
    assert_eq!(
        invocations.get(),
        3,
        "distinct construction has its own node"
    );
    db.write(|tx| tx.replace(&rows, 1, 30))?;
    assert_eq!(invocations.get(), 5, "independent nodes both execute");
    Ok(())
}
