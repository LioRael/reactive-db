use std::hint::black_box;
use std::time::{Duration, Instant};
macro_rules! version {
    ($name:ident, $db:ident) => {
        mod $name {
            use std::hint::black_box;
            use $db::Database;
            pub fn workload(shape: &str) -> Box<dyn FnMut()> {
                let mut db = Database::new();
                let hot = db.create_collection::<u64, i64>("hot").unwrap();
                db.write(|tx| tx.insert(&hot, 0, 0)).unwrap();
                let mut tick = 0;
                if shape.starts_with("fanout") {
                    let size = if shape == "fanout1" { 1 } else { 1024 };
                    let right = db.create_collection::<u64, i64>("right").unwrap();
                    db.write(|tx| {
                        for id in 0..size {
                            tx.insert(&right, id, 0)?;
                        }
                        Ok(())
                    })
                    .unwrap();
                    let view = db
                        .materialize(
                            hot.query()
                                .left_join_on(right.query(), |_, _| (), |_, _| ()),
                        )
                        .unwrap();
                    return Box::new(move || {
                        black_box(&view);
                        tick ^= 1;
                        black_box(db.write(|tx| tx.replace(&hot, 0, tick)).unwrap());
                    });
                }
                if shape == "converging" {
                    let left = hot.query().map(|v| *v);
                    let right = hot.query().map(|v| *v).map(|v| *v);
                    let views = (0..10000)
                        .map(|_| {
                            db.materialize(left.clone().join_on(
                                right.clone(),
                                |_, v| *v,
                                |_, v| *v,
                            ))
                            .unwrap()
                        })
                        .collect::<Vec<_>>();
                    return Box::new(move || {
                        black_box(&views);
                        tick ^= 1;
                        black_box(db.write(|tx| tx.replace(&hot, 0, tick)).unwrap());
                    });
                }
                let idle = db.create_collection::<u64, i64>("idle").unwrap();
                let views = if shape == "layered" {
                    let plans = (0..10000)
                        .map(|_| hot.query().map(|v| *v))
                        .collect::<Vec<_>>();
                    let mut views = plans
                        .iter()
                        .map(|p| db.materialize(p.clone()).unwrap())
                        .collect::<Vec<_>>();
                    views.extend(
                        plans
                            .into_iter()
                            .map(|p| db.materialize(p.map(|v| v + 1)).unwrap()),
                    );
                    views
                } else {
                    let parent = match shape {
                        "unaffected" => idle.query(),
                        "pruned" => hot.query().filter(|v| *v < 0),
                        _ => hot.query(),
                    };
                    (0..10000)
                        .map(|_| db.materialize(parent.clone().map(|v| *v)).unwrap())
                        .collect::<Vec<_>>()
                };
                Box::new(move || {
                    black_box((&views, &idle));
                    tick ^= 1;
                    black_box(db.write(|tx| tx.replace(&hot, 0, tick)).unwrap());
                })
            }
        }
    };
}
version!(before, old_db);
version!(after, new_db);
fn elapsed(work: &mut dyn FnMut(), n: usize) -> Duration {
    let start = Instant::now();
    for _ in 0..n {
        work();
    }
    start.elapsed()
}
fn calibrate(work: &mut dyn FnMut()) -> usize {
    let mut n = 1;
    loop {
        let d = elapsed(work, n);
        if d >= Duration::from_millis(20) {
            return ((n as f64 * 0.1 / d.as_secs_f64()) as usize).max(1);
        }
        n *= 2;
    }
}
fn main() {
    let run: usize = std::env::var("BENCH_RUN")
        .unwrap_or_else(|_| "0".into())
        .parse()
        .unwrap();
    let controls = std::env::var("BENCH_CONTROL").is_ok();
    let cases = if controls {
        vec!["unaffected", "pruned"]
    } else {
        vec![
            "unaffected",
            "pruned",
            "fanout1024",
            "fanout1",
            "layered",
            "converging",
            "affected",
        ]
    };
    let mut cases = cases;
    let count = cases.len();
    cases.rotate_left(run % count);
    println!(
        "run,pid,suite,case,instance,created_first,pair,timed_first,iterations,before_ns,after_ns"
    );
    for case in cases {
        let mut suites = if controls || !["unaffected", "pruned"].contains(&case) {
            vec!["paired"]
        } else {
            vec!["old_null", "new_null", "paired"]
        };
        let count = suites.len();
        suites.rotate_left(run % count);
        for suite in suites {
            let instances = if controls { 4 } else { 8 };
            for instance in 0..instances {
                let make_a = || {
                    if suite == "new_null" {
                        after::workload(case)
                    } else {
                        before::workload(case)
                    }
                };
                let make_b = || {
                    if suite == "old_null" {
                        before::workload(case)
                    } else {
                        after::workload(case)
                    }
                };
                let create_a_first = (instance + run).is_multiple_of(2);
                let (mut a, mut b) = if create_a_first {
                    (make_a(), make_b())
                } else {
                    let b = make_b();
                    (make_a(), b)
                };
                // Calibration order is crossed independently of creation order.
                let n = if (instance / 2 + run).is_multiple_of(2) {
                    calibrate(&mut a).min(calibrate(&mut b))
                } else {
                    let nb = calibrate(&mut b);
                    calibrate(&mut a).min(nb)
                };
                for _ in 0..2 {
                    elapsed(&mut a, n);
                    elapsed(&mut b, n);
                }
                for pair in 0..4 {
                    let time_a_first = (pair + instance / 2 + run).is_multiple_of(2);
                    let (da, db) = if time_a_first {
                        (elapsed(&mut a, n), elapsed(&mut b, n))
                    } else {
                        let db = elapsed(&mut b, n);
                        (elapsed(&mut a, n), db)
                    };
                    println!(
                        "{run},{},{suite},{case},{instance},{},{pair},{},{n},{:.6},{:.6}",
                        std::process::id(),
                        if create_a_first { "before" } else { "after" },
                        if time_a_first { "before" } else { "after" },
                        black_box(da.as_nanos() as f64 / n as f64),
                        black_box(db.as_nanos() as f64 / n as f64)
                    );
                }
            }
        }
    }
}
