//! Diagnostic-only harness; apply batch_index_probe.patch to a temporary baseline copy.
//! See docs/batch-index-cause.md. No diagnostic switch belongs in the product build.
//! Same-instance crossed source patch diagnostic.
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
                if !shape.starts_with("fanout") {
                    db.write(|tx| tx.insert(&hot, 0, 0)).unwrap();
                }
                let mut tick = 0;
                if shape.starts_with("fanout") {
                    let size = if shape == "fanout1" { 1 } else { 1024 };
                    let right = db.create_collection::<u64, i64>("right").unwrap();
                    db.write(|tx| {
                        tx.insert(&hot, 0, 0)?;
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
            return ((n as f64 * 0.05 / d.as_secs_f64()) as usize).max(1);
        }
        n *= 2;
    }
}
fn perturb(mode: &str) -> Vec<Vec<u8>> {
    if mode == "fresh" {
        return Vec::new();
    }
    let blocks = (0..4096)
        .map(|i| vec![i as u8; [32, 48, 64, 128, 256, 1024][i % 6]])
        .collect::<Vec<_>>();
    black_box(&blocks);
    if mode == "churn" {
        drop(blocks);
        Vec::new()
    } else {
        blocks
    }
}

version!(work, reactive_db);

fn main() {
    use reactive_db::probe_inline_patches as set;
    let run: usize = std::env::var("BENCH_RUN")
        .unwrap_or_else(|_| "0".into())
        .parse()
        .unwrap();
    println!("run,case,setup,instance,pair,first,tree_ns,inline_ns");
    for shape in ["fanout1024"] {
        for setup in [false, true] {
            for instance in 0..4 {
                let ballast = perturb("churn");
                set(setup);
                let mut work = work::workload(shape);
                set(false);
                let a = calibrate(&mut work);
                set(true);
                let n = a.min(calibrate(&mut work));
                for _ in 0..2 {
                    set(false);
                    elapsed(&mut work, n);
                    set(true);
                    elapsed(&mut work, n);
                }
                for pair in 0..4 {
                    let inline_first = (run + instance + pair).is_multiple_of(2);
                    let (a, b) = if inline_first {
                        set(true);
                        let b = elapsed(&mut work, n);
                        set(false);
                        (elapsed(&mut work, n), b)
                    } else {
                        set(false);
                        let a = elapsed(&mut work, n);
                        set(true);
                        (a, elapsed(&mut work, n))
                    };
                    println!(
                        "{run},{shape},{setup},{instance},{pair},{inline_first},{},{}",
                        a.as_nanos() as f64 / n as f64,
                        b.as_nanos() as f64 / n as f64
                    );
                }
                black_box(ballast);
            }
        }
    }
}
