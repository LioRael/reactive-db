//! Diagnostic only: apply batch_index_probe.patch to a temporary baseline copy.
//! See docs/batch-index-counters.md; never enable this switch in the product.
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

fn window(work: &mut dyn FnMut(), n: usize, mode: u8) -> Duration {
    reactive_db::probe_inline_patches(mode != 0);
    if mode < 2 {
        return elapsed(work, n);
    }
    let start = Instant::now();
    for _ in 0..n {
        let padding = black_box(vec![0usize; 35]);
        if mode == 2 {
            drop(padding);
            work();
        } else {
            work();
            drop(padding);
        }
    }
    start.elapsed()
}
fn main() {
    use reactive_db::probe_inline_patches as set;
    if std::env::var("ALLOC_PROBE").is_ok() {
        use std::io::Write;
        set(false);
        let mut work = work::workload("fanout1024");
        window(&mut work, 512, 0);
        println!("ready");
        std::io::stdout().flush().unwrap();
        std::io::stdin().read_line(&mut String::new()).unwrap();
        for mode in 0..4 {
            window(&mut work, 10, mode);
            println!("{mode}");
            std::io::stdout().flush().unwrap();
            std::io::stdin().read_line(&mut String::new()).unwrap();
        }
        return;
    }
    let run: usize = std::env::var("BENCH_RUN")
        .unwrap_or_else(|_| "0".into())
        .parse()
        .unwrap();
    println!("run,case,instance,round,condition,sequence,window,role,mode,iterations,ns");
    for shape in [
        "unaffected",
        "pruned",
        "fanout1024",
        "layered",
        "converging",
    ] {
        for instance in 0usize..4 {
            let ballast = perturb(if instance.is_multiple_of(2) {
                "fresh"
            } else {
                "churn"
            });
            set(false);
            let mut work = work::workload(shape);
            for _ in 0..128 {
                window(&mut work, 2, 0);
                window(&mut work, 2, 1);
            }
            set(false);
            let a = calibrate(&mut work);
            set(true);
            let b = calibrate(&mut work);
            let n = (a.min(b) / 2).max(2) / 2 * 2;
            let mut conditions = vec![(0, 0, "tree_null"), (1, 1, "inline_null"), (0, 1, "paired")];
            if shape == "fanout1024" {
                conditions.extend([(1, 2, "padding_before"), (1, 3, "padding_held")]);
            }
            let count = conditions.len();
            conditions.rotate_left((run + instance) % count);
            for round in 0..2 {
                for &(a, b, label) in &conditions {
                    let abba = (run + instance + round).is_multiple_of(2);
                    let modes = if abba { [a, b, b, a] } else { [b, a, a, b] };
                    for (i, mode) in modes.into_iter().enumerate() {
                        let dt = window(&mut work, n, mode);
                        let a_role = if abba {
                            i == 0 || i == 3
                        } else {
                            i == 1 || i == 2
                        };
                        println!(
                            "{run},{shape},{instance},{round},{label},{},{i},{},{mode},{n},{}",
                            if abba { "ABBA" } else { "BAAB" },
                            if a_role { "A" } else { "B" },
                            dt.as_nanos() as f64 / n as f64
                        );
                    }
                }
            }
            black_box(ballast);
        }
    }
}
