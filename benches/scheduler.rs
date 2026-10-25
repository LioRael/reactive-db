//! Graph-width, propagation pruning and dense-update scheduler benchmarks.
use criterion::{BenchmarkId, Criterion};
use reactive_db::Database;
use std::hint::black_box;
use std::time::Duration;

fn main() {
    let mut c = Criterion::default()
        .sample_size(30)
        .warm_up_time(Duration::from_millis(150))
        .measurement_time(Duration::from_millis(500))
        .nresamples(10_000)
        .configure_from_args();
    let mut group = c.benchmark_group("scheduler");
    for width in [0, 100, 1000, 10_000] {
        for shape in ["unaffected", "empty_write", "pruned", "affected"] {
            let mut db = Database::new();
            let hot = db.create_collection::<u64, i64>("hot").unwrap();
            let idle = db.create_collection::<u64, i64>("idle").unwrap();
            db.write(|tx| tx.insert(&hot, 0, 0)).unwrap();
            let parent = match shape {
                "unaffected" | "empty_write" => idle.query(),
                "pruned" => hot.query().filter(|v| *v < 0),
                _ => hot.query(),
            };
            let views = (0..width)
                .map(|_| db.materialize(parent.clone().map(|v| *v)).unwrap())
                .collect::<Vec<_>>();
            let mut tick = 0;
            group.bench_function(BenchmarkId::new(shape, width), |b| {
                b.iter(|| {
                    tick ^= 1;
                    let commit = if shape == "empty_write" {
                        db.write(|_| Ok(()))
                    } else {
                        db.write(|tx| tx.replace(&hot, 0, tick))
                    };
                    black_box(commit.unwrap());
                })
            });
            drop(black_box(views));
        }
    }
    group.finish();
    c.final_summary();
}
