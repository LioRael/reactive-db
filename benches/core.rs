//! Reproducible release benchmarks; setup and model validation are outside timed loops.
use criterion::{BenchmarkId, Criterion};
use reactive_db::{Database, ExecutionBudget};
use std::any::Any;
use std::hint::black_box;
use std::time::Duration;

#[derive(Clone, Copy)]
struct Item {
    group: u64,
    value: i64,
}
fn mutations(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_update");
    for n in [100_u64, 10_000, 100_000] {
        for kind in [
            "source",
            "filter_map",
            "group_stats",
            "distinct",
            "sort",
            "top10",
            "join_one",
        ] {
            let mut db = Database::new();
            let rows = db.create_collection::<u64, Item>("rows").unwrap();
            db.write(|tx| {
                for id in 0..n {
                    tx.insert(
                        &rows,
                        id,
                        Item {
                            group: id % 64,
                            value: id as i64,
                        },
                    )?;
                }
                Ok(())
            })
            .unwrap();
            let q = rows.query();
            let lease: Box<dyn Any> = match kind {
                "source" => Box::new(db.materialize(q).unwrap()),
                "filter_map" => Box::new(
                    db.materialize(q.filter(|v| v.value >= 0).map(|v| v.value * 2))
                        .unwrap(),
                ),
                "group_stats" => Box::new(
                    db.materialize(q.group_by(|v| v.group).stats_i64(|v| v.value))
                        .unwrap(),
                ),
                "distinct" => Box::new(db.materialize(q.distinct_values(|v| v.value)).unwrap()),
                "sort" => Box::new(db.materialize_ordered(q.sort_by_key(|v| v.value)).unwrap()),
                "top10" => Box::new(
                    db.materialize_ordered(q.sort_by_key(|v| v.value).take(10))
                        .unwrap(),
                ),
                "join_one" => {
                    let right = db.create_collection::<u64, u64>("right").unwrap();
                    db.write(|tx| {
                        for id in 0..n {
                            tx.insert(&right, id, id)?;
                        }
                        Ok(())
                    })
                    .unwrap();
                    Box::new(
                        db.materialize(q.join_on(right.query(), |id, _| *id, |id, _| *id))
                            .unwrap(),
                    )
                }
                _ => unreachable!(),
            };
            let mut tick = 0_i64;
            group.bench_with_input(BenchmarkId::new(kind, n), &n, |b, &n| {
                b.iter(|| {
                    tick ^= 1;
                    black_box(
                        db.write(|tx| {
                            tx.replace(
                                &rows,
                                n - 1,
                                Item {
                                    group: (n - 1) % 64,
                                    value: if tick == 0 { -1 } else { n as i64 },
                                },
                            )
                        })
                        .unwrap(),
                    );
                })
            });
            drop(black_box(lease));
        }
    }
    group.finish();
}
fn windows_and_batches(c: &mut Criterion) {
    let mut group = c.benchmark_group("window_and_delta");
    for width in [0, 10, 1000] {
        let mut db = Database::new();
        let rows = db.create_collection::<u64, i64>("rows").unwrap();
        db.write(|tx| {
            for id in 0..10_000 {
                tx.insert(&rows, id, id as i64)?;
            }
            Ok(())
        })
        .unwrap();
        let view = db
            .materialize_ordered(rows.query().sort_by_key(|v| *v).take(width))
            .unwrap();
        let mut tick = 0;
        group.bench_function(BenchmarkId::new("topk", width), |b| {
            b.iter(|| {
                tick ^= 1;
                black_box(
                    db.write(|tx| tx.replace(&rows, 9999, if tick == 0 { -1 } else { 10000 }))
                        .unwrap(),
                );
            })
        });
        drop(black_box(view));
    }
    for delta in [1, 100, 1000] {
        let mut db = Database::new();
        let rows = db.create_collection::<u64, i64>("rows").unwrap();
        db.write(|tx| {
            for id in 0..10_000 {
                tx.insert(&rows, id, id as i64)?;
            }
            Ok(())
        })
        .unwrap();
        let view = db
            .materialize(rows.query().filter(|v| *v >= 0).map(|v| v * 2))
            .unwrap();
        let mut tick = 0;
        group.bench_function(BenchmarkId::new("filter_batch", delta), |b| {
            b.iter(|| {
                tick ^= 1;
                black_box(
                    db.write(|tx| {
                        for id in 0..delta {
                            tx.replace(&rows, id, if tick == 0 { -1 } else { id as i64 })?;
                        }
                        Ok(())
                    })
                    .unwrap(),
                );
            })
        });
        drop(black_box(view));
    }
    group.finish();
}
fn joins(c: &mut Criterion) {
    let mut group = c.benchmark_group("join_shapes");
    for fanout in [1, 64, 1024] {
        let mut db = Database::new();
        let l = db.create_collection::<u64, i64>("l").unwrap();
        let r = db.create_collection::<u64, i64>("r").unwrap();
        db.write(|tx| {
            tx.insert(&l, 0, 0)?;
            for id in 0..fanout {
                tx.insert(&r, id, 0)?;
            }
            Ok(())
        })
        .unwrap();
        let view = db
            .materialize(l.query().left_join_on(r.query(), |_, _| (), |_, _| ()))
            .unwrap();
        let mut tick = 0;
        group.bench_function(BenchmarkId::new("fanout", fanout), |b| {
            b.iter(|| {
                tick ^= 1;
                black_box(db.write(|tx| tx.replace(&l, 0, tick)).unwrap());
            })
        });
        drop(black_box(view));
    }
    for n in [200, 2000] {
        let mut db = Database::new();
        let l = db.create_collection::<u64, u64>("l").unwrap();
        let r = db.create_collection::<u64, u64>("r").unwrap();
        db.write(|tx| {
            for id in 0..n {
                tx.insert(&r, id, 0)?;
            }
            Ok(())
        })
        .unwrap();
        let view = db
            .materialize(l.query().join_on(r.query(), |_, v| *v, |_, v| *v))
            .unwrap();
        let mut populated = false;
        group.bench_function(BenchmarkId::new("retract_bucket", n), |b| {
            b.iter(|| {
                black_box(
                    db.write(|tx| {
                        if populated {
                            for id in 0..100 {
                                tx.remove(&l, &id)?;
                            }
                            for id in 0..n - 1 {
                                tx.insert(&r, id, 0)?;
                            }
                        } else {
                            for id in 0..n - 1 {
                                tx.remove(&r, &id)?;
                            }
                            for id in 0..100 {
                                tx.insert(&l, id, 0)?;
                            }
                        }
                        Ok(())
                    })
                    .unwrap(),
                );
                populated = !populated;
            })
        });
        drop(black_box(view));
    }
    group.finish();
}
fn reads_and_model(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_and_full_model");
    for n in [1000_u64, 10_000, 100_000] {
        let mut db = Database::new();
        let rows = db.create_collection::<u64, i64>("rows").unwrap();
        db.write(|tx| {
            for id in 0..n {
                tx.insert(&rows, id, id as i64)?;
            }
            Ok(())
        })
        .unwrap();
        let view = db
            .materialize_ordered(rows.query().sort_by_key(|v| *v))
            .unwrap();
        group.bench_function(BenchmarkId::new("snapshot", n), |b| {
            b.iter(|| black_box(view.snapshot(&db).unwrap()))
        });
        let mut input = (0..n).map(|id| id as i64).collect::<Vec<_>>();
        let mut tick = 0;
        group.bench_function(BenchmarkId::new("full_filter_map", n), |b| {
            b.iter(|| {
                tick ^= 1;
                input[n as usize - 1] = if tick == 0 { -1 } else { n as i64 };
                black_box(
                    input
                        .iter()
                        .filter(|&&v| v >= 0)
                        .map(|v| v * 2)
                        .collect::<Vec<_>>(),
                );
            })
        });
        group.bench_function(BenchmarkId::new("full_sort_top10", n), |b| {
            b.iter(|| {
                tick ^= 1;
                input[n as usize - 1] = if tick == 0 { -1 } else { n as i64 };
                let mut sorted = input.iter().copied().enumerate().collect::<Vec<_>>();
                sorted.sort_unstable_by_key(|&(id, v)| (v, id));
                sorted.truncate(10);
                black_box(sorted);
            })
        });
    }
    group.finish();
}
fn graph_and_budget(c: &mut Criterion) {
    let mut group = c.benchmark_group("graph_and_budget");
    for depth in [4, 8, 12] {
        let mut db = Database::new();
        let source = db.create_collection::<u64, u64>("source").unwrap();
        let mut q = source.query();
        for _ in 0..depth {
            q = q
                .clone()
                .join_on(q, |_, _| (), |_, _| ())
                .group_by(|_| 0_u64)
                .count();
        }
        let view = db.materialize(q.clone()).unwrap();
        group.bench_function(BenchmarkId::new("rematerialize_diamond", depth), |b| {
            b.iter(|| black_box(db.materialize(q.clone()).unwrap()))
        });
        drop(black_box(view));
    }
    let mut db = Database::new();
    let l = db.create_collection::<u64, u64>("l").unwrap();
    let r = db.create_collection::<u64, u64>("r").unwrap();
    db.write(|tx| {
        for id in 0..10_000 {
            tx.insert(&l, id, 0)?;
        }
        Ok(())
    })
    .unwrap();
    let view = db
        .materialize(l.query().join_on(r.query(), |_, _| (), |_, _| ()))
        .unwrap();
    db.set_execution_budget(ExecutionBudget {
        max_work: 100,
        ..Default::default()
    })
    .unwrap();
    group.bench_function("reject_fanout", |b| {
        b.iter(|| assert!(black_box(db.write(|tx| tx.insert(&r, 0, 0))).is_err()))
    });
    drop(black_box(view));
    group.finish();
}
fn delivery_and_graph_width(c: &mut Criterion) {
    let mut group = c.benchmark_group("delivery_and_graph_width");
    for subscribers in [1, 100] {
        let mut db = Database::new();
        let rows = db.create_collection::<u64, i64>("rows").unwrap();
        db.write(|tx| tx.insert(&rows, 0, 0)).unwrap();
        let view = db
            .materialize_ordered(rows.query().sort_by_key(|v| *v))
            .unwrap();
        let mut subscriptions = (0..subscribers)
            .map(|_| view.subscribe(&mut db).unwrap().1)
            .collect::<Vec<_>>();
        let mut tick = 0;
        group.bench_function(
            BenchmarkId::new("ordered_write_and_poll", subscribers),
            |b| {
                b.iter(|| {
                    tick ^= 1;
                    black_box(db.write(|tx| tx.replace(&rows, 0, tick)).unwrap());
                    for sub in &mut subscriptions {
                        drop(black_box(sub.poll(&db).unwrap()));
                    }
                })
            },
        );
    }
    for width in [0, 100, 1000] {
        let mut db = Database::new();
        let idle = db.create_collection::<u64, i64>("idle").unwrap();
        let changed = db.create_collection::<u64, i64>("changed").unwrap();
        db.write(|tx| tx.insert(&changed, 0, 0)).unwrap();
        let views = (0..width)
            .map(|_| db.materialize(idle.query().map(|v| *v)).unwrap())
            .collect::<Vec<_>>();
        let mut tick = 0;
        group.bench_function(BenchmarkId::new("unaffected_nodes", width), |b| {
            b.iter(|| {
                tick ^= 1;
                black_box(db.write(|tx| tx.replace(&changed, 0, tick)).unwrap());
            })
        });
        drop(black_box(views));
    }
    group.finish();
}
fn isolated_snapshot(c: &mut Criterion) {
    let mut db = Database::new();
    let rows = db.create_collection::<u64, i64>("rows").unwrap();
    db.write(|tx| {
        for id in 0..100_000 {
            tx.insert(&rows, id, id as i64)?;
        }
        Ok(())
    })
    .unwrap();
    let view = db
        .materialize_ordered(rows.query().sort_by_key(|v| *v))
        .unwrap();
    let mut group = c.benchmark_group("snapshot_isolated");
    group
        .sample_size(40)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2));
    group.bench_function("100000", |b| {
        b.iter(|| black_box(view.snapshot(&db).unwrap()))
    });
    group.finish();
}
fn main() {
    let mut c = Criterion::default()
        .sample_size(20)
        .warm_up_time(Duration::from_millis(100))
        .measurement_time(Duration::from_millis(400))
        .nresamples(10_000)
        .configure_from_args();
    if std::env::var("REACTIVE_DB_BENCH_SUITE").as_deref() == Ok("snapshot") {
        isolated_snapshot(&mut c);
        c.final_summary();
        return;
    }
    mutations(&mut c);
    windows_and_batches(&mut c);
    joins(&mut c);
    reads_and_model(&mut c);
    graph_and_budget(&mut c);
    delivery_and_graph_width(&mut c);
    c.final_summary();
}
