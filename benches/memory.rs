//! Isolated lifetime/RSS probe. RSS is diagnostic; Drop counts are the release oracle.
use reactive_db::{Database, Error};
use std::cell::Cell;
use std::rc::Rc;
use std::time::Instant;
struct Payload {
    key: u64,
    bytes: Box<[u8]>,
    live: Rc<Cell<usize>>,
}
impl Payload {
    fn new(key: u64, live: &Rc<Cell<usize>>) -> Self {
        live.set(live.get() + 1);
        Self {
            key,
            bytes: vec![42; 256].into_boxed_slice(),
            live: live.clone(),
        }
    }
}
impl Drop for Payload {
    fn drop(&mut self) {
        self.live.set(self.live.get() - 1);
    }
}
fn rss() -> String {
    std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()
        .filter(|v| v.status.success())
        .map(|v| String::from_utf8_lossy(&v.stdout).trim().to_owned())
        .unwrap_or_else(|| "unavailable".into())
}
fn main() -> Result<(), Error> {
    let live = Rc::new(Cell::new(0));
    println!(
        "cycle,rows,payload_bytes_each,loaded_rss_kib,after_drop_rss_kib,live_after_drop,cycle_ms"
    );
    for cycle in 0..10 {
        let started = Instant::now();
        let n = 10_000;
        let mut db = Database::new();
        let source = db.create_collection::<u64, Payload>("source")?;
        db.write(|tx| {
            for id in 0..n {
                tx.insert(&source, id, Payload::new(id, &live))?;
            }
            Ok(())
        })?;
        let counter = live.clone();
        let mapped = source.query().map(move |p| {
            std::hint::black_box(&p.bytes);
            Payload::new(p.key, &counter)
        });
        let group = db.materialize(
            mapped
                .clone()
                .group_by(|p| p.key % 64)
                .stats_i64(|p| p.key as i64),
        )?;
        let distinct = db.materialize(mapped.clone().distinct_values(|p| p.key))?;
        let ordered = db.materialize_ordered(mapped.clone().sort_by_key(|p| p.key).take(10))?;
        let joined = db.materialize(mapped.join_on(source.query(), |id, _| *id, |id, _| *id))?;
        let (snapshot, sub) = joined.subscribe(&mut db)?;
        assert_eq!(live.get(), 2 * n as usize);
        let loaded = rss();
        drop(db);
        assert_eq!(
            live.get(),
            2 * n as usize,
            "external snapshot retains both operands after database drop"
        );
        drop(snapshot);
        assert_eq!(
            live.get(),
            0,
            "no runtime/index/journal reference survives owner and snapshot"
        );
        drop((source, group, distinct, ordered, joined, sub));
        println!(
            "{cycle},{n},256,{loaded},{},{},{:.3}",
            rss(),
            live.get(),
            started.elapsed().as_secs_f64() * 1000.0
        );
    }
    Ok(())
}
