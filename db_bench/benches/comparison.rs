#[macro_use]
extern crate criterion;
extern crate db_bench;

use criterion::{black_box, Criterion};
use db_bench::*;
use hashing::hash;
use leveldb::database::iterator::Iterable;
use leveldb::database::{management::destroy, options::*};
use std::path::Path;

const N: u64 = 1024;

fn make_colvec_data(n: u64) -> Vec<Entry> {
    (0..n)
        .map(|i| Entry {
            value: Hash256::from_low_u64_be(i),
            state_root: Hash256::from_low_u64_be(u64::max_value() - i),
        })
        .collect()
}

fn column_vec_insert(c: &mut Criterion) {
    let data = make_colvec_data(N);
    c.bench_function("column vec insert", move |b| {
        b.iter(|| {
            let path = Path::new("test.db");
            let store = DiskStore::open(path).expect("open OK");

            for (i, entry) in data.iter().enumerate() {
                entry
                    .db_put(&store, &int_key(i as u64))
                    .expect("no errors pls!");
            }
            drop(store);
            destroy(path, Options::new()).expect("destroy OK");
        })
    });
}

fn column_vec_read(c: &mut Criterion) {
    let data = make_colvec_data(N);
    let path = Path::new("test.db");
    let store = DiskStore::open(path).expect("open OK");

    for (i, entry) in data.iter().enumerate() {
        entry
            .db_put(&store, &int_key(i as u64))
            .expect("no errors pls!");
    }

    c.bench_function("column vec read", move |b| {
        b.iter(|| {
            for i in 0..N {
                let entries = Entries::db_get(&store, &int_key(i))
                    .unwrap()
                    .expect("got a real value");
                black_box(entries);
            }
        })
    });
}

fn column_vec_bulk_read(c: &mut Criterion) {
    let data = make_colvec_data(N);
    let path = Path::new("test.db");
    let store = DiskStore::open(path).expect("open OK");

    for (i, entry) in data.iter().enumerate() {
        entry
            .db_put(&store, &int_key(i as u64))
            .expect("no errors pls!");
    }

    c.bench_function("column vec read", move |b| {
        b.iter(|| {
            let mut iter = store
                .db
                .value_iter(ReadOptions::new())
                .from(&int_keyb(0))
                .to(&int_keyb(N));
            iter.advance();
            let entries_iter =
                iter.map(|bytes| Entries::from_store_bytes(&mut bytes).expect("valid entries"));
            for entry in entries_iter {
                black_box(entry);
            }
        })
    });
}

fn chunk_insert(c: &mut Criterion) {
    let n = 256_u64;
    let data_0: Vec<Chunk> = (0..n)
        .map(|i| Hash256::from_slice(&hash(&i.to_be_bytes())))
        .collect::<Vec<_>>()
        .chunks(CHUNK_COUNT)
        .map(|hash_values| {
            let mut chunk = Chunk::default();
            chunk.values.copy_from_slice(hash_values);
            chunk
        })
        .collect();

    c.bench_function("chunk insert", move |b| {
        b.iter(|| {
            let path = Path::new("test.db");
            let store = DiskStore::open(path).expect("open OK");

            for entry in data_0.iter() {
                let key = entry.values[CHUNK_COUNT - 1];
                entry.db_put(&store, &key).expect("no errors pls!");
            }
            drop(store);
            destroy(path, Options::new()).expect("destroy OK");
        })
    });
}

criterion_group!(benches, column_vec_read);
criterion_main!(benches);
