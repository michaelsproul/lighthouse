#[macro_use]
extern crate criterion;
extern crate db_bench;

use criterion::{black_box, Criterion};
use db_bench::*;
use hashing::hash;
use leveldb::database::iterator::{Iterable, LevelDBIterator};
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
    let path = Path::new("column_vec_read.db");
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

    destroy(path, Options::new()).unwrap();
}

fn column_vec_bulk_read(c: &mut Criterion) {
    let data = make_colvec_data(N);
    let path = Path::new("column_vec_bulk_read.db");
    let store = DiskStore::open(path).expect("open OK");

    for (i, entry) in data.iter().enumerate() {
        entry
            .db_put(&store, &int_key(i as u64))
            .expect("no errors pls!");
    }

    c.bench_function("column vec bulk read", move |b| {
        b.iter(|| {
            let start_key = int_keyb(0);
            let end_key = int_keyb(N);
            let mut iter = store
                .db
                .value_iter(ReadOptions::new())
                .from(&start_key)
                .to(&end_key);
            iter.advance();
            let entries_iter =
                iter.map(|mut bytes| Entries::from_store_bytes(&mut bytes).expect("valid entries"));
            for entry in entries_iter {
                black_box(entry);
            }
        })
    });

    destroy(path, Options::new()).unwrap();
}

fn make_chunk_data(n: u64) -> Vec<Chunk> {
    (0..n)
        .map(|i| Hash256::from_slice(&hash(&i.to_be_bytes())))
        .collect::<Vec<_>>()
        .chunks(CHUNK_COUNT)
        .map(|hash_values| {
            let mut chunk = Chunk::default();
            chunk.values.copy_from_slice(hash_values);
            chunk
        })
        .collect()
}

fn chunk_insert(c: &mut Criterion) {
    let data = make_chunk_data(N);

    c.bench_function("chunk insert", move |b| {
        b.iter(|| {
            let path = Path::new("chunk_read.db");
            let store = DiskStore::open(path).expect("open OK");

            for entry in &data {
                let key = entry.values[CHUNK_COUNT - 1];
                entry.db_put(&store, &key).expect("no errors pls!");
            }
            drop(store);
            destroy(path, Options::new()).expect("destroy OK");
        })
    });
}

fn chunk_read(c: &mut Criterion) {
    let data = make_chunk_data(N);

    let path = Path::new("chunk_read.db");
    let store = DiskStore::open(path).expect("open OK");

    for entry in &data {
        let key = entry.values[CHUNK_COUNT - 1];
        entry.db_put(&store, &key).expect("no errors pls!");
    }

    c.bench_function("chunk read", move |b| {
        b.iter(|| {
            for entry in &data {
                let key = entry.values[CHUNK_COUNT - 1];
                let chunk = Chunk::db_get(&store, &key)
                    .unwrap()
                    .expect("got a real value");
                black_box(chunk);
            }
        })
    });

    destroy(path, Options::new()).expect("destroy OK");
}

criterion_group!(benches, column_vec_read, column_vec_bulk_read, chunk_read);
criterion_main!(benches);
