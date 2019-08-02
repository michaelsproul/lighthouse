#[macro_use]
extern crate criterion;
extern crate db_bench;

use criterion::{black_box, Criterion};
use db_bench::*;
use hashing::hash;
use leveldb::database::iterator::{Iterable, LevelDBIterator};
use leveldb::database::{management::destroy, options::*};
use std::path::Path;

const N: u64 = 16384;

fn make_colvec_data(n: u64) -> Vec<Entry> {
    (0..n)
        .map(|i| Entry {
            value: Hash256::from_low_u64_be(i),
            // state_root: Hash256::from_low_u64_be(u64::max_value() - i),
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

    let start_key = int_keyb(0);
    let end_key = int_keyb(N);

    c.bench_function("column vec bulk read", move |b| {
        b.iter(|| {
            let mut read_options = ReadOptions::new();
            read_options.fill_cache = false;

            let collected_data: Vec<Hash256> = store
                .db
                .value_iter(read_options)
                .from(&start_key)
                .to(&end_key)
                .flat_map(|mut bytes| {
                    let entries = Entries::from_store_bytes(&mut bytes)
                        .expect("valid entries")
                        .0;
                    entries.vec.into_iter().map(|e| e.value).collect::<Vec<_>>()
                    /*
                    Entry::from_store_bytes(&mut bytes)
                        .expect("valid entries")
                        .0
                        .value
                    */
                })
                .collect();
            assert_eq!(collected_data.len() as u64, N);
            black_box(collected_data);
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
            let collected_data: Vec<Hash256> = data
                .iter()
                .flat_map(|entry| {
                    let key = entry.values[CHUNK_COUNT - 1];
                    let chunk = Chunk::db_get(&store, &key)
                        .unwrap()
                        .expect("got a real value");
                    chunk.values.to_vec()
                })
                .collect();
            assert_eq!(collected_data.len() as u64, N);
            black_box(collected_data);
        })
    });

    destroy(path, Options::new()).expect("destroy OK");
}

fn make_hybrid_data(n: u64) -> Vec<ChunkedEntry<Hash256>> {
    (0..n)
        .map(|i| Hash256::from_slice(&hash(&i.to_be_bytes())))
        .collect::<Vec<_>>()
        .chunks(CHUNK_COUNT)
        .enumerate()
        .map(|(i, hash_values)| ChunkedEntry {
            id: int_key(u64::max_value() - i as u64),
            values: hash_values.to_vec(),
        })
        .collect()
}

fn hybrid_read(c: &mut Criterion) {
    let data = make_hybrid_data(N);

    let path = Path::new("hybrid_read.db");
    let store = DiskStore::open(path).expect("open OK");

    for (i, chunked_entry) in data.iter().enumerate() {
        chunked_entry
            .db_put(&store, &int_key(i as u64))
            .expect("no errors pls!");
    }

    c.bench_function("hybrid read", move |b| {
        b.iter(|| {
            let mut read_options = ReadOptions::new();
            read_options.fill_cache = false;

            let collected_data: Vec<Hash256> = store
                .db
                .value_iter(read_options)
                .flat_map(|mut bytes| {
                    ChunkedEntry::from_store_bytes(&mut bytes)
                        .expect("valid entries")
                        .0
                        .values
                })
                .collect();
            assert_eq!(collected_data.len() as u64, N);
            black_box(collected_data);
        })
    });

    destroy(path, Options::new()).expect("destroy OK");
}

criterion_group!(benches, column_vec_bulk_read, chunk_read, hybrid_read);
criterion_main!(benches);
