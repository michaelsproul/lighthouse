#![cfg(all(test, not(debug_assertions)))]
use crate::PeerId;
use libp2p::core::identity::{secp256k1::PublicKey as Libp2pSecpPublicKey, PublicKey};
use libsecp256k1::{PublicKey as SecpPublicKey, SecretKey};
use lru_cache::LRUTimeCache;
use proptest::prelude::*;
use proptest::sample::Index;
use std::time::Duration;

type Cache = LRUTimeCache<PeerId>;

const SHORT_TTL: Duration = Duration::from_millis(5);
const LONG_TTL: Duration = Duration::from_secs(1_000_000_000);

fn arb_secret_key() -> impl Strategy<Value = SecretKey> {
    proptest::array::uniform32(any::<u8>()).prop_map(|mut bytes| loop {
        if let Ok(key) = SecretKey::parse(&bytes) {
            return key;
        }
        bytes[31] += 1;
    })
}

fn arb_public_key() -> impl Strategy<Value = PublicKey> {
    arb_secret_key().prop_map(|sk| {
        let spk = SecpPublicKey::from_secret_key(&sk);
        let bytes = spk.serialize_compressed();
        PublicKey::Secp256k1(Libp2pSecpPublicKey::decode(&bytes).unwrap())
    })
}

fn arb_peer_id() -> impl Strategy<Value = PeerId> {
    arb_public_key().prop_map(|pk| PeerId::from_public_key(&pk))
}

fn arb_index() -> impl Strategy<Value = Index> {
    any::<proptest::sample::Index>()
}

#[derive(Debug, Clone)]
enum Op {
    RawInsert(Index),
    RawRemove(Index),
    RemoveExpired,
    Insert(Index),
    Update,
    Wait(Duration),
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        arb_index().prop_map(Op::RawInsert),
        arb_index().prop_map(Op::RawRemove),
        Just(Op::RemoveExpired),
        arb_index().prop_map(Op::Insert),
        Just(Op::Update),
        arb_index().prop_map(|idx| {
            let millis = idx.index(SHORT_TTL.as_millis() as usize);
            Op::Wait(Duration::from_millis(millis as u64))
        })
    ]
}

fn apply_op(cache: &mut Cache, peer_ids: &[PeerId], op: Op) {
    let n = peer_ids.len();
    let get_peer_id = |idx: Index| {
        let i = idx.index(n);
        peer_ids[i].clone()
    };
    match op {
        Op::RawInsert(i) => {
            cache.raw_insert(get_peer_id(i));
        }
        Op::RawRemove(i) => {
            cache.raw_remove(&get_peer_id(i));
        }
        Op::RemoveExpired => {
            cache.remove_expired();
        }
        Op::Insert(i) => {
            cache.insert(get_peer_id(i));
        }
        Op::Update => {
            cache.update();
        }
        Op::Wait(time) => {
            // This is a bit yuck for a proptest.
            std::thread::sleep(time);
        }
    }
}

proptest! {
    #[test]
    fn proptest_insert_peer_ids(
        peer_ids in proptest::collection::vec(arb_peer_id(), 1..200),
    ) {
        let mut cache = Cache::new(LONG_TTL);
        for peer_id in peer_ids {
            cache.insert(peer_id);
        }
        cache.check_invariant();
    }

    #[test]
    fn proptest_random_ops(
        peer_ids in proptest::collection::vec(arb_peer_id(), 50..100),
        ops in proptest::collection::vec(arb_op(), 1..2048)
    ) {
        let mut cache = Cache::new(SHORT_TTL);
        for op in ops {
            apply_op(&mut cache, &peer_ids, op);
            cache.check_invariant();
        }
    }
}
