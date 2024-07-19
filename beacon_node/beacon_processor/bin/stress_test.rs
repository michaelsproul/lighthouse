use beacon_processor::*;
use lighthouse_network::{
    discovery::CombinedKey,
    rpc::methods::{MetaData, MetaDataV1},
    Enr, NetworkGlobals,
};
use logging::test_logger;
use slot_clock::{ManualSlotClock, SlotClock};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use task_executor::TaskExecutor;
use tokio::runtime::Handle;
use types::{MainnetEthSpec, Slot};

type E = MainnetEthSpec;

#[tokio::main(worker_threads = 32)]
async fn main() {
    let max_workers = 32;

    let log = test_logger();

    let handle = Handle::current();

    let (_exit_tx, exit_rx) = async_channel::unbounded();
    let (signal_tx, _signal_rx) = futures::channel::mpsc::channel(1);

    let executor = TaskExecutor::new(handle, exit_rx, log.clone(), signal_tx);

    let key = CombinedKey::generate_secp256k1();
    let enr = Enr::empty(&key).unwrap();
    let local_metadata = MetaData::<E>::V1(MetaDataV1 {
        seq_number: 0,
        attnets: Default::default(),
    });
    let network_globals = Arc::new(NetworkGlobals::new(enr, local_metadata, vec![], true, &log));

    let queue_lengths = BeaconProcessorQueueLengths::from_active_validator_count::<E>(1_000_000);

    let config = BeaconProcessorConfig {
        max_workers,
        ..BeaconProcessorConfig::default()
    };
    let channels = BeaconProcessorChannels::<E>::new(&config);

    let processor = BeaconProcessor {
        network_globals,
        executor,
        current_workers: 0,
        config,
        log: log.clone(),
    };

    let slot_clock = ManualSlotClock::new(
        Slot::new(0),
        Duration::from_millis(0),
        Duration::from_secs(12),
    );

    processor
        .spawn_manager(
            channels.beacon_processor_rx,
            channels.work_reprocessing_tx,
            channels.work_reprocessing_rx,
            None,
            slot_clock,
            Duration::from_millis(500),
            queue_lengths,
        )
        .unwrap();

    let jobs_completed = Arc::new(AtomicUsize::new(0));
    let time_taken_us = Arc::new(AtomicU64::new(0));

    let worker_jobs_completed = jobs_completed.clone();
    let worker_time_taken_us = time_taken_us.clone();

    let worker_fn = move || {
        let t = Instant::now();
        let reps = 38;
        for _ in 0..reps {
            let mut total = 0u64;
            for i in 1..10_000 {
                total += (i + 1) * (i + 1) / (i * i);
            }
            assert_ne!(total, 0);
        }
        worker_jobs_completed.fetch_add(1, Ordering::Relaxed);
        // TODO: doesn't include time for this fetch_add itself
        worker_time_taken_us.fetch_add(t.elapsed().as_micros() as u64, Ordering::Relaxed);
    };

    let mut jobs_started = 0;
    let jobs_target = 1_000_000;
    let mut last_completed = 0;

    let t = Instant::now();

    while jobs_started < jobs_target {
        let busy_work = WorkEvent {
            drop_during_sync: false,
            work: Work::ApiRequestP0(BlockingOrAsync::Blocking(Box::new(worker_fn.clone()))),
        };
        if let Ok(_) = channels.beacon_processor_tx.try_send(busy_work) {
            jobs_started += 1;
        }

        let completed = jobs_completed.load(Ordering::Relaxed);
        if completed != last_completed && completed % 100 == 0 {
            println!("{completed} of {jobs_started} jobs started complete");
        }
        if completed + 1024 < jobs_started {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        last_completed = completed;
    }

    loop {
        let completed = jobs_completed.load(Ordering::Relaxed);
        if completed != jobs_started {
            println!("waiting for job completion {}/{}", completed, jobs_started);
            tokio::time::sleep(Duration::from_secs(1)).await;
        } else {
            break;
        }
    }

    let wall_time_taken = t.elapsed();
    let cpu_time_taken = Duration::from_micros(time_taken_us.load(Ordering::Relaxed));
    println!("finished after {}s", wall_time_taken.as_secs());
    println!("CPU time: {}s", cpu_time_taken.as_secs());
    println!(
        "wall time per job: {}us",
        wall_time_taken.as_micros() / jobs_started as u128
    );
    println!(
        "CPU time per job: {}us",
        cpu_time_taken.as_micros() / jobs_started as u128
    );
    println!(
        "utilisation: {}",
        cpu_time_taken.as_millis() as f64 / wall_time_taken.as_millis() as f64
    );
}
