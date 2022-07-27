use slog::{o, Drain, Key, Level, Logger, OwnedKVList, Record, Serializer, KV};
use std::fmt::Arguments;

pub struct LogInterceptor {
    /// Unique identifier for this logger (e.g. the node name).
    id: String,
    /// Logging configuration.
    conf: LogConfig,
}

pub struct LogConfig {
    /// Log level at which to panic.
    pub panic_threshold: Option<Level>,
    /// Maximum re-org distance allowed (values greater will cause panics).
    pub max_reorg_length: Option<usize>,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            panic_threshold: Some(Level::Error),
            max_reorg_length: Some(1),
        }
    }
}

impl LogInterceptor {
    pub fn new(id: String, conf: LogConfig) -> Self {
        Self { id, conf }
    }

    pub fn into_logger(self) -> Logger {
        Logger::root(self.ignore_res(), o!())
    }
}

impl Drain for LogInterceptor {
    type Ok = ();
    type Err = ();

    fn log(&self, record: &Record, _: &OwnedKVList) -> Result<(), ()> {
        // Check for messages above the threshold.
        if let Some(panic_threshold) = self.conf.panic_threshold {
            if record.level().is_at_least(panic_threshold) {
                panic!(
                    "{} logged a message above the panic threshold: {} {}, from {}:{}",
                    self.id,
                    record.level().as_short_str(),
                    record.msg(),
                    record.file(),
                    record.line(),
                );
            }
        }

        // Check for re-orgs longer than the re-org limit.
        if let (Some(reorg_limit), Level::Warning) = (self.conf.max_reorg_length, record.level()) {
            let message = format!("{}", record.msg());
            if message == "Beacon chain re-org" {
                let mut snooper = UsizeSnooper::new("reorg_distance");
                record.kv().serialize(record, &mut snooper).unwrap();
                let distance = snooper
                    .value
                    .expect("should extract value for reorg_distance");

                if distance > reorg_limit {
                    panic!(
                        "{} experienced a re-org of length {} (> {})",
                        self.id, distance, reorg_limit
                    );
                }
            }
        }

        Ok(())
    }
}

/// Serializer to snoop on a logged usize value.
pub struct UsizeSnooper {
    key: &'static str,
    value: Option<usize>,
}

impl UsizeSnooper {
    pub fn new(key: &'static str) -> Self {
        Self { key, value: None }
    }
}

impl Serializer for UsizeSnooper {
    fn emit_arguments(&mut self, _: Key, _: &Arguments) -> slog::Result {
        Ok(())
    }

    fn emit_usize(&mut self, key: Key, value: usize) -> slog::Result {
        if key == self.key {
            self.value = Some(value);
        }
        Ok(())
    }
}
