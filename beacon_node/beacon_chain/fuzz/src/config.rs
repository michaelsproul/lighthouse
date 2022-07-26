use std::time::Duration;
use types::ChainSpec;

pub struct Config {
    pub num_honest_nodes: usize,
    pub total_validators: usize,
    pub attacker_validators: usize,
    pub ticks_per_slot: usize,
    pub min_attacker_proposers_per_slot: usize,
    pub max_attacker_proposers_per_slot: usize,
    pub debug_logs: bool,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            num_honest_nodes: 6,
            total_validators: 90,
            attacker_validators: 30,
            ticks_per_slot: 3,
            min_attacker_proposers_per_slot: 0,
            max_attacker_proposers_per_slot: 4,
            debug_logs: false,
        }
    }
}

impl Config {
    pub fn is_valid(&self) -> bool {
        self.ticks_per_slot % 3 == 0
            && self.honest_validators() % self.num_honest_nodes == 0
            && self.max_attacker_proposers_per_slot >= self.min_attacker_proposers_per_slot
    }

    pub fn honest_validators(&self) -> usize {
        self.total_validators - self.attacker_validators
    }

    pub fn honest_validators_per_node(&self) -> usize {
        self.honest_validators() / self.num_honest_nodes
    }

    pub fn attestation_tick(&self) -> usize {
        self.ticks_per_slot / 3
    }

    pub fn is_block_proposal_tick(&self, tick: usize) -> bool {
        tick % self.ticks_per_slot == 0 && tick != 0
    }

    pub fn is_attestation_tick(&self, tick: usize) -> bool {
        tick % self.ticks_per_slot == self.attestation_tick()
    }

    pub fn min_attacker_proposers(&self, available: usize) -> Option<u32> {
        Some(std::cmp::min(self.min_attacker_proposers_per_slot, available) as u32)
    }

    pub fn max_attacker_proposers(&self, available: usize) -> Option<u32> {
        Some(std::cmp::min(self.max_attacker_proposers_per_slot, available) as u32)
    }

    pub fn tick_duration(&self, spec: &ChainSpec) -> Duration {
        Duration::from_secs(spec.seconds_per_slot) / self.ticks_per_slot as u32
    }
}
