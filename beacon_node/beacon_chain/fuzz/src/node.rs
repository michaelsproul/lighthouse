use crate::{Message, TestHarness};
use std::collections::VecDeque;
use types::EthSpec;

pub struct Node<E: EthSpec> {
    pub harness: TestHarness<E>,
    /// Queue of ordered `(tick, message)` pairs.
    ///
    /// Each `message` will be delivered to the node at `tick`.
    pub message_queue: VecDeque<(usize, Message<E>)>,
    /// Validator indices assigned to this node.
    pub validators: Vec<usize>,
}

impl<E: EthSpec> Node<E> {
    pub async fn deliver_message(&self, message: Message<E>) {
        match message {
            Message::Attestation(att) => match self.harness.process_unaggregated_attestation(att) {
                Ok(()) => (),
                Err(_) => (),
            },
            Message::Block(block) => {
                self.harness
                    .process_block_result(block)
                    .await
                    .expect("blocks should always apply");
            }
        }
    }

    pub async fn deliver_queued_at(&mut self, tick: usize) {
        loop {
            match self.message_queue.front() {
                Some((message_tick, _)) if *message_tick == tick => {
                    let (_, message) = self.message_queue.pop_front().unwrap();
                    self.deliver_message(message).await;
                }
                _ => break,
            }
        }
    }
}
