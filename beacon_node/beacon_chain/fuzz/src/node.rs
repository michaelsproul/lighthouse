use crate::{Message, TestHarness};
use beacon_chain::BlockError;
use std::collections::VecDeque;
use types::{EthSpec, Hash256};

pub struct Node<E: EthSpec> {
    pub id: String,
    pub harness: TestHarness<E>,
    /// Queue of ordered `(tick, message)` pairs.
    ///
    /// Each `message` will be delivered to the node at `tick`.
    pub message_queue: VecDeque<(usize, Message<E>)>,
    /// Validator indices assigned to this node.
    pub validators: Vec<usize>,
}

impl<E: EthSpec> Node<E> {
    pub fn queue_message(&mut self, message: Message<E>, arrive_tick: usize) {
        let insert_at = self
            .message_queue
            .partition_point(|&(tick, _)| tick <= arrive_tick);
        self.message_queue.insert(insert_at, (arrive_tick, message));
    }

    pub fn has_messages_queued(&self) -> bool {
        !self.message_queue.is_empty()
    }

    pub fn last_message_tick(&self, current_tick: usize) -> usize {
        self.message_queue
            .back()
            .map_or(current_tick, |(tick, _)| *tick)
    }

    /// Attempt to deliver the message, returning it if is unable to be processed right now.
    ///
    /// Undelivered messages should be requeued to simulate the node queueing them outside the
    /// `BeaconChain` module, or fetching them via network RPC.
    pub async fn deliver_message(&self, message: Message<E>) -> Option<Message<E>> {
        match message {
            Message::Attestation(att) => match self.harness.process_unaggregated_attestation(att) {
                Ok(()) | Err(_) => None,
            },
            Message::Block(block) => {
                match self.harness.process_block_result(block).await {
                    Ok(_) => None,
                    // Re-queue blocks that arrive out of order.
                    Err(BlockError::ParentUnknown(block)) => Some(Message::Block((*block).clone())),
                    Err(e) => panic!("unable to process block: {e:?}"),
                }
            }
        }
    }

    pub async fn deliver_queued_at(
        &mut self,
        tick: usize,
        block_is_viable: impl Fn(Hash256) -> bool + Copy,
    ) {
        loop {
            match self.message_queue.front() {
                Some((message_tick, _)) if *message_tick <= tick => {
                    let (_, message) = self.message_queue.pop_front().unwrap();

                    if let Some(undelivered) = self.deliver_message(message).await {
                        if undelivered.block_root().map_or(false, block_is_viable) {
                            let requeue_tick = self.last_message_tick(tick);
                            self.queue_message(undelivered, requeue_tick);
                        }
                    }
                }
                _ => break,
            }
        }
    }
}
