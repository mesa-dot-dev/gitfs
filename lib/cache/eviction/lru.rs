//! Implements the LRU eviction policy.

use std::{collections::HashSet, future::Future, hash::Hash, pin::Pin, sync::Arc};

use hashlink::LinkedHashSet;
use tokio::{sync::mpsc::Receiver, task::JoinHandle};

/// A trait for deleting keys from the cache. This is used by the LRU eviction tracker to delete
/// keys when they are evicted.
pub trait Deleter<K>: Send + 'static {
    /// Delete the given keys from the cache. The keys are guaranteed to be in the order of
    /// eviction. You absolutely MUST delete the keys in the order they are given, otherwise the
    /// LRU eviction tracker will get very confused and break.
    fn delete<'a>(&mut self, keys: impl Iterator<Item = &'a K>)
    where
        K: 'a;
}

/// Messages sent to the LRU eviction tracker worker.
#[derive(Debug, Clone)]
enum Message<K> {
    /// Notify the LRU eviction tracker that the given key was accessed.
    Accessed(K),
    /// Request an eviction set of the given size.
    Evict(usize),
    /// Notify the LRU eviction tracker that a given key was inserted.
    Inserted(K),
}

#[derive(Debug)]
struct LruProcessingTask<K: Copy, D: Deleter<K>> {
    receiver: Receiver<Message<K>>,

    /// The ordered set of keys, ordered according to the last-used policy.
    ordered_key_set: LinkedHashSet<K>,

    /// Tracks which keys are currently considered evicted. Read the long explanation in
    /// `service_message` for why this is necessary.
    evicted_keys: HashSet<K>,

    /// The deleter to call when we need to evict keys.
    deleter: D,
}

impl<K: Copy + Eq + Hash + Send + 'static, D: Deleter<K>> LruProcessingTask<K, D> {
    fn new(deleter: D, receiver: Receiver<Message<K>>) -> Self {
        Self {
            receiver,
            ordered_key_set: LinkedHashSet::new(),
            evicted_keys: HashSet::new(),
            deleter,
        }
    }

    fn spawn_task(deleter: D, receiver: Receiver<Message<K>>) -> JoinHandle<()> {
        // TODO(markovejnovic): This should have a best-effort drop.
        tokio::spawn(async move {
            let mut task = Self::new(deleter, receiver);
            task.work().await;
        })
    }

    async fn work(&mut self) {
        while let Some(msg) = self.receiver.recv().await
            && self.service_message(msg).await
        {}
    }

    /// Returns true if the task should continue working.
    #[must_use]
    fn service_message(
        &mut self,
        message: Message<K>,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
        Box::pin(async move {
            match message {
                Message::Accessed(k) => {
                    if self.evicted_keys.contains(&k) {
                        // This is a ghost access, happens when a client accesses a key that may not
                        // have been cleaned up yet. Just ignore it.
                        return true;
                    }

                    self.reposition_existing_key(k);
                }
                Message::Evict(max_count) => {
                    // Before we send off the eviction set, we actually need to remove the evicted
                    // keys from the pending queue. Read the following for an explanation.
                    //
                    // The client may access a key AFTER an eviction notice. Until the eviction
                    // notice is processed by the client, the client will have the key available to
                    // it.
                    //
                    // Consequently, the client may very well access the key after it sent out an
                    // eviction notice, but before the eviction notice is processed. This will
                    // result in the key being added as an access message after the eviction
                    // notice, but before the eviction set is sent to the client.
                    //
                    // The problem is, the LruProcessingTask has no way of knowing whether the
                    // message the client sent is a stale message, or a legit message.
                    //
                    // Consider the following queue state in between two loop iterations of the
                    // LruProcessingTask:
                    // A -- access
                    // B -- insert
                    // E -- eviction request
                    // C -- the client thread which made the request
                    //
                    // A1 A2 A3 E1 A1 B1 -- after this point, no clients can access A1
                    // C1 C1 C1 C1 C2 C1 ---------------------------------------------> time
                    //
                    // The scenario here is:
                    // - A1, A2, A3 are used by the clients.
                    // - C1 wants to add B1, but it doesn't have enough space, so it sends an
                    //   eviction request.
                    // - In parallel, C2 accesses A1, which is completely legal.
                    //
                    // The result is that our queue has A1, even after we sent out the eviction
                    // request, and we have no way of knowing whether A1 is a stale message or not.
                    //
                    // To prevent this whole "is this thing stale or not" problem, we need to
                    // "erase" keys evicted in the future. Unfortunately, you can't mutate queues,
                    // so the best we can do is simply mark the stupid keys as "evicted" and ignore
                    // any accesses from these keys until they're marked as "inserted" again.
                    //
                    // We cannot mutate the queue to mark messages as stale, so the best we can do
                    // is mark the keys as "evicted" and ignore any accesses from these keys until
                    // they're marked as "inserted" again.
                    {
                        let take_count = self.ordered_key_set.len().min(max_count);
                        let eviction_set_it = self.ordered_key_set.iter().take(take_count);
                        self.evicted_keys.extend(eviction_set_it.clone());
                        self.deleter.delete(eviction_set_it);
                        for _ in 0..take_count {
                            if self.ordered_key_set.pop_front().is_none() {
                                break;
                            }
                        }
                    }
                }
                Message::Inserted(k) => {
                    debug_assert!(
                        !self.ordered_key_set.contains(&k),
                        "key must not already exist in the ordered set when inserting"
                    );

                    // If the key has been evicted, but is now inserted, that means the key is no
                    // longer stale.
                    if self.evicted_keys.contains(&k) {
                        self.evicted_keys.remove(&k);
                    }

                    self.ordered_key_set.insert(k);
                }
            }

            true
        })
    }

    fn reposition_existing_key(&mut self, key: K) {
        debug_assert!(
            self.ordered_key_set.contains(&key),
            "key must exist in the ordered set before repositioning"
        );

        self.ordered_key_set.remove(&key);
        self.ordered_key_set.insert(key);
    }
}

/// An LRU eviction tracker. This is used to track the least recently used keys in the cache, and
/// to evict keys when necessary.
#[derive(Debug, Clone)]
pub struct LruEvictionTracker<K> {
    worker_message_sender: tokio::sync::mpsc::Sender<Message<K>>,
    _worker: Arc<JoinHandle<()>>,
}

impl<K: Copy + Eq + Send + Hash + 'static> LruEvictionTracker<K> {
    /// Spawn a new LRU eviction tracker with the given deleter and channel size.
    pub fn spawn<D: Deleter<K>>(deleter: D, channel_size: usize) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(channel_size);

        Self {
            worker_message_sender: tx,
            _worker: Arc::new(LruProcessingTask::spawn_task(deleter, rx)),
        }
    }

    /// Notify the LRU eviction tracker that the given key was inserted.
    ///
    /// You MUST call this method for every new key that is inserted into the cache.
    pub async fn insert(&self, key: K) {
        self.send_msg(Message::Inserted(key)).await;
    }

    /// Notify the LRU eviction tracker that the given key was accessed.
    ///
    /// You MUST call this method for every read or update to a key.
    pub async fn access(&self, key: K) {
        self.send_msg(Message::Accessed(key)).await;
    }

    /// Cull the least recently used keys with the given deletion method.
    pub async fn cull(&self, max_count: usize) {
        self.send_msg(Message::Evict(max_count)).await;
    }

    /// Send a message to the worker. This is a helper method to reduce code duplication.
    async fn send_msg(&self, message: Message<K>) {
        if self
            .worker_message_sender
            .send(message.clone())
            .await
            .is_err()
        {
            unreachable!();
        }
    }
}
