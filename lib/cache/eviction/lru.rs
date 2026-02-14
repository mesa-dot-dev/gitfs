//! Implements the LRU eviction policy.

use std::{
    future::Future,
    hash::Hash,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicI64, AtomicUsize},
    },
};

use hashlink::LinkedHashMap;
use tokio::{sync::mpsc::Receiver, task::JoinHandle};

#[derive(Debug)]
struct DeletionIndicator {
    underlying: AtomicI64,
}

impl DeletionIndicator {
    /// Call to mark the start of a batch of deletions.
    fn submit_batch(&self) {
        self.underlying
            .fetch_add(1 << 32, std::sync::atomic::Ordering::Relaxed);
    }

    /// Call to mark a batch as being processed right now.
    fn process_batch(&self, count: u32) {
        self.underlying.fetch_add(
            i64::from(count) - (1 << 32),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    /// Call to mark a deletion as being completed.
    fn observe_deletion(&self) {
        self.underlying
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Check if there are any scheduled or active deletions.
    fn have_pending_deletions(&self) -> bool {
        self.underlying.load(std::sync::atomic::Ordering::Relaxed) != 0
    }

    /// Check if there are any scheduled batches of deletions.
    fn have_pending_batches(&self) -> bool {
        self.underlying.load(std::sync::atomic::Ordering::Relaxed) >= 1 << 32
    }
}

impl Default for DeletionIndicator {
    fn default() -> Self {
        Self {
            underlying: AtomicI64::new(0),
        }
    }
}

/// A trait for deleting keys from the cache. This is used by the LRU eviction tracker to delete
/// keys when they are evicted.
pub trait Deleter<K, Ctx>: Send + Clone + 'static {
    /// Delete the given keys from the cache. The keys are guaranteed to be in the order of
    /// eviction. You absolutely MUST delete the keys in the order they are given, otherwise the
    /// LRU eviction tracker will get very confused and break.
    fn delete(&mut self, key: K, ctx: Ctx) -> impl Future<Output = ()> + Send;
}

/// Messages sent to the LRU eviction tracker worker.
#[derive(Debug, Clone, Copy)]
enum Message<K, C> {
    /// Notify the LRU eviction tracker that the given key was accessed.
    Accessed(K, usize),
    /// Request an eviction set of the given size.
    Evict(u32),
    /// Notify the LRU eviction tracker that a given key was inserted.
    Inserted(K, C),
}

#[derive(Debug)]
struct LruProcessingTask<K: Copy, C, D: Deleter<K, C>> {
    receiver: Receiver<Message<K, C>>,

    /// The ordered set of keys, ordered according to the last-used policy.
    ordered_key_map: LinkedHashMap<K, C>,

    /// The deleter to call when we need to evict keys.
    deleter: D,

    /// Pointer into the shared deletion tracker.
    shared: Arc<WorkerState>,
}

impl<K: Copy + Eq + Hash + Send + 'static, C: Send + 'static, D: Deleter<K, C>>
    LruProcessingTask<K, C, D>
{
    fn new(deleter: D, receiver: Receiver<Message<K, C>>, shared: Arc<WorkerState>) -> Self {
        Self {
            receiver,
            ordered_key_map: LinkedHashMap::new(),
            deleter,
            shared,
        }
    }

    fn spawn_task(
        deleter: D,
        receiver: Receiver<Message<K, C>>,
        shared: Arc<WorkerState>,
    ) -> JoinHandle<()> {
        // TODO(markovejnovic): This should have a best-effort drop.
        tokio::spawn(async move {
            let mut task = Self::new(deleter, receiver, shared);
            task.work().await;
        })
    }

    async fn work(&mut self) {
        while let Some(msg) = self.receiver.recv().await
            && self.service_message(msg)
        {}
    }

    /// Returns true if the task should continue working.
    #[must_use]
    fn service_message(&mut self, message: Message<K, C>) -> bool {
        match message {
            Message::Accessed(k, ev_gen) => {
                if ev_gen
                    < self
                        .shared
                        .eviction_generation
                        .load(std::sync::atomic::Ordering::Relaxed)
                {
                    // This is a ghost access. Happens when one client sends an eviction, but other
                    // clients add accesses to the same keys, before our worker thread had a chance
                    // to clean up.
                    //
                    // We're looking at an access in the "future".
                    return true;
                }

                self.reposition_existing_key(k);
            }
            Message::Evict(max_count) => {
                // We got an eviction notice. Bump the eviction generation to indicate that all
                // accesses after this point are considered in the "future". Read the subsequent
                // comment for an explanation of why we need to do this.
                self.shared
                    .eviction_generation
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
                // This is what the generation counter does -- it allows us to track which messages
                // are considered "stale". There are some problems with the generation counter,
                // however. We drop all messages that are "generationally old". Under high
                // contention, this results in the eviction policy acting like a fuzzy LRU policy,
                // rather than a strict LRU policy, but for the purposes of this cache, this is an
                // acceptable tradeoff.
                //
                // TODO(markovejnovic): Make this a strict LRU.
                //
                // We cannot mutate the queue to mark messages as stale, so the best we can do
                // is mark the keys as "evicted" and ignore any accesses from these keys until
                // they're marked as "inserted" again.
                {
                    // These integer casts are safe, since max_count is guaranteed to fit
                    // within a u32, min(MAX_U32, MAX_USIZE) == MAX_U32.
                    #[expect(clippy::cast_possible_truncation)]
                    let take_count = self.ordered_key_map.len().min(max_count as usize) as u32;
                    self.shared.active_deletions.process_batch(take_count);
                    for _ in 0..take_count {
                        let Some(e) = self.ordered_key_map.pop_front() else {
                            break;
                        };
                        let mut deleter = self.deleter.clone();
                        let shared_clone = Arc::clone(&self.shared);
                        tokio::spawn(async move {
                            deleter.delete(e.0, e.1).await;
                            shared_clone.active_deletions.observe_deletion();
                        });
                    }
                }
            }
            Message::Inserted(k, ctx) => {
                debug_assert!(
                    !self.ordered_key_map.contains_key(&k),
                    "key must not already exist in the ordered set when inserting"
                );

                self.ordered_key_map.insert(k, ctx);
            }
        }

        true
    }

    fn reposition_existing_key(&mut self, key: K) {
        debug_assert!(
            self.ordered_key_map.contains_key(&key),
            "key must exist in the ordered set before repositioning"
        );

        if let Some(c) = self.ordered_key_map.remove(&key) {
            self.ordered_key_map.insert(key, c);
        }
    }
}

#[derive(Debug)]
struct WorkerState {
    active_deletions: DeletionIndicator,
    eviction_generation: AtomicUsize,
    worker: OnceLock<JoinHandle<()>>,
}

/// An LRU eviction tracker. This is used to track the least recently used keys in the cache, and
/// to evict keys when necessary.
#[derive(Debug)]
pub struct LruEvictionTracker<K, C> {
    worker_message_sender: tokio::sync::mpsc::Sender<Message<K, C>>,
    worker_state: Arc<WorkerState>,
}

impl<K: Copy + Eq + Send + Hash + 'static, C: Send + 'static> LruEvictionTracker<K, C> {
    /// Spawn a new LRU eviction tracker with the given deleter and channel size.
    pub fn spawn<D: Deleter<K, C>>(deleter: D, channel_size: usize) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(channel_size);

        let worker_state = Arc::new(WorkerState {
            active_deletions: DeletionIndicator::default(),
            eviction_generation: AtomicUsize::new(0),
            worker: OnceLock::new(),
        });
        let worker = LruProcessingTask::spawn_task(deleter, rx, Arc::clone(&worker_state));
        if worker_state.worker.set(worker).is_err() {
            unreachable!("worker should only be set once, and we just set it");
        }

        Self {
            worker_message_sender: tx,
            worker_state,
        }
    }

    /// Notify the LRU eviction tracker that the given key was inserted.
    ///
    /// You MUST call this method for every new key that is inserted into the cache.
    pub fn insert(&self, key: K, ctx: C) {
        self.send_msg(Message::Inserted(key, ctx));
    }

    /// Notify the LRU eviction tracker that the given key was accessed.
    ///
    /// You MUST call this method for every read or update to a key.
    pub fn access(&self, key: K) {
        self.send_msg(Message::Accessed(
            key,
            self.worker_state
                .eviction_generation
                .load(std::sync::atomic::Ordering::Relaxed),
        ));
    }

    /// Cull the least recently used keys with the given deletion method.
    pub fn cull(&self, max_count: u32) {
        // Culling sets the top-most bit of the active deletion count to indicate that there are
        // queued deletions.
        self.worker_state.active_deletions.submit_batch();
        self.send_msg(Message::Evict(max_count));
    }

    /// Check whether there are culls that are already scheduled.
    #[must_use]
    pub fn have_pending_culls(&self) -> bool {
        self.worker_state.active_deletions.have_pending_batches()
    }

    /// Send a message to the worker. This is a helper method to reduce code duplication.
    fn send_msg(&self, message: Message<K, C>) {
        if self.worker_message_sender.blocking_send(message).is_err() {
            unreachable!();
        }
    }

    /// Get the total number of currently active deletions.
    ///
    /// Useful if you need to spinlock on this, for whatever reason.
    ///
    ///
    /// # Note
    ///
    /// Does not guarantee ordering in respect to the deletion. The only guarantee this method
    /// provides is whether any deletions are pending or not, but it is not safe to use this to
    /// synchronize upon the results of your deletions, since the underlying atomic is used in a
    /// relaxed manner.
    #[must_use]
    pub fn have_active_deletions(&self) -> bool {
        self.worker_state.active_deletions.have_pending_deletions()
    }
}
