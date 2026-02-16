//! Implements the LRU eviction policy.

use std::{future::Future, hash::Hash};

use std::sync::{
    Arc,
    atomic::{self, AtomicI64},
};

use hashlink::LinkedHashMap;
use tokio::sync::mpsc::{Receiver, error::TrySendError};

/// Types that carry a monotonic version for deduplication of out-of-order messages.
pub trait Versioned {
    /// Returns the monotonic version of this value.
    fn version(&self) -> u64;
}

/// A trait for deleting keys from the cache. This is used by the LRU eviction tracker to delete
/// keys when they are evicted.
pub trait Deleter<K, Ctx>: Send + Clone + 'static {
    /// Delete the given key from the cache. Deletions for different keys may be invoked
    /// concurrently and in arbitrary order. Correctness is ensured by the caller through
    /// a context-based guard (e.g. matching on a unique file ID), not by invocation order.
    fn delete(&mut self, key: K, ctx: Ctx) -> impl Future<Output = ()> + Send;
}

/// Messages sent to the LRU eviction tracker worker.
#[derive(Debug, Clone, Copy)]
enum Message<K, C> {
    /// Notify the LRU eviction tracker that the given key was accessed.
    Accessed(K),
    /// Request an eviction set of the given size.
    Evict(u32),
    /// Notify the LRU eviction tracker that a key was inserted or overwritten.
    Upserted(K, C),
}

/// Tracks in-flight eviction batches and individual deletions using a single packed `AtomicI64`.
///
/// Layout: `[upper 32 bits: pending batches | lower 32 bits: active deletions]`
///
/// The lifecycle of an eviction is:
/// 1. Producer calls `submit_batch()` — increments upper half by 1.
/// 2. Producer enqueues the `Evict` message into the channel.
/// 3. Worker receives the message and calls `process_batch(n)` — decrements upper half by 1 and
///    increments lower half by `n` (the actual number of keys to delete).
/// 4. Each spawned deletion task calls `observe_deletion()` when done — decrements lower half by 1.
///
/// The indicator reads zero only when no batches are pending and no deletions are in flight.
#[derive(Debug)]
struct DeletionIndicator {
    underlying: AtomicI64,
}

impl DeletionIndicator {
    fn new() -> Self {
        Self {
            underlying: AtomicI64::new(0),
        }
    }

    /// Mark that a new eviction batch has been submitted by a producer. Called on the producer
    /// side *before* the `Evict` message is sent into the channel.
    fn submit_batch(&self) {
        self.underlying
            .fetch_add(1 << 32, atomic::Ordering::Relaxed);
    }

    /// Undo a `submit_batch` call. Used when the channel `try_send` fails after we already
    /// incremented the pending-batch counter.
    fn undo_submit_batch(&self) {
        self.underlying
            .fetch_sub(1 << 32, atomic::Ordering::Relaxed);
    }

    /// Called by the worker when it begins processing an eviction batch. Atomically decrements
    /// the pending-batch counter (upper half) by 1 and increments the active-deletion counter
    /// (lower half) by `count`.
    fn process_batch(&self, count: u32) {
        self.underlying
            .fetch_add(i64::from(count) - (1 << 32), atomic::Ordering::Relaxed);
    }

    /// Called by a spawned deletion task when it finishes deleting one key.
    fn observe_deletion(&self) {
        self.underlying.fetch_sub(1, atomic::Ordering::Relaxed);
    }

    /// Returns `true` if there is any eviction work in progress — either batches waiting to be
    /// processed by the worker, or individual deletions still in flight.
    fn have_pending_work(&self) -> bool {
        self.underlying.load(atomic::Ordering::Relaxed) != 0
    }
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

impl<K: Copy + Eq + Hash + Send + 'static, C: Versioned + Send + 'static, D: Deleter<K, C>>
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

    fn spawn_task(deleter: D, receiver: Receiver<Message<K, C>>, shared: Arc<WorkerState>) {
        // TODO(markovejnovic): This should have a best-effort drop.
        tokio::spawn(async move {
            let mut task = Self::new(deleter, receiver, shared);
            task.work().await;
        });
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
            Message::Accessed(k) => {
                // The key may have been evicted between the access and this message arriving.
                // If it was, the remove returns None and this is a no-op.
                if let Some(entry) = self.ordered_key_map.remove(&k) {
                    self.ordered_key_map.insert(k, entry);
                }
            }
            Message::Evict(max_count) => {
                {
                    // These integer casts are safe, since max_count is guaranteed to fit
                    // within a u32, min(MAX_U32, MAX_USIZE) == MAX_U32.
                    #[expect(clippy::cast_possible_truncation)]
                    let take_count = self.ordered_key_map.len().min(max_count as usize) as u32;

                    // Atomically transition this batch from "pending" to "active deletions".
                    // This decrements the upper half (pending batches) by 1 and increments the
                    // lower half (active deletions) by take_count. If take_count is 0, this
                    // still correctly clears the pending batch.
                    self.shared.active_deletions.process_batch(take_count);

                    for _ in 0..take_count {
                        let Some((key, ctx)) = self.ordered_key_map.pop_front() else {
                            break;
                        };
                        let mut deleter = self.deleter.clone();
                        let shared_clone = Arc::clone(&self.shared);
                        tokio::spawn(async move {
                            // Drop guard ensures observe_deletion() runs even if the
                            // deletion task is cancelled or panics.
                            struct DeletionGuard(Arc<WorkerState>);
                            impl Drop for DeletionGuard {
                                fn drop(&mut self) {
                                    self.0.active_deletions.observe_deletion();
                                }
                            }
                            let _guard = DeletionGuard(shared_clone);
                            deleter.delete(key, ctx).await;
                        });
                    }
                }
            }
            Message::Upserted(k, ctx) => {
                if let Some(existing) = self.ordered_key_map.get(&k)
                    && ctx.version() < existing.version()
                {
                    // Stale message from a previous incarnation; drop it.
                    return true;
                }
                self.ordered_key_map.remove(&k);
                self.ordered_key_map.insert(k, ctx);
            }
        }

        true
    }
}

#[derive(Debug)]
struct WorkerState {
    /// Packed atomic tracking pending eviction batches (upper 32 bits) and active deletions
    /// (lower 32 bits). See `DeletionIndicator` for the full protocol.
    active_deletions: DeletionIndicator,
}

/// An LRU eviction tracker. This is used to track the least recently used keys in the cache, and
/// to evict keys when necessary.
#[derive(Debug)]
pub struct LruEvictionTracker<K, C> {
    worker_message_sender: tokio::sync::mpsc::Sender<Message<K, C>>,
    worker_state: Arc<WorkerState>,
}

impl<K: Copy + Eq + Send + Hash + 'static, C: Versioned + Copy + Send + 'static>
    LruEvictionTracker<K, C>
{
    /// Spawn a new LRU eviction tracker with the given deleter and channel size.
    pub fn spawn<D: Deleter<K, C>>(deleter: D, channel_size: usize) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(channel_size);

        let worker_state = Arc::new(WorkerState {
            active_deletions: DeletionIndicator::new(),
        });
        // The worker task is intentionally detached: it exits when the channel closes
        // (i.e. when all senders, including the one held by LruEvictionTracker, are dropped).
        LruProcessingTask::spawn_task(deleter, rx, Arc::clone(&worker_state));

        Self {
            worker_message_sender: tx,
            worker_state,
        }
    }

    /// Notify the LRU tracker that a key was inserted or overwritten.
    ///
    /// Non-cancellable: uses `try_send` (sync) with a `tokio::spawn` fallback so that the
    /// notification cannot be lost to task cancellation.
    pub fn upsert(&self, key: K, ctx: C) {
        match self
            .worker_message_sender
            .try_send(Message::Upserted(key, ctx))
        {
            Ok(()) | Err(TrySendError::Closed(_)) => {}
            Err(TrySendError::Full(msg)) => {
                let sender = self.worker_message_sender.clone();
                tokio::spawn(async move {
                    let _ = sender.send(msg).await;
                });
            }
        }
    }

    /// Notify the LRU eviction tracker that the given key was accessed.
    ///
    /// Non-cancellable: uses `try_send` (sync) with a `tokio::spawn` fallback so that the
    /// notification cannot be lost to task cancellation.
    pub fn access(&self, key: K) {
        match self.worker_message_sender.try_send(Message::Accessed(key)) {
            Ok(()) | Err(TrySendError::Closed(_)) => {}
            Err(TrySendError::Full(msg)) => {
                let sender = self.worker_message_sender.clone();
                tokio::spawn(async move {
                    let _ = sender.send(msg).await;
                });
            }
        }
    }

    /// Try to cull the least recently used keys with the given deletion method.
    ///
    /// Returns `true` if the eviction message was successfully enqueued, or `false` if the
    /// channel is full. In the latter case, the caller should yield and retry.
    ///
    /// The ordering here is critical for correctness: we `submit_batch()` *before* `try_send()`
    /// so that the `DeletionIndicator` is always in a state where `have_pending_batches()` returns
    /// `true` by the time anyone observes the enqueued message. If the send fails, we roll back
    /// with `undo_submit_batch()`, which is a net-zero delta on the packed atomic.
    #[must_use]
    pub fn try_cull(&self, max_count: u32) -> bool {
        self.worker_state.active_deletions.submit_batch();
        if self
            .worker_message_sender
            .try_send(Message::Evict(max_count))
            .is_ok()
        {
            true
        } else {
            self.worker_state.active_deletions.undo_submit_batch();
            false
        }
    }

    /// Check whether there are culls that are already scheduled or actively in progress.
    #[must_use]
    pub fn have_pending_culls(&self) -> bool {
        self.worker_state.active_deletions.have_pending_work()
    }
}
