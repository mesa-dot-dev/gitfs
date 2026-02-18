//! Automatic, type-directed cleanup driven by reference counting.
//!
//! [`DropWard`] tracks how many live references exist for a given key and invokes a cleanup
//! callback when a key's count reaches zero. The cleanup logic is selected at the type level
//! through a zero-sized "tag" type that implements [`StatelessDrop`], keeping the ward itself
//! generic over *what* it manages without storing per-key values.
//!
//! This is designed for resources whose lifecycle is bound to an external context (e.g. GPU device
//! handles, connection pools, graphics pipelines) where Rust's built-in `Drop` cannot be used
//! because cleanup requires access to that context.
//!
//! # Design rationale
//!
//! The tag type `T` is constrained to be zero-sized. It exists only to carry the [`StatelessDrop`]
//! implementation at the type level — no `T` value is ever constructed or stored. This means a
//! single `DropWard` instance adds no per-key overhead beyond the key and its `usize` count.
//!
//! # Example
//!
//! ```ignore
//! struct GpuTextureDrop;
//!
//! impl StatelessDrop<wgpu::Device, TextureId> for GpuTextureDrop {
//!     fn delete(device: &wgpu::Device, _key: &TextureId) {
//!         // e.g. flush a deferred-destruction queue
//!         device.poll(wgpu::Maintain::Wait);
//!     }
//! }
//!
//! let mut ward: DropWard<wgpu::Device, TextureId, GpuTextureDrop> = DropWard::new(device);
//!
//! ward.inc(texture_id);  // → 1
//! ward.inc(texture_id);  // → 2
//! ward.dec(&texture_id); // → Some(1)
//! ward.dec(&texture_id); // → Some(0), calls GpuTextureDrop::delete(&device, &texture_id)
//! ```

use std::marker::PhantomData;

use rustc_hash::FxHashMap;

/// Type-level hook for cleanup that requires an external context.
///
/// Implement this on a zero-sized tag type. The tag is never instantiated — it only selects which
/// `delete` implementation a [`DropWard`] will call.
pub trait StatelessDrop<Ctx, K> {
    /// Called exactly once when a key's reference count reaches zero.
    ///
    /// `ctx` is the shared context owned by the [`DropWard`]. `key` is the key whose count just
    /// reached zero. This callback fires synchronously inside [`DropWard::dec`]; avoid blocking or
    /// panicking if the ward is used on a hot path.
    fn delete(ctx: &Ctx, key: &K);
}

/// A reference-counted key set that triggers [`StatelessDrop::delete`] on the associated context
/// when any key's count drops to zero.
///
/// # Type parameters
///
/// - `Ctx` — shared context passed to `T::delete` (e.g. a device handle).
/// - `K`   — the key type being reference-counted.
/// - `T`   — a **zero-sized** tag type carrying the cleanup logic.
///   Will fail to compile if `size_of::<T>() != 0`.
///
/// # Concurrency
///
/// Not thread-safe. All access requires `&mut self`. Wrap in a `Mutex` or similar if shared across
/// threads.
///
#[derive(Debug, Clone)]
pub struct DropWard<Ctx, K, T> {
    map: FxHashMap<K, usize>,
    ctx: Ctx,
    _marker: PhantomData<T>,
}

impl<Ctx, K, T> DropWard<Ctx, K, T>
where
    K: Eq + std::hash::Hash,
    T: StatelessDrop<Ctx, K>,
{
    /// Compile-time guard: `T` must be zero-sized.
    const _ASSERT_ZST: () = assert!(size_of::<T>() == 0, "T must be zero-sized");

    /// Create a new ward that will pass `ctx` to `T::delete` on cleanup.
    pub fn new(ctx: Ctx) -> Self {
        Self {
            map: FxHashMap::default(),
            ctx,
            _marker: PhantomData,
        }
    }

    /// Increment the reference count for `key`, inserting it with a count
    /// of 1 if it does not exist.
    ///
    /// Returns the count **after** incrementing.
    pub fn inc(&mut self, key: K) -> usize {
        *self
            .map
            .entry(key)
            .and_modify(|count| *count += 1)
            .or_insert(1)
    }

    fn dec_by(&mut self, key: &K, by: usize) -> Option<usize> {
        let curr = *self.map.get(key)?;
        let new_count = curr.saturating_sub(by);
        if new_count == 0 {
            self.map.remove(key);
            T::delete(&self.ctx, key);
        } else if let Some(slot) = self.map.get_mut(key) {
            *slot = new_count;
        }
        Some(new_count)
    }

    /// Decrement the reference count for `key`.
    ///
    /// If the count reaches zero, the key is removed and `T::delete` is
    /// called synchronously with the ward's context. Returns `Some(0)` in
    /// this case — the key will no longer be tracked.
    ///
    /// Returns `None` if `key` was not present (no-op).
    pub fn dec(&mut self, key: &K) -> Option<usize> {
        self.dec_by(key, 1)
    }

    /// Decrement the reference count for `key` by `count`.
    pub fn dec_count(&mut self, key: &K, count: usize) -> Option<usize> {
        self.dec_by(key, count)
    }
}
