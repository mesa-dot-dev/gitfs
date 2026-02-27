//! Generic composite filesystem types.
//!
//! A composite filesystem presents multiple child filesystems under a single
//! virtual root directory. The [`CompositeRoot`] trait describes how children
//! are discovered, [`ChildInner`] co-locates an inode table with an
//! [`AsyncFs`](super::async_fs::AsyncFs), and [`CompositeReader`] wraps a
//! child reader so the composite layer can expose it through [`FileReader`].

use std::ffi::{OsStr, OsString};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use crate::cache::async_backed::FutureBackedCache;
use crate::fs::async_fs::{AsyncFs, FileReader, FsDataProvider, OpenFile, OverlayReader};
use crate::fs::bridge::ConcurrentBridge;
use crate::fs::{INode, INodeType, InodeAddr, InodePerms, LoadedAddr, OpenFlags, ROOT_INO};

/// Descriptor for a child filesystem returned by [`CompositeRoot`].
pub struct ChildDescriptor<DP: FsDataProvider> {
    /// The name this child is listed as in the composite root directory.
    pub name: OsString,
    /// The data provider for this child.
    pub provider: DP,
    /// The root inode of the child filesystem.
    pub root_ino: INode,
}

/// Describes the children that a composite filesystem exposes at its root.
///
/// Implementors define domain-specific child resolution: what children exist,
/// and what [`FsDataProvider`] backs each child.
pub trait CompositeRoot: Send + Sync + 'static {
    /// The data provider type for child filesystems.
    type ChildDP: FsDataProvider;

    /// Resolve a child by name, returning its data provider and root inode.
    ///
    /// Called on lookup at the composite root. Returns `None` if the name
    /// does not correspond to a known child.
    fn resolve_child(
        &self,
        name: &OsStr,
    ) -> impl Future<Output = Result<Option<ChildDescriptor<Self::ChildDP>>, std::io::Error>> + Send;

    /// List all children at the composite root.
    ///
    /// Called on readdir at the composite root.
    fn list_children(
        &self,
    ) -> impl Future<Output = Result<Vec<ChildDescriptor<Self::ChildDP>>, std::io::Error>> + Send;
}

/// Co-locates an inode table and [`AsyncFs`].
pub struct ChildInner<DP: FsDataProvider> {
    fs: AsyncFs<DP>,
}

impl<DP: FsDataProvider> ChildInner<DP> {
    pub(crate) fn create(table: FutureBackedCache<InodeAddr, INode>, provider: DP) -> Self {
        let table = Arc::new(table);
        let fs = AsyncFs::new_preseeded(provider, table);
        Self { fs }
    }

    pub(crate) fn get_fs(&self) -> &AsyncFs<DP> {
        &self.fs
    }
}

/// Wraps a child's reader so that the composite layer can expose it as its own
/// [`FileReader`].
pub struct CompositeReader<R: FileReader> {
    inner: Arc<R>,
}

impl<R: FileReader> CompositeReader<R> {
    /// Create a new `CompositeReader` wrapping the given reader.
    #[must_use]
    pub fn new(inner: Arc<R>) -> Self {
        Self { inner }
    }
}

impl<R: FileReader> std::fmt::Debug for CompositeReader<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompositeReader").finish_non_exhaustive()
    }
}

impl<R: FileReader> FileReader for CompositeReader<R> {
    fn read(
        &self,
        offset: u64,
        size: u32,
    ) -> impl Future<Output = Result<Bytes, std::io::Error>> + Send {
        self.inner.read(offset, size)
    }

    fn close(&self) -> impl Future<Output = Result<(), std::io::Error>> + Send {
        self.inner.close()
    }
}

struct ChildSlot<DP: FsDataProvider> {
    inner: Arc<ChildInner<DP>>,
    bridge: Arc<ConcurrentBridge>,
    /// The name under which this child was registered in `name_to_slot`.
    /// Stored here so `forget` can do O(1) removal instead of a linear scan.
    name: OsString,
}

struct CompositeFsInner<R: CompositeRoot> {
    root: R,
    /// Child slots, indexed by slot number.
    slots: scc::HashMap<usize, ChildSlot<R::ChildDP>>,
    /// Maps a composite-level outer inode to its child slot index.
    addr_to_slot: scc::HashMap<InodeAddr, usize>,
    /// Maps child name to slot index (for dedup on concurrent resolve).
    ///
    /// `register_child` uses `entry_sync` on this map for per-name
    /// exclusion, serializing concurrent registrations of the same child
    /// without a global lock. `forget` cleans up entries when a slot's
    /// bridge becomes empty.
    name_to_slot: scc::HashMap<OsString, usize>,
    /// Monotonically increasing slot counter.
    next_slot: AtomicU64,
    /// Monotonically increasing inode counter. Starts at 2 (1 = root).
    next_ino: AtomicU64,
    /// The filesystem owner uid/gid.
    fs_owner: (u32, u32),
}

/// A generic composite filesystem that routes to child `AsyncFs` instances.
///
/// Implements [`FsDataProvider`] so it can be used inside another `AsyncFs`.
/// Clone is cheap (shared `Arc`).
pub struct CompositeFs<R: CompositeRoot> {
    inner: Arc<CompositeFsInner<R>>,
}

impl<R: CompositeRoot> Clone for CompositeFs<R> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<R: CompositeRoot> CompositeFs<R> {
    /// Create a new composite filesystem.
    #[must_use]
    pub fn new(root: R, fs_owner: (u32, u32)) -> Self {
        Self {
            inner: Arc::new(CompositeFsInner {
                root,
                slots: scc::HashMap::new(),
                addr_to_slot: scc::HashMap::new(),
                name_to_slot: scc::HashMap::new(),
                next_slot: AtomicU64::new(0),
                next_ino: AtomicU64::new(ROOT_INO + 1),
                fs_owner,
            }),
        }
    }

    /// Build the root inode for this composite filesystem.
    #[must_use]
    pub fn make_root_inode(&self) -> INode {
        let now = std::time::SystemTime::now();
        INode {
            addr: ROOT_INO,
            permissions: InodePerms::from_bits_truncate(0o755),
            uid: self.inner.fs_owner.0,
            gid: self.inner.fs_owner.1,
            create_time: now,
            last_modified_at: now,
            parent: None,
            size: 0,
            itype: INodeType::Directory,
        }
    }

    fn allocate_ino(&self) -> InodeAddr {
        self.inner.next_ino.fetch_add(1, Ordering::Relaxed)
    }

    fn make_child_dir_inode(&self, addr: InodeAddr) -> INode {
        let now = std::time::SystemTime::now();
        INode {
            addr,
            permissions: InodePerms::from_bits_truncate(0o755),
            uid: self.inner.fs_owner.0,
            gid: self.inner.fs_owner.1,
            create_time: now,
            last_modified_at: now,
            parent: Some(ROOT_INO),
            size: 0,
            itype: INodeType::Directory,
        }
    }

    /// Allocate a new child slot with a fresh inode table and bridge mapping.
    ///
    /// Returns `(outer_ino, slot_idx)` for the newly created slot.
    fn create_child_slot(&self, desc: &ChildDescriptor<R::ChildDP>) -> (InodeAddr, usize)
    where
        R::ChildDP: Clone,
    {
        let outer_ino = self.allocate_ino();
        #[expect(
            clippy::cast_possible_truncation,
            reason = "slot index fits in usize on 64-bit"
        )]
        let slot_idx = self.inner.next_slot.fetch_add(1, Ordering::Relaxed) as usize;

        let table = FutureBackedCache::default();
        table.insert_sync(desc.root_ino.addr, desc.root_ino);
        let child_inner = Arc::new(ChildInner::create(table, desc.provider.clone()));

        let bridge = Arc::new(ConcurrentBridge::new());
        bridge.insert(outer_ino, desc.root_ino.addr);

        drop(self.inner.slots.insert_sync(
            slot_idx,
            ChildSlot {
                inner: child_inner,
                bridge,
                name: desc.name.clone(),
            },
        ));
        let _ = self.inner.addr_to_slot.insert_sync(outer_ino, slot_idx);

        (outer_ino, slot_idx)
    }

    /// Register a child, returning the composite-level outer inode address.
    ///
    /// If the child is already registered by name, the existing outer address
    /// is returned. Otherwise a new slot is created with a fresh inode table
    /// and bridge mapping.
    ///
    /// Uses `entry_sync` on `name_to_slot` for per-name exclusion:
    /// concurrent registrations of the same child are serialized by the
    /// `scc::HashMap` bucket lock, while different names proceed in
    /// parallel. `forget` may remove entries from `name_to_slot` when a
    /// slot's bridge becomes empty, but this is safe — outer inode addresses
    /// are monotonic and never reused,
    /// so `forget` cannot corrupt a replacement slot.
    fn register_child(&self, desc: &ChildDescriptor<R::ChildDP>) -> InodeAddr
    where
        R::ChildDP: Clone,
    {
        match self.inner.name_to_slot.entry_sync(desc.name.clone()) {
            scc::hash_map::Entry::Occupied(mut occ) => {
                let old_slot_idx = *occ.get();
                let bridge = self
                    .inner
                    .slots
                    .read_sync(&old_slot_idx, |_, slot| Arc::clone(&slot.bridge));
                if let Some(outer) = bridge.and_then(|b| b.backward(desc.root_ino.addr)) {
                    return outer;
                }
                // Slot exists but bridge has no mapping — replace it.
                let (outer_ino, new_slot_idx) = self.create_child_slot(desc);
                *occ.get_mut() = new_slot_idx;
                self.inner.slots.remove_sync(&old_slot_idx);
                outer_ino
            }
            scc::hash_map::Entry::Vacant(vac) => {
                let (outer_ino, slot_idx) = self.create_child_slot(desc);
                vac.insert_entry(slot_idx);
                outer_ino
            }
        }
    }
}

impl<R: CompositeRoot> FsDataProvider for CompositeFs<R>
where
    R::ChildDP: Clone,
    <<R as CompositeRoot>::ChildDP as FsDataProvider>::Reader: 'static,
{
    type Reader =
        CompositeReader<OverlayReader<<<R as CompositeRoot>::ChildDP as FsDataProvider>::Reader>>;

    async fn lookup(&self, parent: INode, name: &OsStr) -> Result<INode, std::io::Error> {
        if parent.addr == ROOT_INO {
            let desc = self
                .inner
                .root
                .resolve_child(name)
                .await?
                .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

            let outer_ino = self.register_child(&desc);
            Ok(self.make_child_dir_inode(outer_ino))
        } else {
            let slot_idx = self
                .inner
                .addr_to_slot
                .read_sync(&parent.addr, |_, &v| v)
                .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

            // Extract Arc<ChildInner>, bridge, and inner parent address under the guard.
            let (child, bridge, inner_parent) = self
                .inner
                .slots
                .read_sync(&slot_idx, |_, slot| {
                    (
                        Arc::clone(&slot.inner),
                        Arc::clone(&slot.bridge),
                        slot.bridge.forward(parent.addr),
                    )
                })
                .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

            let inner_parent =
                inner_parent.ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

            // Await the lookup outside any scc guard.
            let tracked = child
                .get_fs()
                .lookup(LoadedAddr::new_unchecked(inner_parent), name)
                .await?;
            let child_inode = tracked.inode;

            // Translate inner address back to composite-level address (outside scc guard).
            let fallback = self.allocate_ino();
            let outer_ino = bridge.backward_or_insert(child_inode.addr, fallback);

            let _ = self.inner.addr_to_slot.insert_sync(outer_ino, slot_idx);

            Ok(INode {
                addr: outer_ino,
                ..child_inode
            })
        }
    }

    async fn readdir(&self, parent: INode) -> Result<Vec<(OsString, INode)>, std::io::Error> {
        if parent.addr == ROOT_INO {
            let children = self.inner.root.list_children().await?;
            let mut entries = Vec::with_capacity(children.len());
            for desc in &children {
                let outer_ino = self.register_child(desc);
                entries.push((desc.name.clone(), self.make_child_dir_inode(outer_ino)));
            }
            Ok(entries)
        } else {
            let slot_idx = self
                .inner
                .addr_to_slot
                .read_sync(&parent.addr, |_, &v| v)
                .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

            let (child, bridge, inner_parent) = self
                .inner
                .slots
                .read_sync(&slot_idx, |_, slot| {
                    (
                        Arc::clone(&slot.inner),
                        Arc::clone(&slot.bridge),
                        slot.bridge.forward(parent.addr),
                    )
                })
                .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

            let inner_parent =
                inner_parent.ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

            // Collect child entries outside the guard.
            let mut child_entries = Vec::new();
            child
                .get_fs()
                .readdir(LoadedAddr::new_unchecked(inner_parent), 0, |de, _offset| {
                    child_entries.push((de.name.to_os_string(), de.inode));
                    false
                })
                .await?;

            // Translate all inner addresses to composite-level addresses (outside scc guard).
            let mut entries = Vec::with_capacity(child_entries.len());
            for (name, child_inode) in child_entries {
                let fallback = self.allocate_ino();
                let outer_ino = bridge.backward_or_insert(child_inode.addr, fallback);

                let _ = self.inner.addr_to_slot.insert_sync(outer_ino, slot_idx);
                entries.push((
                    name,
                    INode {
                        addr: outer_ino,
                        ..child_inode
                    },
                ));
            }
            Ok(entries)
        }
    }

    async fn open(&self, inode: INode, flags: OpenFlags) -> Result<Self::Reader, std::io::Error> {
        let slot_idx = self
            .inner
            .addr_to_slot
            .read_sync(&inode.addr, |_, &v| v)
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

        let (child, inner_ino) = self
            .inner
            .slots
            .read_sync(&slot_idx, |_, slot| {
                (Arc::clone(&slot.inner), slot.bridge.forward(inode.addr))
            })
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

        let inner_ino = inner_ino.ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

        let open_file: OpenFile<
            OverlayReader<<<R as CompositeRoot>::ChildDP as FsDataProvider>::Reader>,
        > = child
            .get_fs()
            .open(LoadedAddr::new_unchecked(inner_ino), flags)
            .await?;

        Ok(CompositeReader {
            inner: open_file.reader,
        })
    }

    async fn create(
        &self,
        parent: INode,
        name: &OsStr,
        mode: u32,
    ) -> Result<INode, std::io::Error> {
        if parent.addr == ROOT_INO {
            return Err(std::io::Error::from_raw_os_error(libc::EROFS));
        }

        let slot_idx = self
            .inner
            .addr_to_slot
            .read_sync(&parent.addr, |_, &v| v)
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

        let (child, bridge, inner_parent) = self
            .inner
            .slots
            .read_sync(&slot_idx, |_, slot| {
                (
                    Arc::clone(&slot.inner),
                    Arc::clone(&slot.bridge),
                    slot.bridge.forward(parent.addr),
                )
            })
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

        let inner_parent =
            inner_parent.ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

        let child_inode = child
            .get_fs()
            .create(LoadedAddr::new_unchecked(inner_parent), name, mode)
            .await?
            .0;

        let fallback = self.allocate_ino();
        let outer_ino = bridge.backward_or_insert(child_inode.addr, fallback);
        let _ = self.inner.addr_to_slot.insert_sync(outer_ino, slot_idx);

        Ok(INode {
            addr: outer_ino,
            ..child_inode
        })
    }

    async fn write(&self, inode: INode, offset: u64, data: Bytes) -> Result<u32, std::io::Error> {
        if inode.addr == ROOT_INO {
            return Err(std::io::Error::from_raw_os_error(libc::EROFS));
        }

        let slot_idx = self
            .inner
            .addr_to_slot
            .read_sync(&inode.addr, |_, &v| v)
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

        let (child, inner_addr) = self
            .inner
            .slots
            .read_sync(&slot_idx, |_, slot| {
                (Arc::clone(&slot.inner), slot.bridge.forward(inode.addr))
            })
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

        let inner_addr =
            inner_addr.ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

        child
            .get_fs()
            .write(LoadedAddr::new_unchecked(inner_addr), offset, data)
            .await
    }

    /// Removes the composite-level address from the child's bridge map and
    /// then from `addr_to_slot`. When the bridge becomes empty, the slot
    /// and its `name_to_slot` entry are garbage-collected.
    ///
    /// **Ordering invariant:** the bridge mapping is removed *before*
    /// `addr_to_slot` so that a concurrent [`lookup`](Self::lookup)
    /// calling `backward_or_insert` will allocate a *fresh* outer address
    /// (since the old inner→outer entry is already gone from the bridge)
    /// rather than returning the about-to-be-forgotten address. Because
    /// the fresh address differs from the forgotten one, the subsequent
    /// `addr_to_slot.remove_sync` here cannot destroy the concurrent
    /// lookup's mapping.
    ///
    /// The slot removal uses `remove_if_sync` with a re-check of
    /// `bridge.is_empty_locked()`, which acquires the bridge's
    /// coordination mutex to serialize with a concurrent
    /// `backward_or_insert` that may be mid-insert.
    ///
    /// The root inode is never forgotten.
    fn forget(&self, addr: InodeAddr) {
        if addr == ROOT_INO {
            return;
        }
        let Some(slot_idx) = self.inner.addr_to_slot.read_sync(&addr, |_, &v| v) else {
            return;
        };
        // Remove from the bridge FIRST. The bridge's internal mutex
        // serializes this with `backward_or_insert`, ensuring that any
        // concurrent lookup that arrives after this point will allocate a
        // fresh outer address rather than reusing the forgotten `addr`.
        let (removed_inner, bridge_empty) = self
            .inner
            .slots
            .read_sync(&slot_idx, |_, slot| slot.bridge.remove_by_outer(addr))
            .unwrap_or((None, false));

        // Propagate forget to the child's inode table and data provider so
        // inner inodes don't leak until the entire slot is GC'd.
        //
        // Clone the `Arc<ChildInner>` out of the scc bucket guard before
        // calling `evict`. `evict` does O(k) work (targeted removal via
        // `IndexedLookupCache::evict_addr`), and holding the `slots`
        // bucket lock for the entire duration would block concurrent
        // lookups and forgets that hash to the same bucket.
        if let Some(inner_addr) = removed_inner {
            let child = self
                .inner
                .slots
                .read_sync(&slot_idx, |_, slot| Arc::clone(&slot.inner));
            if let Some(child) = child {
                child.get_fs().evict(inner_addr);
            }
        }

        // Now safe to remove from addr_to_slot — concurrent lookups that
        // raced with us either:
        // (a) ran backward_or_insert BEFORE our bridge removal and got
        //     `addr` back (same key we are removing — acceptable, see
        //     below), or
        // (b) ran AFTER and got a fresh fallback address (different key,
        //     unaffected by this removal).
        //
        // Case (a) is a FUSE protocol-level race: the kernel sent
        // `forget` for this address while a lookup resolved to the same
        // inner entity. In practice, this should not occur because
        // `forget` fires only when nlookup reaches zero.
        self.inner.addr_to_slot.remove_sync(&addr);
        if bridge_empty {
            // Verify emptiness under the bridge's coordination mutex,
            // **outside** any scc bucket guard. This establishes the
            // ordering `bridge.mu → (released) → scc_bucket(slots)`,
            // avoiding the nested `scc_bucket(slots) → bridge.mu`
            // ordering that would be one refactor away from a deadlock
            // cycle.
            let still_empty = self
                .inner
                .slots
                .read_sync(&slot_idx, |_, slot| Arc::clone(&slot.bridge))
                .is_some_and(|b| b.is_empty_locked());
            if still_empty {
                // Use the non-locking `is_empty()` inside the predicate.
                // A concurrent `backward_or_insert` that races between
                // our `is_empty_locked()` above and this `remove_if_sync`
                // will cause `is_empty()` to return false, preventing the
                // removal. The worst case is a missed GC opportunity,
                // retried on the next forget.
                let removed = self
                    .inner
                    .slots
                    .remove_if_sync(&slot_idx, |slot| slot.bridge.is_empty());
                if let Some((_, slot)) = removed {
                    // Guard: only remove from name_to_slot if it still
                    // points to this slot. A concurrent `register_child`
                    // may have replaced it with a new slot index for the
                    // same child name; removing unconditionally would
                    // orphan the replacement slot.
                    self.inner
                        .name_to_slot
                        .remove_if_sync(&slot.name, |idx| *idx == slot_idx);
                }
            }
        }
    }
}
