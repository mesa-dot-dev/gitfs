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

use bytes::Bytes;

use crate::fs::INode;
use crate::fs::async_fs::{FileReader, FsDataProvider};

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

mod child_inner_impl {
    #![allow(clippy::future_not_send, clippy::mem_forget)]

    use ouroboros::self_referencing;

    use crate::cache::async_backed::FutureBackedCache;
    use crate::fs::async_fs::{AsyncFs, FsDataProvider};
    use crate::fs::{INode, InodeAddr};

    /// Self-referential struct co-locating an inode table and [`AsyncFs`].
    ///
    /// The `AsyncFs` borrows from the table directly, avoiding an extra
    /// indirection. This mirrors the [`FuseBridgeInner`](super::super::fuser)
    /// pattern.
    #[self_referencing]
    pub struct ChildInner<DP: FsDataProvider> {
        pub(super) table: FutureBackedCache<InodeAddr, INode>,
        #[borrows(table)]
        #[covariant]
        pub(super) fs: AsyncFs<'this, DP>,
    }

    impl<DP: FsDataProvider> ChildInner<DP> {
        #[expect(dead_code, reason = "used by CompositeFs in a follow-up commit")]
        pub(super) fn create(table: FutureBackedCache<InodeAddr, INode>, provider: DP) -> Self {
            ChildInnerBuilder {
                table,
                fs_builder: |tbl| AsyncFs::new_preseeded(provider, tbl),
            }
            .build()
        }

        #[expect(dead_code, reason = "used by CompositeFs in a follow-up commit")]
        pub(super) fn get_fs(&self) -> &AsyncFs<'_, DP> {
            self.borrow_fs()
        }
    }
}

pub use child_inner_impl::ChildInner;

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
