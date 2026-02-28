#![allow(missing_docs, clippy::unwrap_used)]

use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use bytes::Bytes;

use tokio::sync::Mutex;

use mesafs::fs::async_fs::{FileReader, FsDataProvider};
use mesafs::fs::dcache::DCache;
use mesafs::fs::{INode, INodeType, InodeAddr, InodePerms, OpenFlags};

/// Recorded rename calls: `(old_parent_addr, old_name, new_parent_addr, new_name)`.
type RenameCalls = Vec<(InodeAddr, OsString, InodeAddr, OsString)>;

/// Create a default `Arc<DCache>` rooted at `/` for test use.
pub fn make_dcache() -> Arc<DCache> {
    Arc::new(DCache::new(PathBuf::from("/")))
}

/// Builds an `INode` with sensible defaults. Only `addr` and `itype` are required.
pub fn make_inode(addr: u64, itype: INodeType, size: u64, parent: Option<u64>) -> INode {
    INode {
        addr,
        permissions: InodePerms::OWNER_RWX | InodePerms::GROUP_READ | InodePerms::OTHER_READ,
        uid: 1000,
        gid: 1000,
        create_time: SystemTime::UNIX_EPOCH,
        last_modified_at: SystemTime::UNIX_EPOCH,
        parent,
        size,
        itype,
    }
}

/// A mock `FileReader` that returns a fixed byte slice for any read.
#[derive(Debug, Clone)]
pub struct MockFileReader {
    pub data: Bytes,
}

impl FileReader for MockFileReader {
    #[expect(
        clippy::cast_possible_truncation,
        reason = "test mock — offsets stay small"
    )]
    async fn read(&self, offset: u64, size: u32) -> Result<Bytes, std::io::Error> {
        let start = (offset as usize).min(self.data.len());
        let end = (start + size as usize).min(self.data.len());
        Ok(self.data.slice(start..end))
    }
}

/// Shared state backing `MockFsDataProvider`.
#[derive(Debug, Default)]
pub struct MockFsState {
    /// `(parent_addr, child_name) -> child_inode`
    pub lookups: HashMap<(u64, OsString), INode>,
    /// `parent_addr -> vec of (child_name, child_inode)`
    pub directories: HashMap<u64, Vec<(OsString, INode)>>,
    /// `inode_addr -> file content bytes`
    pub file_contents: HashMap<u64, Bytes>,
    /// Mutable overrides for `lookups`. When populated, entries here take
    /// precedence and are consumed on use (removed after the first hit).
    /// Existing tests are unaffected because this defaults to empty.
    pub refresh_lookups: scc::HashMap<(u64, OsString), INode>,
    /// Counts how many times `readdir` has been called on this provider.
    pub readdir_count: std::sync::atomic::AtomicU64,
    /// Tracks addresses passed to `forget`. Used to verify propagation.
    pub forgotten_addrs: scc::HashSet<u64>,
    /// Monotonically increasing address counter for `create`. Tests that use
    /// `create` should set this to a high value (e.g. 100) to avoid inode
    /// address collisions with statically allocated test inodes.
    pub next_addr: std::sync::atomic::AtomicU64,
    /// Records calls to `write` as `(inode_addr, offset, data)` tuples.
    pub write_calls: Arc<Mutex<Vec<(InodeAddr, u64, Bytes)>>>,
    /// When `Some`, `write` will return this error instead of `Ok(data.len())`.
    pub write_error: Option<Arc<str>>,
    /// Records calls to `unlink` as `(parent_addr, name)` tuples.
    pub unlink_calls: Arc<Mutex<Vec<(InodeAddr, OsString)>>>,
    /// When `Some`, `unlink` will return this error instead of `Ok(())`.
    pub unlink_error: Option<Arc<str>>,
    /// Notified after each `write` call completes. Tests can await this
    /// instead of using `tokio::time::sleep`.
    pub write_notify: Arc<tokio::sync::Notify>,
    /// Notified after each `unlink` call completes.
    pub unlink_notify: Arc<tokio::sync::Notify>,
    /// Records calls to `rename` as `(old_parent_addr, old_name, new_parent_addr, new_name)` tuples.
    pub rename_calls: Arc<Mutex<RenameCalls>>,
    /// When `Some`, `rename` will return this error instead of `Ok(())`.
    pub rename_error: Option<Arc<str>>,
    /// Notified after each `rename` call completes.
    pub rename_notify: Arc<tokio::sync::Notify>,
}

/// A clonable mock data provider for `AsyncFs` tests.
#[derive(Debug, Clone)]
pub struct MockFsDataProvider {
    pub state: Arc<MockFsState>,
}

impl MockFsDataProvider {
    pub fn new(state: MockFsState) -> Self {
        Self {
            state: Arc::new(state),
        }
    }
}

impl FsDataProvider for MockFsDataProvider {
    type Reader = MockFileReader;

    async fn lookup(&self, parent: INode, name: &OsStr) -> Result<INode, std::io::Error> {
        let key = (parent.addr, name.to_os_string());
        // Check mutable overrides first (consumed on use).
        if let Some((_, inode)) = self.state.refresh_lookups.remove_sync(&key) {
            return Ok(inode);
        }
        self.state
            .lookups
            .get(&key)
            .copied()
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))
    }

    async fn readdir(&self, parent: INode) -> Result<Vec<(OsString, INode)>, std::io::Error> {
        self.state
            .readdir_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.state
            .directories
            .get(&parent.addr)
            .cloned()
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))
    }

    async fn open(
        &self,
        inode: INode,
        _flags: OpenFlags,
    ) -> Result<MockFileReader, std::io::Error> {
        let data = self
            .state
            .file_contents
            .get(&inode.addr)
            .cloned()
            .unwrap_or_default();
        Ok(MockFileReader { data })
    }

    fn forget(&self, addr: InodeAddr) {
        let _ = self.state.forgotten_addrs.insert_sync(addr);
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "test mock — data stays small"
    )]
    async fn write(&self, inode: INode, offset: u64, data: Bytes) -> Result<u32, std::io::Error> {
        self.state
            .write_calls
            .lock()
            .await
            .push((inode.addr, offset, data.clone()));
        let result = if let Some(ref msg) = self.state.write_error {
            Err(std::io::Error::other(msg.as_ref()))
        } else {
            Ok(data.len() as u32)
        };
        self.state.write_notify.notify_one();
        result
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "test mock — mode fits in u16"
    )]
    async fn create(
        &self,
        parent: INode,
        _name: &OsStr,
        mode: u32,
    ) -> Result<INode, std::io::Error> {
        let addr = self
            .state
            .next_addr
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let inode = INode {
            addr,
            permissions: InodePerms::from_bits_truncate(mode as u16),
            uid: 1000,
            gid: 1000,
            create_time: SystemTime::now(),
            last_modified_at: SystemTime::now(),
            parent: Some(parent.addr),
            size: 0,
            itype: INodeType::File,
        };
        Ok(inode)
    }

    async fn unlink(&self, parent: INode, name: &OsStr) -> Result<(), std::io::Error> {
        self.state
            .unlink_calls
            .lock()
            .await
            .push((parent.addr, name.to_os_string()));
        let result = if let Some(ref msg) = self.state.unlink_error {
            Err(std::io::Error::other(msg.as_ref()))
        } else {
            Ok(())
        };
        self.state.unlink_notify.notify_one();
        result
    }

    async fn rename(
        &self,
        old_parent: INode,
        old_name: &OsStr,
        new_parent: INode,
        new_name: &OsStr,
    ) -> Result<(), std::io::Error> {
        self.state.rename_calls.lock().await.push((
            old_parent.addr,
            old_name.to_os_string(),
            new_parent.addr,
            new_name.to_os_string(),
        ));
        let result = if let Some(ref msg) = self.state.rename_error {
            Err(std::io::Error::other(msg.as_ref()))
        } else {
            Ok(())
        };
        self.state.rename_notify.notify_one();
        result
    }
}
