#![allow(missing_docs, clippy::unwrap_used)]

use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::sync::Arc;
use std::time::SystemTime;

use bytes::Bytes;

use git_fs::fs::async_fs::{FileReader, FsDataProvider};
use git_fs::fs::{INode, INodeType, InodePerms, OpenFlags};

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
}
