//! An implementation of a filesystem accessible on a per-repo basis.
//!
//! This module directly accesses the mesa repo through the Rust SDK, on a per-repo basis.

use std::future::Future;
use std::{collections::HashMap, ffi::OsStr, path::PathBuf, time::SystemTime};

use base64::Engine as _;
use bytes::Bytes;
use mesa_dev::MesaClient;
use mesa_dev::low_level::content::{Content, DirEntry as MesaDirEntry};
use num_traits::cast::ToPrimitive as _;
use tracing::{Instrument as _, instrument, trace, warn};

use crate::fs::icache::{AsyncICache, FileTable, IcbResolver};
use crate::fs::r#trait::{
    DirEntry, DirEntryType, FileAttr, FileHandle, FileOpenOptions, FilesystemStats, Fs, Inode,
    LockOwner, OpenFile, OpenFlags,
};

use super::common::MesaApiError;
pub use super::common::{
    GetAttrError, LookupError, OpenError, ReadDirError, ReadError, ReleaseError,
};
use super::icache as mescloud_icache;
use super::icache::{InodeControlBlock, MescloudICache};

pub(super) struct RepoResolver {
    client: MesaClient,
    org_name: String,
    repo_name: String,
    ref_: String,
    fs_owner: (u32, u32),
    block_size: u32,
}

impl IcbResolver for RepoResolver {
    type Icb = InodeControlBlock;
    type Error = LookupError;

    fn resolve(
        &self,
        ino: Inode,
        stub: Option<InodeControlBlock>,
        cache: &AsyncICache<Self>,
    ) -> impl Future<Output = Result<InodeControlBlock, LookupError>> + Send
    where
        Self: Sized,
    {
        let client = self.client.clone();
        let org_name = self.org_name.clone();
        let repo_name = self.repo_name.clone();
        let ref_ = self.ref_.clone();
        let fs_owner = self.fs_owner;
        let block_size = self.block_size;

        async move {
            let stub = stub.ok_or(LookupError::InodeNotFound)?;
            let file_path = build_repo_path(stub.parent, &stub.path, cache, RepoFs::ROOT_INO).await;

            // Non-root inodes must have a resolvable path.
            if stub.parent.is_some() && file_path.is_none() {
                return Err(LookupError::InodeNotFound);
            }

            let content = client
                .org(&org_name)
                .repos()
                .at(&repo_name)
                .content()
                .get(Some(ref_.as_str()), file_path.as_deref(), Some(1u64))
                .await
                .map_err(MesaApiError::from)?;

            let now = SystemTime::now();
            let attr = match &content {
                Content::File(f) => {
                    let size = f.size.to_u64().unwrap_or(0);
                    FileAttr::RegularFile {
                        common: mescloud_icache::make_common_file_attr(
                            ino, 0o644, now, now, fs_owner, block_size,
                        ),
                        size,
                        blocks: mescloud_icache::blocks_of_size(block_size, size),
                    }
                }
                Content::Symlink(s) => {
                    let size = s.size.to_u64().unwrap_or(0);
                    FileAttr::RegularFile {
                        common: mescloud_icache::make_common_file_attr(
                            ino, 0o644, now, now, fs_owner, block_size,
                        ),
                        size,
                        blocks: mescloud_icache::blocks_of_size(block_size, size),
                    }
                }
                Content::Dir(_) => FileAttr::Directory {
                    common: mescloud_icache::make_common_file_attr(
                        ino, 0o755, now, now, fs_owner, block_size,
                    ),
                },
            };

            let children = match content {
                Content::Dir(d) => Some(
                    d.entries
                        .into_iter()
                        .filter_map(|e| {
                            let (name, kind) = match e {
                                MesaDirEntry::File(f) => (f.name?, DirEntryType::RegularFile),
                                // TODO(MES-712): return DirEntryType::Symlink once readlink is wired up.
                                MesaDirEntry::Symlink(s) => (s.name?, DirEntryType::RegularFile),
                                MesaDirEntry::Dir(d) => (d.name?, DirEntryType::Directory),
                            };
                            Some((name, kind))
                        })
                        .collect(),
                ),
                Content::File(_) | Content::Symlink(_) => None,
            };

            Ok(InodeControlBlock {
                parent: stub.parent,
                path: stub.path,
                rc: stub.rc,
                attr: Some(attr),
                children,
            })
        }
        .instrument(tracing::info_span!("RepoResolver::resolve", ino))
    }
}

/// Walk the parent chain in the cache to build the repo-relative path.
/// Returns `None` for the root inode (maps to `path=None` in the mesa content API).
async fn build_repo_path(
    parent: Option<Inode>,
    name: &std::path::Path,
    cache: &AsyncICache<RepoResolver>,
    root_ino: Inode,
) -> Option<String> {
    /// Maximum parent-chain depth before bailing out. Prevents infinite loops
    /// if a bug creates a cycle in the parent pointers.
    const MAX_DEPTH: usize = 1024;

    let parent = parent?;
    if parent == root_ino {
        return name.to_str().map(String::from);
    }

    let mut components = vec![name.to_path_buf()];
    let mut current = parent;
    for _ in 0..MAX_DEPTH {
        if current == root_ino {
            break;
        }
        let (path, next_parent) = cache
            .get_icb(current, |icb| (icb.path.clone(), icb.parent))
            .await?;
        components.push(path);
        current = next_parent?;
    }
    if current != root_ino {
        tracing::warn!("build_repo_path: exceeded MAX_DEPTH={MAX_DEPTH}, possible parent cycle");
        return None;
    }
    components.reverse();
    let joined: PathBuf = components.iter().collect();
    joined.to_str().map(String::from)
}

/// A filesystem rooted at a single mesa repository.
///
/// Implements [`Fs`] for navigating files and directories within one repo.
/// Does not handle organizations or multi-repo hierarchy — that is [`super::MesaFS`]'s job.
pub struct RepoFs {
    client: MesaClient,
    org_name: String,
    repo_name: String,
    ref_: String,

    icache: MescloudICache<RepoResolver>,
    file_table: FileTable,
    readdir_buf: Vec<DirEntry>,
    open_files: HashMap<FileHandle, Inode>,
}

impl RepoFs {
    pub(crate) const ROOT_INO: Inode = 1;
    const BLOCK_SIZE: u32 = 4096;

    /// Create a new `RepoFs` for a specific org and repo.
    pub fn new(
        client: MesaClient,
        org_name: String,
        repo_name: String,
        ref_: String,
        fs_owner: (u32, u32),
    ) -> Self {
        let resolver = RepoResolver {
            client: client.clone(),
            org_name: org_name.clone(),
            repo_name: repo_name.clone(),
            ref_: ref_.clone(),
            fs_owner,
            block_size: Self::BLOCK_SIZE,
        };
        Self {
            client,
            org_name,
            repo_name,
            ref_,
            icache: MescloudICache::new(resolver, Self::ROOT_INO, fs_owner, Self::BLOCK_SIZE),
            file_table: FileTable::new(),
            readdir_buf: Vec::new(),
            open_files: HashMap::new(),
        }
    }

    /// The name of the repository this filesystem is rooted at.
    pub(crate) fn repo_name(&self) -> &str {
        &self.repo_name
    }

    /// Build the repo-relative path for an inode by walking up the parent chain.
    ///
    /// Returns `None` for the root inode (the repo top-level maps to `path=None` in the
    /// mesa content API).
    async fn path_of_inode(&self, ino: Inode) -> Option<String> {
        /// Maximum parent-chain depth before bailing out.
        const MAX_DEPTH: usize = 1024;

        if ino == Self::ROOT_INO {
            return None;
        }

        let mut components = Vec::new();
        let mut current = ino;
        for _ in 0..MAX_DEPTH {
            if current == Self::ROOT_INO {
                break;
            }
            let (path, parent) = self
                .icache
                .get_icb(current, |icb| (icb.path.clone(), icb.parent))
                .await?;
            components.push(path);
            current = parent?;
        }
        if current != Self::ROOT_INO {
            tracing::warn!(
                ino,
                "path_of_inode: exceeded MAX_DEPTH={MAX_DEPTH}, possible parent cycle"
            );
            return None;
        }
        components.reverse();
        let joined: PathBuf = components.iter().collect();
        joined.to_str().map(String::from)
    }

    /// Spawn a background task to prefetch the root directory listing so
    /// subsequent readdir hits cache.
    pub(crate) fn prefetch_root(&self) {
        self.icache.spawn_prefetch([Self::ROOT_INO]);
    }
}

#[async_trait::async_trait]
impl super::common::InodeCachePeek for RepoFs {
    async fn peek_attr(&self, ino: Inode) -> Option<FileAttr> {
        self.icache.get_attr(ino).await
    }
}

#[async_trait::async_trait]
impl Fs for RepoFs {
    type LookupError = LookupError;
    type GetAttrError = GetAttrError;
    type OpenError = OpenError;
    type ReadError = ReadError;
    type ReaddirError = ReadDirError;
    type ReleaseError = ReleaseError;

    #[instrument(name = "RepoFs::lookup", skip(self), fields(repo = %self.repo_name))]
    async fn lookup(&mut self, parent: Inode, name: &OsStr) -> Result<FileAttr, LookupError> {
        debug_assert!(
            self.icache.contains(parent),
            "lookup: parent inode {parent} not in inode table"
        );

        let ino = self.icache.ensure_child_ino(parent, name).await;
        let attr = self
            .icache
            .get_or_resolve(ino, |icb| icb.attr)
            .await?
            .ok_or(LookupError::InodeNotFound)?;

        let rc = self
            .icache
            .inc_rc(ino)
            .await
            .ok_or(LookupError::InodeNotFound)?;
        trace!(ino, ?name, rc, "resolved inode");
        Ok(attr)
    }

    #[instrument(name = "RepoFs::getattr", skip(self), fields(repo = %self.repo_name))]
    async fn getattr(
        &mut self,
        ino: Inode,
        _fh: Option<FileHandle>,
    ) -> Result<FileAttr, GetAttrError> {
        self.icache.get_attr(ino).await.ok_or_else(|| {
            warn!(ino, "getattr on unknown inode");
            GetAttrError::InodeNotFound
        })
    }

    #[instrument(name = "RepoFs::readdir", skip(self), fields(repo = %self.repo_name))]
    async fn readdir(&mut self, ino: Inode) -> Result<&[DirEntry], ReadDirError> {
        debug_assert!(
            self.icache.contains(ino),
            "readdir: inode {ino} not in inode table"
        );
        debug_assert!(
            matches!(
                self.icache.get_attr(ino).await,
                Some(FileAttr::Directory { .. }) | None
            ),
            "readdir: inode {ino} has non-directory cached attr"
        );

        let children = self
            .icache
            .get_or_resolve(ino, |icb| icb.children.clone())
            .await?
            .ok_or(ReadDirError::NotADirectory)?;

        trace!(
            ino,
            count = children.len(),
            "readdir: resolved directory listing from icache"
        );

        self.icache.evict_zero_rc_children(ino).await;

        let mut entries = Vec::with_capacity(children.len());
        for (name, kind) in &children {
            let child_ino = self.icache.ensure_child_ino(ino, OsStr::new(name)).await;
            // Only cache directory attrs in readdir. File attrs are left as
            // None so that lookup triggers the resolver to fetch the real file
            // size. Caching placeholder file attrs (size=0) would poison
            // needs_resolve(), preventing resolution on subsequent lookups.
            if *kind == DirEntryType::Directory {
                let now = SystemTime::now();
                let attr = FileAttr::Directory {
                    common: mescloud_icache::make_common_file_attr(
                        child_ino,
                        0o755,
                        now,
                        now,
                        self.icache.fs_owner(),
                        self.icache.block_size(),
                    ),
                };
                self.icache.cache_attr(child_ino, attr).await;
            }
            entries.push(DirEntry {
                ino: child_ino,
                name: name.clone().into(),
                kind: *kind,
            });
        }

        let subdir_inodes: Vec<Inode> = entries
            .iter()
            .filter(|e| e.kind == DirEntryType::Directory)
            .map(|e| e.ino)
            .collect();
        if !subdir_inodes.is_empty() {
            trace!(
                ino,
                subdir_count = subdir_inodes.len(),
                "readdir: prefetching subdirectory children"
            );
            self.icache.spawn_prefetch(subdir_inodes);
        }

        self.readdir_buf = entries;
        Ok(&self.readdir_buf)
    }

    #[instrument(name = "RepoFs::open", skip(self), fields(repo = %self.repo_name))]
    async fn open(&mut self, ino: Inode, _flags: OpenFlags) -> Result<OpenFile, OpenError> {
        if !self.icache.contains(ino) {
            warn!(ino, "open on unknown inode");
            return Err(OpenError::InodeNotFound);
        }
        debug_assert!(
            matches!(
                self.icache.get_attr(ino).await,
                Some(FileAttr::RegularFile { .. }) | None
            ),
            "open: inode {ino} has non-file cached attr"
        );
        let fh = self.file_table.allocate();
        self.open_files.insert(fh, ino);
        trace!(ino, fh, "assigned file handle");
        Ok(OpenFile {
            handle: fh,
            options: FileOpenOptions::empty(),
        })
    }

    #[instrument(name = "RepoFs::read", skip(self), fields(repo = %self.repo_name))]
    async fn read(
        &mut self,
        ino: Inode,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
    ) -> Result<Bytes, ReadError> {
        let &file_ino = self.open_files.get(&fh).ok_or_else(|| {
            warn!(fh, "read on unknown file handle");
            ReadError::FileNotOpen
        })?;
        debug_assert!(
            file_ino == ino,
            "read: file handle {fh} maps to inode {file_ino}, but caller passed inode {ino}"
        );
        debug_assert!(
            matches!(
                self.icache.get_attr(ino).await,
                Some(FileAttr::RegularFile { .. }) | None
            ),
            "read: inode {ino} has non-file cached attr"
        );

        let file_path = self.path_of_inode(ino).await;

        // Non-root inodes must have a resolvable path.
        if ino != Self::ROOT_INO && file_path.is_none() {
            warn!(ino, "read: path_of_inode returned None for non-root inode");
            return Err(ReadError::InodeNotFound);
        }

        let content = self
            .client
            .org(&self.org_name)
            .repos()
            .at(&self.repo_name)
            .content()
            .get(Some(self.ref_.as_str()), file_path.as_deref(), None)
            .await
            .map_err(MesaApiError::from)?;

        let encoded_content = match content {
            Content::File(f) => f.content.unwrap_or_default(),
            // TODO(MES-712): return ReadError::NotAFile once symlinks are surfaced as
            // DirEntryType::Symlink, and implement readlink to return the link target.
            Content::Symlink(s) => s.content.unwrap_or_default(),
            Content::Dir(_) => return Err(ReadError::NotAFile),
        };

        let decoded = base64::engine::general_purpose::STANDARD.decode(&encoded_content)?;

        let start = usize::try_from(offset)
            .unwrap_or(decoded.len())
            .min(decoded.len());
        let end = start.saturating_add(size as usize).min(decoded.len());
        trace!(ino, fh, path = ?file_path, decoded_len = decoded.len(), start, end, "read content");
        Ok(Bytes::copy_from_slice(&decoded[start..end]))
    }

    #[instrument(name = "RepoFs::release", skip(self), fields(repo = %self.repo_name))]
    async fn release(
        &mut self,
        ino: Inode,
        fh: FileHandle,
        _flags: OpenFlags,
        _flush: bool,
    ) -> Result<(), ReleaseError> {
        let released_ino = self.open_files.remove(&fh).ok_or_else(|| {
            warn!(fh, "release on unknown file handle");
            ReleaseError::FileNotOpen
        })?;
        debug_assert!(
            released_ino == ino,
            "release: file handle {fh} mapped to inode {released_ino}, but caller passed inode {ino}"
        );
        trace!(ino = released_ino, fh, "closed file handle");
        Ok(())
    }

    #[instrument(name = "RepoFs::forget", skip(self), fields(repo = %self.repo_name))]
    async fn forget(&mut self, ino: Inode, nlookups: u64) {
        debug_assert!(
            self.icache.contains(ino),
            "forget: inode {ino} not in inode table"
        );

        self.icache.forget(ino, nlookups).await;
    }

    async fn statfs(&mut self) -> Result<FilesystemStats, std::io::Error> {
        Ok(self.icache.statfs())
    }
}
