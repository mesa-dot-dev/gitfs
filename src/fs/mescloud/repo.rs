//! An implementation of a filesystem accessible on a per-repo basis.
//!
//! This module directly accesses the mesa repo through the Rust SDK, on a per-repo basis.

use std::{collections::HashMap, ffi::OsStr, path::PathBuf, time::SystemTime};

use base64::Engine as _;
use bytes::Bytes;
use mesa_dev::Mesa as MesaClient;
use tracing::{instrument, trace, warn};

use crate::fs::r#trait::{
    DirEntry, DirEntryType, FileAttr, FileHandle, FileOpenOptions, FilesystemStats, Fs, Inode,
    LockOwner, OpenFile, OpenFlags,
};

pub use super::common::{
    GetAttrError, LookupError, OpenError, ReadDirError, ReadError, ReleaseError,
};
use super::icache as mescloud_icache;
use super::icache::MescloudICache;

/// A filesystem rooted at a single mesa repository.
///
/// Implements [`Fs`] for navigating files and directories within one repo.
/// Does not handle organizations or multi-repo hierarchy — that is [`super::MesaFS`]'s job.
pub struct RepoFs {
    client: MesaClient,
    org_name: String,
    repo_name: String,
    ref_: String,

    icache: MescloudICache,
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
        Self {
            client,
            org_name,
            repo_name,
            ref_,
            icache: MescloudICache::new(Self::ROOT_INO, fs_owner, Self::BLOCK_SIZE),
            open_files: HashMap::new(),
        }
    }

    /// The name of the repository this filesystem is rooted at.
    pub(crate) fn repo_name(&self) -> &str {
        &self.repo_name
    }

    /// Get the cached attr for an inode, if present.
    pub(crate) fn inode_table_get_attr(&self, ino: Inode) -> Option<FileAttr> {
        self.icache.get_attr(ino)
    }

    /// Build the repo-relative path for an inode by walking up the parent chain.
    ///
    /// Returns `None` for the root inode (the repo top-level maps to `path=None` in the
    /// mesa content API).
    fn path_of_inode(&self, ino: Inode) -> Option<String> {
        if ino == Self::ROOT_INO {
            return None;
        }

        let mut components = Vec::new();
        let mut current = ino;
        while current != Self::ROOT_INO {
            let icb = self.icache.get_icb(current)?;
            components.push(icb.path.clone());
            current = icb.parent?;
        }
        components.reverse();
        let joined: PathBuf = components.iter().collect();
        joined.to_str().map(String::from)
    }

    /// Build the repo-relative path for a child of `parent`.
    fn path_of_child(&self, parent: Inode, name: &OsStr) -> Option<String> {
        if parent == Self::ROOT_INO {
            return name.to_str().map(String::from);
        }
        self.path_of_inode(parent).and_then(|p| {
            let mut pb = PathBuf::from(p);
            pb.push(name);
            pb.to_str().map(String::from)
        })
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

    #[instrument(skip(self), fields(repo = %self.repo_name))]
    async fn lookup(&mut self, parent: Inode, name: &OsStr) -> Result<FileAttr, LookupError> {
        debug_assert!(
            self.icache.contains(parent),
            "lookup: parent inode {parent} not in inode table"
        );

        let file_path = self.path_of_child(parent, name);

        let content = self
            .client
            .content(&self.org_name, &self.repo_name)
            .get(file_path.as_deref(), Some(self.ref_.as_str()))
            .await?;

        let kind = match &content {
            mesa_dev::models::Content::File { .. } => DirEntryType::RegularFile,
            mesa_dev::models::Content::Dir { .. } => DirEntryType::Directory,
        };

        let (ino, _) = self.icache.ensure_child_inode(parent, name, kind);

        let now = SystemTime::now();
        let attr = match content {
            mesa_dev::models::Content::File { size, .. } => FileAttr::RegularFile {
                common: self.icache.make_common_file_attr(ino, 0o644, now, now),
                size,
                blocks: mescloud_icache::blocks_of_size(Self::BLOCK_SIZE, size),
            },
            mesa_dev::models::Content::Dir { .. } => FileAttr::Directory {
                common: self.icache.make_common_file_attr(ino, 0o755, now, now),
            },
        };
        self.icache.cache_attr(ino, attr);

        let rc = self.icache.inc_rc(ino);
        trace!(ino, path = ?file_path, rc, "resolved inode");
        Ok(attr)
    }

    #[instrument(skip(self), fields(repo = %self.repo_name))]
    async fn getattr(
        &mut self,
        ino: Inode,
        _fh: Option<FileHandle>,
    ) -> Result<FileAttr, GetAttrError> {
        self.icache.get_attr(ino).ok_or_else(|| {
            warn!(ino, "getattr on unknown inode");
            GetAttrError::InodeNotFound
        })
    }

    #[instrument(skip(self), fields(repo = %self.repo_name))]
    async fn readdir(&mut self, ino: Inode) -> Result<&[DirEntry], ReadDirError> {
        debug_assert!(
            self.icache.contains(ino),
            "readdir: inode {ino} not in inode table"
        );
        debug_assert!(
            matches!(
                self.icache.get_attr(ino),
                Some(FileAttr::Directory { .. }) | None
            ),
            "readdir: inode {ino} has non-directory cached attr"
        );

        let file_path = self.path_of_inode(ino);

        let content = self
            .client
            .content(&self.org_name, &self.repo_name)
            .get(file_path.as_deref(), Some(self.ref_.as_str()))
            .await?;

        let mesa_entries = match content {
            mesa_dev::models::Content::Dir { entries, .. } => entries,
            mesa_dev::models::Content::File { .. } => return Err(ReadDirError::NotADirectory),
        };

        let collected: Vec<_> = mesa_entries
            .into_iter()
            .map(|e| {
                let kind = match e.entry_type {
                    mesa_dev::models::DirEntryType::File => DirEntryType::RegularFile,
                    mesa_dev::models::DirEntryType::Dir => DirEntryType::Directory,
                };
                (e.name, kind)
            })
            .collect();

        trace!(ino, path = ?file_path, count = collected.len(), "fetched directory listing");

        let mut entries = Vec::with_capacity(collected.len());
        for (name, kind) in &collected {
            let (child_ino, _) = self.icache.ensure_child_inode(ino, OsStr::new(name), *kind);
            entries.push(DirEntry {
                ino: child_ino,
                name: name.clone().into(),
                kind: *kind,
            });
        }

        let icb = self
            .icache
            .get_icb_mut(ino)
            .ok_or(ReadDirError::InodeNotFound)?;
        Ok(icb.children.insert(entries))
    }

    #[instrument(skip(self), fields(repo = %self.repo_name))]
    async fn open(&mut self, ino: Inode, _flags: OpenFlags) -> Result<OpenFile, OpenError> {
        if !self.icache.contains(ino) {
            warn!(ino, "open on unknown inode");
            return Err(OpenError::InodeNotFound);
        }
        debug_assert!(
            matches!(
                self.icache.get_attr(ino),
                Some(FileAttr::RegularFile { .. }) | None
            ),
            "open: inode {ino} has non-file cached attr"
        );
        let fh = self.icache.allocate_fh();
        self.open_files.insert(fh, ino);
        trace!(ino, fh, "assigned file handle");
        Ok(OpenFile {
            handle: fh,
            options: FileOpenOptions::empty(),
        })
    }

    #[instrument(skip(self), fields(repo = %self.repo_name))]
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
                self.icache.get_attr(ino),
                Some(FileAttr::RegularFile { .. }) | None
            ),
            "read: inode {ino} has non-file cached attr"
        );

        let file_path = self.path_of_inode(ino);

        let content = self
            .client
            .content(&self.org_name, &self.repo_name)
            .get(file_path.as_deref(), Some(self.ref_.as_str()))
            .await?;

        let encoded_content = match content {
            mesa_dev::models::Content::File { content, .. } => content,
            mesa_dev::models::Content::Dir { .. } => return Err(ReadError::NotAFile),
        };

        let decoded = base64::engine::general_purpose::STANDARD.decode(&encoded_content)?;

        let start = usize::try_from(offset)
            .unwrap_or(decoded.len())
            .min(decoded.len());
        let end = start.saturating_add(size as usize).min(decoded.len());
        trace!(ino, fh, path = ?file_path, decoded_len = decoded.len(), start, end, "read content");
        Ok(Bytes::copy_from_slice(&decoded[start..end]))
    }

    #[instrument(skip(self), fields(repo = %self.repo_name))]
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

    #[instrument(skip(self), fields(repo = %self.repo_name))]
    async fn forget(&mut self, ino: Inode, nlookups: u64) {
        debug_assert!(
            self.icache.contains(ino),
            "forget: inode {ino} not in inode table"
        );

        self.icache.forget(ino, nlookups);
    }

    async fn statfs(&mut self) -> Result<FilesystemStats, std::io::Error> {
        Ok(self.icache.statfs())
    }
}
