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

use super::common::{self, InodeControlBlock, InodeFactory};
pub use super::common::{
    GetAttrError, LookupError, OpenError, ReadDirError, ReadError, ReleaseError,
};

/// A filesystem rooted at a single mesa repository.
///
/// Implements [`Fs`] for navigating files and directories within one repo.
/// Does not handle organizations or multi-repo hierarchy — that is [`super::MesaFS`]'s job.
pub struct RepoFs {
    client: MesaClient,
    org_name: String,
    repo_name: String,
    ref_: String,

    fs_owner: (u32, u32),

    inode_table: HashMap<Inode, InodeControlBlock>,
    inode_factory: InodeFactory,

    next_fh: FileHandle,
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
        let now = SystemTime::now();

        let mut inode_table = HashMap::new();
        inode_table.insert(
            Self::ROOT_INO,
            InodeControlBlock {
                rc: 1,
                parent: None,
                path: "/".into(),
                children: None,
                attr: None,
            },
        );

        let mut fs = Self {
            client,
            org_name,
            repo_name,
            ref_,
            fs_owner,
            inode_table,
            inode_factory: InodeFactory::new(Self::ROOT_INO + 1),
            next_fh: 1,
            open_files: HashMap::new(),
        };

        let root_attr = FileAttr::Directory {
            common: common::make_common_file_attr(
                fs.fs_owner,
                Self::BLOCK_SIZE,
                Self::ROOT_INO,
                0o755,
                now,
                now,
            ),
        };
        common::cache_attr(&mut fs.inode_table, Self::ROOT_INO, root_attr);
        fs
    }

    /// The name of the repository this filesystem is rooted at.
    pub(crate) fn repo_name(&self) -> &str {
        &self.repo_name
    }

    /// Get the cached attr for an inode, if present.
    pub(crate) fn inode_table_get_attr(&self, ino: Inode) -> Option<FileAttr> {
        self.inode_table.get(&ino).and_then(|icb| icb.attr)
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
            let icb = self.inode_table.get(&current)?;
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
            self.inode_table.contains_key(&parent),
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

        let (ino, _) = common::ensure_child_inode(
            &mut self.inode_table,
            &mut self.inode_factory,
            self.fs_owner,
            Self::BLOCK_SIZE,
            parent,
            name,
            kind,
        );

        let now = SystemTime::now();
        let attr = match content {
            mesa_dev::models::Content::File { size, .. } => FileAttr::RegularFile {
                common: common::make_common_file_attr(
                    self.fs_owner,
                    Self::BLOCK_SIZE,
                    ino,
                    0o644,
                    now,
                    now,
                ),
                size,
                blocks: common::blocks_of_size(Self::BLOCK_SIZE, size),
            },
            mesa_dev::models::Content::Dir { .. } => FileAttr::Directory {
                common: common::make_common_file_attr(
                    self.fs_owner,
                    Self::BLOCK_SIZE,
                    ino,
                    0o755,
                    now,
                    now,
                ),
            },
        };
        common::cache_attr(&mut self.inode_table, ino, attr);

        let icb = self
            .inode_table
            .get_mut(&ino)
            .unwrap_or_else(|| unreachable!("inode {ino} was just ensured"));
        icb.rc += 1;
        trace!(ino, path = ?file_path, rc = icb.rc, "resolved inode");
        Ok(attr)
    }

    #[instrument(skip(self), fields(repo = %self.repo_name))]
    async fn getattr(
        &mut self,
        ino: Inode,
        _fh: Option<FileHandle>,
    ) -> Result<FileAttr, GetAttrError> {
        let icb = self.inode_table.get(&ino).ok_or_else(|| {
            warn!(ino, "getattr on unknown inode");
            GetAttrError::InodeNotFound
        })?;
        icb.attr.ok_or_else(|| {
            warn!(ino, "getattr on inode with no cached attr");
            GetAttrError::InodeNotFound
        })
    }

    #[instrument(skip(self), fields(repo = %self.repo_name))]
    async fn readdir(&mut self, ino: Inode) -> Result<&[DirEntry], ReadDirError> {
        debug_assert!(
            self.inode_table.contains_key(&ino),
            "readdir: inode {ino} not in inode table"
        );
        debug_assert!(
            matches!(
                self.inode_table.get(&ino).and_then(|icb| icb.attr),
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
            let (child_ino, _) = common::ensure_child_inode(
                &mut self.inode_table,
                &mut self.inode_factory,
                self.fs_owner,
                Self::BLOCK_SIZE,
                ino,
                OsStr::new(name),
                *kind,
            );
            entries.push(DirEntry {
                ino: child_ino,
                name: name.clone().into(),
                kind: *kind,
            });
        }

        let icb = self
            .inode_table
            .get_mut(&ino)
            .ok_or(ReadDirError::InodeNotFound)?;
        Ok(icb.children.insert(entries))
    }

    #[instrument(skip(self), fields(repo = %self.repo_name))]
    async fn open(&mut self, ino: Inode, _flags: OpenFlags) -> Result<OpenFile, OpenError> {
        if !self.inode_table.contains_key(&ino) {
            warn!(ino, "open on unknown inode");
            return Err(OpenError::InodeNotFound);
        }
        debug_assert!(
            matches!(
                self.inode_table.get(&ino).and_then(|icb| icb.attr),
                Some(FileAttr::RegularFile { .. }) | None
            ),
            "open: inode {ino} has non-file cached attr"
        );
        let fh = self.next_fh;
        self.next_fh += 1;
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
                self.inode_table.get(&ino).and_then(|icb| icb.attr),
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
            self.inode_table.contains_key(&ino),
            "forget: inode {ino} not in inode table"
        );

        match self.inode_table.entry(ino) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                if entry.get().rc <= nlookups {
                    trace!(ino, "evicting inode");
                    entry.remove();
                } else {
                    entry.get_mut().rc -= nlookups;
                    trace!(ino, new_rc = entry.get().rc, "decremented rc");
                }
            }
            std::collections::hash_map::Entry::Vacant(_) => {
                warn!(ino, "forget on unknown inode");
            }
        }
    }

    async fn statfs(&mut self) -> Result<FilesystemStats, std::io::Error> {
        Ok(FilesystemStats {
            block_size: Self::BLOCK_SIZE,
            fragment_size: u64::from(Self::BLOCK_SIZE),
            total_blocks: 0,
            free_blocks: 0,
            available_blocks: 0,
            total_inodes: self.inode_table.len() as u64,
            free_inodes: 0,
            available_inodes: 0,
            filesystem_id: 0,
            mount_flags: 0,
            max_filename_length: 255,
        })
    }
}
