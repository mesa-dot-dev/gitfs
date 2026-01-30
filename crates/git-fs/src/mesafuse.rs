use std::{
    ffi::{OsStr, OsString},
    sync::Arc,
    time::Duration,
};

use crate::{
    commit_worker::{CommitRequest, CommitWorkerConfig, spawn_commit_worker},
    domain::GhRepoInfo,
    ssfs::{
        GetINodeError, INodeHandle, INodeKind, SsFs, SsfsBackend, SsfsBackendError, SsfsDirEntry,
        SsfsOk, SsfsResolutionError,
    },
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use fuser::{
    Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyWrite, Request,
};
use mesa_dev::Mesa;
use mesa_dev::models::{Author as MesaAuthor, Content, DirEntryType};
use tokio::sync::mpsc;
use tracing::{info, instrument};

/// Convert an inode handle to FUSE file attributes.
fn inode_to_file_attr(handle: INodeHandle, writable: bool) -> fuser::FileAttr {
    let (kind, perm) = match handle.kind {
        INodeKind::File => {
            let perm = if writable { 0o644 } else { 0o444 };
            (fuser::FileType::RegularFile, perm)
        }
        INodeKind::Directory => (fuser::FileType::Directory, 0o755),
    };

    // TODO(markovejnovic): A lot of these falues are placeholders.
    fuser::FileAttr {
        ino: u64::from(handle.ino),
        size: handle.size,
        blocks: 0,
        atime: std::time::SystemTime::now(),
        mtime: std::time::SystemTime::now(),
        ctime: std::time::SystemTime::now(),
        crtime: std::time::SystemTime::now(),
        kind,
        perm,
        nlink: if matches!(handle.kind, INodeKind::Directory) {
            2
        } else {
            1
        },
        uid: nix::unistd::getuid().as_raw(),
        gid: nix::unistd::getgid().as_raw(),
        rdev: 0,
        flags: 0,
        blksize: 0,
    }
}

fn inode_kind_to_file_type(kind: INodeKind) -> fuser::FileType {
    match kind {
        INodeKind::File => fuser::FileType::RegularFile,
        INodeKind::Directory => fuser::FileType::Directory,
    }
}

fn ssfs_err_to_errno(err: &SsfsResolutionError) -> i32 {
    match err {
        SsfsResolutionError::DoesNotExist => libc::ENOENT,
        SsfsResolutionError::EntryIsNotDirectory => libc::ENOTDIR,
        SsfsResolutionError::EntryIsNotFile => libc::EISDIR,
        SsfsResolutionError::IoError => libc::EIO,
    }
}

fn get_inode_err_to_errno(err: &GetINodeError) -> i32 {
    match err {
        GetINodeError::DoesNotExist => libc::ENOENT,
    }
}

fn backend_err_to_errno(err: &SsfsBackendError) -> i32 {
    match err {
        SsfsBackendError::NotFound => libc::ENOENT,
        SsfsBackendError::ReadOnly => libc::EROFS,
        SsfsBackendError::Io(_) => libc::EIO,
    }
}

#[derive(Clone)]
pub struct MesaBackend {
    mesa: Mesa,
    org: String,
    repo: String,
    git_ref: Option<String>,
    commit_tx: Arc<mpsc::UnboundedSender<CommitRequest>>,
}

impl SsfsBackend for MesaBackend {
    async fn readdir(&self, path: &str) -> Result<Vec<SsfsDirEntry>, SsfsBackendError> {
        let path_arg = if path.is_empty() { None } else { Some(path) };

        let result = self
            .mesa
            .content(&self.org, &self.repo)
            .get(path_arg, self.git_ref.as_deref())
            .await;

        match result {
            Ok(Content::Dir { entries, .. }) => {
                let dir_entries = entries
                    .into_iter()
                    .map(|e| SsfsDirEntry {
                        name: OsString::from(e.name),
                        kind: match e.entry_type {
                            DirEntryType::Dir => INodeKind::Directory,
                            DirEntryType::File => INodeKind::File,
                        },
                        size: e.size.unwrap_or(0),
                    })
                    .collect();
                Ok(dir_entries)
            }
            Ok(Content::File { .. }) => Err(SsfsBackendError::NotFound),
            Err(e) => Err(SsfsBackendError::Io(Box::new(e))),
        }
    }

    async fn read_file(&self, path: &str) -> Result<Vec<u8>, SsfsBackendError> {
        let result = self
            .mesa
            .content(&self.org, &self.repo)
            .get(Some(path), self.git_ref.as_deref())
            .await;

        match result {
            Ok(Content::File {
                content, encoding, ..
            }) => {
                if encoding != "base64" {
                    return Err(SsfsBackendError::Io(
                        format!("unsupported encoding: {encoding}").into(),
                    ));
                }
                // Mesa/GitHub line-wraps base64 at 76 chars; strip whitespace before decoding.
                let cleaned: String = content
                    .chars()
                    .filter(|c| !c.is_ascii_whitespace())
                    .collect();
                BASE64
                    .decode(&cleaned)
                    .map_err(|e| SsfsBackendError::Io(Box::new(e)))
            }
            Ok(Content::Dir { .. }) => Err(SsfsBackendError::NotFound),
            Err(e) => Err(SsfsBackendError::Io(Box::new(e))),
        }
    }

    async fn create_file(&self, path: &str, content: &[u8]) -> Result<(), SsfsBackendError> {
        let request = CommitRequest::Create {
            path: path.to_owned(),
            content: content.to_vec(),
        };
        self.commit_tx
            .send(request)
            .map_err(|_| SsfsBackendError::Io("channel closed".into()))
    }

    async fn update_file(&self, path: &str, content: &[u8]) -> Result<(), SsfsBackendError> {
        let request = CommitRequest::Update {
            path: path.to_owned(),
            content: content.to_vec(),
        };
        self.commit_tx
            .send(request)
            .map_err(|_| SsfsBackendError::Io("channel closed".into()))
    }

    async fn delete_file(&self, path: &str) -> Result<(), SsfsBackendError> {
        let request = CommitRequest::Delete {
            path: path.to_owned(),
        };
        self.commit_tx
            .send(request)
            .map_err(|_| SsfsBackendError::Io("channel closed".into()))
    }
}

pub struct MesaFS {
    /// The core tokio runtime running all the tasks. HTTP requests are scheduled on this runtime.
    ///
    /// TODO(markovejnovic): One of the problems with tokio is that it doesn't support us
    /// scheduling low-latency tasks to the top of the queue. This means that when the user wants
    /// to read a file, they might hit a latency snag while they wait for tokio to schedule their
    /// request.
    rt: tokio::runtime::Runtime,

    /// This is the backing memory for the filesystem. `SsFs` is responsible for managing how it
    /// serializes to disk and how it loads from disk. We are responsible for giving it the true
    /// state of reality.
    ssfs: SsFs<MesaBackend>,

    /// Whether the filesystem is mounted in writable mode.
    writable: bool,
}

/// Mesa's FUSE filesystem implementation.
impl MesaFS {
    /// The time-to-live for kernel attributes. After this time, the kernel will ask us for updated
    /// attributes.
    const KERNEL_TTL: Duration = Duration::from_mins(1);

    #[expect(clippy::expect_used)] // Runtime creation is infallible in practice; no recovery path.
    pub fn new(
        api_key: &str,
        gh_repo: GhRepoInfo,
        git_ref: Option<&str>,
        author: Option<crate::Author>,
    ) -> Self {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");

        // Create channel for commit requests
        let (commit_tx, commit_rx) = mpsc::unbounded_channel();

        // Extract data from inputs
        let mesa = Mesa::builder(api_key).build();
        let org = gh_repo.org;
        let repo = gh_repo.repo;
        let branch = git_ref.map_or_else(|| "main".to_owned(), ToOwned::to_owned);
        let writable = author.is_some();

        // Spawn background commit worker if we have an author
        if let Some(author) = author {
            let config = CommitWorkerConfig {
                mesa: mesa.clone(),
                org: org.clone(),
                repo: repo.clone(),
                branch,
                author: MesaAuthor {
                    name: author.name,
                    email: author.email,
                    date: None,
                },
            };
            spawn_commit_worker(&rt, config, commit_rx);
        }

        let backend = Arc::new(MesaBackend {
            mesa,
            org,
            repo,
            git_ref: git_ref.map(ToOwned::to_owned),
            commit_tx: Arc::new(commit_tx),
        });

        let ssfs = SsFs::new(backend, rt.handle().clone());

        Self { rt, ssfs, writable }
    }
}

#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap
)]
impl Filesystem for MesaFS {
    #[instrument(skip(self, _req, name, reply))]
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        match self.ssfs.lookup(parent as u32, name) {
            Ok(entry) => match entry {
                SsfsOk::Resolved(inode_handle) => {
                    let attr = inode_to_file_attr(inode_handle, self.writable);
                    reply.entry(&Self::KERNEL_TTL, &attr, 0);
                }
                SsfsOk::Future(fut) => match self.rt.block_on(fut) {
                    Ok(inode_handle) => {
                        let attr = inode_to_file_attr(inode_handle, self.writable);
                        reply.entry(&Self::KERNEL_TTL, &attr, 0);
                    }
                    Err(err) => reply.error(ssfs_err_to_errno(&err)),
                },
            },
            Err(err) => reply.error(ssfs_err_to_errno(&err)),
        }
    }

    #[instrument(skip(self, _req, ino, _fh, reply))]
    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        match self.ssfs.get_inode(ino as u32) {
            Ok(entry) => match entry {
                SsfsOk::Resolved(inode_handle) => {
                    let attr = inode_to_file_attr(inode_handle, self.writable);
                    reply.attr(&Self::KERNEL_TTL, &attr);
                }
                SsfsOk::Future(fut) => match self.rt.block_on(fut) {
                    Ok(inode_handle) => {
                        let attr = inode_to_file_attr(inode_handle, self.writable);
                        reply.attr(&Self::KERNEL_TTL, &attr);
                    }
                    Err(err) => reply.error(ssfs_err_to_errno(&err)),
                },
            },
            Err(err) => reply.error(get_inode_err_to_errno(&err)),
        }
    }

    #[instrument(skip(self, _req, ino, _fh, offset, size, _flags, _lock_owner, reply))]
    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let data = match self.ssfs.read(ino as u32) {
            Ok(SsfsOk::Future(fut)) => match self.rt.block_on(fut) {
                Ok(data) => data,
                Err(err) => {
                    reply.error(ssfs_err_to_errno(&err));
                    return;
                }
            },
            Ok(SsfsOk::Resolved(data)) => data,
            Err(err) => {
                reply.error(ssfs_err_to_errno(&err));
                return;
            }
        };

        let offset = offset as usize;
        let size = size as usize;
        if offset >= data.len() {
            reply.data(&[]);
        } else {
            let end = (offset + size).min(data.len());
            reply.data(&data[offset..end]);
        }
    }

    #[instrument(skip(self, _req, ino, _fh, offset, reply))]
    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let ino = ino as u32;

        // Get the directory's parent for the .. entry.
        let parent_ino = match self.ssfs.get_inode(ino) {
            Ok(SsfsOk::Resolved(handle)) => handle.parent,
            Ok(SsfsOk::Future(fut)) => match self.rt.block_on(fut) {
                Ok(handle) => handle.parent,
                Err(err) => {
                    reply.error(ssfs_err_to_errno(&err));
                    return;
                }
            },
            Err(err) => {
                reply.error(get_inode_err_to_errno(&err));
                return;
            }
        };

        // Get directory children.
        let children = match self.ssfs.readdir(ino) {
            Ok(SsfsOk::Resolved(entries)) => entries,
            Ok(SsfsOk::Future(fut)) => match self.rt.block_on(fut) {
                Ok(entries) => entries,
                Err(err) => {
                    reply.error(ssfs_err_to_errno(&err));
                    return;
                }
            },
            Err(err) => {
                reply.error(ssfs_err_to_errno(&err));
                return;
            }
        };

        // Prefetch subdirectories in the background.
        self.ssfs.prefetch_subdirectories(ino);

        let offset = offset as usize;
        let mut i = 0usize;

        // . entry
        if i >= offset
            && reply.add(
                u64::from(ino),
                (i + 1) as i64,
                fuser::FileType::Directory,
                OsStr::new("."),
            )
        {
            reply.ok();
            return;
        }
        i += 1;

        // .. entry
        if i >= offset
            && reply.add(
                u64::from(parent_ino),
                (i + 1) as i64,
                fuser::FileType::Directory,
                OsStr::new(".."),
            )
        {
            reply.ok();
            return;
        }
        i += 1;

        // Child entries
        for (child_ino, kind, name) in &children {
            if i >= offset {
                let file_type = inode_kind_to_file_type(*kind);
                if reply.add(u64::from(*child_ino), (i + 1) as i64, file_type, name) {
                    reply.ok();
                    return;
                }
            }
            i += 1;
        }

        reply.ok();
    }

    #[instrument(skip(self, _req, name, _mode, _umask, _flags, reply))]
    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let parent_ino = parent as u32;

        // Build the full path
        let Some(parent_path) = self.ssfs.get_path(parent_ino) else {
            reply.error(libc::ENOENT);
            return;
        };

        let name_str = name.to_string_lossy();
        let full_path = if parent_path.is_empty() {
            name_str.to_string()
        } else {
            format!("{parent_path}/{name_str}")
        };

        // Create the file via backend (empty content)
        info!(path = %full_path, "creating file");
        let backend = self.ssfs.backend();
        let result = self.rt.block_on(backend.create_file(&full_path, &[]));

        match result {
            Ok(()) => {
                info!(path = %full_path, "file created, queued for commit");
                // Update cache
                if let Some(handle) = self.ssfs.insert_file(parent_ino, name, 0) {
                    let attr = inode_to_file_attr(handle, self.writable);
                    reply.created(&Self::KERNEL_TTL, &attr, 0, 0, 0);
                } else {
                    reply.error(libc::EIO);
                }
            }
            Err(ref e) => {
                info!(path = %full_path, error = ?e, "file creation failed (queue error)");
                reply.error(backend_err_to_errno(e));
            }
        }
    }

    #[instrument(skip(
        self,
        _req,
        ino,
        _fh,
        offset,
        data,
        _write_flags,
        _flags,
        _lock_owner,
        reply
    ))]
    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let ino = ino as u32;

        // Get the file path
        let Some(path) = self.ssfs.get_path(ino) else {
            reply.error(libc::ENOENT);
            return;
        };

        // Read current content
        let current_content = match self.ssfs.read(ino) {
            Ok(SsfsOk::Future(fut)) => match self.rt.block_on(fut) {
                Ok(data) => data,
                Err(ref e) => {
                    reply.error(ssfs_err_to_errno(e));
                    return;
                }
            },
            Ok(SsfsOk::Resolved(data)) => data,
            Err(ref e) => {
                // If file doesn't exist yet (new file), start with empty content
                if matches!(e, SsfsResolutionError::DoesNotExist) {
                    Vec::new()
                } else {
                    reply.error(ssfs_err_to_errno(e));
                    return;
                }
            }
        };

        // Apply the write at offset
        let offset = offset as usize;
        let mut new_content = current_content;

        // Extend if needed
        if offset + data.len() > new_content.len() {
            new_content.resize(offset + data.len(), 0);
        }

        // Copy data at offset
        new_content[offset..offset + data.len()].copy_from_slice(data);

        // Update via backend
        info!(
            path = %path,
            offset,
            write_size = data.len(),
            new_total_size = new_content.len(),
            "writing to file"
        );
        let backend = self.ssfs.backend();
        let result = self.rt.block_on(backend.update_file(&path, &new_content));

        match result {
            Ok(()) => {
                info!(path = %path, size = new_content.len(), "file updated, queued for commit");
                // Update cache with new size
                self.ssfs.update_file_size(ino, new_content.len() as u64);
                reply.written(data.len() as u32);
            }
            Err(ref e) => {
                info!(path = %path, error = ?e, "file write failed (queue error)");
                reply.error(backend_err_to_errno(e));
            }
        }
    }

    #[instrument(skip(self, _req, name, reply))]
    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let parent_ino = parent as u32;

        // Build the full path
        let Some(parent_path) = self.ssfs.get_path(parent_ino) else {
            reply.error(libc::ENOENT);
            return;
        };

        let name_str = name.to_string_lossy();
        let full_path = if parent_path.is_empty() {
            name_str.to_string()
        } else {
            format!("{parent_path}/{name_str}")
        };

        // Delete via backend
        info!(path = %full_path, "deleting file");
        let backend = self.ssfs.backend();
        let result = self.rt.block_on(backend.delete_file(&full_path));

        match result {
            Ok(()) => {
                info!(path = %full_path, "file deleted, queued for commit");
                // Update cache
                self.ssfs.remove_file(parent_ino, name);
                reply.ok();
            }
            Err(ref e) => {
                info!(path = %full_path, error = ?e, "file deletion failed (queue error)");
                reply.error(backend_err_to_errno(e));
            }
        }
    }

    #[instrument(skip(
        self, _req, ino, _mode, _uid, _gid, size, _atime, _mtime, _ctime, _fh, _crtime, _chgtime,
        _bkuptime, _flags, reply
    ))]
    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<std::time::SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<std::time::SystemTime>,
        _chgtime: Option<std::time::SystemTime>,
        _bkuptime: Option<std::time::SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let ino = ino as u32;

        // Handle truncate
        if let Some(new_size) = size {
            let Some(path) = self.ssfs.get_path(ino) else {
                reply.error(libc::ENOENT);
                return;
            };

            // Read current content
            let current_content = match self.ssfs.read(ino) {
                Ok(SsfsOk::Future(fut)) => self.rt.block_on(fut).unwrap_or_default(),
                Ok(SsfsOk::Resolved(data)) => data,
                Err(_) => Vec::new(),
            };

            let old_size = current_content.len();

            // Truncate or extend
            let mut new_content = current_content;
            new_content.resize(new_size as usize, 0);

            // Update via backend
            info!(path = %path, old_size, new_size, "truncating file");
            let backend = self.ssfs.backend();
            let result = self.rt.block_on(backend.update_file(&path, &new_content));

            if let Err(ref e) = result {
                info!(path = %path, error = ?e, "file truncate failed (queue error)");
                reply.error(backend_err_to_errno(e));
                return;
            }

            info!(path = %path, new_size, "file truncated, queued for commit");
            // Update cache
            self.ssfs.update_file_size(ino, new_size);
        }

        // Return current attributes
        match self.ssfs.get_inode(ino) {
            Ok(SsfsOk::Resolved(handle)) => {
                let attr = inode_to_file_attr(handle, self.writable);
                reply.attr(&Self::KERNEL_TTL, &attr);
            }
            Ok(SsfsOk::Future(fut)) => match self.rt.block_on(fut) {
                Ok(handle) => {
                    let attr = inode_to_file_attr(handle, self.writable);
                    reply.attr(&Self::KERNEL_TTL, &attr);
                }
                Err(ref e) => reply.error(ssfs_err_to_errno(e)),
            },
            Err(ref e) => reply.error(get_inode_err_to_errno(e)),
        }
    }

    fn fsync(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        // No-op: commits are pushed asynchronously via the channel
        reply.ok();
    }
}
