use std::{ffi::{OsStr, OsString}, sync::Arc, time::Duration};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use fuser::{Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, Request};
use mesa_dev::Mesa;
use mesa_dev::models::{Content, DirEntryType};
use crate::{
    domain::GhRepoInfo,
    ssfs::{
        GetINodeError, INodeHandle, INodeKind, SsFs, SsfsBackend, SsfsBackendError,
        SsfsDirEntry, SsfsOk, SsfsResolutionError,
    },
};
use tracing::instrument;

impl Into<fuser::FileAttr> for INodeHandle {
    fn into(self) -> fuser::FileAttr {
        let (kind, perm) = match self.kind {
            INodeKind::File => (fuser::FileType::RegularFile, 0o444),
            INodeKind::Directory => (fuser::FileType::Directory, 0o755),
        };

        // TODO(markovejnovic): A lot of these falues are placeholders.
        fuser::FileAttr {
            ino: self.ino as u64,
            size: self.size,
            blocks: 0,
            atime: std::time::SystemTime::now(),
            mtime: std::time::SystemTime::now(),
            ctime: std::time::SystemTime::now(),
            crtime: std::time::SystemTime::now(),
            kind,
            perm,
            nlink: if matches!(self.kind, INodeKind::Directory) { 2 } else { 1 },
            uid: 1000,
            gid: 1000,
            rdev: 0,
            flags: 0,
            blksize: 0,
        }
    }
}

fn inode_kind_to_file_type(kind: &INodeKind) -> fuser::FileType {
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

pub struct MesaBackend {
    mesa: Mesa,
    org: String,
    repo: String,
    git_ref: Option<String>,
}

impl SsfsBackend for MesaBackend {
    fn readdir(
        &self,
        path: &str,
    ) -> impl Future<Output = Result<Vec<SsfsDirEntry>, SsfsBackendError>> + Send {
        let path_arg: Option<String> = if path.is_empty() {
            None
        } else {
            Some(path.to_owned())
        };
        let org = self.org.clone();
        let repo = self.repo.clone();
        let git_ref = self.git_ref.clone();
        let mesa = self.mesa.clone();

        async move {
            let result = mesa
                .content(&org, &repo)
                .get(path_arg.as_deref(), git_ref.as_deref())
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
                },
                Ok(Content::File { .. }) => Err(SsfsBackendError::NotFound),
                Err(e) => Err(SsfsBackendError::Io(Box::new(e))),
            }
        }
    }

    fn read_file(
        &self,
        path: &str,
    ) -> impl Future<Output = Result<Vec<u8>, SsfsBackendError>> + Send {
        let org = self.org.clone();
        let repo = self.repo.clone();
        let git_ref = self.git_ref.clone();
        let mesa = self.mesa.clone();
        let path = path.to_owned();

        async move {
            let result = mesa
                .content(&org, &repo)
                .get(Some(&path), git_ref.as_deref())
                .await;

            match result {
                Ok(Content::File { content, encoding, .. }) => {
                    if encoding != "base64" {
                        return Err(SsfsBackendError::Io(
                            format!("unsupported encoding: {encoding}").into(),
                        ));
                    }
                    // Mesa/GitHub line-wraps base64 at 76 chars; strip whitespace before decoding.
                    let cleaned: String = content.chars().filter(|c| !c.is_ascii_whitespace()).collect();
                    BASE64.decode(&cleaned).map_err(|e| SsfsBackendError::Io(Box::new(e)))
                },
                Ok(Content::Dir { .. }) => Err(SsfsBackendError::NotFound),
                Err(e) => Err(SsfsBackendError::Io(Box::new(e))),
            }
        }
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

    /// This is the backing memory for the filesystem. SsFs is responsible for managing how it
    /// serializes to disk and how it loads from disk. We are responsible for giving it the true
    /// state of reality.
    ssfs: SsFs<MesaBackend>,
}

/// Mesa's FUSE filesystem implementation.
impl MesaFS {
    /// The time-to-live for kernel attributes. After this time, the kernel will ask us for updated
    /// attributes.
    const KERNEL_TTL: Duration = Duration::from_mins(1);

    pub fn new(api_key: &str, gh_repo: GhRepoInfo, git_ref: Option<&str>) -> Self {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");

        let backend = Arc::new(MesaBackend {
            mesa: Mesa::builder(api_key).build(),
            org: gh_repo.org,
            repo: gh_repo.repo,
            git_ref: git_ref.map(|s| s.to_string()),
        });

        let ssfs = SsFs::new(backend, rt.handle().clone());

        Self { rt, ssfs }
    }
}

impl Filesystem for MesaFS {
    #[instrument(skip(self, _req, name, reply))]
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        match self.ssfs.lookup(parent as u32, name) {
            Ok(entry) => match entry {
                SsfsOk::Resolved(inode_handle) => {
                    let attr: fuser::FileAttr = inode_handle.into();
                    reply.entry(&Self::KERNEL_TTL, &attr, 0);
                },
                SsfsOk::Future(fut) => {
                    match self.rt.block_on(fut) {
                        Ok(inode_handle) => {
                            let attr: fuser::FileAttr = inode_handle.into();
                            reply.entry(&Self::KERNEL_TTL, &attr, 0);
                        },
                        Err(err) => reply.error(ssfs_err_to_errno(&err)),
                    }
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
                    let attr: fuser::FileAttr = inode_handle.into();
                    reply.attr(&Self::KERNEL_TTL, &attr);
                },
                SsfsOk::Future(fut) => {
                    match self.rt.block_on(fut) {
                        Ok(inode_handle) => {
                            let attr: fuser::FileAttr = inode_handle.into();
                            reply.attr(&Self::KERNEL_TTL, &attr);
                        },
                        Err(err) => reply.error(ssfs_err_to_errno(&err)),
                    }
                },
            },
            Err(err) => reply.error(get_inode_err_to_errno(&err)),
        }
    }

    #[instrument(skip(self, _req, ino, _fh, offset, size, _flags, _lock_owner, reply))]
    fn read(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, offset: i64, size: u32, _flags: i32, _lock_owner: Option<u64>, reply: ReplyData) {
        let data = match self.ssfs.read(ino as u32) {
            Ok(SsfsOk::Future(fut)) => {
                match self.rt.block_on(fut) {
                    Ok(data) => data,
                    Err(err) => {
                        reply.error(ssfs_err_to_errno(&err));
                        return;
                    },
                }
            },
            Ok(SsfsOk::Resolved(data)) => data,
            Err(err) => {
                reply.error(ssfs_err_to_errno(&err));
                return;
            },
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
    fn readdir(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, offset: i64, mut reply: ReplyDirectory) {
        let ino = ino as u32;

        // Get the directory's parent for the .. entry.
        let parent_ino = match self.ssfs.get_inode(ino) {
            Ok(SsfsOk::Resolved(handle)) => handle.parent,
            Ok(SsfsOk::Future(fut)) => {
                match self.rt.block_on(fut) {
                    Ok(handle) => handle.parent,
                    Err(err) => {
                        reply.error(ssfs_err_to_errno(&err));
                        return;
                    },
                }
            },
            Err(err) => {
                reply.error(get_inode_err_to_errno(&err));
                return;
            },
        };

        // Get directory children.
        let children = match self.ssfs.readdir(ino) {
            Ok(SsfsOk::Resolved(entries)) => entries,
            Ok(SsfsOk::Future(fut)) => {
                match self.rt.block_on(fut) {
                    Ok(entries) => entries,
                    Err(err) => {
                        reply.error(ssfs_err_to_errno(&err));
                        return;
                    },
                }
            },
            Err(err) => {
                reply.error(ssfs_err_to_errno(&err));
                return;
            },
        };

        // Prefetch subdirectories in the background.
        self.ssfs.prefetch_subdirectories(ino);

        let offset = offset as usize;
        let mut i = 0usize;

        // . entry
        if i >= offset {
            if reply.add(ino as u64, (i + 1) as i64, fuser::FileType::Directory, OsStr::new(".")) {
                reply.ok();
                return;
            }
        }
        i += 1;

        // .. entry
        if i >= offset {
            if reply.add(parent_ino as u64, (i + 1) as i64, fuser::FileType::Directory, OsStr::new("..")) {
                reply.ok();
                return;
            }
        }
        i += 1;

        // Child entries
        for (child_ino, kind, name) in &children {
            if i >= offset {
                let file_type = inode_kind_to_file_type(kind);
                if reply.add(*child_ino as u64, (i + 1) as i64, file_type, name) {
                    reply.ok();
                    return;
                }
            }
            i += 1;
        }

        reply.ok();
    }
}
