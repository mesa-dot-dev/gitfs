use fuser::{FUSE_ROOT_ID, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, Request};
use mesa_dev::{ApiErrorCode, Mesa, MesaError, models::{Content, DirEntryType}};
use crate::{domain::GhRepoInfo, util::critical_bug};
use tracing::{error, instrument};

#[derive(Debug)]
pub enum InodeKind {
    File,
    Directory,
}

#[derive(Debug)]
struct Inode {
    ino: u64,
    path: String,
    children: Option<Vec<Inode>>,
}

impl Inode {
    pub fn empty_dir(ino: u64, path: String) -> Self {
        Self { ino, path, children: Some(vec![]) }
    }

    pub fn file(ino: u64, path: String) -> Self {
        Self { ino, path, children: None }
    }

    pub fn kind(&self) -> &InodeKind {
        match &self.children {
            Some(_) => &InodeKind::Directory,
            None => &InodeKind::File,
        }
    }

    pub fn add_children(&mut self, children: impl IntoIterator<Item = Inode>) {
        if let Some(ref mut existing_children) = self.children {
            existing_children.extend(children);
        } else {
            // TODO(markovejnovic): This is me being lazy. Better type design would prevent this
            // case.
            panic!("Cannot add children to a file inode.");
        }
    }

    pub fn iter_children(&self) -> impl Iterator<Item = &Inode> {
        self.children.as_ref().map(|children| children.iter()).into_iter().flatten()
    }
}

/// Incredibly naive FS implementation. We need to map between inodes and paths in the remote, so
/// this registry will hold that state.
///
/// TODO(markovejnovic): This implementation is absolutely awful for cache locality. Need to
///                      determine the access patterns and optimize for that.
#[derive(Debug)]
struct InodeRegistry {
    pub root: Inode,
}

impl Default for InodeRegistry {
    fn default() -> Self {
        Self {
            root: Inode::empty_dir(FUSE_ROOT_ID, "/".to_string()),
        }
    }
}

#[derive(Debug)]
pub struct MesaFS {
    /// The Mesa client.
    mesa: Mesa,

    /// The core tokio runtime running all the tasks. HTTP requests are scheduled on this runtime.
    rt: tokio::runtime::Runtime,

    /// This is the information on the GitHub repository being mounted.
    gh_repo: GhRepoInfo,

    /// The git reference (branch, tag, commit SHA) to mount. If `None`, defaults to the
    /// repository's default branch.
    git_ref: Option<String>,

    /// However, there's a huge amount of state that needs to be tracked as well. How do we
    /// actually map between inodes and file paths in the repository? That's handled here.
    inodes: InodeRegistry,
}

/// Mesa's FUSE filesystem implementation.
impl MesaFS {
    pub fn new(api_key: &str, gh_repo: GhRepoInfo, git_ref: Option<&str>) -> Self {
        Self {
            mesa: Mesa::builder(api_key).build(),
            gh_repo,
            git_ref: git_ref.map(|s| s.to_string()),
            rt: tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime"),
            inodes: InodeRegistry::default(),
        }
    }

    fn dir_reply<'a>(mut reply: ReplyDirectory, mut offset: i64, iterable: impl IntoIterator<Item = (u64, fuser::FileType, &'a str)>) {
        reply.add(FUSE_ROOT_ID, offset, fuser::FileType::Directory, ".");
        offset += 1;
        reply.add(FUSE_ROOT_ID, offset, fuser::FileType::Directory, "..");
        offset += 1;

        for (ino, file_type, name) in iterable {
            reply.add(ino, offset, file_type, name);
            offset += 1;
        }
        reply.ok();
    }

    fn refresh_root(&mut self) -> Result<(), MesaError> {
        return match self.rt.block_on(
            self.mesa.content(
                self.gh_repo.org.as_str(),
                self.gh_repo.repo.as_str()
            ).get(None, self.git_ref.as_deref())
        ) {
            Ok(content) => {
                match content {
                    Content::File { name, path, sha, size, encoding, content } => {
                        critical_bug!("Root content is a file, expected directory.");
                    },
                    Content::Dir { name, path, sha, entries, next_cursor, has_more } => {
                        self.inodes.root.add_children(
                            entries.iter().map(|entry| {
                                match entry.entry_type {
                                    DirEntryType::File => Inode::file(0, entry.path.clone()),
                                    DirEntryType::Dir => Inode::empty_dir(0, entry.path.clone()),
                                }
                            })
                        );
                        Ok(())
                    },
                }
            },
            Err(err) => {
                error!(error = %err, "Failed to read root directory.");
                Err(err)
            },
        };
    }
}

impl Filesystem for MesaFS {
    #[instrument(skip(self, req, name, reply))]
    fn lookup(&mut self, req: &Request<'_>, parent: u64, name: &std::ffi::OsStr, reply: ReplyEntry) {
    }

    #[instrument(skip(self, _req, ino, _fh, reply))]
    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        if ino == FUSE_ROOT_ID {
            self.refresh_root().unwrap_or_else(|err| {
                error!(error = %err, "Failed to refresh root directory.");
                reply.error(libc::EIO);
            });

            let attr = fuser::FileAttr {
                ino: FUSE_ROOT_ID,
                // TODO(markovejnovic): Everything from here down is completely wrong.
                size: 0,
                blocks: 0,
                atime: std::time::SystemTime::now(),
                mtime: std::time::SystemTime::now(),
                ctime: std::time::SystemTime::now(),
                crtime: std::time::SystemTime::now(),
                kind: fuser::FileType::Directory,
                perm: 0o755,
                nlink: 2,
                uid: 1000,
                gid: 1000,
                rdev: 0,
                flags: 0,
            };
        }
    }

    #[instrument(skip(self, req, ino, fh, offset, size, flags, lock_owner, reply))]
    fn read(&mut self, req: &Request<'_>, ino: u64, fh: u64, offset: i64, size: u32, flags: i32, lock_owner: Option<u64>, reply: ReplyData) {
        unimplemented!();
    }

    #[instrument(skip(self, req, ino, fh, offset, reply))]
    fn readdir(&mut self, req: &Request<'_>, ino: u64, fh: u64, offset: i64, reply: ReplyDirectory) {
        if ino == FUSE_ROOT_ID {
            match self.rt.block_on(
                self.mesa.content(
                    self.gh_repo.org.as_str(),
                    self.gh_repo.repo.as_str()).get(None, self.git_ref.as_deref())
            ) {
                Ok(content) => {
                    match content {
                        Content::File { name, path, sha, size, encoding, content } => {
                            critical_bug!("Root content is a file, expected directory.");
                        },
                        Content::Dir { name, path, sha, entries, next_cursor, has_more } => {
                            self.inodes.root.add_children(
                                entries.iter().map(|entry| {
                                    match entry.entry_type {
                                        DirEntryType::File => Inode::file(0, entry.path.clone()),
                                        DirEntryType::Dir => Inode::empty_dir(0, entry.path.clone()),
                                    }
                                })
                            );

                            Self::dir_reply(reply, offset, self.inodes.root.iter_children().map(|inode| {
                                let file_type = match inode.kind() {
                                    InodeKind::Directory => fuser::FileType::Directory,
                                    InodeKind::File => fuser::FileType::RegularFile,
                                };
                                (inode.ino, file_type, inode.path.as_str())
                            }));
                            return;
                        },
                    };
                },
                Err(err) => {
                    if let mesa_dev::error::MesaError::Api { code, .. } = &err && *code == ApiErrorCode::NotFound {
                        reply.error(libc::ENOENT);
                        return;
                    }

                    error!(error = %err, "Failed to read root directory.");
                    reply.error(libc::EIO);
                },
            };
        }
    }
}
