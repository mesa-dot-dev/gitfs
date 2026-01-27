use std::{collections::HashMap, ffi::{OsStr, OsString}, time::Duration};

use fuser::{FUSE_ROOT_ID, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, Request};
use mesa_dev::{ApiErrorCode, Mesa, MesaError, models::{Content, DirEntryType}};
use crate::{domain::GhRepoInfo, util::critical_bug};
use tracing::{error, instrument, debug};

#[derive(Debug)]
pub enum InodeKind {
    File,
    Directory,
}

#[derive(Debug)]
struct Inode {
    ino: u64,
    path: OsString,
    size: u64,
    children: Option<Vec<u64>>,
}

impl Inode {
    pub fn empty_dir(ino: u64, path: OsString) -> Self {
        Self { ino, path, children: Some(vec![]), size: 0 }
    }

    pub fn file(ino: u64, path: OsString, size: u64) -> Self {
        Self { ino, path, children: None, size }
    }

    pub fn kind(&self) -> &InodeKind {
        match &self.children {
            Some(_) => &InodeKind::Directory,
            None => &InodeKind::File,
        }
    }

    pub fn name(&self) -> &OsStr {
        std::path::Path::new(&self.path)
            .file_name()
            .unwrap_or(&self.path)
    }
}

/// Incredibly naive FS implementation. We need to map between inodes and paths in the remote, so
/// this registry will hold that state.
///
/// TODO(markovejnovic): This implementation is absolutely awful for cache locality. Need to
///                      determine the access patterns and optimize for that.
#[derive(Debug)]
struct InodeRegistry {
    pub inodes: HashMap<u64, Inode>,
}

impl InodeRegistry {
    pub fn add_children_to(&mut self, parent_ino: u64, children: impl IntoIterator<Item = Inode>) -> bool {
        if !self.inodes.contains_key(&parent_ino) {
            return false;
        }

        let children: Vec<Inode> = children.into_iter().collect();
        let child_inos: Vec<u64> = children.iter().map(|c| c.ino).collect();

        for child in children {
            self.inodes.insert(child.ino, child);
        }

        self.inodes.get_mut(&parent_ino).unwrap().children = Some(child_inos);
        true
    }

    pub fn iter_children_of(&self, parent_ino: u64) -> Option<impl Iterator<Item = &Inode>> {
        if let Some(parent) = self.inodes.get(&parent_ino) {
            if let Some(child_inos) = &parent.children {
                let children_iter = child_inos.iter().filter_map(move |child_ino| {
                    self.inodes.get(child_ino)
                });
                return Some(children_iter);
            }
        }
        None
    }

    pub fn get(&self, ino: u64) -> Option<&Inode> {
        self.inodes.get(&ino)
    }

    pub fn find_child_by_name(&self, parent_ino: u64, name: &OsStr) -> Option<&Inode> {
        self.iter_children_of(parent_ino)?
            .find(|child| child.name() == name)
    }

    pub fn root(&self) -> Option<&Inode> {
        self.inodes.get(&FUSE_ROOT_ID)
    }
}

impl Default for InodeRegistry {
    fn default() -> Self {
        let mut inodes = HashMap::new();
        inodes.insert(FUSE_ROOT_ID, Inode::empty_dir(FUSE_ROOT_ID, OsString::from("/")));

        Self { inodes }
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

    /// Simple inode counter for generating new inodes.
    /// TODO(markovejnovic): This is obviously not safe for concurrent access.
    inode_counter: u64,
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
            inode_counter: FUSE_ROOT_ID,
        }
    }

    fn dir_reply<'a>(mut reply: ReplyDirectory, offset: i64, iterable: impl IntoIterator<Item = (u64, fuser::FileType, &'a str)>) {
        let dots: [(u64, fuser::FileType, &str); 2] = [
            (FUSE_ROOT_ID, fuser::FileType::Directory, "."),
            (FUSE_ROOT_ID, fuser::FileType::Directory, ".."),
        ];

        for (i, (ino, file_type, name)) in dots.into_iter().chain(iterable).enumerate().skip(offset as usize) {
            if reply.add(ino, (i + 1) as i64, file_type, name) {
                break;
            }
        }
        reply.ok();
    }

    fn file_attr_for(inode: &Inode) -> fuser::FileAttr {
        let (kind, perm) = match inode.kind() {
            InodeKind::File => (fuser::FileType::RegularFile, 0o444),
            InodeKind::Directory => (fuser::FileType::Directory, 0o755),
        };
        fuser::FileAttr {
            ino: inode.ino,
            size: inode.size,
            blocks: 0,
            atime: std::time::SystemTime::now(),
            mtime: std::time::SystemTime::now(),
            ctime: std::time::SystemTime::now(),
            crtime: std::time::SystemTime::now(),
            kind,
            perm,
            nlink: if matches!(inode.kind(), InodeKind::Directory) { 2 } else { 1 },
            uid: 1000,
            gid: 1000,
            rdev: 0,
            flags: 0,
            blksize: 0,
        }
    }

    fn refresh_dir(&mut self, ino: u64) -> Result<(), MesaError> {
        let api_path = {
            let inode = self.inodes.get(ino)
                .unwrap_or_else(|| critical_bug!("refresh_dir called with unknown inode {}", ino));
            let path = inode.path.to_str()
                .unwrap_or_else(|| critical_bug!("inode path is not valid UTF-8"));
            if path == "/" { None } else { Some(path.to_string()) }
        };

        // TODO(markovejnovic): This doesn't actually paginate.
        match self.rt.block_on(
            self.mesa.content(
                self.gh_repo.org.as_str(),
                self.gh_repo.repo.as_str(),
            ).get(api_path.as_deref(), self.git_ref.as_deref())
        ) {
            Ok(Content::Dir { entries, .. }) => {
                self.inodes.add_children_to(
                    ino,
                    entries.into_iter().map(|entry| {
                        self.inode_counter += 1;
                        match entry.entry_type {
                            DirEntryType::File => Inode::file(
                                self.inode_counter,
                                entry.path.into(),
                                entry.size.unwrap_or(0),
                            ),
                            DirEntryType::Dir => Inode::empty_dir(
                                self.inode_counter,
                                entry.path.into(),
                            ),
                        }
                    })
                );
                Ok(())
            }
            Ok(Content::File { .. }) => {
                critical_bug!("refresh_dir called on inode {} but API returned a file", ino);
            }
            Err(err) => {
                error!(error = %err, ino, "Failed to refresh directory.");
                Err(err)
            }
        }
    }
}

impl Filesystem for MesaFS {
    #[instrument(skip(self, _req, name, reply))]
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        // Try cached first.
        if let Some(child) = self.inodes.find_child_by_name(parent, name) {
            let attr = Self::file_attr_for(child);
            reply.entry(&Duration::from_mins(1), &attr, 0);
            return;
        }

        // Not cached — refresh the parent directory and retry.
        if let Some(parent_inode) = self.inodes.get(parent) {
            if matches!(parent_inode.kind(), InodeKind::Directory) {
                if let Err(_) = self.refresh_dir(parent) {
                    reply.error(libc::ENOENT);
                    return;
                }
                if let Some(child) = self.inodes.find_child_by_name(parent, name) {
                    let attr = Self::file_attr_for(child);
                    reply.entry(&Duration::from_mins(1), &attr, 0);
                    return;
                }
            }
        }

        reply.error(libc::ENOENT);
    }

    #[instrument(skip(self, _req, ino, _fh, reply))]
    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        if let Some(inode) = self.inodes.get(ino) {
            let attr = Self::file_attr_for(inode);
            reply.attr(&Duration::from_mins(1), &attr);
        } else {
            reply.error(libc::ENOENT);
        }
    }

    #[instrument(skip(self, req, ino, fh, offset, size, flags, lock_owner, reply))]
    fn read(&mut self, req: &Request<'_>, ino: u64, fh: u64, offset: i64, size: u32, flags: i32, lock_owner: Option<u64>, reply: ReplyData) {
        unimplemented!();
    }

    #[instrument(skip(self, _req, ino, _fh, offset, reply))]
    fn readdir(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, offset: i64, reply: ReplyDirectory) {
        if let Err(err) = self.refresh_dir(ino) {
            if let MesaError::Api { code, .. } = &err && *code == ApiErrorCode::NotFound {
                reply.error(libc::ENOENT);
                return;
            }

            error!(error = %err, ino, "Failed to refresh directory.");
            reply.error(libc::EIO);
            return;
        }

        let entries: Vec<(u64, fuser::FileType, String)> = self.inodes
            .iter_children_of(ino)
            .into_iter()
            .flatten()
            .map(|child| {
                let ft = match child.kind() {
                    InodeKind::File => fuser::FileType::RegularFile,
                    InodeKind::Directory => fuser::FileType::Directory,
                };
                let name = child.name().to_str().unwrap_or("").to_string();
                (child.ino, ft, name)
            })
            .collect();

        Self::dir_reply(
            reply,
            offset,
            entries.iter().map(|(ino, ft, name)| (*ino, *ft, name.as_str())),
        );
    }
}
