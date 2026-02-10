use std::collections::HashMap;
use std::ffi::OsStr;
use std::future::Future;
use std::time::SystemTime;

use bytes::Bytes;
use futures::TryStreamExt as _;
use mesa_dev::MesaClient;
use secrecy::SecretString;
use tracing::{instrument, trace, warn};

pub use super::common::{
    GetAttrError, LookupError, OpenError, ReadDirError, ReadError, ReleaseError,
};
use super::common::{InodeControlBlock, MesaApiError};
use super::composite::{ChildSlot, CompositeFs};
use super::icache as mescloud_icache;
use super::icache::MescloudICache;
use super::repo::RepoFs;
use crate::fs::icache::bridge::HashMapBridge;
use crate::fs::icache::{AsyncICache, FileTable, IcbResolver};
use crate::fs::r#trait::{
    DirEntry, DirEntryType, FileAttr, FileHandle, FilesystemStats, Fs, Inode, LockOwner, OpenFile,
    OpenFlags,
};

pub(super) struct OrgResolver {
    fs_owner: (u32, u32),
    block_size: u32,
}

impl IcbResolver for OrgResolver {
    type Icb = InodeControlBlock;
    type Error = LookupError;

    fn resolve(
        &self,
        ino: Inode,
        stub: Option<InodeControlBlock>,
        _cache: &AsyncICache<Self>,
    ) -> impl Future<Output = Result<InodeControlBlock, LookupError>> + Send
    where
        Self: Sized,
    {
        let fs_owner = self.fs_owner;
        let block_size = self.block_size;
        async move {
            let stub = stub.unwrap_or_else(|| InodeControlBlock {
                parent: None,
                path: "/".into(),
                rc: 0,
                attr: None,
                children: None,
            });
            let now = SystemTime::now();
            let attr = FileAttr::Directory {
                common: mescloud_icache::make_common_file_attr(
                    ino, 0o755, now, now, fs_owner, block_size,
                ),
            };
            Ok(InodeControlBlock {
                attr: Some(attr),
                children: Some(vec![]),
                ..stub
            })
        }
    }
}

#[derive(Debug, Clone)]
pub struct OrgConfig {
    pub name: String,
    pub api_key: SecretString,
}

/// Classifies an inode by its role in the org hierarchy.
enum InodeRole {
    /// The org root directory.
    OrgRoot,
    /// A virtual owner directory (github only).
    OwnerDir,
    /// An inode owned by some repo.
    RepoOwned,
}

/// A filesystem rooted at a single organization.
///
/// Composes multiple [`RepoFs`] instances, each with its own inode namespace,
/// delegating to [`CompositeFs`] for inode/fh translation at each boundary.
pub struct OrgFs {
    name: String,
    client: MesaClient,
    composite: CompositeFs<OrgResolver, RepoFs>,
    /// Maps org-level owner-dir inodes to owner name (github only).
    owner_inodes: HashMap<Inode, String>,
}

impl OrgFs {
    pub(crate) const ROOT_INO: Inode = 1;
    const BLOCK_SIZE: u32 = 4096;

    /// The name of the organization.
    #[must_use]
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// Whether this org uses the github two-level owner/repo hierarchy.
    /// TODO(MES-674): Cleanup "special" casing for github.
    fn is_github(&self) -> bool {
        self.name == "github"
    }

    /// Encode "owner/repo" to base64 for API calls.
    /// TODO(MES-674): Cleanup "special" casing for github.
    fn encode_github_repo_name(decoded: &str) -> String {
        use base64::Engine as _;
        base64::engine::general_purpose::STANDARD.encode(decoded)
    }

    /// Ensure an inode exists for a virtual owner directory (github only). Does NOT bump rc.
    /// TODO(MES-674): Cleanup "special" casing for github.
    async fn ensure_owner_inode(&mut self, owner: &str) -> (Inode, FileAttr) {
        // Check existing
        for (&ino, existing_owner) in &self.owner_inodes {
            if existing_owner == owner {
                if let Some(attr) = self.composite.icache.get_attr(ino).await {
                    return (ino, attr);
                }
                let now = SystemTime::now();
                let attr = FileAttr::Directory {
                    common: mescloud_icache::make_common_file_attr(
                        ino,
                        0o755,
                        now,
                        now,
                        self.composite.icache.fs_owner(),
                        self.composite.icache.block_size(),
                    ),
                };
                self.composite.icache.cache_attr(ino, attr).await;
                return (ino, attr);
            }
        }

        // Allocate new
        let ino = self.composite.icache.allocate_inode();
        let now = SystemTime::now();
        self.composite
            .icache
            .insert_icb(
                ino,
                InodeControlBlock {
                    rc: 0,
                    path: owner.into(),
                    parent: Some(Self::ROOT_INO),
                    attr: None,
                    children: None,
                },
            )
            .await;
        self.owner_inodes.insert(ino, owner.to_owned());
        let attr = FileAttr::Directory {
            common: mescloud_icache::make_common_file_attr(
                ino,
                0o755,
                now,
                now,
                self.composite.icache.fs_owner(),
                self.composite.icache.block_size(),
            ),
        };
        self.composite.icache.cache_attr(ino, attr).await;
        (ino, attr)
    }

    #[must_use]
    pub fn new(name: String, client: MesaClient, fs_owner: (u32, u32)) -> Self {
        let resolver = OrgResolver {
            fs_owner,
            block_size: Self::BLOCK_SIZE,
        };
        Self {
            name,
            client,
            composite: CompositeFs {
                icache: MescloudICache::new(resolver, Self::ROOT_INO, fs_owner, Self::BLOCK_SIZE),
                file_table: FileTable::new(),
                readdir_buf: Vec::new(),
                child_inodes: HashMap::new(),
                slots: Vec::new(),
            },
            owner_inodes: HashMap::new(),
        }
    }

    /// Classify an inode by its role.
    async fn inode_role(&self, ino: Inode) -> InodeRole {
        if ino == Self::ROOT_INO {
            return InodeRole::OrgRoot;
        }
        if self.owner_inodes.contains_key(&ino) {
            return InodeRole::OwnerDir;
        }
        if self.composite.child_inodes.contains_key(&ino) {
            return InodeRole::RepoOwned;
        }
        if self.composite.slot_for_inode(ino).await.is_some() {
            return InodeRole::RepoOwned;
        }
        debug_assert!(false, "inode {ino} not found in any repo slot");
        InodeRole::OrgRoot
    }

    /// Ensure an inode + `RepoFs` exists for the given repo name.
    /// Does NOT bump rc.
    ///
    /// - `repo_name`: name used for API calls / `RepoFs` (base64-encoded for github)
    /// - `display_name`: name shown in filesystem ("linux" for github, same as `repo_name` otherwise)
    /// - `parent_ino`: owner-dir inode for github, `ROOT_INO` otherwise
    async fn ensure_repo_inode(
        &mut self,
        repo_name: &str,
        display_name: &str,
        default_branch: &str,
        parent_ino: Inode,
    ) -> (Inode, FileAttr) {
        // Check existing repos.
        for (&ino, &idx) in &self.composite.child_inodes {
            if self.composite.slots[idx].inner.repo_name() == repo_name {
                if let Some(attr) = self.composite.icache.get_attr(ino).await {
                    let rc = self
                        .composite
                        .icache
                        .get_icb(ino, |icb| icb.rc)
                        .await
                        .unwrap_or(0);
                    trace!(ino, repo = repo_name, rc, "ensure_repo_inode: reusing");
                    return (ino, attr);
                }
                // Attr missing — rebuild.
                warn!(
                    ino,
                    repo = repo_name,
                    "ensure_repo_inode: attr missing, rebuilding"
                );
                let now = SystemTime::now();
                let attr = FileAttr::Directory {
                    common: mescloud_icache::make_common_file_attr(
                        ino,
                        0o755,
                        now,
                        now,
                        self.composite.icache.fs_owner(),
                        self.composite.icache.block_size(),
                    ),
                };
                self.composite.icache.cache_attr(ino, attr).await;
                return (ino, attr);
            }
        }

        // Allocate new.
        let ino = self.composite.icache.allocate_inode();
        trace!(
            ino,
            repo = repo_name,
            "ensure_repo_inode: allocated new inode"
        );

        let now = SystemTime::now();
        self.composite
            .icache
            .insert_icb(
                ino,
                InodeControlBlock {
                    rc: 0,
                    path: display_name.into(),
                    parent: Some(parent_ino),
                    attr: None,
                    children: None,
                },
            )
            .await;

        let repo = RepoFs::new(
            self.client.clone(),
            self.name.clone(),
            repo_name.to_owned(),
            default_branch.to_owned(),
            self.composite.icache.fs_owner(),
        );

        let mut bridge = HashMapBridge::new();
        bridge.insert_inode(ino, RepoFs::ROOT_INO);

        let idx = self.composite.slots.len();
        self.composite.slots.push(ChildSlot {
            inner: repo,
            bridge,
        });
        self.composite.child_inodes.insert(ino, idx);

        let attr = FileAttr::Directory {
            common: mescloud_icache::make_common_file_attr(
                ino,
                0o755,
                now,
                now,
                self.composite.icache.fs_owner(),
                self.composite.icache.block_size(),
            ),
        };
        self.composite.icache.cache_attr(ino, attr).await;
        (ino, attr)
    }

    /// Fetch a repo by name via the API.
    async fn wait_for_sync(
        &self,
        repo_name: &str,
    ) -> Result<mesa_dev::models::PostByOrgRepos201Response, MesaApiError> {
        self.client
            .org(&self.name)
            .repos()
            .at(repo_name)
            .get()
            .await
            .map_err(MesaApiError::from)
    }
}

#[async_trait::async_trait]
impl super::common::InodeCachePeek for OrgFs {
    async fn peek_attr(&self, ino: Inode) -> Option<FileAttr> {
        self.composite.icache.get_attr(ino).await
    }
}

#[async_trait::async_trait]
impl Fs for OrgFs {
    type LookupError = LookupError;
    type GetAttrError = GetAttrError;
    type OpenError = OpenError;
    type ReadError = ReadError;
    type ReaddirError = ReadDirError;
    type ReleaseError = ReleaseError;

    #[instrument(skip(self), fields(org = %self.name))]
    async fn lookup(&mut self, parent: Inode, name: &OsStr) -> Result<FileAttr, LookupError> {
        match self.inode_role(parent).await {
            InodeRole::OrgRoot => {
                // TODO(MES-674): Cleanup "special" casing for github.
                let name_str = name.to_str().ok_or(LookupError::InodeNotFound)?;

                if self.is_github() {
                    // name is an owner like "torvalds" — create lazily, no API validation.
                    trace!(owner = name_str, "lookup: resolving github owner dir");
                    let (ino, attr) = self.ensure_owner_inode(name_str).await;
                    self.composite.icache.inc_rc(ino).await;
                    Ok(attr)
                } else {
                    // Children of org root are repos.
                    trace!(repo = name_str, "lookup: resolving repo");

                    // Validate repo exists via API.
                    let repo = self.wait_for_sync(name_str).await?;

                    let (ino, attr) = self
                        .ensure_repo_inode(name_str, name_str, &repo.default_branch, Self::ROOT_INO)
                        .await;
                    let rc = self.composite.icache.inc_rc(ino).await;
                    trace!(ino, repo = name_str, rc, "lookup: resolved repo inode");
                    Ok(attr)
                }
            }
            InodeRole::OwnerDir => {
                // TODO(MES-674): Cleanup "special" casing for github.
                // Parent is an owner dir, name is a repo like "linux".
                let owner = self
                    .owner_inodes
                    .get(&parent)
                    .ok_or(LookupError::InodeNotFound)?
                    .clone();
                let repo_name_str = name.to_str().ok_or(LookupError::InodeNotFound)?;
                let full_decoded = format!("{owner}/{repo_name_str}");
                let encoded = Self::encode_github_repo_name(&full_decoded);

                trace!(
                    owner = %owner,
                    repo = repo_name_str,
                    encoded = %encoded,
                    "lookup: resolving github repo via owner dir"
                );

                // Validate via API (uses encoded name).
                let repo = self.wait_for_sync(&encoded).await?;

                let (ino, attr) = self
                    .ensure_repo_inode(&encoded, repo_name_str, &repo.default_branch, parent)
                    .await;
                self.composite.icache.inc_rc(ino).await;
                Ok(attr)
            }
            InodeRole::RepoOwned => self.composite.delegated_lookup(parent, name).await,
        }
    }

    #[instrument(skip(self), fields(org = %self.name))]
    async fn getattr(
        &mut self,
        ino: Inode,
        _fh: Option<FileHandle>,
    ) -> Result<FileAttr, GetAttrError> {
        self.composite.delegated_getattr(ino).await
    }

    #[instrument(skip(self), fields(org = %self.name))]
    async fn readdir(&mut self, ino: Inode) -> Result<&[DirEntry], ReadDirError> {
        match self.inode_role(ino).await {
            InodeRole::OrgRoot => {
                // TODO(MES-674): Cleanup "special" casing for github.
                if self.is_github() {
                    return Err(ReadDirError::NotPermitted);
                }

                // List repos via API.
                let repos: Vec<mesa_dev::models::GetByOrgRepos200ResponseReposInner> = self
                    .client
                    .org(&self.name)
                    .repos()
                    .list(None)
                    .try_collect()
                    .await
                    .map_err(MesaApiError::from)?;

                let repo_infos: Vec<(String, String)> = repos
                    .into_iter()
                    .filter_map(|r| {
                        let name = r.name?;
                        let branch = r.default_branch.unwrap_or_else(|| "main".to_owned());
                        Some((name, branch))
                    })
                    .collect();
                trace!(count = repo_infos.len(), "readdir: fetched repo list");

                let mut entries = Vec::with_capacity(repo_infos.len());
                for (repo_name, default_branch) in &repo_infos {
                    let (repo_ino, _) = self
                        .ensure_repo_inode(repo_name, repo_name, default_branch, Self::ROOT_INO)
                        .await;
                    entries.push(DirEntry {
                        ino: repo_ino,
                        name: repo_name.clone().into(),
                        kind: DirEntryType::Directory,
                    });
                }

                self.composite.readdir_buf = entries;
                Ok(&self.composite.readdir_buf)
            }
            InodeRole::OwnerDir if self.is_github() => {
                // TODO(MES-674): Cleanup "special" casing for github.
                Err(ReadDirError::NotPermitted)
            }
            InodeRole::OwnerDir => Err(ReadDirError::NotADirectory),
            InodeRole::RepoOwned => self.composite.delegated_readdir(ino).await,
        }
    }

    #[instrument(skip(self), fields(org = %self.name))]
    async fn open(&mut self, ino: Inode, flags: OpenFlags) -> Result<OpenFile, OpenError> {
        self.composite.delegated_open(ino, flags).await
    }

    #[instrument(skip(self), fields(org = %self.name))]
    async fn read(
        &mut self,
        ino: Inode,
        fh: FileHandle,
        offset: u64,
        size: u32,
        flags: OpenFlags,
        lock_owner: Option<LockOwner>,
    ) -> Result<Bytes, ReadError> {
        self.composite
            .delegated_read(ino, fh, offset, size, flags, lock_owner)
            .await
    }

    #[instrument(skip(self), fields(org = %self.name))]
    async fn release(
        &mut self,
        ino: Inode,
        fh: FileHandle,
        flags: OpenFlags,
        flush: bool,
    ) -> Result<(), ReleaseError> {
        self.composite
            .delegated_release(ino, fh, flags, flush)
            .await
    }

    #[instrument(skip(self), fields(org = %self.name))]
    async fn forget(&mut self, ino: Inode, nlookups: u64) {
        let evicted = self.composite.delegated_forget(ino, nlookups).await;
        if evicted {
            self.owner_inodes.remove(&ino);
        }
    }

    async fn statfs(&mut self) -> Result<FilesystemStats, std::io::Error> {
        Ok(self.composite.delegated_statfs())
    }
}
