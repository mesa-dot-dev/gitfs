use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::time::SystemTime;

use bytes::Bytes;
use futures::TryStreamExt as _;
use git_fs::fs::{FileHandle, INode, INodeType, InodeAddr, InodePerms, OpenFlags};
use mesa_dev::MesaClient;
use secrecy::SecretString;
use tracing::{instrument, trace, warn};

use super::common::{ChildFs, MesaApiError};
pub use super::common::{LookupError, OpenError, ReadDirError, ReadError, ReleaseError};
use super::composite::CompositeFs;
use super::repo::RepoFs;
use crate::app_config::CacheConfig;

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
    /// An inode owned by some repo (either a child-root or delegated).
    RepoOwned,
}

/// A filesystem rooted at a single organization.
///
/// Composes multiple [`RepoFs`] instances, each with its own inode namespace,
/// delegating to [`CompositeFs`] for inode/fh translation at each boundary.
pub struct OrgFs {
    name: String,
    client: MesaClient,
    composite: CompositeFs<RepoFs>,
    /// Maps org-level owner-dir inodes to owner name (github only).
    owner_inodes: HashMap<InodeAddr, String>,
    cache_config: CacheConfig,
}

impl OrgFs {
    pub(crate) const ROOT_INO: InodeAddr = CompositeFs::<RepoFs>::ROOT_INO;
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
    async fn ensure_owner_inode(&mut self, owner: &str) -> (InodeAddr, INode) {
        // Check existing
        let mut stale_ino = None;
        for (&ino, existing_owner) in &self.owner_inodes {
            if existing_owner == owner {
                if let Ok(inode) = self.composite.delegated_getattr(ino).await {
                    return (ino, inode);
                }
                stale_ino = Some(ino);
                break;
            }
        }
        if let Some(ino) = stale_ino {
            self.owner_inodes.remove(&ino);
        }

        let ino = self.composite.allocate_inode();
        let now = SystemTime::now();
        let inode = INode {
            addr: ino,
            permissions: InodePerms::from_bits_truncate(0o755),
            uid: self.composite.fs_owner().0,
            gid: self.composite.fs_owner().1,
            create_time: now,
            last_modified_at: now,
            parent: Some(Self::ROOT_INO),
            size: 0,
            itype: INodeType::Directory,
        };
        self.composite.cache_inode_and_init_rc(inode);
        self.owner_inodes.insert(ino, owner.to_owned());
        (ino, inode)
    }

    #[must_use]
    pub fn new(
        name: String,
        client: MesaClient,
        fs_owner: (u32, u32),
        cache_config: CacheConfig,
    ) -> Self {
        Self {
            name,
            client,
            composite: CompositeFs::new(fs_owner, Self::BLOCK_SIZE),
            owner_inodes: HashMap::new(),
            cache_config,
        }
    }

    /// Classify an inode by its role.
    fn inode_role(&self, ino: InodeAddr) -> Option<InodeRole> {
        if ino == Self::ROOT_INO {
            return Some(InodeRole::OrgRoot);
        }
        if self.owner_inodes.contains_key(&ino) {
            return Some(InodeRole::OwnerDir);
        }
        if self.composite.child_inodes.contains_key(&ino) {
            return Some(InodeRole::RepoOwned);
        }
        if self.composite.slot_for_inode(ino).is_some() {
            return Some(InodeRole::RepoOwned);
        }
        None
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
        parent_ino: InodeAddr,
    ) -> (InodeAddr, INode) {
        // Check existing repos.
        for (&ino, &idx) in &self.composite.child_inodes {
            if self.composite.slots[idx].inner.repo_name() == repo_name {
                if let Ok(inode) = self.composite.delegated_getattr(ino).await {
                    trace!(ino, repo = repo_name, "ensure_repo_inode: reusing");
                    return (ino, inode);
                }
                warn!(
                    ino,
                    repo = repo_name,
                    "ensure_repo_inode: attr missing, rebuilding"
                );
                return self.make_repo_dir_inode(ino);
            }
        }

        // Create new RepoFs and register as child.
        let repo = RepoFs::new(
            self.client.clone(),
            self.name.clone(),
            repo_name.to_owned(),
            default_branch.to_owned(),
            self.composite.fs_owner(),
            self.cache_config.clone(),
        )
        .await;

        let outer_ino = self
            .composite
            .add_child_with_parent(repo, RepoFs::ROOT_INO, parent_ino);
        trace!(
            ino = outer_ino,
            repo = repo_name,
            "ensure_repo_inode: allocated new inode"
        );

        // Register in directory cache so readdir sees it.
        self.composite
            .directory_cache
            .insert(
                git_fs::fs::LoadedAddr(parent_ino),
                OsString::from(display_name),
                git_fs::fs::LoadedAddr(outer_ino),
                true,
            )
            .await;

        let inode = self
            .composite
            .delegated_getattr(outer_ino)
            .await
            .unwrap_or_else(|_| {
                let now = SystemTime::now();
                INode {
                    addr: outer_ino,
                    permissions: InodePerms::from_bits_truncate(0o755),
                    uid: self.composite.fs_owner().0,
                    gid: self.composite.fs_owner().1,
                    create_time: now,
                    last_modified_at: now,
                    parent: Some(parent_ino),
                    size: 0,
                    itype: INodeType::Directory,
                }
            });
        (outer_ino, inode)
    }

    /// Build a directory inode for `ino`, returning `(ino, inode)`.
    fn make_repo_dir_inode(&self, ino: InodeAddr) -> (InodeAddr, INode) {
        let now = SystemTime::now();
        let inode = INode {
            addr: ino,
            permissions: InodePerms::from_bits_truncate(0o755),
            uid: self.composite.fs_owner().0,
            gid: self.composite.fs_owner().1,
            create_time: now,
            last_modified_at: now,
            parent: None,
            size: 0,
            itype: INodeType::Directory,
        };
        self.composite.cache_inode(inode);
        (ino, inode)
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
impl ChildFs for OrgFs {
    #[instrument(name = "OrgFs::lookup", skip(self), fields(org = %self.name))]
    async fn lookup(&mut self, parent: InodeAddr, name: &OsStr) -> Result<INode, LookupError> {
        let role = self.inode_role(parent).ok_or(LookupError::InodeNotFound)?;
        match role {
            InodeRole::OrgRoot => {
                let name_str = name.to_str().ok_or(LookupError::InodeNotFound)?;

                if self.is_github() {
                    trace!(owner = name_str, "lookup: resolving github owner dir");
                    let (ino, inode) = self.ensure_owner_inode(name_str).await;
                    self.composite
                        .inc_rc(ino)
                        .ok_or(LookupError::InodeNotFound)?;
                    Ok(inode)
                } else {
                    trace!(repo = name_str, "lookup: resolving repo");
                    let repo = self.wait_for_sync(name_str).await?;
                    let (ino, inode) = self
                        .ensure_repo_inode(name_str, name_str, &repo.default_branch, Self::ROOT_INO)
                        .await;
                    let rc = self
                        .composite
                        .inc_rc(ino)
                        .ok_or(LookupError::InodeNotFound)?;
                    trace!(ino, repo = name_str, rc, "lookup: resolved repo inode");
                    Ok(inode)
                }
            }
            InodeRole::OwnerDir => {
                let owner = self
                    .owner_inodes
                    .get(&parent)
                    .ok_or(LookupError::InodeNotFound)?
                    .clone();
                let repo_name_str = name.to_str().ok_or(LookupError::InodeNotFound)?;
                let full_decoded = format!("{owner}/{repo_name_str}");
                let encoded = Self::encode_github_repo_name(&full_decoded);

                trace!(
                    owner = %owner, repo = repo_name_str, encoded = %encoded,
                    "lookup: resolving github repo via owner dir"
                );

                let repo = self.wait_for_sync(&encoded).await?;
                let (ino, inode) = self
                    .ensure_repo_inode(&encoded, repo_name_str, &repo.default_branch, parent)
                    .await;
                self.composite
                    .inc_rc(ino)
                    .ok_or(LookupError::InodeNotFound)?;
                Ok(inode)
            }
            InodeRole::RepoOwned => self.composite.delegated_lookup(parent, name).await,
        }
    }

    #[instrument(name = "OrgFs::readdir", skip(self), fields(org = %self.name))]
    async fn readdir(&mut self, ino: InodeAddr) -> Result<Vec<(OsString, INode)>, ReadDirError> {
        let role = self.inode_role(ino).ok_or(ReadDirError::InodeNotFound)?;
        match role {
            InodeRole::OrgRoot => {
                if self.is_github() {
                    return Err(ReadDirError::NotPermitted);
                }

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
                    let (_, inode) = self
                        .ensure_repo_inode(repo_name, repo_name, default_branch, Self::ROOT_INO)
                        .await;
                    entries.push((OsString::from(repo_name), inode));
                }

                Ok(entries)
            }
            InodeRole::OwnerDir if self.is_github() => Err(ReadDirError::NotPermitted),
            InodeRole::OwnerDir => Err(ReadDirError::NotADirectory),
            InodeRole::RepoOwned => {
                let dir_entries: Vec<_> = self
                    .composite
                    .delegated_readdir(ino)
                    .await?
                    .iter()
                    .map(|e| (e.name.clone(), e.ino))
                    .collect();
                let mut entries = Vec::with_capacity(dir_entries.len());
                for (name, child_ino) in dir_entries {
                    if let Some(inode) = self.composite.inode_table.get(&child_ino).await {
                        entries.push((name, inode));
                    }
                }
                Ok(entries)
            }
        }
    }

    #[instrument(name = "OrgFs::open", skip(self), fields(org = %self.name))]
    async fn open(&mut self, ino: InodeAddr, flags: OpenFlags) -> Result<FileHandle, OpenError> {
        self.composite.delegated_open(ino, flags).await
    }

    #[instrument(name = "OrgFs::read", skip(self), fields(org = %self.name))]
    async fn read(
        &mut self,
        _ino: InodeAddr,
        fh: FileHandle,
        offset: u64,
        size: u32,
    ) -> Result<Bytes, ReadError> {
        self.composite.delegated_read(fh, offset, size).await
    }

    #[instrument(name = "OrgFs::release", skip(self), fields(org = %self.name))]
    async fn release(&mut self, _ino: InodeAddr, fh: FileHandle) -> Result<(), ReleaseError> {
        self.composite.delegated_release(fh).await
    }
}
