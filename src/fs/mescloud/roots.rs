//! Domain-specific [`CompositeRoot`] implementations and the [`OrgChildDP`] enum.
//!
//! Bridges the generic `CompositeFs` from `lib/fs/composite.rs` with
//! Mesa/GitHub-specific org and repo resolution logic.
//!
//! These types are not yet wired into the daemon entry point; they will be
//! connected in a follow-up change that replaces the old `MesaFS` + `OrgFs`
//! pipeline.
#![expect(dead_code, reason = "wired in the follow-up daemon change")]

use std::ffi::{OsStr, OsString};
use std::future::Future;
use std::sync::Arc;
use std::time::SystemTime;

use base64::Engine as _;
use futures::TryStreamExt as _;
use mesa_dev::MesaClient;
use tracing::warn;

use git_fs::cache::fcache::FileCache;
use git_fs::fs::async_fs::{FileReader, FsDataProvider};
use git_fs::fs::composite::{ChildDescriptor, CompositeFs, CompositeReader, CompositeRoot};
use git_fs::fs::{INode, INodeType, InodeAddr, InodePerms, OpenFlags};

use super::common::MesaApiError;
use super::repo::{MesFileReader, MesRepoProvider};
use crate::app_config::CacheConfig;

const CHILD_ROOT_ADDR: InodeAddr = 1;

fn mesa_api_error_to_io(e: MesaApiError) -> std::io::Error {
    match &e {
        MesaApiError::Response { status, .. } if *status == 404 => {
            std::io::Error::from_raw_os_error(libc::ENOENT)
        }
        MesaApiError::Reqwest(_)
        | MesaApiError::ReqwestMiddleware(_)
        | MesaApiError::Serde(_)
        | MesaApiError::SerdePath(_)
        | MesaApiError::Io(_)
        | MesaApiError::Response { .. } => std::io::Error::other(e),
    }
}

/// Create a [`MesRepoProvider`] and its root [`INode`] for a given repo.
async fn create_repo_provider(
    client: &MesaClient,
    org_name: &str,
    repo_name: &str,
    ref_: &str,
    fs_owner: (u32, u32),
    cache_config: &CacheConfig,
) -> (MesRepoProvider, INode) {
    let file_cache = match cache_config.max_size {
        Some(max_size) if max_size.as_u64() > 0 => {
            let cache_dir = cache_config.path.join(org_name).join(repo_name);
            let max_bytes = max_size.as_u64().try_into().unwrap_or(usize::MAX);
            match FileCache::new(&cache_dir, max_bytes).await {
                Ok(cache) => Some(Arc::new(cache)),
                Err(e) => {
                    warn!(error = ?e, org = %org_name, repo = %repo_name,
                        "failed to create file cache, continuing without caching");
                    None
                }
            }
        }
        _ => None,
    };

    let provider = MesRepoProvider::new(
        client.clone(),
        org_name.to_owned(),
        repo_name.to_owned(),
        ref_.to_owned(),
        fs_owner,
        file_cache,
    );

    provider.seed_root_path(CHILD_ROOT_ADDR);

    let now = SystemTime::now();
    let root_ino = INode {
        addr: CHILD_ROOT_ADDR,
        permissions: InodePerms::from_bits_truncate(0o755),
        uid: fs_owner.0,
        gid: fs_owner.1,
        create_time: now,
        last_modified_at: now,
        parent: None,
        size: 0,
        itype: INodeType::Directory,
    };

    (provider, root_ino)
}

/// Returns `Ok(())` if the error is a 404; otherwise returns the IO error.
///
/// Callers use this to treat 404 as "not found" (return `Ok(None)`) while
/// propagating all other API errors.
fn check_not_found(e: MesaApiError) -> Result<(), std::io::Error> {
    match &e {
        MesaApiError::Response { status, .. } if *status == 404 => Ok(()),
        MesaApiError::Reqwest(_)
        | MesaApiError::ReqwestMiddleware(_)
        | MesaApiError::Serde(_)
        | MesaApiError::SerdePath(_)
        | MesaApiError::Io(_)
        | MesaApiError::Response { .. } => Err(mesa_api_error_to_io(e)),
    }
}

pub(super) struct StandardOrgRoot {
    client: MesaClient,
    org_name: String,
    cache_config: CacheConfig,
    fs_owner: (u32, u32),
}

impl StandardOrgRoot {
    pub(super) fn new(
        client: MesaClient,
        org_name: String,
        cache_config: CacheConfig,
        fs_owner: (u32, u32),
    ) -> Self {
        Self {
            client,
            org_name,
            cache_config,
            fs_owner,
        }
    }
}

impl CompositeRoot for StandardOrgRoot {
    type ChildDP = MesRepoProvider;

    async fn resolve_child(
        &self,
        name: &OsStr,
    ) -> Result<Option<ChildDescriptor<Self::ChildDP>>, std::io::Error> {
        let name_str = name.to_str().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "repo name contains non-UTF-8 characters",
            )
        })?;

        let repo = match self
            .client
            .org(&self.org_name)
            .repos()
            .at(name_str)
            .get()
            .await
            .map_err(MesaApiError::from)
        {
            Ok(repo) => repo,
            Err(e) => {
                check_not_found(e)?;
                return Ok(None);
            }
        };

        // Single-repo GET returns `default_branch: String` (non-optional),
        // unlike the list endpoint which returns `Option<String>`.
        let (provider, root_ino) = create_repo_provider(
            &self.client,
            &self.org_name,
            name_str,
            &repo.default_branch,
            self.fs_owner,
            &self.cache_config,
        )
        .await;

        Ok(Some(ChildDescriptor {
            name: name.to_os_string(),
            provider,
            root_ino,
        }))
    }

    async fn list_children(&self) -> Result<Vec<ChildDescriptor<Self::ChildDP>>, std::io::Error> {
        let repos: Vec<mesa_dev::models::GetByOrgRepos200ResponseReposInner> = self
            .client
            .org(&self.org_name)
            .repos()
            .list(None)
            .try_collect()
            .await
            .map_err(MesaApiError::from)
            .map_err(mesa_api_error_to_io)?;

        let mut children = Vec::with_capacity(repos.len());
        for repo in repos {
            let Some(repo_name) = repo.name else {
                continue;
            };
            let default_branch = repo.default_branch.unwrap_or_else(|| "main".to_owned());

            let (provider, root_ino) = create_repo_provider(
                &self.client,
                &self.org_name,
                &repo_name,
                &default_branch,
                self.fs_owner,
                &self.cache_config,
            )
            .await;

            children.push(ChildDescriptor {
                name: OsString::from(repo_name),
                provider,
                root_ino,
            });
        }

        Ok(children)
    }
}

pub(super) struct GithubRepoRoot {
    client: MesaClient,
    org_name: String,
    owner: String,
    cache_config: CacheConfig,
    fs_owner: (u32, u32),
}

impl CompositeRoot for GithubRepoRoot {
    type ChildDP = MesRepoProvider;

    async fn resolve_child(
        &self,
        name: &OsStr,
    ) -> Result<Option<ChildDescriptor<Self::ChildDP>>, std::io::Error> {
        let repo_name = name.to_str().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "repo name contains non-UTF-8 characters",
            )
        })?;

        let full_decoded = format!("{}/{}", self.owner, repo_name);
        let encoded = base64::engine::general_purpose::STANDARD.encode(&full_decoded);

        let repo = match self
            .client
            .org(&self.org_name)
            .repos()
            .at(&encoded)
            .get()
            .await
            .map_err(MesaApiError::from)
        {
            Ok(repo) => repo,
            Err(e) => {
                check_not_found(e)?;
                return Ok(None);
            }
        };

        // Single-repo GET returns `default_branch: String` (non-optional).
        let (provider, root_ino) = create_repo_provider(
            &self.client,
            &self.org_name,
            &encoded,
            &repo.default_branch,
            self.fs_owner,
            &self.cache_config,
        )
        .await;

        Ok(Some(ChildDescriptor {
            name: name.to_os_string(),
            provider,
            root_ino,
        }))
    }

    async fn list_children(&self) -> Result<Vec<ChildDescriptor<Self::ChildDP>>, std::io::Error> {
        Err(std::io::Error::from_raw_os_error(libc::EPERM))
    }
}

pub(super) struct GithubOrgRoot {
    client: MesaClient,
    org_name: String,
    cache_config: CacheConfig,
    fs_owner: (u32, u32),
}

impl GithubOrgRoot {
    pub(super) fn new(
        client: MesaClient,
        org_name: String,
        cache_config: CacheConfig,
        fs_owner: (u32, u32),
    ) -> Self {
        Self {
            client,
            org_name,
            cache_config,
            fs_owner,
        }
    }
}

impl CompositeRoot for GithubOrgRoot {
    type ChildDP = CompositeFs<GithubRepoRoot>;

    async fn resolve_child(
        &self,
        name: &OsStr,
    ) -> Result<Option<ChildDescriptor<Self::ChildDP>>, std::io::Error> {
        let owner = name.to_str().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "owner name contains non-UTF-8 characters",
            )
        })?;

        let repo_root = GithubRepoRoot {
            client: self.client.clone(),
            org_name: self.org_name.clone(),
            owner: owner.to_owned(),
            cache_config: self.cache_config.clone(),
            fs_owner: self.fs_owner,
        };

        let composite = CompositeFs::new(repo_root, self.fs_owner);
        let root_ino = composite.make_root_inode();

        Ok(Some(ChildDescriptor {
            name: name.to_os_string(),
            provider: composite,
            root_ino,
        }))
    }

    async fn list_children(&self) -> Result<Vec<ChildDescriptor<Self::ChildDP>>, std::io::Error> {
        Err(std::io::Error::from_raw_os_error(libc::EPERM))
    }
}

#[derive(Clone)]
pub(super) enum OrgChildDP {
    Standard(CompositeFs<StandardOrgRoot>),
    Github(CompositeFs<GithubOrgRoot>),
}

impl OrgChildDP {
    fn make_root_inode(&self) -> INode {
        match self {
            Self::Standard(c) => c.make_root_inode(),
            Self::Github(c) => c.make_root_inode(),
        }
    }
}

impl FsDataProvider for OrgChildDP {
    type Reader = OrgChildReader;

    fn lookup(
        &self,
        parent: INode,
        name: &OsStr,
    ) -> impl Future<Output = Result<INode, std::io::Error>> + Send {
        let this = self.clone();
        let name = name.to_os_string();
        async move {
            match this {
                Self::Standard(c) => c.lookup(parent, &name).await,
                Self::Github(c) => c.lookup(parent, &name).await,
            }
        }
    }

    fn readdir(
        &self,
        parent: INode,
    ) -> impl Future<Output = Result<Vec<(OsString, INode)>, std::io::Error>> + Send {
        let this = self.clone();
        async move {
            match this {
                Self::Standard(c) => c.readdir(parent).await,
                Self::Github(c) => c.readdir(parent).await,
            }
        }
    }

    fn open(
        &self,
        inode: INode,
        flags: OpenFlags,
    ) -> impl Future<Output = Result<Self::Reader, std::io::Error>> + Send {
        let this = self.clone();
        async move {
            match this {
                Self::Standard(c) => c.open(inode, flags).await.map(OrgChildReader::Standard),
                Self::Github(c) => c.open(inode, flags).await.map(OrgChildReader::Github),
            }
        }
    }
}

pub(super) enum OrgChildReader {
    Standard(CompositeReader<MesFileReader>),
    Github(CompositeReader<CompositeReader<MesFileReader>>),
}

impl std::fmt::Debug for OrgChildReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Standard(_) => f.debug_tuple("Standard").finish(),
            Self::Github(_) => f.debug_tuple("Github").finish(),
        }
    }
}

impl FileReader for OrgChildReader {
    fn read(
        &self,
        offset: u64,
        size: u32,
    ) -> impl Future<Output = Result<bytes::Bytes, std::io::Error>> + Send {
        match self {
            Self::Standard(r) => futures::future::Either::Left(r.read(offset, size)),
            Self::Github(r) => futures::future::Either::Right(r.read(offset, size)),
        }
    }

    fn close(&self) -> impl Future<Output = Result<(), std::io::Error>> + Send {
        match self {
            Self::Standard(r) => futures::future::Either::Left(r.close()),
            Self::Github(r) => futures::future::Either::Right(r.close()),
        }
    }
}

pub(super) struct MesaRoot {
    orgs: Vec<(OsString, OrgChildDP)>,
}

impl MesaRoot {
    pub(super) fn new(orgs: Vec<(OsString, OrgChildDP)>) -> Self {
        Self { orgs }
    }
}

impl CompositeRoot for MesaRoot {
    type ChildDP = OrgChildDP;

    async fn resolve_child(
        &self,
        name: &OsStr,
    ) -> Result<Option<ChildDescriptor<Self::ChildDP>>, std::io::Error> {
        let found = self.orgs.iter().find(|(n, _)| n == name);
        match found {
            Some((_, dp)) => Ok(Some(ChildDescriptor {
                name: name.to_os_string(),
                provider: dp.clone(),
                root_ino: dp.make_root_inode(),
            })),
            None => Ok(None),
        }
    }

    async fn list_children(&self) -> Result<Vec<ChildDescriptor<Self::ChildDP>>, std::io::Error> {
        Ok(self
            .orgs
            .iter()
            .map(|(name, dp)| ChildDescriptor {
                name: name.clone(),
                provider: dp.clone(),
                root_ino: dp.make_root_inode(),
            })
            .collect())
    }
}
