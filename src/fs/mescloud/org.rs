use std::collections::HashMap;
use std::ffi::OsStr;
use std::future::Future;
use std::time::SystemTime;

use bytes::Bytes;
use futures::TryStreamExt as _;
use mesa_dev::MesaClient;
use secrecy::SecretString;
use tracing::{instrument, trace, warn};

use super::common::InodeCachePeek as _;
pub use super::common::{
    GetAttrError, LookupError, OpenError, ReadDirError, ReadError, ReleaseError,
};
use super::common::{InodeControlBlock, MesaApiError};
use super::icache as mescloud_icache;
use super::icache::MescloudICache;
use super::repo::RepoFs;
use crate::fs::icache::bridge::HashMapBridge;
use crate::fs::icache::{AsyncICache, FileTable, IcbResolver};
use crate::fs::r#trait::{
    DirEntry, DirEntryType, FileAttr, FileHandle, FilesystemStats, Fs, Inode, LockOwner, OpenFile,
    OpenFlags,
};

// ---------------------------------------------------------------------------
// OrgResolver
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// OrgFs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct OrgConfig {
    pub name: String,
    pub api_key: SecretString,
}

/// Per-repo wrapper with inode and file handle translation.
struct RepoSlot {
    repo: RepoFs,
    bridge: HashMapBridge, // left = org, right = repo
}

/// Classifies an inode by its role in the org hierarchy.
enum InodeRole {
    /// The org root directory.
    OrgRoot,
    /// A virtual owner directory (github only).
    OwnerDir,
    /// An inode owned by some repo.
    RepoOwned { idx: usize },
}

/// A filesystem rooted at a single organization.
///
/// Owns multiple [`RepoFs`] instances and translates inodes between its namespace
/// and each repo's namespace using [`HashMapBridge`].
pub struct OrgFs {
    name: String,
    client: MesaClient,

    icache: MescloudICache<OrgResolver>,
    file_table: FileTable,
    readdir_buf: Vec<DirEntry>,

    /// Maps org-level repo-root inodes → index into `repos`.
    repo_inodes: HashMap<Inode, usize>,
    /// Maps org-level owner-dir inodes → owner name.
    /// Only populated when org name is "github".
    owner_inodes: HashMap<Inode, String>,
    repos: Vec<RepoSlot>,
}

impl OrgFs {
    pub(crate) const ROOT_INO: Inode = 1;
    const BLOCK_SIZE: u32 = 4096;

    /// The name of the organization.
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// Whether this org uses the github two-level owner/repo hierarchy.
    /// TODO(MES-674): Cleanup "special" casing for github.
    fn is_github(&self) -> bool {
        self.name == "github"
    }

    /// Decode a base64-encoded repo name from the API. Returns "owner/repo".
    /// TODO(MES-674): Cleanup "special" casing for github.
    #[expect(dead_code)]
    fn decode_github_repo_name(encoded: &str) -> Option<String> {
        use base64::Engine as _;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .ok()?;
        let decoded = String::from_utf8(bytes).ok()?;
        decoded.contains('/').then_some(decoded)
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
                if let Some(attr) = self.icache.get_attr(ino).await {
                    return (ino, attr);
                }
                let now = SystemTime::now();
                let attr = FileAttr::Directory {
                    common: mescloud_icache::make_common_file_attr(
                        ino,
                        0o755,
                        now,
                        now,
                        self.icache.fs_owner(),
                        self.icache.block_size(),
                    ),
                };
                self.icache.cache_attr(ino, attr).await;
                return (ino, attr);
            }
        }

        // Allocate new
        let ino = self.icache.allocate_inode();
        let now = SystemTime::now();
        self.icache
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
                self.icache.fs_owner(),
                self.icache.block_size(),
            ),
        };
        self.icache.cache_attr(ino, attr).await;
        (ino, attr)
    }

    pub fn new(name: String, client: MesaClient, fs_owner: (u32, u32)) -> Self {
        let resolver = OrgResolver {
            fs_owner,
            block_size: Self::BLOCK_SIZE,
        };
        Self {
            name,
            client,
            icache: MescloudICache::new(resolver, Self::ROOT_INO, fs_owner, Self::BLOCK_SIZE),
            file_table: FileTable::new(),
            readdir_buf: Vec::new(),
            repo_inodes: HashMap::new(),
            owner_inodes: HashMap::new(),
            repos: Vec::new(),
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
        if let Some(&idx) = self.repo_inodes.get(&ino) {
            return InodeRole::RepoOwned { idx };
        }
        // Walk parent chain to find owning repo.
        if let Some(idx) = self.repo_slot_for_inode(ino).await {
            return InodeRole::RepoOwned { idx };
        }
        // Shouldn't happen — all non-root inodes should be repo-owned.
        trace!(
            ino,
            "inode_role: inode not found in any repo slot, falling back to OrgRoot"
        );
        debug_assert!(false, "inode {ino} not found in any repo slot");
        InodeRole::OrgRoot
    }

    /// Find the repo slot index that owns `ino` by walking the parent chain.
    async fn repo_slot_for_inode(&self, ino: Inode) -> Option<usize> {
        // Direct repo root?
        if let Some(&idx) = self.repo_inodes.get(&ino) {
            return Some(idx);
        }
        // Walk parents.
        let mut current = ino;
        loop {
            let parent = self
                .icache
                .get_icb(current, |icb| icb.parent)
                .await
                .flatten()?;
            if let Some(&idx) = self.repo_inodes.get(&parent) {
                return Some(idx);
            }
            current = parent;
        }
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
        for (&ino, &idx) in &self.repo_inodes {
            if self.repos[idx].repo.repo_name() == repo_name {
                if let Some(attr) = self.icache.get_attr(ino).await {
                    let rc = self.icache.get_icb(ino, |icb| icb.rc).await.unwrap_or(0);
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
                        self.icache.fs_owner(),
                        self.icache.block_size(),
                    ),
                };
                self.icache.cache_attr(ino, attr).await;
                return (ino, attr);
            }
        }

        // Allocate new.
        let ino = self.icache.allocate_inode();
        trace!(
            ino,
            repo = repo_name,
            "ensure_repo_inode: allocated new inode"
        );

        let now = SystemTime::now();
        self.icache
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
            self.icache.fs_owner(),
        );

        let mut bridge = HashMapBridge::new();
        bridge.insert_inode(ino, RepoFs::ROOT_INO);

        let idx = self.repos.len();
        self.repos.push(RepoSlot { repo, bridge });
        self.repo_inodes.insert(ino, idx);

        let attr = FileAttr::Directory {
            common: mescloud_icache::make_common_file_attr(
                ino,
                0o755,
                now,
                now,
                self.icache.fs_owner(),
                self.icache.block_size(),
            ),
        };
        self.icache.cache_attr(ino, attr).await;
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

    /// Allocate an org-level file handle and map it through the bridge.
    fn alloc_fh(&mut self, slot_idx: usize, repo_fh: FileHandle) -> FileHandle {
        let fh = self.file_table.allocate();
        self.repos[slot_idx].bridge.insert_fh(fh, repo_fh);
        fh
    }

    /// Translate a repo inode to an org inode, allocating if needed.
    /// Also mirrors the ICB into the org's inode table.
    async fn translate_repo_ino_to_org(
        &mut self,
        slot_idx: usize,
        repo_ino: Inode,
        parent_org_ino: Inode,
        name: &OsStr,
        _kind: DirEntryType,
    ) -> Inode {
        let org_ino = self.repos[slot_idx]
            .bridge
            .backward_or_insert_inode(repo_ino, || self.icache.allocate_inode());

        self.icache
            .entry_or_insert_icb(
                org_ino,
                || {
                    trace!(
                        org_ino,
                        repo_ino,
                        parent = parent_org_ino,
                        ?name,
                        "translate: created new org ICB"
                    );
                    InodeControlBlock {
                        rc: 0,
                        path: name.into(),
                        parent: Some(parent_org_ino),
                        attr: None,
                        children: None,
                    }
                },
                |icb| {
                    if icb.rc > 0 || icb.attr.is_some() {
                        trace!(org_ino, repo_ino, "translate: reused existing org ICB");
                    }
                },
            )
            .await;

        org_ino
    }
}

#[async_trait::async_trait]
impl super::common::InodeCachePeek for OrgFs {
    async fn peek_attr(&self, ino: Inode) -> Option<FileAttr> {
        self.icache.get_attr(ino).await
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
                    self.icache.inc_rc(ino).await;
                    Ok(attr)
                } else {
                    // Children of org root are repos.
                    trace!(repo = name_str, "lookup: resolving repo");

                    // Validate repo exists via API.
                    let repo = self.wait_for_sync(name_str).await?;

                    let (ino, attr) = self
                        .ensure_repo_inode(name_str, name_str, &repo.default_branch, Self::ROOT_INO)
                        .await;
                    let rc = self.icache.inc_rc(ino).await;
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
                self.icache.inc_rc(ino).await;
                Ok(attr)
            }
            InodeRole::RepoOwned { idx } => {
                // Delegate to repo.
                let repo_parent = self.repos[idx]
                    .bridge
                    .forward_or_insert_inode(parent, || unreachable!("forward should find parent"));
                // ^ forward should always find parent since it was previously mapped.
                // Using forward_or_insert just for safety, but the allocate closure should never run.

                let repo_attr = self.repos[idx].repo.lookup(repo_parent, name).await?;
                let repo_ino = repo_attr.common().ino;

                // Translate back to org namespace.
                let kind: DirEntryType = repo_attr.into();
                let org_ino = self
                    .translate_repo_ino_to_org(idx, repo_ino, parent, name, kind)
                    .await;

                // Rebuild attr with org inode.
                let org_attr = self.repos[idx].bridge.attr_backward(repo_attr);
                self.icache.cache_attr(org_ino, org_attr).await;
                let rc = self.icache.inc_rc(org_ino).await;
                trace!(org_ino, repo_ino, rc, "lookup: resolved content inode");
                Ok(org_attr)
            }
        }
    }

    #[instrument(skip(self), fields(org = %self.name))]
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

                self.readdir_buf = entries;
                Ok(&self.readdir_buf)
            }
            InodeRole::OwnerDir if self.is_github() => {
                // TODO(MES-674): Cleanup "special" casing for github.
                Err(ReadDirError::NotPermitted)
            }
            InodeRole::OwnerDir => Err(ReadDirError::NotADirectory),
            InodeRole::RepoOwned { idx } => {
                // Delegate to repo.
                let repo_ino = self.repos[idx]
                    .bridge
                    .forward_or_insert_inode(ino, || unreachable!("readdir: ino should be mapped"));

                let repo_entries = self.repos[idx].repo.readdir(repo_ino).await?;
                // Clone entries to release borrow on repo before mutating self.
                let repo_entries: Vec<DirEntry> = repo_entries.to_vec();

                let mut org_entries = Vec::with_capacity(repo_entries.len());
                for entry in &repo_entries {
                    let org_child_ino = self
                        .translate_repo_ino_to_org(idx, entry.ino, ino, &entry.name, entry.kind)
                        .await;

                    // Cache attr from repo if available.
                    if let Some(repo_icb_attr) = self.repos[idx].repo.peek_attr(entry.ino).await {
                        let org_attr = self.repos[idx].bridge.attr_backward(repo_icb_attr);
                        self.icache.cache_attr(org_child_ino, org_attr).await;
                    } else {
                        trace!(
                            repo_ino = entry.ino,
                            org_ino = org_child_ino,
                            "readdir: no cached attr from repo to propagate"
                        );
                    }

                    org_entries.push(DirEntry {
                        ino: org_child_ino,
                        name: entry.name.clone(),
                        kind: entry.kind,
                    });
                }

                self.readdir_buf = org_entries;
                Ok(&self.readdir_buf)
            }
        }
    }

    #[instrument(skip(self), fields(org = %self.name))]
    async fn open(&mut self, ino: Inode, flags: OpenFlags) -> Result<OpenFile, OpenError> {
        let idx = self.repo_slot_for_inode(ino).await.ok_or_else(|| {
            warn!(ino, "open on inode not belonging to any repo");
            OpenError::InodeNotFound
        })?;

        let repo_ino = self.repos[idx]
            .bridge
            .forward_or_insert_inode(ino, || unreachable!("open: ino should be mapped"));

        let repo_open = self.repos[idx].repo.open(repo_ino, flags).await?;
        let org_fh = self.alloc_fh(idx, repo_open.handle);

        trace!(
            ino,
            org_fh,
            repo_fh = repo_open.handle,
            "open: assigned file handle"
        );
        Ok(OpenFile {
            handle: org_fh,
            options: repo_open.options,
        })
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
        let idx = self.repo_slot_for_inode(ino).await.ok_or_else(|| {
            warn!(ino, "read on inode not belonging to any repo");
            ReadError::InodeNotFound
        })?;

        let repo_ino = self.repos[idx]
            .bridge
            .forward_or_insert_inode(ino, || unreachable!("read: ino should be mapped"));
        let repo_fh = self.repos[idx].bridge.fh_forward(fh).ok_or_else(|| {
            warn!(fh, "read: no fh mapping found");
            ReadError::FileNotOpen
        })?;

        trace!(
            ino,
            fh, repo_ino, repo_fh, offset, size, "read: delegating to repo"
        );
        self.repos[idx]
            .repo
            .read(repo_ino, repo_fh, offset, size, flags, lock_owner)
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
        let idx = self.repo_slot_for_inode(ino).await.ok_or_else(|| {
            warn!(ino, "release on inode not belonging to any repo");
            ReleaseError::FileNotOpen
        })?;

        let repo_ino = self.repos[idx]
            .bridge
            .forward_or_insert_inode(ino, || unreachable!("release: ino should be mapped"));
        let repo_fh = self.repos[idx].bridge.fh_forward(fh).ok_or_else(|| {
            warn!(fh, "release: no fh mapping found");
            ReleaseError::FileNotOpen
        })?;

        trace!(ino, fh, repo_ino, repo_fh, "release: delegating to repo");
        let result = self.repos[idx]
            .repo
            .release(repo_ino, repo_fh, flags, flush)
            .await;

        // Clean up fh mapping.
        self.repos[idx].bridge.remove_fh_by_left(fh);
        trace!(ino, fh, "release: cleaned up fh mapping");

        result
    }

    #[instrument(skip(self), fields(org = %self.name))]
    async fn forget(&mut self, ino: Inode, nlookups: u64) {
        // Propagate forget to inner repo if applicable.
        if let Some(idx) = self.repo_slot_for_inode(ino).await {
            if let Some(&repo_ino) = self.repos[idx].bridge.inode_map_get_by_left(ino) {
                self.repos[idx].repo.forget(repo_ino, nlookups).await;
            } else {
                trace!(
                    ino,
                    "forget: no bridge mapping found, skipping repo propagation"
                );
            }
        }

        if self.icache.forget(ino, nlookups).await.is_some() {
            // Clean up repo_inodes and owner_inodes mappings.
            self.repo_inodes.remove(&ino);
            self.owner_inodes.remove(&ino);
            // Clean up bridge mapping — find which slot, remove.
            for slot in &mut self.repos {
                slot.bridge.remove_inode_by_left(ino);
            }
        }
    }

    async fn statfs(&mut self) -> Result<FilesystemStats, std::io::Error> {
        Ok(self.icache.statfs())
    }
}
