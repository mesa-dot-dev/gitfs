use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::ffi::OsStr;
use std::time::SystemTime;

use bytes::Bytes;
use futures::TryStreamExt as _;
use mesa_dev::Mesa as MesaClient;
use secrecy::SecretString;
use tracing::{instrument, trace, warn};

use super::common::{self, InodeControlBlock, InodeFactory};
pub use super::common::{
    GetAttrError, LookupError, OpenError, ReadDirError, ReadError, ReleaseError,
};
use super::repo::RepoFs;
use crate::fs::inode_bridge::HashMapBridge;
use crate::fs::r#trait::{
    DirEntry, DirEntryType, FileAttr, FileHandle, FilesystemStats, Fs, Inode, LockOwner, OpenFile,
    OpenFlags,
};

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
    fs_owner: (u32, u32),

    inode_table: HashMap<Inode, InodeControlBlock>,
    inode_factory: InodeFactory,
    next_fh: FileHandle,

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
    fn ensure_owner_inode(&mut self, owner: &str) -> (Inode, FileAttr) {
        // Check existing
        for (&ino, existing_owner) in &self.owner_inodes {
            if existing_owner == owner {
                if let Some(icb) = self.inode_table.get(&ino)
                    && let Some(attr) = icb.attr
                {
                    return (ino, attr);
                }
                let now = SystemTime::now();
                let attr = FileAttr::Directory {
                    common: common::make_common_file_attr(
                        self.fs_owner,
                        Self::BLOCK_SIZE,
                        ino,
                        0o755,
                        now,
                        now,
                    ),
                };
                common::cache_attr(&mut self.inode_table, ino, attr);
                return (ino, attr);
            }
        }

        // Allocate new
        let ino = self.inode_factory.allocate();
        let now = SystemTime::now();
        self.inode_table.insert(
            ino,
            InodeControlBlock {
                rc: 0,
                path: owner.into(),
                parent: Some(Self::ROOT_INO),
                children: None,
                attr: None,
            },
        );
        self.owner_inodes.insert(ino, owner.to_owned());
        let attr = FileAttr::Directory {
            common: common::make_common_file_attr(
                self.fs_owner,
                Self::BLOCK_SIZE,
                ino,
                0o755,
                now,
                now,
            ),
        };
        common::cache_attr(&mut self.inode_table, ino, attr);
        (ino, attr)
    }

    /// Get the cached attr for an inode, if present.
    pub(crate) fn inode_table_get_attr(&self, ino: Inode) -> Option<FileAttr> {
        self.inode_table.get(&ino).and_then(|icb| icb.attr)
    }

    pub fn new(name: String, client: MesaClient, fs_owner: (u32, u32)) -> Self {
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
            name,
            client,
            fs_owner,
            inode_table,
            inode_factory: InodeFactory::new(Self::ROOT_INO + 1),
            next_fh: 1,
            repo_inodes: HashMap::new(),
            owner_inodes: HashMap::new(),
            repos: Vec::new(),
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

    /// Classify an inode by its role.
    fn inode_role(&self, ino: Inode) -> InodeRole {
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
        if let Some(idx) = self.repo_slot_for_inode(ino) {
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
    fn repo_slot_for_inode(&self, ino: Inode) -> Option<usize> {
        // Direct repo root?
        if let Some(&idx) = self.repo_inodes.get(&ino) {
            return Some(idx);
        }
        // Walk parents.
        let mut current = ino;
        while let Some(parent) = self.inode_table.get(&current).and_then(|icb| icb.parent) {
            if let Some(&idx) = self.repo_inodes.get(&parent) {
                return Some(idx);
            }
            current = parent;
        }
        trace!(
            ino,
            "repo_slot_for_inode: exhausted parent chain without finding repo"
        );
        None
    }

    /// Ensure an inode + `RepoFs` exists for the given repo name.
    /// Does NOT bump rc.
    ///
    /// - `repo_name`: name used for API calls / `RepoFs` (base64-encoded for github)
    /// - `display_name`: name shown in filesystem ("linux" for github, same as `repo_name` otherwise)
    /// - `parent_ino`: owner-dir inode for github, `ROOT_INO` otherwise
    fn ensure_repo_inode(
        &mut self,
        repo_name: &str,
        display_name: &str,
        default_branch: &str,
        parent_ino: Inode,
    ) -> (Inode, FileAttr) {
        // Check existing repos.
        for (&ino, &idx) in &self.repo_inodes {
            if self.repos[idx].repo.repo_name() == repo_name {
                if let Some(icb) = self.inode_table.get(&ino)
                    && let Some(attr) = icb.attr
                {
                    trace!(
                        ino,
                        repo = repo_name,
                        rc = icb.rc,
                        "ensure_repo_inode: reusing"
                    );
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
                    common: common::make_common_file_attr(
                        self.fs_owner,
                        Self::BLOCK_SIZE,
                        ino,
                        0o755,
                        now,
                        now,
                    ),
                };
                common::cache_attr(&mut self.inode_table, ino, attr);
                return (ino, attr);
            }
        }

        // Allocate new.
        let ino = self.inode_factory.allocate();
        trace!(
            ino,
            repo = repo_name,
            "ensure_repo_inode: allocated new inode"
        );

        let now = SystemTime::now();
        self.inode_table.insert(
            ino,
            InodeControlBlock {
                rc: 0,
                path: display_name.into(),
                parent: Some(parent_ino),
                children: None,
                attr: None,
            },
        );

        let repo = RepoFs::new(
            self.client.clone(),
            self.name.clone(),
            repo_name.to_owned(),
            default_branch.to_owned(),
            self.fs_owner,
        );

        let mut bridge = HashMapBridge::new();
        bridge.insert_inode(ino, RepoFs::ROOT_INO);

        let idx = self.repos.len();
        self.repos.push(RepoSlot { repo, bridge });
        self.repo_inodes.insert(ino, idx);

        let attr = FileAttr::Directory {
            common: common::make_common_file_attr(
                self.fs_owner,
                Self::BLOCK_SIZE,
                ino,
                0o755,
                now,
                now,
            ),
        };
        common::cache_attr(&mut self.inode_table, ino, attr);
        (ino, attr)
    }

    /// Poll `repos().get()` until the repo is no longer syncing.
    async fn wait_for_sync(
        &self,
        repo_name: &str,
    ) -> Result<mesa_dev::models::Repo, mesa_dev::error::MesaError> {
        let mut repo = self.client.repos(&self.name).get(repo_name).await?;
        while repo.status.is_some() {
            trace!(repo = repo_name, "repo is syncing, waiting...");
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            repo = self.client.repos(&self.name).get(repo_name).await?;
        }
        Ok(repo)
    }

    /// Allocate an org-level file handle and map it through the bridge.
    fn alloc_fh(&mut self, slot_idx: usize, repo_fh: FileHandle) -> FileHandle {
        let fh = self.next_fh;
        self.next_fh += 1;
        self.repos[slot_idx].bridge.insert_fh(fh, repo_fh);
        fh
    }

    /// Translate a repo inode to an org inode, allocating if needed.
    /// Also mirrors the ICB into the org's `inode_table`.
    fn translate_repo_ino_to_org(
        &mut self,
        slot_idx: usize,
        repo_ino: Inode,
        parent_org_ino: Inode,
        name: &OsStr,
        _kind: DirEntryType,
    ) -> Inode {
        let org_ino = self.repos[slot_idx]
            .bridge
            .backward_or_insert_inode(repo_ino, || self.inode_factory.allocate());

        // Ensure there's an ICB in the org table.
        match self.inode_table.entry(org_ino) {
            Entry::Vacant(entry) => {
                trace!(
                    org_ino,
                    repo_ino,
                    parent = parent_org_ino,
                    ?name,
                    "translate: created new org ICB"
                );
                entry.insert(InodeControlBlock {
                    rc: 0,
                    path: name.into(),
                    parent: Some(parent_org_ino),
                    children: None,
                    attr: None,
                });
            }
            Entry::Occupied(_) => {
                trace!(org_ino, repo_ino, "translate: reused existing org ICB");
            }
        }

        org_ino
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
        debug_assert!(
            self.inode_table.contains_key(&parent),
            "lookup: parent inode {parent} not in inode table"
        );

        match self.inode_role(parent) {
            InodeRole::OrgRoot => {
                // TODO(MES-674): Cleanup "special" casing for github.
                let name_str = name.to_str().ok_or(LookupError::InodeNotFound)?;

                if self.is_github() {
                    // name is an owner like "torvalds" — create lazily, no API validation.
                    trace!(owner = name_str, "lookup: resolving github owner dir");
                    let (ino, attr) = self.ensure_owner_inode(name_str);
                    let icb = self
                        .inode_table
                        .get_mut(&ino)
                        .unwrap_or_else(|| unreachable!("inode {ino} was just ensured"));
                    icb.rc += 1;
                    Ok(attr)
                } else {
                    // Children of org root are repos.
                    trace!(repo = name_str, "lookup: resolving repo");

                    // Validate repo exists via API, waiting for sync if needed.
                    let repo = self.wait_for_sync(name_str).await?;

                    let (ino, attr) = self.ensure_repo_inode(
                        name_str,
                        name_str,
                        &repo.default_branch,
                        Self::ROOT_INO,
                    );
                    let icb = self
                        .inode_table
                        .get_mut(&ino)
                        .unwrap_or_else(|| unreachable!("inode {ino} was just ensured"));
                    icb.rc += 1;
                    trace!(
                        ino,
                        repo = name_str,
                        rc = icb.rc,
                        "lookup: resolved repo inode"
                    );
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

                // Validate via API (uses encoded name), waiting for sync if needed.
                let repo = self.wait_for_sync(&encoded).await?;

                let (ino, attr) =
                    self.ensure_repo_inode(&encoded, repo_name_str, &repo.default_branch, parent);
                let icb = self
                    .inode_table
                    .get_mut(&ino)
                    .unwrap_or_else(|| unreachable!("inode {ino} was just ensured"));
                icb.rc += 1;
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
                let org_ino = self.translate_repo_ino_to_org(idx, repo_ino, parent, name, kind);

                // Rebuild attr with org inode.
                let org_attr = self.repos[idx].bridge.attr_backward(repo_attr);
                common::cache_attr(&mut self.inode_table, org_ino, org_attr);
                let icb = self
                    .inode_table
                    .get_mut(&org_ino)
                    .unwrap_or_else(|| unreachable!("inode {org_ino} was just cached"));
                icb.rc += 1;
                trace!(
                    org_ino,
                    repo_ino,
                    rc = icb.rc,
                    "lookup: resolved content inode"
                );
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
        let icb = self.inode_table.get(&ino).ok_or_else(|| {
            warn!(ino, "getattr on unknown inode");
            GetAttrError::InodeNotFound
        })?;
        icb.attr.ok_or_else(|| {
            warn!(ino, "getattr on inode with no cached attr");
            GetAttrError::InodeNotFound
        })
    }

    #[instrument(skip(self), fields(org = %self.name))]
    async fn readdir(&mut self, ino: Inode) -> Result<&[DirEntry], ReadDirError> {
        debug_assert!(
            self.inode_table.contains_key(&ino),
            "readdir: inode {ino} not in inode table"
        );

        match self.inode_role(ino) {
            InodeRole::OrgRoot => {
                // TODO(MES-674): Cleanup "special" casing for github.
                if self.is_github() {
                    return Err(ReadDirError::NotPermitted);
                }

                // List repos via API.
                let repos: Vec<mesa_dev::models::Repo> = self
                    .client
                    .repos(&self.name)
                    .list_all()
                    .try_collect()
                    .await?;

                let repo_infos: Vec<(String, String)> = repos
                    .into_iter()
                    .filter(|r| r.status.is_none()) // skip repos still syncing
                    .map(|r| (r.name, r.default_branch))
                    .collect();
                trace!(count = repo_infos.len(), "readdir: fetched repo list");

                let mut entries = Vec::with_capacity(repo_infos.len());
                for (repo_name, default_branch) in &repo_infos {
                    let (repo_ino, _) = self.ensure_repo_inode(
                        repo_name,
                        repo_name,
                        default_branch,
                        Self::ROOT_INO,
                    );
                    entries.push(DirEntry {
                        ino: repo_ino,
                        name: repo_name.clone().into(),
                        kind: DirEntryType::Directory,
                    });
                }

                let icb = self
                    .inode_table
                    .get_mut(&ino)
                    .ok_or(ReadDirError::InodeNotFound)?;
                Ok(icb.children.insert(entries))
            }
            InodeRole::OwnerDir if self.is_github() => {
                // TODO(MES-674): Cleanup "special" casing for github.
                return Err(ReadDirError::NotPermitted);
            }
            InodeRole::OwnerDir => {
                return Err(ReadDirError::NotADirectory);
            }
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
                    let org_child_ino = self.translate_repo_ino_to_org(
                        idx,
                        entry.ino,
                        ino,
                        &entry.name,
                        entry.kind,
                    );

                    // Cache attr from repo if available.
                    if let Some(repo_icb_attr) =
                        self.repos[idx].repo.inode_table_get_attr(entry.ino)
                    {
                        let org_attr = self.repos[idx].bridge.attr_backward(repo_icb_attr);
                        common::cache_attr(&mut self.inode_table, org_child_ino, org_attr);
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

                let icb = self
                    .inode_table
                    .get_mut(&ino)
                    .ok_or(ReadDirError::InodeNotFound)?;
                Ok(icb.children.insert(org_entries))
            }
        }
    }

    #[instrument(skip(self), fields(org = %self.name))]
    async fn open(&mut self, ino: Inode, flags: OpenFlags) -> Result<OpenFile, OpenError> {
        let idx = self.repo_slot_for_inode(ino).ok_or_else(|| {
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
        let idx = self.repo_slot_for_inode(ino).ok_or_else(|| {
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
        let idx = self.repo_slot_for_inode(ino).ok_or_else(|| {
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
        debug_assert!(
            self.inode_table.contains_key(&ino),
            "forget: inode {ino} not in inode table"
        );

        // Propagate forget to inner repo if applicable.
        if let Some(idx) = self.repo_slot_for_inode(ino) {
            if let Some(&repo_ino) = self.repos[idx].bridge.inode_map_get_by_left(ino) {
                self.repos[idx].repo.forget(repo_ino, nlookups).await;
            } else {
                trace!(
                    ino,
                    "forget: no bridge mapping found, skipping repo propagation"
                );
            }
        }

        match self.inode_table.entry(ino) {
            Entry::Occupied(mut entry) => {
                if entry.get().rc <= nlookups {
                    trace!(ino, "evicting inode");
                    entry.remove();
                    // Clean up repo_inodes and owner_inodes mappings.
                    self.repo_inodes.remove(&ino);
                    self.owner_inodes.remove(&ino);
                    // Clean up bridge mapping — find which slot, remove.
                    for slot in &mut self.repos {
                        slot.bridge.remove_inode_by_left(ino);
                    }
                } else {
                    entry.get_mut().rc -= nlookups;
                    trace!(ino, new_rc = entry.get().rc, "forget: decremented rc");
                }
            }
            Entry::Vacant(_) => {
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
