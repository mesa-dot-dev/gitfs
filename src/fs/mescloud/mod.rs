use std::collections::HashMap;
use std::ffi::OsStr;
use std::future::Future;
use std::time::SystemTime;

use bytes::Bytes;
use mesa_dev::MesaClient;
use secrecy::ExposeSecret as _;
use tracing::{Instrument as _, instrument, trace, warn};

use crate::fs::icache::bridge::HashMapBridge;
use crate::fs::icache::{AsyncICache, FileTable, IcbResolver};
use crate::fs::r#trait::{
    DirEntry, DirEntryType, FileAttr, FileHandle, FilesystemStats, Fs, Inode, LockOwner, OpenFile,
    OpenFlags,
};

use composite::{ChildSlot, CompositeFs};

#[cfg(feature = "staging")]
const MESA_API_BASE_URL: &str = "https://staging.depot.mesa.dev/api/v1";
#[cfg(not(feature = "staging"))]
const MESA_API_BASE_URL: &str = "https://depot.mesa.dev/api/v1";

mod common;
mod composite;
use common::InodeControlBlock;
pub use common::{GetAttrError, LookupError, OpenError, ReadDirError, ReadError, ReleaseError};

use icache as mescloud_icache;
use icache::MescloudICache;

mod org;
pub use org::OrgConfig;
use org::OrgFs;

pub mod icache;
pub mod repo;

struct MesaResolver {
    fs_owner: (u32, u32),
    block_size: u32,
}

impl IcbResolver for MesaResolver {
    type Icb = InodeControlBlock;
    type Error = std::convert::Infallible;

    fn resolve(
        &self,
        ino: Inode,
        stub: Option<InodeControlBlock>,
        _cache: &AsyncICache<Self>,
    ) -> impl Future<Output = Result<InodeControlBlock, std::convert::Infallible>> + Send
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
        .instrument(tracing::info_span!("MesaResolver::resolve", ino))
    }
}

/// Classifies an inode by its role in the mesa hierarchy.
enum InodeRole {
    /// The filesystem root (ino == 1).
    Root,
    /// An inode owned by some org.
    OrgOwned,
}

/// The top-level `MesaFS` filesystem.
///
/// Composes multiple [`OrgFs`] instances, each with its own inode namespace,
/// delegating to [`CompositeFs`] for inode/fh translation at each boundary.
pub struct MesaFS {
    composite: CompositeFs<MesaResolver, OrgFs>,
}

impl MesaFS {
    const ROOT_NODE_INO: Inode = 1;
    const BLOCK_SIZE: u32 = 4096;

    /// Create a new `MesaFS` instance.
    #[must_use]
    pub fn new(orgs: impl Iterator<Item = OrgConfig>, fs_owner: (u32, u32)) -> Self {
        let resolver = MesaResolver {
            fs_owner,
            block_size: Self::BLOCK_SIZE,
        };
        Self {
            composite: CompositeFs {
                icache: MescloudICache::new(
                    resolver,
                    Self::ROOT_NODE_INO,
                    fs_owner,
                    Self::BLOCK_SIZE,
                ),
                file_table: FileTable::new(),
                readdir_buf: Vec::new(),
                child_inodes: HashMap::new(),
                inode_to_slot: HashMap::new(),
                slots: orgs
                    .map(|org_conf| {
                        let client = MesaClient::builder()
                            .with_api_key(org_conf.api_key.expose_secret())
                            .with_base_path(MESA_API_BASE_URL)
                            .build();
                        let org = OrgFs::new(org_conf.name, client, fs_owner);
                        ChildSlot {
                            inner: org,
                            bridge: HashMapBridge::new(),
                        }
                    })
                    .collect(),
            },
        }
    }

    /// Classify an inode by its role.
    fn inode_role(&self, ino: Inode) -> Option<InodeRole> {
        if ino == Self::ROOT_NODE_INO {
            return Some(InodeRole::Root);
        }
        if self.composite.child_inodes.contains_key(&ino) {
            return Some(InodeRole::OrgOwned);
        }
        if self.composite.slot_for_inode(ino).is_some() {
            return Some(InodeRole::OrgOwned);
        }
        None
    }

    /// Ensure a mesa-level inode exists for the org at `org_idx`.
    /// Seeds the bridge with (`mesa_org_ino`, `OrgFs::ROOT_INO`).
    /// Does NOT bump rc.
    async fn ensure_org_inode(&mut self, org_idx: usize) -> (Inode, FileAttr) {
        // Check if an inode already exists.
        let existing_ino = self
            .composite
            .child_inodes
            .iter()
            .find(|&(_, &idx)| idx == org_idx)
            .map(|(&ino, _)| ino);

        if let Some(existing_ino) = existing_ino {
            if let Some(attr) = self.composite.icache.get_attr(existing_ino).await {
                let rc = self
                    .composite
                    .icache
                    .get_icb(existing_ino, |icb| icb.rc)
                    .await
                    .unwrap_or(0);
                trace!(
                    ino = existing_ino,
                    org_idx, rc, "ensure_org_inode: reusing existing inode"
                );
                return (existing_ino, attr);
            }
            if self.composite.icache.contains(existing_ino) {
                // ICB exists but attr missing — rebuild and cache.
                warn!(
                    ino = existing_ino,
                    org_idx, "ensure_org_inode: attr missing, rebuilding"
                );
                let now = SystemTime::now();
                let attr = FileAttr::Directory {
                    common: mescloud_icache::make_common_file_attr(
                        existing_ino,
                        0o755,
                        now,
                        now,
                        self.composite.icache.fs_owner(),
                        self.composite.icache.block_size(),
                    ),
                };
                self.composite.icache.cache_attr(existing_ino, attr).await;
                return (existing_ino, attr);
            }
            // ICB was evicted — clean up stale tracking entries.
            warn!(
                ino = existing_ino,
                org_idx, "ensure_org_inode: ICB evicted, cleaning up stale entry"
            );
            self.composite.child_inodes.remove(&existing_ino);
            self.composite.inode_to_slot.remove(&existing_ino);
        }

        // Allocate new.
        let org_name = self.composite.slots[org_idx].inner.name().to_owned();
        let ino = self.composite.icache.allocate_inode();
        trace!(ino, org_idx, org = %org_name, "ensure_org_inode: allocated new inode");

        let now = SystemTime::now();
        self.composite
            .icache
            .insert_icb(
                ino,
                InodeControlBlock {
                    rc: 0,
                    path: org_name.as_str().into(),
                    parent: Some(Self::ROOT_NODE_INO),
                    attr: None,
                    children: None,
                },
            )
            .await;

        self.composite.child_inodes.insert(ino, org_idx);
        self.composite.inode_to_slot.insert(ino, org_idx);

        // Reset bridge (may have stale mappings from a previous eviction cycle)
        // and seed: mesa org-root <-> OrgFs::ROOT_INO.
        self.composite.slots[org_idx].bridge = HashMapBridge::new();
        self.composite.slots[org_idx]
            .bridge
            .insert_inode(ino, OrgFs::ROOT_INO);

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
}

#[async_trait::async_trait]
impl Fs for MesaFS {
    type LookupError = LookupError;
    type GetAttrError = GetAttrError;
    type OpenError = OpenError;
    type ReadError = ReadError;
    type ReaddirError = ReadDirError;
    type ReleaseError = ReleaseError;

    #[instrument(name = "MesaFS::lookup", skip(self))]
    async fn lookup(&mut self, parent: Inode, name: &OsStr) -> Result<FileAttr, LookupError> {
        let role = self.inode_role(parent).ok_or(LookupError::InodeNotFound)?;
        match role {
            InodeRole::Root => {
                let org_name = name.to_str().ok_or(LookupError::InodeNotFound)?;
                let org_idx = self
                    .composite
                    .slots
                    .iter()
                    .position(|s| s.inner.name() == org_name)
                    .ok_or(LookupError::InodeNotFound)?;

                trace!(org = org_name, "lookup: matched org");
                let (ino, attr) = self.ensure_org_inode(org_idx).await;
                let rc = self
                    .composite
                    .icache
                    .inc_rc(ino)
                    .await
                    .ok_or(LookupError::InodeNotFound)?;
                trace!(ino, org = org_name, rc, "lookup: resolved org inode");
                Ok(attr)
            }
            InodeRole::OrgOwned => self.composite.delegated_lookup(parent, name).await,
        }
    }

    #[instrument(name = "MesaFS::getattr", skip(self))]
    async fn getattr(
        &mut self,
        ino: Inode,
        _fh: Option<FileHandle>,
    ) -> Result<FileAttr, GetAttrError> {
        self.composite.delegated_getattr(ino).await
    }

    #[instrument(name = "MesaFS::readdir", skip(self))]
    async fn readdir(&mut self, ino: Inode) -> Result<&[DirEntry], ReadDirError> {
        let role = self.inode_role(ino).ok_or(ReadDirError::InodeNotFound)?;
        match role {
            InodeRole::Root => {
                let org_info: Vec<(usize, String)> = self
                    .composite
                    .slots
                    .iter()
                    .enumerate()
                    .map(|(idx, s)| (idx, s.inner.name().to_owned()))
                    .collect();

                let mut entries = Vec::with_capacity(org_info.len());
                for (org_idx, name) in &org_info {
                    let (org_ino, _) = self.ensure_org_inode(*org_idx).await;
                    entries.push(DirEntry {
                        ino: org_ino,
                        name: name.clone().into(),
                        kind: DirEntryType::Directory,
                    });
                }

                trace!(entry_count = entries.len(), "readdir: listing orgs");
                self.composite.readdir_buf = entries;
                Ok(&self.composite.readdir_buf)
            }
            InodeRole::OrgOwned => self.composite.delegated_readdir(ino).await,
        }
    }

    #[instrument(name = "MesaFS::open", skip(self))]
    async fn open(&mut self, ino: Inode, flags: OpenFlags) -> Result<OpenFile, OpenError> {
        self.composite.delegated_open(ino, flags).await
    }

    #[instrument(name = "MesaFS::read", skip(self))]
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

    #[instrument(name = "MesaFS::release", skip(self))]
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

    #[instrument(name = "MesaFS::forget", skip(self))]
    async fn forget(&mut self, ino: Inode, nlookups: u64) {
        // MesaFS has no extra state to clean up on eviction (unlike OrgFs::owner_inodes).
        let _ = self.composite.delegated_forget(ino, nlookups).await;
    }

    async fn statfs(&mut self) -> Result<FilesystemStats, std::io::Error> {
        Ok(self.composite.delegated_statfs())
    }
}
