use std::collections::HashMap;
use std::ffi::OsStr;
use std::future::Future;
use std::time::SystemTime;

use bytes::Bytes;
use mesa_dev::MesaClient;
use secrecy::ExposeSecret as _;
use tracing::{instrument, trace, warn};

use crate::fs::icache::bridge::HashMapBridge;
use crate::fs::icache::{AsyncICache, IcbResolver};
use crate::fs::r#trait::{
    DirEntry, DirEntryType, FileAttr, FileHandle, FilesystemStats, Fs, Inode, LockOwner, OpenFile,
    OpenFlags,
};

#[cfg(feature = "staging")]
const MESA_API_BASE_URL: &str = "https://staging.depot.mesa.dev/api/v1";
#[cfg(not(feature = "staging"))]
const MESA_API_BASE_URL: &str = "https://depot.mesa.dev/api/v1";

mod common;
use common::InodeControlBlock;
pub use common::{GetAttrError, LookupError, OpenError, ReadDirError, ReadError, ReleaseError};

use icache::MescloudICache;
use icache as mescloud_icache;

mod org;
pub use org::OrgConfig;
use org::OrgFs;

pub mod icache;
pub mod repo;

// ---------------------------------------------------------------------------
// MesaResolver
// ---------------------------------------------------------------------------

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
            });
            let now = SystemTime::now();
            let attr = FileAttr::Directory {
                common: mescloud_icache::make_common_file_attr(
                    ino, 0o755, now, now, fs_owner, block_size,
                ),
            };
            Ok(InodeControlBlock {
                attr: Some(attr),
                ..stub
            })
        }
    }
}

// ---------------------------------------------------------------------------
// MesaFS
// ---------------------------------------------------------------------------

/// Per-org wrapper with inode and file handle translation.
struct OrgSlot {
    org: OrgFs,
    bridge: HashMapBridge, // left = mesa, right = org
}

/// Classifies an inode by its role in the mesa hierarchy.
enum InodeRole {
    /// The filesystem root (ino == 1).
    Root,
    /// An inode owned by some org.
    OrgOwned { idx: usize },
}

/// The top-level `MesaFS` filesystem.
///
/// Composes multiple [`OrgFs`] instances, each with its own inode namespace,
/// using [`HashMapBridge`] for bidirectional inode/fh translation at each boundary.
pub struct MesaFS {
    icache: MescloudICache<MesaResolver>,
    readdir_buf: Vec<DirEntry>,

    /// Maps mesa-level org-root inodes → index into `org_slots`.
    org_inodes: HashMap<Inode, usize>,
    org_slots: Vec<OrgSlot>,
}

impl MesaFS {
    const ROOT_NODE_INO: Inode = 1;
    const BLOCK_SIZE: u32 = 4096;

    /// Create a new `MesaFS` instance.
    pub fn new(orgs: impl Iterator<Item = OrgConfig>, fs_owner: (u32, u32)) -> Self {
        let resolver = MesaResolver {
            fs_owner,
            block_size: Self::BLOCK_SIZE,
        };
        Self {
            icache: MescloudICache::new(resolver, Self::ROOT_NODE_INO, fs_owner, Self::BLOCK_SIZE),
            readdir_buf: Vec::new(),
            org_inodes: HashMap::new(),
            org_slots: orgs
                .map(|org_conf| {
                    let client = MesaClient::builder()
                        .with_api_key(org_conf.api_key.expose_secret())
                        .with_base_path(MESA_API_BASE_URL)
                        .build();
                    let org = OrgFs::new(org_conf.name, client, fs_owner);
                    OrgSlot {
                        org,
                        bridge: HashMapBridge::new(),
                    }
                })
                .collect(),
        }
    }

    /// Classify an inode by its role.
    async fn inode_role(&self, ino: Inode) -> InodeRole {
        if ino == Self::ROOT_NODE_INO {
            return InodeRole::Root;
        }
        if let Some(&idx) = self.org_inodes.get(&ino) {
            return InodeRole::OrgOwned { idx };
        }
        // Walk parent chain.
        if let Some(idx) = self.org_slot_for_inode(ino).await {
            return InodeRole::OrgOwned { idx };
        }
        debug_assert!(false, "inode {ino} not found in any org slot");
        InodeRole::Root
    }

    /// Find the org slot index that owns `ino` by walking the parent chain.
    async fn org_slot_for_inode(&self, ino: Inode) -> Option<usize> {
        if let Some(&idx) = self.org_inodes.get(&ino) {
            return Some(idx);
        }
        let mut current = ino;
        loop {
            let parent = self
                .icache
                .get_icb(current, |icb| icb.parent)
                .await
                .flatten()?;
            if let Some(&idx) = self.org_inodes.get(&parent) {
                return Some(idx);
            }
            current = parent;
        }
    }

    /// Ensure a mesa-level inode exists for the org at `org_idx`.
    /// Seeds the bridge with (`mesa_org_ino`, `OrgFs::ROOT_INO`).
    /// Does NOT bump rc.
    async fn ensure_org_inode(&mut self, org_idx: usize) -> (Inode, FileAttr) {
        // Check if an inode already exists.
        if let Some((&existing_ino, _)) = self.org_inodes.iter().find(|&(_, &idx)| idx == org_idx) {
            if let Some(attr) = self.icache.get_attr(existing_ino).await {
                let rc = self
                    .icache
                    .get_icb(existing_ino, |icb| icb.rc)
                    .await
                    .unwrap_or(0);
                trace!(
                    ino = existing_ino,
                    org_idx,
                    rc,
                    "ensure_org_inode: reusing existing inode"
                );
                return (existing_ino, attr);
            }
            // Attr missing — rebuild.
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
                    self.icache.fs_owner(),
                    self.icache.block_size(),
                ),
            };
            self.icache.cache_attr(existing_ino, attr).await;
            return (existing_ino, attr);
        }

        // Allocate new.
        let org_name = self.org_slots[org_idx].org.name().to_owned();
        let ino = self.icache.allocate_inode();
        trace!(ino, org_idx, org = %org_name, "ensure_org_inode: allocated new inode");

        let now = SystemTime::now();
        self.icache
            .insert_icb(
                ino,
                InodeControlBlock {
                    rc: 0,
                    path: org_name.as_str().into(),
                    parent: Some(Self::ROOT_NODE_INO),
                    attr: None,
                },
            )
            .await;

        self.org_inodes.insert(ino, org_idx);

        // Seed bridge: mesa org-root ↔ OrgFs::ROOT_INO.
        self.org_slots[org_idx]
            .bridge
            .insert_inode(ino, OrgFs::ROOT_INO);

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

    /// Allocate a mesa-level file handle and map it through the bridge.
    fn alloc_fh(&mut self, slot_idx: usize, org_fh: FileHandle) -> FileHandle {
        let fh = self.icache.allocate_fh();
        self.org_slots[slot_idx].bridge.insert_fh(fh, org_fh);
        fh
    }

    /// Translate an org inode to a mesa inode, allocating if needed.
    /// Also mirrors the ICB into the mesa `inode_table`.
    async fn translate_org_ino_to_mesa(
        &mut self,
        slot_idx: usize,
        org_ino: Inode,
        parent_mesa_ino: Inode,
        name: &OsStr,
    ) -> Inode {
        let mesa_ino = self.org_slots[slot_idx]
            .bridge
            .backward_or_insert_inode(org_ino, || self.icache.allocate_inode());

        self.icache
            .entry_or_insert_icb(
                mesa_ino,
                || InodeControlBlock {
                    rc: 0,
                    path: name.into(),
                    parent: Some(parent_mesa_ino),
                    attr: None,
                },
                |_| {},
            )
            .await;

        mesa_ino
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

    #[instrument(skip(self))]
    async fn lookup(&mut self, parent: Inode, name: &OsStr) -> Result<FileAttr, LookupError> {
        match self.inode_role(parent).await {
            InodeRole::Root => {
                // Children of root are orgs.
                let org_name = name.to_str().ok_or(LookupError::InodeNotFound)?;
                let org_idx = self
                    .org_slots
                    .iter()
                    .position(|s| s.org.name() == org_name)
                    .ok_or(LookupError::InodeNotFound)?;

                trace!(org = org_name, "lookup: matched org");
                let (ino, attr) = self.ensure_org_inode(org_idx).await;
                let rc = self.icache.inc_rc(ino).await;
                trace!(ino, org = org_name, rc, "lookup: resolved org inode");
                Ok(attr)
            }
            InodeRole::OrgOwned { idx } => {
                // Delegate to org.
                let org_parent = self.org_slots[idx]
                    .bridge
                    .forward_or_insert_inode(parent, || unreachable!("forward should find parent"));

                let org_attr = self.org_slots[idx].org.lookup(org_parent, name).await?;
                let org_ino = org_attr.common().ino;

                let mesa_ino =
                    self.translate_org_ino_to_mesa(idx, org_ino, parent, name).await;

                let mesa_attr = self.org_slots[idx].bridge.attr_backward(org_attr);
                self.icache.cache_attr(mesa_ino, mesa_attr).await;
                let rc = self.icache.inc_rc(mesa_ino).await;
                trace!(mesa_ino, org_ino, rc, "lookup: resolved via org delegation");
                Ok(mesa_attr)
            }
        }
    }

    #[instrument(skip(self))]
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

    #[instrument(skip(self))]
    async fn readdir(&mut self, ino: Inode) -> Result<&[DirEntry], ReadDirError> {
        match self.inode_role(ino).await {
            InodeRole::Root => {
                let org_info: Vec<(usize, String)> = self
                    .org_slots
                    .iter()
                    .enumerate()
                    .map(|(idx, s)| (idx, s.org.name().to_owned()))
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

                self.readdir_buf = entries;
                Ok(&self.readdir_buf)
            }
            InodeRole::OrgOwned { idx } => {
                let org_ino = self.org_slots[idx]
                    .bridge
                    .forward_or_insert_inode(ino, || unreachable!("readdir: ino should be mapped"));

                let org_entries = self.org_slots[idx].org.readdir(org_ino).await?;
                let org_entries: Vec<DirEntry> = org_entries.to_vec();

                let mut mesa_entries = Vec::with_capacity(org_entries.len());
                for entry in &org_entries {
                    let mesa_child_ino =
                        self.translate_org_ino_to_mesa(idx, entry.ino, ino, &entry.name).await;

                    // Cache attr from org if available.
                    if let Some(org_icb_attr) =
                        self.org_slots[idx].org.inode_table_get_attr(entry.ino).await
                    {
                        let mesa_attr = self.org_slots[idx].bridge.attr_backward(org_icb_attr);
                        self.icache.cache_attr(mesa_child_ino, mesa_attr).await;
                    }

                    mesa_entries.push(DirEntry {
                        ino: mesa_child_ino,
                        name: entry.name.clone(),
                        kind: entry.kind,
                    });
                }

                self.readdir_buf = mesa_entries;
                Ok(&self.readdir_buf)
            }
        }
    }

    #[instrument(skip(self))]
    async fn open(&mut self, ino: Inode, flags: OpenFlags) -> Result<OpenFile, OpenError> {
        let idx = self.org_slot_for_inode(ino).await.ok_or_else(|| {
            warn!(ino, "open on inode not belonging to any org");
            OpenError::InodeNotFound
        })?;

        let org_ino = self.org_slots[idx]
            .bridge
            .forward_or_insert_inode(ino, || unreachable!("open: ino should be mapped"));

        let org_open = self.org_slots[idx].org.open(org_ino, flags).await?;
        let mesa_fh = self.alloc_fh(idx, org_open.handle);

        trace!(
            ino,
            mesa_fh,
            org_fh = org_open.handle,
            "open: assigned file handle"
        );
        Ok(OpenFile {
            handle: mesa_fh,
            options: org_open.options,
        })
    }

    #[instrument(skip(self))]
    async fn read(
        &mut self,
        ino: Inode,
        fh: FileHandle,
        offset: u64,
        size: u32,
        flags: OpenFlags,
        lock_owner: Option<LockOwner>,
    ) -> Result<Bytes, ReadError> {
        let idx = self.org_slot_for_inode(ino).await.ok_or_else(|| {
            warn!(ino, "read on inode not belonging to any org");
            ReadError::InodeNotFound
        })?;

        let org_ino = self.org_slots[idx]
            .bridge
            .forward_or_insert_inode(ino, || unreachable!("read: ino should be mapped"));
        let org_fh = self.org_slots[idx].bridge.fh_forward(fh).ok_or_else(|| {
            warn!(fh, "read: no fh mapping found");
            ReadError::FileNotOpen
        })?;

        self.org_slots[idx]
            .org
            .read(org_ino, org_fh, offset, size, flags, lock_owner)
            .await
    }

    #[instrument(skip(self))]
    async fn release(
        &mut self,
        ino: Inode,
        fh: FileHandle,
        flags: OpenFlags,
        flush: bool,
    ) -> Result<(), ReleaseError> {
        let idx = self.org_slot_for_inode(ino).await.ok_or_else(|| {
            warn!(ino, "release on inode not belonging to any org");
            ReleaseError::FileNotOpen
        })?;

        let org_ino = self.org_slots[idx]
            .bridge
            .forward_or_insert_inode(ino, || unreachable!("release: ino should be mapped"));
        let org_fh = self.org_slots[idx].bridge.fh_forward(fh).ok_or_else(|| {
            warn!(fh, "release: no fh mapping found");
            ReleaseError::FileNotOpen
        })?;

        let result = self.org_slots[idx]
            .org
            .release(org_ino, org_fh, flags, flush)
            .await;

        self.org_slots[idx].bridge.remove_fh_by_left(fh);
        trace!(ino, fh, "release: cleaned up fh mapping");

        result
    }

    #[instrument(skip(self))]
    async fn forget(&mut self, ino: Inode, nlookups: u64) {
        // Propagate forget to inner org if applicable.
        if let Some(idx) = self.org_slot_for_inode(ino).await
            && let Some(&org_ino) = self.org_slots[idx].bridge.inode_map_get_by_left(ino)
        {
            self.org_slots[idx].org.forget(org_ino, nlookups).await;
        }

        if self.icache.forget(ino, nlookups).await.is_some() {
            self.org_inodes.remove(&ino);
            for slot in &mut self.org_slots {
                slot.bridge.remove_inode_by_left(ino);
            }
        }
    }

    async fn statfs(&mut self) -> Result<FilesystemStats, std::io::Error> {
        Ok(self.icache.statfs())
    }
}
