use std::collections::HashMap;
use std::ffi::OsStr;
use std::time::SystemTime;

use bytes::Bytes;
use mesa_dev::Mesa as MesaClient;
use secrecy::ExposeSecret as _;
use tracing::{instrument, trace, warn};

use crate::fs::inode_bridge::HashMapBridge;
use crate::fs::r#trait::{
    DirEntry, DirEntryType, FileAttr, FileHandle, FilesystemStats, Fs, Inode, LockOwner, OpenFile,
    OpenFlags,
};

#[cfg(feature = "staging")]
const MESA_API_BASE_URL: &str = "https://staging.depot.mesa.dev/api/v1";
#[cfg(not(feature = "staging"))]
const MESA_API_BASE_URL: &str = "https://depot.mesa.dev/api/v1";

mod common;
pub use common::{GetAttrError, LookupError, OpenError, ReadDirError, ReadError, ReleaseError};
use common::{InodeControlBlock, InodeFactory};

mod dcache;

mod org;
pub use org::OrgConfig;
use org::OrgFs;

pub mod repo;

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
    fs_owner: (u32, u32),

    inode_table: HashMap<Inode, InodeControlBlock>,
    inode_factory: InodeFactory,
    next_fh: FileHandle,

    /// Maps mesa-level org-root inodes → index into `org_slots`.
    org_inodes: HashMap<Inode, usize>,
    org_slots: Vec<OrgSlot>,
}

impl MesaFS {
    const ROOT_NODE_INO: Inode = 1;
    const BLOCK_SIZE: u32 = 4096;

    /// Create a new `MesaFS` instance.
    pub fn new(orgs: impl Iterator<Item = OrgConfig>, fs_owner: (u32, u32)) -> Self {
        let now = SystemTime::now();

        let mut inode_table = HashMap::new();
        inode_table.insert(
            Self::ROOT_NODE_INO,
            InodeControlBlock {
                rc: 1,
                parent: None,
                path: "/".into(),
                children: None,
                attr: None,
            },
        );

        let mut fs = Self {
            inode_table,
            org_inodes: HashMap::new(),
            org_slots: orgs
                .map(|org_conf| {
                    let client = MesaClient::builder(org_conf.api_key.expose_secret())
                        .base_url(MESA_API_BASE_URL)
                        .build();
                    let org = OrgFs::new(org_conf.name, client, fs_owner);
                    OrgSlot {
                        org,
                        bridge: HashMapBridge::new(),
                    }
                })
                .collect(),
            inode_factory: InodeFactory::new(Self::ROOT_NODE_INO + 1),
            fs_owner,
            next_fh: 1,
        };

        let root_attr = FileAttr::Directory {
            common: common::make_common_file_attr(
                fs.fs_owner,
                Self::BLOCK_SIZE,
                Self::ROOT_NODE_INO,
                0o755,
                now,
                now,
            ),
        };
        common::cache_attr(&mut fs.inode_table, Self::ROOT_NODE_INO, root_attr);

        fs
    }

    /// Classify an inode by its role.
    fn inode_role(&self, ino: Inode) -> InodeRole {
        if ino == Self::ROOT_NODE_INO {
            return InodeRole::Root;
        }
        if let Some(&idx) = self.org_inodes.get(&ino) {
            return InodeRole::OrgOwned { idx };
        }
        // Walk parent chain.
        if let Some(idx) = self.org_slot_for_inode(ino) {
            return InodeRole::OrgOwned { idx };
        }
        debug_assert!(false, "inode {ino} not found in any org slot");
        InodeRole::Root
    }

    /// Find the org slot index that owns `ino` by walking the parent chain.
    fn org_slot_for_inode(&self, ino: Inode) -> Option<usize> {
        if let Some(&idx) = self.org_inodes.get(&ino) {
            return Some(idx);
        }
        let mut current = ino;
        while let Some(parent) = self.inode_table.get(&current).and_then(|icb| icb.parent) {
            if let Some(&idx) = self.org_inodes.get(&parent) {
                return Some(idx);
            }
            current = parent;
        }
        None
    }

    /// Ensure a mesa-level inode exists for the org at `org_idx`.
    /// Seeds the bridge with (`mesa_org_ino`, `OrgFs::ROOT_INO`).
    /// Does NOT bump rc.
    fn ensure_org_inode(&mut self, org_idx: usize) -> (Inode, FileAttr) {
        // Check if an inode already exists.
        if let Some((&existing_ino, _)) = self.org_inodes.iter().find(|&(_, &idx)| idx == org_idx) {
            if let Some(icb) = self.inode_table.get(&existing_ino)
                && let Some(attr) = icb.attr
            {
                trace!(
                    ino = existing_ino,
                    org_idx,
                    rc = icb.rc,
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
                common: common::make_common_file_attr(
                    self.fs_owner,
                    Self::BLOCK_SIZE,
                    existing_ino,
                    0o755,
                    now,
                    now,
                ),
            };
            common::cache_attr(&mut self.inode_table, existing_ino, attr);
            return (existing_ino, attr);
        }

        // Allocate new.
        let org_name = self.org_slots[org_idx].org.name().to_owned();
        let ino = self.inode_factory.allocate();
        trace!(ino, org_idx, org = %org_name, "ensure_org_inode: allocated new inode");

        let now = SystemTime::now();
        self.inode_table.insert(
            ino,
            InodeControlBlock {
                rc: 0,
                path: org_name.as_str().into(),
                parent: Some(Self::ROOT_NODE_INO),
                children: None,
                attr: None,
            },
        );

        self.org_inodes.insert(ino, org_idx);

        // Seed bridge: mesa org-root ↔ OrgFs::ROOT_INO.
        self.org_slots[org_idx]
            .bridge
            .insert_inode(ino, OrgFs::ROOT_INO);

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

    /// Allocate a mesa-level file handle and map it through the bridge.
    fn alloc_fh(&mut self, slot_idx: usize, org_fh: FileHandle) -> FileHandle {
        let fh = self.next_fh;
        self.next_fh += 1;
        self.org_slots[slot_idx].bridge.insert_fh(fh, org_fh);
        fh
    }

    /// Translate an org inode to a mesa inode, allocating if needed.
    /// Also mirrors the ICB into the mesa `inode_table`.
    fn translate_org_ino_to_mesa(
        &mut self,
        slot_idx: usize,
        org_ino: Inode,
        parent_mesa_ino: Inode,
        name: &OsStr,
    ) -> Inode {
        let mesa_ino = self.org_slots[slot_idx]
            .bridge
            .backward_or_insert_inode(org_ino, || self.inode_factory.allocate());

        self.inode_table
            .entry(mesa_ino)
            .or_insert_with(|| InodeControlBlock {
                rc: 0,
                path: name.into(),
                parent: Some(parent_mesa_ino),
                children: None,
                attr: None,
            });

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
        debug_assert!(
            self.inode_table.contains_key(&parent),
            "lookup: parent inode {parent} not in inode table"
        );

        match self.inode_role(parent) {
            InodeRole::Root => {
                // Children of root are orgs.
                let org_name = name.to_str().ok_or(LookupError::InodeNotFound)?;
                let org_idx = self
                    .org_slots
                    .iter()
                    .position(|s| s.org.name() == org_name)
                    .ok_or(LookupError::InodeNotFound)?;

                trace!(org = org_name, "lookup: matched org");
                let (ino, attr) = self.ensure_org_inode(org_idx);
                let icb = self
                    .inode_table
                    .get_mut(&ino)
                    .unwrap_or_else(|| unreachable!("inode {ino} was just ensured"));
                icb.rc += 1;
                trace!(
                    ino,
                    org = org_name,
                    rc = icb.rc,
                    "lookup: resolved org inode"
                );
                Ok(attr)
            }
            InodeRole::OrgOwned { idx } => {
                // Delegate to org.
                let org_parent = self.org_slots[idx]
                    .bridge
                    .forward_or_insert_inode(parent, || unreachable!("forward should find parent"));

                let org_attr = self.org_slots[idx].org.lookup(org_parent, name).await?;
                let org_ino = org_attr.common().ino;

                let mesa_ino = self.translate_org_ino_to_mesa(idx, org_ino, parent, name);

                let mesa_attr = self.org_slots[idx].bridge.attr_backward(org_attr);
                common::cache_attr(&mut self.inode_table, mesa_ino, mesa_attr);
                let icb = self
                    .inode_table
                    .get_mut(&mesa_ino)
                    .unwrap_or_else(|| unreachable!("inode {mesa_ino} was just cached"));
                icb.rc += 1;
                trace!(
                    mesa_ino,
                    org_ino,
                    rc = icb.rc,
                    "lookup: resolved via org delegation"
                );
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
        let icb = self.inode_table.get(&ino).ok_or_else(|| {
            warn!(ino, "getattr on unknown inode");
            GetAttrError::InodeNotFound
        })?;
        icb.attr.ok_or_else(|| {
            warn!(ino, "getattr on inode with no cached attr");
            GetAttrError::InodeNotFound
        })
    }

    #[instrument(skip(self))]
    async fn readdir(&mut self, ino: Inode) -> Result<&[DirEntry], ReadDirError> {
        debug_assert!(
            self.inode_table.contains_key(&ino),
            "readdir: inode {ino} not in inode table"
        );

        match self.inode_role(ino) {
            InodeRole::Root => {
                let org_info: Vec<(usize, String)> = self
                    .org_slots
                    .iter()
                    .enumerate()
                    .map(|(idx, s)| (idx, s.org.name().to_owned()))
                    .collect();

                let mut entries = Vec::with_capacity(org_info.len());
                for (org_idx, name) in &org_info {
                    let (org_ino, _) = self.ensure_org_inode(*org_idx);
                    entries.push(DirEntry {
                        ino: org_ino,
                        name: name.clone().into(),
                        kind: DirEntryType::Directory,
                    });
                }

                trace!(entry_count = entries.len(), "readdir: listing orgs");

                let icb = self
                    .inode_table
                    .get_mut(&ino)
                    .ok_or(ReadDirError::InodeNotFound)?;
                Ok(icb.children.insert(entries))
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
                        self.translate_org_ino_to_mesa(idx, entry.ino, ino, &entry.name);

                    // Cache attr from org if available.
                    if let Some(org_icb_attr) =
                        self.org_slots[idx].org.inode_table_get_attr(entry.ino)
                    {
                        let mesa_attr = self.org_slots[idx].bridge.attr_backward(org_icb_attr);
                        common::cache_attr(&mut self.inode_table, mesa_child_ino, mesa_attr);
                    }

                    mesa_entries.push(DirEntry {
                        ino: mesa_child_ino,
                        name: entry.name.clone(),
                        kind: entry.kind,
                    });
                }

                let icb = self
                    .inode_table
                    .get_mut(&ino)
                    .ok_or(ReadDirError::InodeNotFound)?;
                Ok(icb.children.insert(mesa_entries))
            }
        }
    }

    #[instrument(skip(self))]
    async fn open(&mut self, ino: Inode, flags: OpenFlags) -> Result<OpenFile, OpenError> {
        let idx = self.org_slot_for_inode(ino).ok_or_else(|| {
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
        let idx = self.org_slot_for_inode(ino).ok_or_else(|| {
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
        let idx = self.org_slot_for_inode(ino).ok_or_else(|| {
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
        debug_assert!(
            self.inode_table.contains_key(&ino),
            "forget: inode {ino} not in inode table"
        );

        // Propagate forget to inner org if applicable.
        if let Some(idx) = self.org_slot_for_inode(ino)
            && let Some(&org_ino) = self.org_slots[idx].bridge.inode_map_get_by_left(ino)
        {
            self.org_slots[idx].org.forget(org_ino, nlookups).await;
        }

        match self.inode_table.entry(ino) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                if entry.get().rc <= nlookups {
                    trace!(ino, "evicting inode");
                    entry.remove();
                    self.org_inodes.remove(&ino);
                    for slot in &mut self.org_slots {
                        slot.bridge.remove_inode_by_left(ino);
                    }
                } else {
                    entry.get_mut().rc -= nlookups;
                    trace!(ino, new_rc = entry.get().rc, "forget: decremented rc");
                }
            }
            std::collections::hash_map::Entry::Vacant(_) => {
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
