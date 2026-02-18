use std::ffi::{OsStr, OsString};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::SystemTime;

use bytes::Bytes;
use git_fs::fs::{FileHandle, INode, INodeType, InodeAddr, InodePerms, OpenFlags};
use mesa_dev::MesaClient;
use opentelemetry::propagation::Injector;
use secrecy::ExposeSecret as _;
use tracing::{instrument, trace, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::app_config::CacheConfig;

pub use common::FsDirEntry;
use composite::CompositeFs;

pub use common::{GetAttrError, LookupError, OpenError, ReadDirError, ReadError, ReleaseError};

#[cfg(feature = "staging")]
const MESA_API_BASE_URL: &str = "https://staging.depot.mesa.dev/api/v1";
#[cfg(not(feature = "staging"))]
const MESA_API_BASE_URL: &str = "https://depot.mesa.dev/api/v1";

mod common;
mod composite;

mod org;
pub use org::OrgConfig;
use org::OrgFs;

pub mod repo;

struct HeaderInjector<'a>(&'a mut reqwest::header::HeaderMap);

impl Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(name), Ok(val)) = (
            reqwest::header::HeaderName::from_bytes(key.as_bytes()),
            reqwest::header::HeaderValue::from_str(&value),
        ) {
            self.0.insert(name, val);
        }
    }
}

/// Middleware that injects W3C `traceparent`/`tracestate` headers from the
/// current `tracing` span into every outgoing HTTP request.
struct OtelPropagationMiddleware;

#[async_trait::async_trait]
impl reqwest_middleware::Middleware for OtelPropagationMiddleware {
    async fn handle(
        &self,
        mut req: reqwest::Request,
        extensions: &mut http::Extensions,
        next: reqwest_middleware::Next<'_>,
    ) -> reqwest_middleware::Result<reqwest::Response> {
        let cx = tracing::Span::current().context();
        opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&cx, &mut HeaderInjector(req.headers_mut()));
        });
        tracing::debug!(
            traceparent = req.headers().get("traceparent").and_then(|v| v.to_str().ok()),
            url = %req.url(),
            "outgoing request"
        );
        next.run(req, extensions).await
    }
}

fn build_mesa_client(api_key: &str) -> MesaClient {
    let client = reqwest_middleware::ClientBuilder::new(reqwest::Client::new())
        .with(OtelPropagationMiddleware)
        .build();
    MesaClient::builder()
        .with_api_key(api_key)
        .with_base_path(MESA_API_BASE_URL)
        .with_client(client)
        .build()
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
    composite: CompositeFs<OrgFs>,
}

impl MesaFS {
    const ROOT_NODE_INO: InodeAddr = CompositeFs::<OrgFs>::ROOT_INO;
    const BLOCK_SIZE: u32 = 4096;

    /// Create a new `MesaFS` instance.
    #[must_use]
    pub fn new(
        orgs: impl Iterator<Item = OrgConfig>,
        fs_owner: (u32, u32),
        cache: &CacheConfig,
    ) -> Self {
        let mut composite = CompositeFs::new(fs_owner, Self::BLOCK_SIZE);
        for org_conf in orgs {
            let client = build_mesa_client(org_conf.api_key.expose_secret());
            let org = OrgFs::new(org_conf.name, client, fs_owner, cache.clone());
            composite.add_child(org, OrgFs::ROOT_INO);
        }
        Self { composite }
    }

    /// Classify an inode by its role.
    fn inode_role(&self, ino: InodeAddr) -> Option<InodeRole> {
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
    /// Does NOT bump rc.
    async fn ensure_org_inode(&mut self, org_idx: usize) -> (InodeAddr, INode) {
        let existing_ino = self
            .composite
            .child_inodes
            .iter()
            .find(|&(_, &idx)| idx == org_idx)
            .map(|(&ino, _)| ino);

        if let Some(existing_ino) = existing_ino {
            if let Ok(inode) = self.composite.delegated_getattr(existing_ino).await {
                trace!(
                    ino = existing_ino,
                    org_idx, "ensure_org_inode: reusing existing inode"
                );
                return (existing_ino, inode);
            }
            warn!(
                ino = existing_ino,
                org_idx, "ensure_org_inode: evicted, rebuilding"
            );
            let now = SystemTime::now();
            let inode = INode {
                addr: existing_ino,
                permissions: InodePerms::from_bits_truncate(0o755),
                uid: self.composite.fs_owner().0,
                gid: self.composite.fs_owner().1,
                create_time: now,
                last_modified_at: now,
                parent: Some(Self::ROOT_NODE_INO),
                size: 0,
                itype: INodeType::Directory,
            };
            self.composite.cache_inode(inode);
            self.composite.inode_to_slot.insert(existing_ino, org_idx);
            self.composite.child_inodes.insert(existing_ino, org_idx);
            return (existing_ino, inode);
        }

        warn!(
            org_idx,
            "ensure_org_inode: no child_inodes entry for org slot"
        );
        let org_name = self.composite.slots[org_idx].inner.name().to_owned();
        let ino = self.composite.allocate_inode();
        let now = SystemTime::now();
        let inode = INode {
            addr: ino,
            permissions: InodePerms::from_bits_truncate(0o755),
            uid: self.composite.fs_owner().0,
            gid: self.composite.fs_owner().1,
            create_time: now,
            last_modified_at: now,
            parent: Some(Self::ROOT_NODE_INO),
            size: 0,
            itype: INodeType::Directory,
        };
        self.composite.cache_inode(inode);
        self.composite.child_inodes.insert(ino, org_idx);
        self.composite.inode_to_slot.insert(ino, org_idx);
        trace!(ino, org_idx, org = %org_name, "ensure_org_inode: allocated new inode");
        (ino, inode)
    }

    #[instrument(name = "MesaFS::lookup", skip(self))]
    pub async fn lookup(&mut self, parent: InodeAddr, name: &OsStr) -> Result<INode, LookupError> {
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
                let (ino, inode) = self.ensure_org_inode(org_idx).await;
                self.composite
                    .inc_rc(ino)
                    .ok_or(LookupError::InodeNotFound)?;
                Ok(inode)
            }
            InodeRole::OrgOwned => self.composite.delegated_lookup(parent, name).await,
        }
    }

    #[instrument(name = "MesaFS::getattr", skip(self))]
    pub async fn getattr(&self, ino: InodeAddr) -> Result<INode, GetAttrError> {
        self.composite.delegated_getattr(ino).await
    }

    #[instrument(name = "MesaFS::readdir", skip(self))]
    pub async fn readdir(&mut self, ino: InodeAddr) -> Result<&[FsDirEntry], ReadDirError> {
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
                    let (entry_ino, _) = self.ensure_org_inode(*org_idx).await;
                    entries.push(FsDirEntry {
                        ino: entry_ino,
                        name: name.clone().into(),
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
    pub async fn open(
        &mut self,
        ino: InodeAddr,
        flags: OpenFlags,
    ) -> Result<FileHandle, OpenError> {
        self.composite.delegated_open(ino, flags).await
    }

    #[instrument(name = "MesaFS::read", skip(self))]
    pub async fn read(
        &mut self,
        fh: FileHandle,
        offset: u64,
        size: u32,
    ) -> Result<Bytes, ReadError> {
        self.composite.delegated_read(fh, offset, size).await
    }

    #[instrument(name = "MesaFS::release", skip(self))]
    pub async fn release(&mut self, fh: FileHandle) -> Result<(), ReleaseError> {
        self.composite.delegated_release(fh).await
    }
}

/// A file reader that delegates reads to `MesaFS` through a shared mutex.
///
/// Resources are released via [`FileReader::close`](git_fs::fs::async_fs::FileReader::close),
/// which is called by the FUSE adapter during `release`. Dropping without
/// calling `close()` emits a diagnostic warning.
pub struct MesaFsReader {
    inner: Arc<tokio::sync::Mutex<MesaFS>>,
    fh: FileHandle,
    closed: AtomicBool,
}

impl git_fs::fs::async_fs::FileReader for MesaFsReader {
    fn read(
        &self,
        offset: u64,
        size: u32,
    ) -> impl Future<Output = Result<Bytes, std::io::Error>> + Send {
        let inner = Arc::clone(&self.inner);
        let fh = self.fh;
        async move {
            let mut guard = inner.lock().await;
            guard
                .read(fh, offset, size)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))
        }
    }

    fn close(&self) -> impl Future<Output = Result<(), std::io::Error>> + Send {
        self.closed.store(true, Ordering::Relaxed);
        let inner = Arc::clone(&self.inner);
        let fh = self.fh;
        async move {
            let mut guard = inner.lock().await;
            guard
                .release(fh)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))
        }
    }
}

impl Drop for MesaFsReader {
    fn drop(&mut self) {
        if !self.closed.load(Ordering::Relaxed) {
            tracing::warn!(fh = self.fh, "MesaFsReader dropped without close()");
        }
    }
}

/// A [`FsDataProvider`](git_fs::fs::async_fs::FsDataProvider) that wraps
/// `MesaFS` behind a shared mutex.
#[derive(Clone)]
pub struct MesaFsProvider {
    inner: Arc<tokio::sync::Mutex<MesaFS>>,
}

impl MesaFsProvider {
    /// Create a new provider wrapping the given `MesaFS`.
    pub fn new(mesa_fs: MesaFS) -> Self {
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(mesa_fs)),
        }
    }
}

fn lookup_error_to_io(e: LookupError) -> std::io::Error {
    match e {
        LookupError::InodeNotFound => std::io::Error::from_raw_os_error(libc::ENOENT),
        LookupError::RemoteMesaError(api) => std::io::Error::other(api.to_string()),
    }
}

fn readdir_error_to_io(e: ReadDirError) -> std::io::Error {
    match e {
        ReadDirError::InodeNotFound => std::io::Error::from_raw_os_error(libc::ENOENT),
        ReadDirError::NotADirectory => std::io::Error::from_raw_os_error(libc::ENOTDIR),
        ReadDirError::NotPermitted => std::io::Error::from_raw_os_error(libc::EPERM),
        ReadDirError::RemoteMesaError(api) => std::io::Error::other(api.to_string()),
    }
}

fn open_error_to_io(e: OpenError) -> std::io::Error {
    match e {
        OpenError::InodeNotFound => std::io::Error::from_raw_os_error(libc::ENOENT),
    }
}

impl git_fs::fs::async_fs::FsDataProvider for MesaFsProvider {
    type Reader = MesaFsReader;

    fn lookup(
        &self,
        parent: INode,
        name: &OsStr,
    ) -> impl Future<Output = Result<INode, std::io::Error>> + Send {
        let inner = Arc::clone(&self.inner);
        let name = name.to_os_string();
        async move {
            let mut guard = inner.lock().await;
            guard
                .lookup(parent.addr, &name)
                .await
                .map_err(lookup_error_to_io)
        }
    }

    fn readdir(
        &self,
        parent: INode,
    ) -> impl Future<Output = Result<Vec<(OsString, INode)>, std::io::Error>> + Send {
        let inner = Arc::clone(&self.inner);
        async move {
            let mut guard = inner.lock().await;
            let dir_entries: Vec<(OsString, InodeAddr)> = {
                let entries = guard
                    .readdir(parent.addr)
                    .await
                    .map_err(readdir_error_to_io)?;
                entries.iter().map(|e| (e.name.clone(), e.ino)).collect()
            };
            let mut result = Vec::with_capacity(dir_entries.len());
            for (name, ino) in dir_entries {
                if let Ok(inode) = guard.getattr(ino).await {
                    result.push((name, inode));
                }
            }
            Ok(result)
        }
    }

    fn open(
        &self,
        inode: INode,
        flags: OpenFlags,
    ) -> impl Future<Output = Result<Self::Reader, std::io::Error>> + Send {
        let inner = Arc::clone(&self.inner);
        async move {
            let mut guard = inner.lock().await;
            let fh = guard
                .open(inode.addr, flags)
                .await
                .map_err(open_error_to_io)?;
            Ok(MesaFsReader {
                inner: Arc::clone(&inner),
                fh,
                closed: AtomicBool::new(false),
            })
        }
    }
}
