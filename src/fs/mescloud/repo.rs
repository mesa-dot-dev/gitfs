//! An implementation of a filesystem accessible on a per-repo basis.
//!
//! This module directly accesses the mesa repo through the Rust SDK, on a per-repo basis.

use std::ffi::{OsStr, OsString};
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use base64::Engine as _;
use bytes::Bytes;
use mesa_dev::MesaClient;
use mesa_dev::low_level::content::{Content, DirEntry as MesaDirEntry};
use num_traits::cast::ToPrimitive as _;
use tracing::warn;

use git_fs::cache::fcache::FileCache;
use git_fs::cache::traits::{AsyncReadableCache as _, AsyncWritableCache as _};
use git_fs::fs::LoadedAddr;
use git_fs::fs::async_fs::{FileReader, FsDataProvider};
use git_fs::fs::dcache::DCache;
use git_fs::fs::{INode, INodeType, InodeAddr, InodePerms, OpenFlags as AsyncOpenFlags, ROOT_INO};

use super::common::{MesaApiError, mesa_api_error_to_io};

#[derive(Clone)]
pub struct MesRepoProvider {
    inner: Arc<MesRepoProviderInner>,
}

struct ChangeState {
    client: mesa_dev::client::ChangeClient,
    change_id: mesa_dev::grpc::ChangeId,
    /// Bare branch name (e.g. `"main"`), used by [`Self::snapshot_and_flush`]
    /// to move the bookmark after snapshotting.
    branch: String,
    /// Full ref name (e.g. `"refs/heads/main"`), used by [`Self::snapshot_and_flush`]
    /// to resolve the current update sequence.
    full_ref: String,
}

/// A queued remote operation, processed in FIFO order by the background
/// consumer task. This guarantees that `create_file` always runs before
/// the first `modify_file` for the same path.
enum RemoteOp {
    Create { path: String },
    Write { path: String, data: Bytes },
    Unlink { path: String },
}

impl ChangeState {
    /// Snapshot the change, then move the bookmark so the new commit is
    /// visible via the REST content API.
    async fn snapshot_and_flush(&self, message: &str) -> Result<(), std::io::Error> {
        let snap = self
            .client
            .snapshot(&self.change_id, message)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        let commit_oid = snap
            .commit_oid
            .ok_or_else(|| std::io::Error::other("snapshot returned no commit_oid"))?;

        let ref_info = self
            .client
            .resolve_ref(&self.full_ref)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        // NOTE(MES-857): move_bookmark uses resolve_ref's update_seq for
        // optimistic concurrency, but an external push between resolve_ref
        // and move_bookmark will cause a stale-sequence error. Needs a
        // retry-with-rebase protocol.
        self.client
            .move_bookmark(&self.branch, &commit_oid.value, ref_info.update_seq)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        Ok(())
    }
}

struct MesRepoProviderInner {
    client: MesaClient,
    org_name: String,
    repo_name: String,
    ref_: String,
    fs_owner: (u32, u32),
    next_addr: AtomicU64,
    /// Maps inode addresses to repo-relative paths (e.g. `"src/main.rs"`).
    /// Root maps to an empty `PathBuf`.
    ///
    /// Exists alongside the [`DCache`](git_fs::fs::dcache::DCache) because
    /// they serve different purposes: the dcache maps
    /// `(parent_addr, child_name) -> child_addr` (single-hop name resolution),
    /// while this map provides the full repo-relative path needed for Mesa API
    /// calls. Reconstructing the full path from the dcache would require
    /// walking parent pointers to the root on every API call; this map
    /// materializes that walk as an O(1) lookup.
    ///
    /// Entries are inserted during `lookup`/`readdir` and removed via
    /// [`forget`](Self::remove_path) when the FUSE refcount reaches zero.
    path_map: scc::HashMap<InodeAddr, PathBuf>,
    file_cache: Option<Arc<FileCache<InodeAddr>>>,
    dcache: Arc<DCache>,
    /// Send side of the ordered operation channel. Operations are processed
    /// sequentially by a background task, guaranteeing FIFO ordering.
    op_tx: tokio::sync::mpsc::UnboundedSender<RemoteOp>,
}

impl MesRepoProviderInner {
    /// Return the full Git ref name (e.g. `"refs/heads/main"`) from the bare
    /// branch name stored in `ref_`.
    fn full_ref_name(&self) -> String {
        format!("refs/heads/{}", self.ref_)
    }

    /// If `child_path` is a `.gitignore` or `.mesafs-ignore` file, fetch its
    /// content from the API and feed it to the dcache ignore matcher.
    async fn maybe_observe_ignore_file(
        &self,
        parent_addr: InodeAddr,
        child_path: &std::path::Path,
    ) {
        let Some(file_name @ (".gitignore" | ".mesafs-ignore")) =
            child_path.file_name().and_then(|n| n.to_str())
        else {
            return;
        };

        let Some(path_str) = child_path.to_str() else {
            return;
        };

        // Fetch the file content from the remote API.
        let content = match self
            .client
            .org(&self.org_name)
            .repos()
            .at(&self.repo_name)
            .content()
            .get(Some(self.ref_.as_str()), Some(path_str), None)
            .await
        {
            Ok(content) => content,
            Err(e) => {
                tracing::debug!(%e, path = %path_str, "failed to fetch ignore file content");
                return;
            }
        };

        let encoded = match content {
            Content::File(f) => f.content.unwrap_or_default(),
            Content::Symlink(_) | Content::Dir(_) => return,
        };

        let decoded = match base64::engine::general_purpose::STANDARD.decode(&encoded) {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::debug!(%e, path = %path_str, "failed to decode ignore file content");
                return;
            }
        };

        let Ok(text) = std::str::from_utf8(&decoded) else {
            tracing::debug!(path = %path_str, "ignore file is not valid UTF-8");
            return;
        };

        self.dcache
            .set_ignore_rules(LoadedAddr::new_unchecked(parent_addr), file_name, text);
    }
}

async fn init_change_state(inner: &MesRepoProviderInner) -> Result<ChangeState, std::io::Error> {
    let change_client = inner
        .client
        .org(&inner.org_name)
        .repos()
        .at(&inner.repo_name)
        .change()
        .await
        .map_err(|e| std::io::Error::other(e.to_string()))?;

    let change = change_client
        .create_from_ref(&inner.full_ref_name())
        .await
        .map_err(|e| std::io::Error::other(e.to_string()))?;

    let change_id = change
        .id
        .ok_or_else(|| std::io::Error::other("server returned change without id"))?;

    Ok(ChangeState {
        client: change_client,
        change_id,
        branch: inner.ref_.clone(),
        full_ref: inner.full_ref_name(),
    })
}

async fn execute_create(state: &ChangeState, path: &str) -> Result<(), std::io::Error> {
    state
        .client
        .create_file(&state.change_id, path, &[], None)
        .await
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    state.snapshot_and_flush(&format!("Create {path}")).await
}

async fn execute_write(state: &ChangeState, path: &str, data: &[u8]) -> Result<(), std::io::Error> {
    state
        .client
        .modify_file(&state.change_id, path, data)
        .await
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    state.snapshot_and_flush(&format!("Update {path}")).await
}

async fn execute_unlink(state: &ChangeState, path: &str) -> Result<(), std::io::Error> {
    state
        .client
        .delete_path(&state.change_id, path, false)
        .await
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    state.snapshot_and_flush(&format!("Delete {path}")).await
}

/// Background task that processes remote operations in FIFO order.
///
/// Lazily initializes the `ChangeState` on first operation. If a gRPC
/// error indicates staleness, resets the state so the next operation
/// re-initializes.
async fn remote_op_consumer(
    inner: Arc<MesRepoProviderInner>,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<RemoteOp>,
) {
    let mut change_state: Option<ChangeState> = None;

    while let Some(op) = rx.recv().await {
        if change_state.is_none() {
            match init_change_state(&inner).await {
                Ok(state) => change_state = Some(state),
                Err(e) => {
                    tracing::warn!(%e, "failed to initialize change state, dropping op");
                    continue;
                }
            }
        }
        let Some(state) = change_state.as_ref() else {
            continue;
        };
        let result = match &op {
            RemoteOp::Create { path } => execute_create(state, path).await,
            RemoteOp::Write { path, data } => execute_write(state, path, data).await,
            RemoteOp::Unlink { path } => execute_unlink(state, path).await,
        };
        if let Err(e) = result {
            // NOTE(MES-854): no backoff — re-init is attempted on every
            // subsequent operation, which can hammer the server during outages.
            tracing::warn!(%e, "remote operation failed, resetting change state");
            change_state = None;
        }
    }
}

impl MesRepoProvider {
    pub(super) fn new(
        client: MesaClient,
        org_name: String,
        repo_name: String,
        ref_: String,
        fs_owner: (u32, u32),
        file_cache: Option<Arc<FileCache<InodeAddr>>>,
    ) -> Self {
        let dcache = Arc::new(DCache::new(PathBuf::from("/")));
        // NOTE(MES-855): unbounded — can grow without limit if remote is slow.
        let (op_tx, op_rx) = tokio::sync::mpsc::unbounded_channel();
        let inner = Arc::new(MesRepoProviderInner {
            client,
            org_name,
            repo_name,
            ref_,
            fs_owner,
            next_addr: AtomicU64::new(ROOT_INO + 1),
            path_map: scc::HashMap::new(),
            file_cache,
            dcache,
            op_tx,
        });
        tokio::spawn(remote_op_consumer(Arc::clone(&inner), op_rx));
        Self { inner }
    }

    /// Store the path for the root inode address.
    pub(super) fn seed_root_path(&self, root_addr: InodeAddr) {
        // Root maps to empty PathBuf (no path prefix for API calls)
        drop(self.inner.path_map.insert_sync(root_addr, PathBuf::new()));
    }

    /// Remove the path entry for an inode. Called during forget/cleanup.
    fn remove_path(&self, addr: InodeAddr) {
        self.inner.path_map.remove_sync(&addr);
    }

    /// The name of the repository.
    #[expect(
        dead_code,
        reason = "useful diagnostic accessor retained for future use"
    )]
    pub(super) fn repo_name(&self) -> &str {
        &self.inner.repo_name
    }
}

impl FsDataProvider for MesRepoProvider {
    type Reader = MesFileReader;

    fn dcache(&self) -> Option<Arc<DCache>> {
        Some(Arc::clone(&self.inner.dcache))
    }

    fn lookup(
        &self,
        parent: INode,
        name: &OsStr,
    ) -> impl Future<Output = Result<INode, std::io::Error>> + Send {
        let inner = Arc::clone(&self.inner);
        let name = name.to_os_string();
        async move {
            let parent_path = inner
                .path_map
                .get_async(&parent.addr)
                .await
                .map(|e| e.get().clone())
                .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

            let child_path = parent_path.join(&name);
            let child_path_str = child_path.to_str().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "path contains non-UTF-8 characters",
                )
            })?;

            let content = inner
                .client
                .org(&inner.org_name)
                .repos()
                .at(&inner.repo_name)
                .content()
                .get(Some(inner.ref_.as_str()), Some(child_path_str), Some(1u64))
                .await
                .map_err(MesaApiError::from)
                .map_err(mesa_api_error_to_io)?;

            let now = SystemTime::now();
            let (uid, gid) = inner.fs_owner;

            // Symlinks are mapped to File because FuserAdapter does not implement readlink.
            let (itype, size) = match &content {
                Content::File(f) => (INodeType::File, f.size.to_u64().unwrap_or(0)),
                Content::Symlink(s) => (INodeType::File, s.size.to_u64().unwrap_or(0)),
                Content::Dir(_) => (INodeType::Directory, 0),
            };

            let perms = if itype == INodeType::Directory {
                InodePerms::from_bits_truncate(0o755)
            } else {
                InodePerms::from_bits_truncate(0o644)
            };

            // TODO(MES-777): Address allocation is racy — two concurrent lookups for the same
            // child path each allocate a fresh address and insert into `path_map`, causing
            // the second insert to silently overwrite the first. The `AsyncFs` lookup cache
            // deduplicates in practice, but a content-addressed scheme (hash path → addr)
            // would eliminate the race structurally.
            //
            // This also interacts badly with cache eviction: when an inode is evicted and
            // later re-looked-up, a fresh address is minted, leaking stale bridge and
            // addr_to_slot entries for the old address. A stable, content-addressed scheme
            // would make re-lookup return the same address and avoid the leak.
            let addr = inner.next_addr.fetch_add(1, Ordering::Relaxed);
            drop(inner.path_map.insert_async(addr, child_path.clone()).await);

            inner
                .maybe_observe_ignore_file(parent.addr, &child_path)
                .await;

            Ok(INode {
                addr,
                permissions: perms,
                uid,
                gid,
                create_time: now,
                last_modified_at: now,
                parent: Some(parent.addr),
                size,
                itype,
            })
        }
    }

    fn readdir(
        &self,
        parent: INode,
    ) -> impl Future<Output = Result<Vec<(OsString, INode)>, std::io::Error>> + Send {
        let inner = Arc::clone(&self.inner);
        async move {
            let parent_path = inner
                .path_map
                .get_async(&parent.addr)
                .await
                .map(|e| e.get().clone())
                .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

            let api_path = if parent_path.as_os_str().is_empty() {
                None
            } else {
                Some(
                    parent_path
                        .to_str()
                        .ok_or_else(|| {
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "path contains non-UTF-8 characters",
                            )
                        })?
                        .to_owned(),
                )
            };

            let content = inner
                .client
                .org(&inner.org_name)
                .repos()
                .at(&inner.repo_name)
                .content()
                .get(Some(inner.ref_.as_str()), api_path.as_deref(), Some(1u64))
                .await
                .map_err(MesaApiError::from)
                .map_err(mesa_api_error_to_io)?;

            let dir = match content {
                Content::Dir(d) => d,
                Content::File(_) | Content::Symlink(_) => {
                    return Err(std::io::Error::from_raw_os_error(libc::ENOTDIR));
                }
            };

            let now = SystemTime::now();
            let (uid, gid) = inner.fs_owner;
            let mut entries = Vec::with_capacity(dir.entries.len());

            for entry in dir.entries {
                let (name, itype, size) = match entry {
                    MesaDirEntry::File(f) => {
                        let Some(name) = f.name else { continue };
                        (name, INodeType::File, f.size.to_u64().unwrap_or(0))
                    }
                    MesaDirEntry::Symlink(s) => {
                        let Some(name) = s.name else { continue };
                        (name, INodeType::File, s.size.to_u64().unwrap_or(0))
                    }
                    MesaDirEntry::Dir(d) => {
                        let Some(name) = d.name else { continue };
                        (name, INodeType::Directory, 0)
                    }
                };

                let perms = if itype == INodeType::Directory {
                    InodePerms::from_bits_truncate(0o755)
                } else {
                    InodePerms::from_bits_truncate(0o644)
                };

                let addr = inner.next_addr.fetch_add(1, Ordering::Relaxed);
                let child_path = parent_path.join(&name);
                drop(inner.path_map.insert_async(addr, child_path.clone()).await);

                inner
                    .maybe_observe_ignore_file(parent.addr, &child_path)
                    .await;

                let inode = INode {
                    addr,
                    permissions: perms,
                    uid,
                    gid,
                    create_time: now,
                    last_modified_at: now,
                    parent: Some(parent.addr),
                    size,
                    itype,
                };

                entries.push((OsString::from(name), inode));
            }

            Ok(entries)
        }
    }

    fn open(
        &self,
        inode: INode,
        _flags: AsyncOpenFlags,
    ) -> impl Future<Output = Result<Self::Reader, std::io::Error>> + Send {
        let inner = Arc::clone(&self.inner);
        async move {
            let path = inner
                .path_map
                .get_async(&inode.addr)
                .await
                .map(|e| e.get().clone())
                .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

            Ok(MesFileReader {
                inner: Arc::new(MesFileReaderCtx {
                    client: inner.client.clone(),
                    org_name: inner.org_name.clone(),
                    repo_name: inner.repo_name.clone(),
                    ref_: inner.ref_.clone(),
                    path,
                    file_cache: inner.file_cache.clone(),
                    inode_addr: inode.addr,
                }),
            })
        }
    }

    /// Evicts the inode's entry from [`path_map`](MesRepoProviderInner::path_map).
    /// Called automatically by `InodeForget` when the FUSE refcount drops to zero.
    fn forget(&self, addr: InodeAddr) {
        self.remove_path(addr);
    }

    #[expect(clippy::cast_possible_truncation, reason = "mode fits in u16")]
    fn create(
        &self,
        parent: INode,
        name: &OsStr,
        mode: u32,
    ) -> impl Future<Output = Result<INode, std::io::Error>> + Send {
        let inner = Arc::clone(&self.inner);
        let name = name.to_os_string();
        async move {
            let parent_path = inner
                .path_map
                .get_async(&parent.addr)
                .await
                .map(|e| e.get().clone())
                .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

            let child_path = parent_path.join(&name);
            let addr = inner.next_addr.fetch_add(1, Ordering::Relaxed);
            drop(inner.path_map.insert_async(addr, child_path.clone()).await);

            let now = SystemTime::now();
            let (uid, gid) = inner.fs_owner;

            if let Some(path_str) = child_path.to_str() {
                if inner.dcache.is_name_ignored(
                    LoadedAddr::new_unchecked(parent.addr),
                    &name,
                    false,
                ) {
                    tracing::debug!(path = %path_str, "skipping remote create for ignored file");
                } else {
                    drop(inner.op_tx.send(RemoteOp::Create {
                        path: path_str.to_owned(),
                    }));
                }
            }

            Ok(INode {
                addr,
                permissions: InodePerms::from_bits_truncate(mode as u16),
                uid,
                gid,
                create_time: now,
                last_modified_at: now,
                parent: Some(parent.addr),
                size: 0,
                itype: INodeType::File,
            })
        }
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "data.len() fits in u32 (FUSE writes are at most 128 KiB)"
    )]
    fn write(
        &self,
        inode: INode,
        _offset: u64,
        data: Bytes,
    ) -> impl Future<Output = Result<u32, std::io::Error>> + Send {
        let inner = Arc::clone(&self.inner);
        async move {
            let path = inner
                .path_map
                .get_async(&inode.addr)
                .await
                .map(|e| e.get().clone())
                .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

            if inner
                .dcache
                .is_ignored(LoadedAddr::new_unchecked(inode.addr))
            {
                tracing::debug!(path = ?path, "skipping remote write for ignored file");
                let written = data.len() as u32;
                return Ok(written);
            }

            let path_str = path.to_str().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "path contains non-UTF-8 characters",
                )
            })?;

            drop(inner.op_tx.send(RemoteOp::Write {
                path: path_str.to_owned(),
                data: data.clone(),
            }));

            // If this is an ignore file, update the dcache rules directly.
            if let Some(fname @ (".gitignore" | ".mesafs-ignore")) =
                path.file_name().and_then(|n| n.to_str())
                && let Ok(text) = std::str::from_utf8(&data)
                && let Some(parent) = inner
                    .dcache
                    .parent_of(LoadedAddr::new_unchecked(inode.addr))
            {
                inner.dcache.set_ignore_rules(parent, fname, text);
            }

            let written = data.len() as u32;
            Ok(written)
        }
    }

    fn unlink(
        &self,
        parent: INode,
        name: &OsStr,
    ) -> impl Future<Output = Result<(), std::io::Error>> + Send {
        let inner = Arc::clone(&self.inner);
        let name = name.to_os_string();
        async move {
            let parent_path = inner
                .path_map
                .get_async(&parent.addr)
                .await
                .map(|e| e.get().clone())
                .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

            let child_path = parent_path.join(&name);
            let path_str = child_path.to_str().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "path contains non-UTF-8 characters",
                )
            })?;

            if inner
                .dcache
                .is_name_ignored(LoadedAddr::new_unchecked(parent.addr), &name, false)
            {
                tracing::debug!(path = %path_str, "skipping remote unlink for ignored file");
                return Ok(());
            }

            drop(inner.op_tx.send(RemoteOp::Unlink {
                path: path_str.to_owned(),
            }));

            if let Some(fname @ (".gitignore" | ".mesafs-ignore")) =
                child_path.file_name().and_then(|n| n.to_str())
            {
                inner
                    .dcache
                    .clear_ignore_rules(LoadedAddr::new_unchecked(parent.addr), fname);
            }

            Ok(())
        }
    }
}

pub struct MesFileReader {
    inner: Arc<MesFileReaderCtx>,
}

struct MesFileReaderCtx {
    client: MesaClient,
    org_name: String,
    repo_name: String,
    ref_: String,
    path: PathBuf,
    file_cache: Option<Arc<FileCache<InodeAddr>>>,
    inode_addr: InodeAddr,
}

impl FileReader for MesFileReader {
    fn read(
        &self,
        offset: u64,
        size: u32,
    ) -> impl Future<Output = Result<Bytes, std::io::Error>> + Send {
        let ctx = Arc::clone(&self.inner);

        async move {
            // Try the file cache first.
            if let Some(cache) = &ctx.file_cache
                && let Some(data) = cache.get(&ctx.inode_addr).await
            {
                let start = usize::try_from(offset)
                    .unwrap_or(data.len())
                    .min(data.len());
                let end = start.saturating_add(size as usize).min(data.len());
                return Ok(Bytes::copy_from_slice(&data[start..end]));
            }

            // Cache miss -- fetch from the Mesa API.
            let path_str = ctx.path.to_str().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "path contains non-UTF-8 characters",
                )
            })?;

            let api_path = if path_str.is_empty() {
                None
            } else {
                Some(path_str)
            };

            let content = ctx
                .client
                .org(&ctx.org_name)
                .repos()
                .at(&ctx.repo_name)
                .content()
                .get(Some(ctx.ref_.as_str()), api_path, None)
                .await
                .map_err(MesaApiError::from)
                .map_err(mesa_api_error_to_io)?;

            let encoded_content = match content {
                Content::File(f) => f.content.unwrap_or_default(),
                Content::Symlink(s) => s.content.unwrap_or_default(),
                Content::Dir(_) => {
                    return Err(std::io::Error::from_raw_os_error(libc::EISDIR));
                }
            };

            let decoded = base64::engine::general_purpose::STANDARD
                .decode(&encoded_content)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

            let start = usize::try_from(offset)
                .unwrap_or(decoded.len())
                .min(decoded.len());
            let end = start.saturating_add(size as usize).min(decoded.len());
            let result = Bytes::copy_from_slice(&decoded[start..end]);

            // Store the decoded content in the cache for future reads.
            if let Some(cache) = &ctx.file_cache
                && let Err(e) = cache.insert(&ctx.inode_addr, decoded).await
            {
                warn!(error = ?e, inode_addr = ctx.inode_addr, "failed to cache file content");
            }

            Ok(result)
        }
    }
}
