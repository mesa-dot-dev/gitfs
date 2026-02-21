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
use git_fs::fs::async_fs::{FileReader, FsDataProvider};
use git_fs::fs::{INode, INodeType, InodeAddr, InodePerms, OpenFlags as AsyncOpenFlags};

use super::common::{MesaApiError, mesa_api_error_to_io};

#[derive(Clone)]
pub struct MesRepoProvider {
    inner: Arc<MesRepoProviderInner>,
}

struct MesRepoProviderInner {
    client: MesaClient,
    org_name: String,
    repo_name: String,
    ref_: String,
    fs_owner: (u32, u32),
    next_addr: AtomicU64,
    /// Maps inode addresses to repo-relative paths (e.g., "src/main.rs").
    /// Root directory maps to an empty `PathBuf`.
    path_map: scc::HashMap<InodeAddr, PathBuf>,
    file_cache: Option<Arc<FileCache<InodeAddr>>>,
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
        Self {
            inner: Arc::new(MesRepoProviderInner {
                client,
                org_name,
                repo_name,
                ref_,
                fs_owner,
                next_addr: AtomicU64::new(2), // 1 is reserved for root
                path_map: scc::HashMap::new(),
                file_cache,
            }),
        }
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

            let addr = inner.next_addr.fetch_add(1, Ordering::Relaxed);
            drop(inner.path_map.insert_async(addr, child_path).await);

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
                drop(inner.path_map.insert_async(addr, child_path).await);

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

    fn forget(&self, addr: InodeAddr) {
        self.remove_path(addr);
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
