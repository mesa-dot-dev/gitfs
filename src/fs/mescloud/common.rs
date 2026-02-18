//! Shared types and helpers used by both `MesaFS` and `RepoFs`.

use std::ffi::{OsStr, OsString};

use bytes::Bytes;
use git_fs::fs::{FileHandle, INode, InodeAddr, OpenFlags as LibOpenFlags};
use mesa_dev::low_level::apis;
use thiserror::Error;

/// A concrete error type that preserves the structure of `mesa_dev::low_level::apis::Error<T>`
/// without the generic parameter.
#[derive(Debug, Error)]
pub enum MesaApiError {
    #[error("HTTP request error")]
    Reqwest(#[from] reqwest::Error),

    #[error("HTTP middleware error")]
    ReqwestMiddleware(#[from] reqwest_middleware::Error),

    #[error("JSON deserialization error")]
    Serde(#[from] serde_json::Error),

    #[error("JSON deserialization error at path")]
    SerdePath(#[from] serde_path_to_error::Error<serde_json::Error>),

    #[error("IO error")]
    Io(#[from] std::io::Error),

    #[error("API returned HTTP {status}")]
    Response { status: u16, body: String },
}

impl<T: std::fmt::Debug + Send + Sync + 'static> From<apis::Error<T>> for MesaApiError {
    fn from(e: apis::Error<T>) -> Self {
        match e {
            apis::Error::Reqwest(e) => Self::Reqwest(e),
            apis::Error::ReqwestMiddleware(e) => Self::ReqwestMiddleware(e),
            apis::Error::Serde(e) => Self::Serde(e),
            apis::Error::SerdePathToError(e) => Self::SerdePath(e),
            apis::Error::Io(e) => Self::Io(e),
            apis::Error::ResponseError(rc) => Self::Response {
                status: rc.status.as_u16(),
                body: rc.content,
            },
        }
    }
}

#[derive(Debug, Error)]
pub enum LookupError {
    #[error("inode not found")]
    InodeNotFound,

    #[error("remote mesa error")]
    RemoteMesaError(#[from] MesaApiError),
}

#[derive(Debug, Error)]
pub enum GetAttrError {
    #[error("inode not found")]
    InodeNotFound,
}

#[derive(Debug, Clone, Copy, Error)]
pub enum OpenError {
    #[error("inode not found")]
    InodeNotFound,
}

#[derive(Debug, Error)]
pub enum ReadError {
    #[error("file not open")]
    FileNotOpen,

    #[error("inode not found")]
    InodeNotFound,

    #[error("remote mesa error")]
    RemoteMesaError(#[from] MesaApiError),

    #[error("content is not a file")]
    NotAFile,

    #[error("base64 decode error: {0}")]
    Base64Decode(#[from] base64::DecodeError),
}

#[derive(Debug, Error)]
pub enum ReadDirError {
    #[error("inode not found")]
    InodeNotFound,

    #[error("remote mesa error")]
    RemoteMesaError(#[from] MesaApiError),

    #[error("inode is not a directory")]
    NotADirectory,

    #[error("operation not permitted")]
    NotPermitted,
}

impl From<LookupError> for ReadDirError {
    fn from(e: LookupError) -> Self {
        match e {
            LookupError::RemoteMesaError(api) => Self::RemoteMesaError(api),
            LookupError::InodeNotFound => Self::InodeNotFound,
        }
    }
}

#[derive(Debug, Error)]
pub enum ReleaseError {
    #[error("file not open")]
    FileNotOpen,
}

/// A directory entry for readdir results, using lib types.
pub struct FsDirEntry {
    pub ino: InodeAddr,
    pub name: OsString,
}

/// Trait for child filesystems composed by [`CompositeFs`](super::composite::CompositeFs).
///
/// Uses lib types (`INode`, `InodeAddr`) directly — no conversion to/from `FileAttr`.
/// Replaces the old `Fs + InodeCachePeek` bound.
#[async_trait::async_trait]
pub(super) trait ChildFs: Send + Sync {
    /// Look up a child by name within the given parent directory.
    async fn lookup(&mut self, parent: InodeAddr, name: &OsStr) -> Result<INode, LookupError>;

    /// List all children of a directory, returning full `INode` data for each.
    async fn readdir(&mut self, ino: InodeAddr) -> Result<Vec<(OsString, INode)>, ReadDirError>;

    /// Open a file for reading.
    async fn open(&mut self, ino: InodeAddr, flags: LibOpenFlags) -> Result<FileHandle, OpenError>;

    /// Read data from an open file.
    async fn read(
        &mut self,
        ino: InodeAddr,
        fh: FileHandle,
        offset: u64,
        size: u32,
    ) -> Result<Bytes, ReadError>;

    /// Release (close) a file handle.
    async fn release(&mut self, ino: InodeAddr, fh: FileHandle) -> Result<(), ReleaseError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_inode_not_found_converts_to_readdir_inode_not_found() {
        let err: ReadDirError = LookupError::InodeNotFound.into();
        assert!(matches!(err, ReadDirError::InodeNotFound));
    }

    #[test]
    fn lookup_remote_error_converts_to_readdir_remote_error() {
        let api_err = MesaApiError::Response {
            status: 500,
            body: "test".to_owned(),
        };
        let err: ReadDirError = LookupError::RemoteMesaError(api_err).into();
        assert!(matches!(err, ReadDirError::RemoteMesaError(_)));
    }
}
