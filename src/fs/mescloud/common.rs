//! Shared types and helpers used by both `MesaFS` and `RepoFs`.

use mesa_dev::low_level::apis;
use thiserror::Error;

use crate::fs::r#trait::{FileAttr, Inode};

pub(super) use super::icache::InodeControlBlock;

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

    #[error("file does not exist")]
    FileDoesNotExist,

    #[error("remote mesa error")]
    RemoteMesaError(#[from] MesaApiError),
}

impl From<LookupError> for i32 {
    fn from(e: LookupError) -> Self {
        match e {
            LookupError::InodeNotFound | LookupError::FileDoesNotExist => libc::ENOENT,
            LookupError::RemoteMesaError(_) => libc::EIO,
        }
    }
}

#[derive(Debug, Error)]
pub enum GetAttrError {
    #[error("inode not found")]
    InodeNotFound,
}

impl From<GetAttrError> for i32 {
    fn from(e: GetAttrError) -> Self {
        match e {
            GetAttrError::InodeNotFound => libc::ENOENT,
        }
    }
}

#[derive(Debug, Error)]
pub enum OpenError {
    #[error("inode not found")]
    InodeNotFound,
}

impl From<OpenError> for i32 {
    fn from(e: OpenError) -> Self {
        match e {
            OpenError::InodeNotFound => libc::ENOENT,
        }
    }
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

impl From<ReadError> for i32 {
    fn from(e: ReadError) -> Self {
        match e {
            ReadError::FileNotOpen => libc::EBADF,
            ReadError::InodeNotFound => libc::ENOENT,
            ReadError::RemoteMesaError(_) | ReadError::Base64Decode(_) => libc::EIO,
            ReadError::NotAFile => libc::EISDIR,
        }
    }
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
            LookupError::InodeNotFound | LookupError::FileDoesNotExist => Self::InodeNotFound,
        }
    }
}

impl From<ReadDirError> for i32 {
    fn from(e: ReadDirError) -> Self {
        match e {
            ReadDirError::InodeNotFound => libc::ENOENT,
            ReadDirError::RemoteMesaError(_) => libc::EIO,
            ReadDirError::NotADirectory => libc::ENOTDIR,
            ReadDirError::NotPermitted => libc::EPERM,
        }
    }
}

#[derive(Debug, Error)]
pub enum ReleaseError {
    #[error("file not open")]
    FileNotOpen,
}

impl From<ReleaseError> for i32 {
    fn from(e: ReleaseError) -> Self {
        match e {
            ReleaseError::FileNotOpen => libc::EBADF,
        }
    }
}

/// Allows a parent compositor to peek at cached attrs from a child filesystem.
#[async_trait::async_trait]
pub(super) trait InodeCachePeek {
    async fn peek_attr(&self, ino: Inode) -> Option<FileAttr>;
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
    fn lookup_file_does_not_exist_converts_to_readdir_inode_not_found() {
        let err: ReadDirError = LookupError::FileDoesNotExist.into();
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
