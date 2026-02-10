//! Shared types and helpers used by both `MesaFS` and `RepoFs`.

use thiserror::Error;

pub(super) use super::icache::InodeControlBlock;

#[derive(Debug, Error)]
pub enum LookupError {
    #[error("inode not found")]
    InodeNotFound,

    #[error("file does not exist")]
    FileDoesNotExist,

    #[error("remote mesa error: {0}")]
    RemoteMesaError(#[from] mesa_dev::error::MesaError),
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

    #[error("remote mesa error: {0}")]
    RemoteMesaError(#[from] mesa_dev::error::MesaError),

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

    #[error("remote mesa error: {0}")]
    RemoteMesaError(#[from] mesa_dev::error::MesaError),

    #[error("inode is not a directory")]
    NotADirectory,

    #[error("operation not permitted")]
    NotPermitted,
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
