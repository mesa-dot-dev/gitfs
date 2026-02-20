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

pub(super) fn mesa_api_error_to_io(e: MesaApiError) -> std::io::Error {
    match &e {
        MesaApiError::Response { status, .. } if *status == 404 => {
            std::io::Error::from_raw_os_error(libc::ENOENT)
        }
        MesaApiError::Reqwest(_)
        | MesaApiError::ReqwestMiddleware(_)
        | MesaApiError::Serde(_)
        | MesaApiError::SerdePath(_)
        | MesaApiError::Io(_)
        | MesaApiError::Response { .. } => std::io::Error::other(e),
    }
}
