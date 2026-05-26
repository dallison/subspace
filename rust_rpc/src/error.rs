// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusCode {
    Ok,
    Internal,
    DeadlineExceeded,
    Cancelled,
    NotFound,
    InvalidArgument,
}

impl fmt::Display for StatusCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StatusCode::Ok => write!(f, "OK"),
            StatusCode::Internal => write!(f, "INTERNAL"),
            StatusCode::DeadlineExceeded => write!(f, "DEADLINE_EXCEEDED"),
            StatusCode::Cancelled => write!(f, "CANCELLED"),
            StatusCode::NotFound => write!(f, "NOT_FOUND"),
            StatusCode::InvalidArgument => write!(f, "INVALID_ARGUMENT"),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    #[error("subspace error: {0}")]
    Subspace(#[from] subspace_client::SubspaceError),

    #[error("session not open")]
    SessionNotOpen,

    #[error("session already open")]
    SessionAlreadyOpen,

    #[error("method not found: {0}")]
    MethodNotFound(String),

    #[error("timeout: {0}")]
    Timeout(String),

    #[error("deadline exceeded: {0}")]
    DeadlineExceeded(String),

    #[error("cancelled")]
    Cancelled,

    #[error("server error: {0}")]
    ServerError(String),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("protobuf encode error: {0}")]
    Encode(#[from] prost::EncodeError),

    #[error("protobuf decode error: {0}")]
    Decode(#[from] prost::DecodeError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid response: {0}")]
    InvalidResponse(String),
}

pub type Result<T> = std::result::Result<T, RpcError>;

impl RpcError {
    pub fn is_timeout(&self) -> bool {
        matches!(self, RpcError::Timeout(_) | RpcError::DeadlineExceeded(_))
    }

    pub fn is_cancelled(&self) -> bool {
        matches!(self, RpcError::Cancelled)
    }
}
