// Copyright 2024-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

use std::io;

#[derive(Debug, thiserror::Error)]
pub enum SubspaceError {
    #[error("internal error: {0}")]
    Internal(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("not connected: {0}")]
    NotConnected(String),

    #[error("timeout: {0}")]
    Timeout(String),

    #[error("checksum error")]
    ChecksumError,

    #[error("io error: {0}")]
    Io(#[from] io::Error),

    #[error("protobuf encode error: {0}")]
    ProtobufEncode(#[from] prost::EncodeError),

    #[error("protobuf decode error: {0}")]
    ProtobufDecode(#[from] prost::DecodeError),

    #[error("nix error: {0}")]
    Nix(#[from] nix::Error),

    #[error("server error: {0}")]
    ServerError(String),
}

pub type Result<T> = std::result::Result<T, SubspaceError>;
