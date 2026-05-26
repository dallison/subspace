// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

extern crate self as subspace_rpc;

pub mod async_io;
pub mod client;
pub mod common;
pub mod error;
pub mod server;
pub mod stream;

pub use client::RpcClient;
pub use common::MethodOptions;
pub use error::{RpcError, Result};
pub use server::{MethodHandler, RpcServer, StreamingMethodHandler};
pub use stream::{ResponseStream, StreamWriter, TypedStreamWriter};

/// Proto types generated from rpc_test.proto (for testing).
pub mod test_proto {
    include!(concat!(env!("OUT_DIR"), "/rpc.rs"));
}

/// Generated service stubs for testing (produced by the subspace_rpc protoc plugin).
#[allow(unused_imports, dead_code)]
pub mod generated {
    pub use crate::test_proto::*;
    #[allow(unused_imports, dead_code)]
    pub mod rpc_test_client {
        include!(concat!(env!("OUT_DIR"), "/rpc_test.subspace.rpc_client.rs"));
    }
    #[allow(unused_imports, dead_code)]
    pub mod rpc_test_server {
        include!(concat!(env!("OUT_DIR"), "/rpc_test.subspace.rpc_server.rs"));
    }
}
