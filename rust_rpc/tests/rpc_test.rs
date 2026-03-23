// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use subspace_rpc::error::{RpcError, Result};

static FIXTURE_COUNTER: AtomicU32 = AtomicU32::new(0);
use subspace_rpc::generated::rpc_test_client::TestServiceClient;
use subspace_rpc::generated::rpc_test_server::{TestServiceHandler, TestServiceServer};
use subspace_rpc::stream::TypedStreamWriter;
use subspace_rpc::test_proto::*;

// ---------------------------------------------------------------------------
// Test fixture: starts a subspace server subprocess
// ---------------------------------------------------------------------------

struct SubspaceTestFixture {
    server_process: tokio::process::Child,
    socket_path: String,
}

impl SubspaceTestFixture {
    async fn new() -> Self {
        let id = FIXTURE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let socket_path = format!(
            "/tmp/subspace_rust_test_{}_{}",
            std::process::id(),
            id
        );

        let _ = std::fs::remove_file(&socket_path);

        let (read_fd, write_fd) = nix_pipe();

        let server_bin = std::env::var("SUBSPACE_SERVER_BIN")
            .unwrap_or_else(|_| {
                let workspace = env!("CARGO_MANIFEST_DIR");
                format!("{}/bazel-bin/server/subspace_server", workspace.trim_end_matches("/rust_rpc"))
            });

        let child = tokio::process::Command::new(&server_bin)
            .arg("--socket")
            .arg(&socket_path)
            .arg("--local")
            .arg("--notify_fd")
            .arg(write_fd.to_string())
            .spawn()
            .unwrap_or_else(|e| panic!("Failed to start subspace server at {}: {}", server_bin, e));

        unsafe { libc::close(write_fd); }

        let mut buf = [0u8; 8];
        tokio::task::spawn_blocking(move || {
            unsafe { libc::read(read_fd, buf.as_mut_ptr() as *mut libc::c_void, 8) };
            unsafe { libc::close(read_fd); }
        })
        .await
        .expect("Failed waiting for server readiness");

        tokio::time::sleep(Duration::from_millis(100)).await;

        Self {
            server_process: child,
            socket_path,
        }
    }

    fn socket(&self) -> &str {
        &self.socket_path
    }

    async fn shutdown(mut self) {
        self.server_process.kill().await.ok();
        self.server_process.wait().await.ok();
        let _ = std::fs::remove_file(&self.socket_path);
    }
}

fn nix_pipe() -> (i32, i32) {
    let mut fds = [0i32; 2];
    let ret = unsafe { libc::pipe(fds.as_mut_ptr()) };
    assert_eq!(ret, 0, "pipe() failed");
    (fds[0], fds[1])
}

// ---------------------------------------------------------------------------
// Test handler implementation
// ---------------------------------------------------------------------------

struct TestHandler;

#[async_trait::async_trait]
impl TestServiceHandler for TestHandler {
    async fn test_method(&self, request: TestRequest) -> Result<TestResponse> {
        Ok(TestResponse {
            message: format!("hello {}", request.message),
            foo: 0,
        })
    }

    async fn perf_method(&self, request: PerfRequest) -> Result<PerfResponse> {
        Ok(PerfResponse {
            client_send_time: request.send_time,
            server_send_time: 0,
        })
    }

    async fn stream_method(
        &self,
        request: TestRequest,
        writer: TypedStreamWriter<TestResponse>,
    ) -> Result<()> {
        for i in 0..5 {
            let resp = TestResponse {
                message: format!("{} stream {}", request.message, i),
                foo: i,
            };
            if !writer.write(&resp).await? {
                return Ok(());
            }
        }
        writer.finish().await?;
        Ok(())
    }

    async fn error_method(&self, _request: TestRequest) -> Result<TestResponse> {
        Err(RpcError::Internal("test error".into()))
    }

    async fn timeout_method(&self, _request: TestRequest) -> Result<TestResponse> {
        tokio::time::sleep(Duration::from_secs(10)).await;
        Ok(TestResponse {
            message: "should not reach".into(),
            foo: 0,
        })
    }
}

// ---------------------------------------------------------------------------
// Helper: start server + client
// ---------------------------------------------------------------------------

async fn setup() -> (
    SubspaceTestFixture,
    tokio::sync::watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
) {
    let fixture = SubspaceTestFixture::new().await;

    let socket = fixture.socket().to_string();

    let handler = Arc::new(TestHandler);
    let rpc_server = TestServiceServer::new(&socket, handler);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let server_handle = tokio::spawn(async move {
        let _ = rpc_server.run(shutdown_rx).await;
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    (fixture, shutdown_tx, server_handle)
}

async fn teardown(
    fixture: SubspaceTestFixture,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    server_handle: tokio::task::JoinHandle<()>,
) {
    let _ = shutdown_tx.send(true);
    tokio::time::sleep(Duration::from_millis(100)).await;
    server_handle.abort();
    let _ = server_handle.await;
    fixture.shutdown().await;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_basic_rpc() {
    let (fixture, shutdown_tx, server_handle) = setup().await;

    let mut client = TestServiceClient::create(1, fixture.socket())
        .await
        .expect("Failed to create client");

    let request = TestRequest {
        message: "world".into(),
        stream_period: 0,
    };
    let response = client
        .test_method(&request, Some(Duration::from_secs(5)))
        .await
        .expect("test_method failed");

    assert_eq!(response.message, "hello world");

    client.close().await.expect("Failed to close client");
    teardown(fixture, shutdown_tx, server_handle).await;
}

#[tokio::test]
async fn test_streaming_rpc() {
    let (fixture, shutdown_tx, server_handle) = setup().await;

    let mut client = TestServiceClient::create(2, fixture.socket())
        .await
        .expect("Failed to create client");

    let request = TestRequest {
        message: "test".into(),
        stream_period: 0,
    };
    let mut stream = client
        .stream_method(&request, Some(Duration::from_secs(5)))
        .await
        .expect("stream_method failed");

    let mut responses = Vec::new();
    while let Some(result) = stream.next().await {
        let resp = result.expect("Stream error");
        responses.push(resp.message);
    }

    assert_eq!(responses.len(), 5);
    for (i, msg) in responses.iter().enumerate() {
        assert_eq!(msg, &format!("test stream {}", i));
    }

    client.close().await.expect("Failed to close client");
    teardown(fixture, shutdown_tx, server_handle).await;
}

#[tokio::test]
async fn test_error_rpc() {
    let (fixture, shutdown_tx, server_handle) = setup().await;

    let mut client = TestServiceClient::create(3, fixture.socket())
        .await
        .expect("Failed to create client");

    let request = TestRequest {
        message: "error".into(),
        stream_period: 0,
    };
    let result = client
        .error_method(&request, Some(Duration::from_secs(5)))
        .await;

    assert!(result.is_err(), "Expected error from error_method");
    let err = result.unwrap_err();
    let err_msg = format!("{}", err);
    assert!(
        err_msg.contains("test error"),
        "Expected 'test error' in error message, got: {}",
        err_msg
    );

    client.close().await.expect("Failed to close client");
    teardown(fixture, shutdown_tx, server_handle).await;
}

#[tokio::test]
async fn test_timeout_rpc() {
    let (fixture, shutdown_tx, server_handle) = setup().await;

    let mut client = TestServiceClient::create(4, fixture.socket())
        .await
        .expect("Failed to create client");

    let request = TestRequest {
        message: "timeout".into(),
        stream_period: 0,
    };
    let result = client
        .timeout_method(&request, Some(Duration::from_millis(500)))
        .await;

    assert!(result.is_err(), "Expected timeout error");
    let err = result.unwrap_err();
    assert!(
        err.is_timeout(),
        "Expected timeout error, got: {}",
        err
    );

    client.close().await.expect("Failed to close client");
    teardown(fixture, shutdown_tx, server_handle).await;
}
