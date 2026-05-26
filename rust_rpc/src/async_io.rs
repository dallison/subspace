// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

use std::os::unix::io::RawFd;
use std::time::Duration;

use crate::error::{RpcError, Result};

/// Wait until the given fd is readable using a short-polling approach.
/// Uses 100ms poll intervals so that tokio::select can cancel promptly.
pub(crate) async fn wait_readable(fd: RawFd) -> Result<()> {
    loop {
        let result = tokio::task::spawn_blocking(move || {
            poll_fd(fd, 100)
        })
        .await
        .map_err(|e| RpcError::Internal(format!("spawn_blocking join error: {}", e)))?;
        match result {
            Ok(()) => return Ok(()),
            Err(RpcError::DeadlineExceeded(_)) => {
                tokio::task::yield_now().await;
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Wait until the given fd is readable, or timeout expires.
pub(crate) async fn wait_readable_timeout(fd: RawFd, timeout: Duration) -> Result<()> {
    tokio::select! {
        result = wait_readable(fd) => result,
        _ = tokio::time::sleep(timeout) => {
            Err(RpcError::DeadlineExceeded("Timeout waiting for fd".into()))
        }
    }
}

fn poll_fd(fd: RawFd, timeout_ms: i32) -> Result<()> {
    let mut pfd = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    let ret = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
    if ret < 0 {
        return Err(RpcError::Io(std::io::Error::last_os_error()));
    }
    if ret == 0 {
        return Err(RpcError::DeadlineExceeded("poll timeout".into()));
    }
    Ok(())
}

/// Encode a protobuf message to bytes.
pub(crate) fn encode_to_vec(msg: &impl prost::Message) -> Vec<u8> {
    let mut buf = Vec::with_capacity(msg.encoded_len());
    msg.encode(&mut buf).expect("protobuf encode to vec should not fail");
    buf
}

/// Pack a protobuf message into a `prost_types::Any`.
pub fn pack_any<M: prost::Message>(msg: &M, type_url: &str) -> prost_types::Any {
    prost_types::Any {
        type_url: type_url.to_string(),
        value: encode_to_vec(msg),
    }
}

/// Unpack a `prost_types::Any` into a concrete protobuf message.
pub fn unpack_any<T: prost::Message + Default>(any: &prost_types::Any) -> Result<T> {
    T::decode(any.value.as_slice()).map_err(RpcError::Decode)
}

/// Publish a serialized protobuf message on a Subspace publisher.
pub(crate) fn publish_message(
    publisher: &subspace_client::Publisher,
    data: &[u8],
) -> Result<()> {
    let max_size = data.len() as i32;
    let buf_opt = publisher.get_message_buffer(max_size)?;
    let (buf_ptr, _buf_size) = buf_opt.ok_or_else(|| {
        RpcError::Internal(format!("No buffer available for publish on {} ({} bytes requested)", publisher.name(), max_size))
    })?;

    unsafe {
        std::ptr::copy_nonoverlapping(data.as_ptr(), buf_ptr, data.len());
    }

    publisher.publish_message(data.len() as i64)?;
    Ok(())
}

/// Read a protobuf message from a Subspace subscriber.
pub(crate) fn read_message<T: prost::Message + Default>(
    subscriber: &subspace_client::Subscriber,
) -> Result<Option<T>> {
    let msg = subscriber.read_message(subspace_client::ReadMode::ReadNext)?;
    if msg.is_empty() {
        return Ok(None);
    }
    let data = unsafe { msg.as_slice() };
    let decoded = T::decode(data)?;
    Ok(Some(decoded))
}

/// Wait for a publisher to have at least one subscriber, asynchronously.
pub(crate) async fn wait_for_subscribers(
    publisher: &subspace_client::Publisher,
    timeout: Option<Duration>,
) -> Result<()> {
    if publisher.num_subscribers(-1) > 0 {
        return Ok(());
    }
    let fd = publisher.get_poll_fd();
    match timeout {
        Some(t) => wait_readable_timeout(fd, t).await?,
        None => wait_readable(fd).await?,
    }
    Ok(())
}
