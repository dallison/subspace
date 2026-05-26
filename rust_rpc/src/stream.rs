// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use subspace_client::{Publisher, Subscriber};

use crate::async_io;
use crate::error::{RpcError, Result};

// ---------------------------------------------------------------------------
// Server-side StreamWriter
// ---------------------------------------------------------------------------

/// Low-level stream writer that sends `RpcResponse` frames to a client.
///
/// Used internally by the server dispatch loop and by typed wrappers
/// generated from IDL definitions.
pub struct StreamWriter {
    pub(crate) response_publisher: Publisher,
    pub(crate) session_id: i32,
    pub(crate) request_id: i32,
    pub(crate) client_id: u64,
    pub(crate) cancelled: Arc<AtomicBool>,
}

impl StreamWriter {
    /// Send a streaming response frame.  Returns `false` if the stream
    /// has been cancelled by the client.
    pub async fn write(&self, result: &prost_types::Any) -> Result<bool> {
        if self.is_cancelled() {
            return Ok(false);
        }

        let resp = subspace_client::proto::RpcResponse {
            error: String::new(),
            result: Some(result.clone()),
            session_id: self.session_id,
            request_id: self.request_id,
            client_id: self.client_id,
            is_last: false,
            is_cancelled: false,
        };

        async_io::wait_for_subscribers(&self.response_publisher, None).await?;
        let data = async_io::encode_to_vec(&resp);
        async_io::publish_message(&self.response_publisher, &data)?;
        Ok(true)
    }

    /// Send the terminal frame that marks the end of the stream.
    pub async fn finish(&self) -> Result<()> {
        let resp = subspace_client::proto::RpcResponse {
            error: String::new(),
            result: None,
            session_id: self.session_id,
            request_id: self.request_id,
            client_id: self.client_id,
            is_last: true,
            is_cancelled: self.is_cancelled(),
        };

        async_io::wait_for_subscribers(&self.response_publisher, None).await?;
        let data = async_io::encode_to_vec(&resp);
        async_io::publish_message(&self.response_publisher, &data)?;
        Ok(())
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

/// Typed wrapper around [`StreamWriter`] for a specific response type.
///
/// This is what generated server stubs expose to user handler implementations.
pub struct TypedStreamWriter<T: prost::Message> {
    inner: StreamWriter,
    type_url: String,
    _phantom: PhantomData<T>,
}

impl<T: prost::Message> TypedStreamWriter<T> {
    pub fn new(inner: StreamWriter, type_url: String) -> Self {
        Self {
            inner,
            type_url,
            _phantom: PhantomData,
        }
    }

    pub async fn write(&self, response: &T) -> Result<bool> {
        let any = async_io::pack_any(response, &self.type_url);
        self.inner.write(&any).await
    }

    pub async fn finish(&self) -> Result<()> {
        self.inner.finish().await
    }

    pub fn cancel(&self) {
        self.inner.cancel();
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.is_cancelled()
    }
}

// ---------------------------------------------------------------------------
// Client-side ResponseStream
// ---------------------------------------------------------------------------

/// Async stream of responses from a server-streaming RPC.
///
/// Implements [`futures::Stream`] so it can be consumed with
/// `StreamExt::next()` and friends.
pub struct ResponseStream<T: prost::Message + Default> {
    response_subscriber: Subscriber,
    session_id: i32,
    request_id: i32,
    client_id: u64,
    timeout: Option<Duration>,
    finished: bool,
    cancel_publisher: Option<Publisher>,
    _phantom: PhantomData<T>,
}

impl<T: prost::Message + Default> ResponseStream<T> {
    pub(crate) fn new(
        response_subscriber: Subscriber,
        session_id: i32,
        request_id: i32,
        client_id: u64,
        timeout: Option<Duration>,
        cancel_publisher: Option<Publisher>,
    ) -> Self {
        Self {
            response_subscriber,
            session_id,
            request_id,
            client_id,
            timeout,
            finished: false,
            cancel_publisher,
            _phantom: PhantomData,
        }
    }

    /// Cancel this streaming RPC.
    pub fn cancel(&self) -> Result<()> {
        if let Some(ref cancel_pub) = self.cancel_publisher {
            let cancel_req = subspace_client::proto::RpcCancelRequest {
                session_id: self.session_id,
                request_id: self.request_id,
                client_id: self.client_id,
            };
            let data = async_io::encode_to_vec(&cancel_req);
            async_io::publish_message(cancel_pub, &data)?;
        }
        Ok(())
    }

    /// Read the next response, asynchronously.
    ///
    /// Returns `None` when the stream is complete.
    pub async fn next(&mut self) -> Option<Result<T>> {
        if self.finished {
            return None;
        }

        loop {
            let fd = self.response_subscriber.get_poll_fd();
            let wait_result = match self.timeout {
                Some(t) => async_io::wait_readable_timeout(fd, t).await,
                None => async_io::wait_readable(fd).await,
            };
            if let Err(e) = wait_result {
                self.finished = true;
                return Some(Err(e));
            }

            let rpc_resp: subspace_client::proto::RpcResponse =
                match async_io::read_message(&self.response_subscriber) {
                    Ok(Some(r)) => r,
                    Ok(None) => continue,
                    Err(e) => {
                        self.finished = true;
                        return Some(Err(e));
                    }
                };

            if rpc_resp.client_id != self.client_id
                || rpc_resp.request_id != self.request_id
                || rpc_resp.session_id != self.session_id
            {
                continue;
            }

            if rpc_resp.is_cancelled {
                self.finished = true;
                return Some(Err(RpcError::Cancelled));
            }

            if !rpc_resp.error.is_empty() {
                self.finished = true;
                return Some(Err(RpcError::ServerError(rpc_resp.error)));
            }

            if rpc_resp.is_last {
                self.finished = true;
                if let Some(ref any) = rpc_resp.result {
                    return Some(async_io::unpack_any::<T>(any));
                }
                return None;
            }

            if let Some(ref any) = rpc_resp.result {
                return Some(async_io::unpack_any::<T>(any));
            }
        }
    }
}

