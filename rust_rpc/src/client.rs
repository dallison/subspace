// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;

use subspace_client::{Client, Publisher, PublisherOptions, Subscriber, SubscriberOptions};

use crate::async_io;
use crate::common::*;
use crate::error::{RpcError, Result};
use crate::stream::ResponseStream;

pub struct RpcClient {
    service: String,
    client_id: u64,
    subspace_socket: String,
    session_id: i32,
    next_request_id: AtomicI32,

    client: Option<Client>,
    service_pub: Option<Publisher>,
    service_sub: Option<Subscriber>,
    methods: HashMap<i32, MethodInfo>,
    method_name_to_id: HashMap<String, i32>,
    closed: bool,
}

impl RpcClient {
    pub fn new(client_id: u64, subspace_socket: &str, service: &str) -> Self {
        Self {
            service: service.to_string(),
            client_id,
            subspace_socket: subspace_socket.to_string(),
            session_id: 0,
            next_request_id: AtomicI32::new(0),
            client: None,
            service_pub: None,
            service_sub: None,
            methods: HashMap::new(),
            method_name_to_id: HashMap::new(),
            closed: false,
        }
    }

    pub fn session_id(&self) -> i32 {
        self.session_id
    }

    pub fn client_id(&self) -> u64 {
        self.client_id
    }

    fn next_request_id(&self) -> i32 {
        self.next_request_id.fetch_add(1, Ordering::Relaxed)
    }

    pub async fn open(&mut self, timeout: Option<Duration>) -> Result<()> {
        if self.session_id != 0 {
            return Err(RpcError::SessionAlreadyOpen);
        }

        log::info!("RpcClient: connecting to {} at {}", self.service, self.subspace_socket);
        let client = Client::new(&self.subspace_socket, &self.service)?;
        self.client = Some(client);

        let request_channel = service_request_channel(&self.service);
        let response_channel = service_response_channel(&self.service);

        let pub_opts = PublisherOptions {
            slot_size: RPC_REQUEST_SLOT_SIZE,
            num_slots: RPC_REQUEST_NUM_SLOTS,
            reliable: true,
            channel_type: "subspace.RpcServerRequest".to_string(),
            ..Default::default()
        };
        self.service_pub = Some(
            self.client
                .as_ref()
                .unwrap()
                .create_publisher(&request_channel, &pub_opts)?,
        );

        let sub_opts = SubscriberOptions {
            reliable: true,
            channel_type: "subspace.RpcServerResponse".to_string(),
            ..Default::default()
        };
        self.service_sub = Some(
            self.client
                .as_ref()
                .unwrap()
                .create_subscriber(&response_channel, &sub_opts)?,
        );

        let request_id = self.next_request_id();
        let req = subspace_client::proto::RpcServerRequest {
            client_id: self.client_id,
            request_id,
            request: Some(
                subspace_client::proto::rpc_server_request::Request::Open(
                    subspace_client::proto::RpcOpenRequest {},
                ),
            ),
        };
        log::info!("RpcClient: publishing open request");
        self.publish_server_request(&req, timeout).await?;
        log::info!("RpcClient: waiting for open response");
        let resp = self.read_server_response(request_id, timeout).await?;
        log::info!("RpcClient: got open response");
        let open = match resp.response {
            Some(subspace_client::proto::rpc_server_response::Response::Open(o)) => o,
            _ => {
                return Err(RpcError::InvalidResponse(
                    "Expected open response".into(),
                ))
            }
        };

        self.session_id = open.session_id;

        for method in &open.methods {
            let req_chan = method
                .request_channel
                .as_ref()
                .ok_or_else(|| RpcError::InvalidResponse("Missing request channel".into()))?;
            let resp_chan = method
                .response_channel
                .as_ref()
                .ok_or_else(|| RpcError::InvalidResponse("Missing response channel".into()))?;

            let method_pub_opts = PublisherOptions {
                slot_size: req_chan.slot_size,
                num_slots: req_chan.num_slots,
                reliable: true,
                channel_type: req_chan.r#type.clone(),
                ..Default::default()
            };
            let method_pub = self
                .client
                .as_ref()
                .unwrap()
                .create_publisher(&req_chan.name, &method_pub_opts)?;

            let method_sub_opts = SubscriberOptions {
                reliable: true,
                channel_type: resp_chan.r#type.clone(),
                ..Default::default()
            };
            let method_sub = self
                .client
                .as_ref()
                .unwrap()
                .create_subscriber(&resp_chan.name, &method_sub_opts)?;

            let cancel_pub = if !method.cancel_channel.is_empty() {
                let cancel_opts = PublisherOptions {
                    slot_size: CANCEL_CHANNEL_SLOT_SIZE,
                    num_slots: CANCEL_CHANNEL_NUM_SLOTS,
                    reliable: true,
                    ..Default::default()
                };
                Some(
                    self.client
                        .as_ref()
                        .unwrap()
                        .create_publisher(&method.cancel_channel, &cancel_opts)?,
                )
            } else {
                None
            };

            let info = MethodInfo {
                name: method.name.clone(),
                id: method.id,
                request_type: req_chan.r#type.clone(),
                response_type: resp_chan.r#type.clone(),
                slot_size: req_chan.slot_size,
                num_slots: req_chan.num_slots,
                request_publisher: method_pub,
                response_subscriber: method_sub,
                cancel_publisher: cancel_pub,
            };

            self.method_name_to_id
                .insert(info.name.clone(), info.id);
            self.methods.insert(info.id, info);
        }

        log::info!(
            "Opened service {} with session ID: {}",
            self.service,
            self.session_id
        );
        Ok(())
    }

    pub async fn close(&mut self, timeout: Option<Duration>) -> Result<()> {
        if self.closed {
            return Err(RpcError::Internal("Client is closed".into()));
        }
        if self.session_id == 0 {
            return Err(RpcError::SessionNotOpen);
        }

        let request_id = self.next_request_id();
        let req = subspace_client::proto::RpcServerRequest {
            client_id: self.client_id,
            request_id,
            request: Some(
                subspace_client::proto::rpc_server_request::Request::Close(
                    subspace_client::proto::RpcCloseRequest {
                        session_id: self.session_id,
                    },
                ),
            ),
        };

        self.publish_server_request(&req, timeout).await?;
        let _resp = self.read_server_response(request_id, timeout).await?;
        self.session_id = 0;
        log::info!("Closed service {}", self.service);
        Ok(())
    }

    /// Invoke a unary RPC by method ID.
    pub async fn call<Req, Resp>(
        &self,
        method_id: i32,
        request: &Req,
        timeout: Option<Duration>,
    ) -> Result<Resp>
    where
        Req: prost::Message,
        Resp: prost::Message + Default,
    {
        let any = self.invoke_method(method_id, request, timeout).await?;
        async_io::unpack_any::<Resp>(&any)
    }

    /// Invoke a server-streaming RPC by method ID.
    pub async fn call_server_streaming<Req, Resp>(
        &self,
        method_id: i32,
        request: &Req,
        timeout: Option<Duration>,
    ) -> Result<ResponseStream<Resp>>
    where
        Req: prost::Message,
        Resp: prost::Message + Default,
    {
        if self.closed {
            return Err(RpcError::Internal("Client is closed".into()));
        }

        let method = self
            .methods
            .get(&method_id)
            .ok_or_else(|| RpcError::MethodNotFound(format!("Method {} not found", method_id)))?;

        let request_id = self.next_request_id();
        let any = async_io::pack_any(request, &method.request_type);

        let rpc_req = subspace_client::proto::RpcRequest {
            method: method_id,
            argument: Some(any),
            session_id: self.session_id,
            request_id,
            client_id: self.client_id,
        };

        self.publish_method_request(method, &rpc_req, timeout)
            .await?;

        Ok(ResponseStream::new(
            method.response_subscriber.clone(),
            self.session_id,
            request_id,
            self.client_id,
            timeout,
            method.cancel_publisher.clone(),
        ))
    }

    /// Find a method ID by name.
    pub fn find_method(&self, name: &str) -> Option<i32> {
        self.method_name_to_id.get(name).copied()
    }

    async fn invoke_method<Req: prost::Message>(
        &self,
        method_id: i32,
        request: &Req,
        timeout: Option<Duration>,
    ) -> Result<prost_types::Any> {
        if self.closed {
            return Err(RpcError::Internal("Client is closed".into()));
        }

        let method = self
            .methods
            .get(&method_id)
            .ok_or_else(|| RpcError::MethodNotFound(format!("Method {} not found", method_id)))?;

        let request_id = self.next_request_id();
        let any = async_io::pack_any(request, &method.request_type);

        let rpc_req = subspace_client::proto::RpcRequest {
            method: method_id,
            argument: Some(any),
            session_id: self.session_id,
            request_id,
            client_id: self.client_id,
        };

        self.publish_method_request(method, &rpc_req, timeout)
            .await?;

        self.read_method_response(method, request_id, timeout)
            .await
    }

    async fn publish_server_request(
        &self,
        req: &subspace_client::proto::RpcServerRequest,
        timeout: Option<Duration>,
    ) -> Result<()> {
        let pub_ = self
            .service_pub
            .as_ref()
            .ok_or(RpcError::SessionNotOpen)?;

        async_io::wait_for_subscribers(pub_, timeout).await?;

        let data = async_io::encode_to_vec(req);
        loop {
            match async_io::publish_message(pub_, &data) {
                Ok(()) => {
                    return Ok(());
                }
                Err(RpcError::Internal(_)) => {
                    let fd = pub_.get_poll_fd();
                    match timeout {
                        Some(t) => async_io::wait_readable_timeout(fd, t).await?,
                        None => async_io::wait_readable(fd).await?,
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn read_server_response(
        &self,
        request_id: i32,
        timeout: Option<Duration>,
    ) -> Result<subspace_client::proto::RpcServerResponse> {
        let sub = self
            .service_sub
            .as_ref()
            .ok_or(RpcError::SessionNotOpen)?;

        loop {
            let fd = sub.get_poll_fd();
            match timeout {
                Some(t) => async_io::wait_readable_timeout(fd, t).await?,
                None => async_io::wait_readable(fd).await?,
            }

            loop {
                match async_io::read_message::<subspace_client::proto::RpcServerResponse>(sub)? {
                    Some(resp) => {
                        if resp.client_id == self.client_id && resp.request_id == request_id {
                            if !resp.error.is_empty() {
                                return Err(RpcError::ServerError(resp.error));
                            }
                            return Ok(resp);
                        }
                    }
                    None => break,
                }
            }
        }
    }

    async fn publish_method_request(
        &self,
        method: &MethodInfo,
        req: &subspace_client::proto::RpcRequest,
        timeout: Option<Duration>,
    ) -> Result<()> {
        async_io::wait_for_subscribers(&method.request_publisher, timeout).await?;

        let data = async_io::encode_to_vec(req);
        loop {
            match async_io::publish_message(&method.request_publisher, &data) {
                Ok(()) => return Ok(()),
                Err(RpcError::Internal(_)) => {
                    let fd = method.request_publisher.get_poll_fd();
                    match timeout {
                        Some(t) => async_io::wait_readable_timeout(fd, t).await?,
                        None => async_io::wait_readable(fd).await?,
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn read_method_response(
        &self,
        method: &MethodInfo,
        request_id: i32,
        timeout: Option<Duration>,
    ) -> Result<prost_types::Any> {
        loop {
            let fd = method.response_subscriber.get_poll_fd();
            match timeout {
                Some(t) => async_io::wait_readable_timeout(fd, t).await?,
                None => async_io::wait_readable(fd).await?,
            }

            loop {
                match async_io::read_message::<subspace_client::proto::RpcResponse>(
                    &method.response_subscriber,
                )? {
                    Some(rpc_resp) => {
                        if rpc_resp.client_id == self.client_id
                            && rpc_resp.request_id == request_id
                            && rpc_resp.session_id == self.session_id
                        {
                            if !rpc_resp.error.is_empty() {
                                return Err(RpcError::Internal(format!(
                                    "Error waiting for response: {}",
                                    rpc_resp.error
                                )));
                            }
                            return rpc_resp.result.ok_or_else(|| {
                                RpcError::InvalidResponse("Missing result in response".into())
                            });
                        }
                    }
                    None => break,
                }
            }
        }
    }
}

impl Drop for RpcClient {
    fn drop(&mut self) {
        if self.session_id != 0 {
            log::warn!(
                "RpcClient for service {} dropped without closing session {}",
                self.service,
                self.session_id
            );
        }
    }
}
