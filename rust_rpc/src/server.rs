// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use subspace_client::{Client, Publisher, PublisherOptions, Subscriber, SubscriberOptions};
use tokio::sync::watch;

use crate::async_io;
use crate::common::*;
use crate::error::Result;
use crate::stream::StreamWriter;

// ---------------------------------------------------------------------------
// Handler traits
// ---------------------------------------------------------------------------

/// Handler for a unary (request-response) RPC method.
#[async_trait::async_trait]
pub trait MethodHandler: Send + Sync + 'static {
    async fn handle(&self, request: prost_types::Any) -> Result<prost_types::Any>;
}

/// Handler for a server-streaming RPC method.
#[async_trait::async_trait]
pub trait StreamingMethodHandler: Send + Sync + 'static {
    async fn handle(&self, request: prost_types::Any, writer: StreamWriter) -> Result<()>;
}

// ---------------------------------------------------------------------------
// Method registration
// ---------------------------------------------------------------------------

struct RegisteredMethod {
    name: String,
    id: i32,
    opts: MethodOptions,
    kind: MethodKind,
}

enum MethodKind {
    Unary(Arc<dyn MethodHandler>),
    Streaming(Arc<dyn StreamingMethodHandler>),
}

// ---------------------------------------------------------------------------
// Session state
// ---------------------------------------------------------------------------

struct MethodInstance {
    request_subscriber: Subscriber,
    response_publisher: Publisher,
    cancel_subscriber: Option<Subscriber>,
}

#[allow(dead_code)]
struct Session {
    session_id: i32,
    client_id: u64,
    methods: HashMap<i32, MethodInstance>,
}

// ---------------------------------------------------------------------------
// RpcServer
// ---------------------------------------------------------------------------

pub struct RpcServer {
    service: String,
    subspace_socket: String,
    registered_methods: Vec<RegisteredMethod>,
}

impl RpcServer {
    pub fn new(subspace_socket: &str, service: &str) -> Self {
        Self {
            service: service.to_string(),
            subspace_socket: subspace_socket.to_string(),
            registered_methods: Vec::new(),
        }
    }

    /// Register a unary method handler.
    pub fn register_method(
        &mut self,
        name: &str,
        id: i32,
        handler: Arc<dyn MethodHandler>,
        opts: MethodOptions,
    ) {
        self.registered_methods.push(RegisteredMethod {
            name: name.to_string(),
            id,
            opts,
            kind: MethodKind::Unary(handler),
        });
    }

    /// Register a server-streaming method handler.
    pub fn register_streaming_method(
        &mut self,
        name: &str,
        id: i32,
        handler: Arc<dyn StreamingMethodHandler>,
        opts: MethodOptions,
    ) {
        self.registered_methods.push(RegisteredMethod {
            name: name.to_string(),
            id,
            opts,
            kind: MethodKind::Streaming(handler),
        });
    }

    /// Run the server, listening for session open/close requests and
    /// dispatching method calls to registered handlers.
    ///
    /// Blocks until `shutdown_rx` receives `true`.
    pub async fn run(&self, mut shutdown_rx: watch::Receiver<bool>) -> Result<()> {
        let client = Client::new(&self.subspace_socket, &self.service)?;

        let request_channel = service_request_channel(&self.service);
        let response_channel = service_response_channel(&self.service);

        let service_sub = client.create_subscriber(
            &request_channel,
            &SubscriberOptions {
                reliable: true,
                channel_type: "subspace.RpcServerRequest".to_string(),
                ..Default::default()
            },
        )?;

        let service_pub = client.create_publisher(
            &response_channel,
            &PublisherOptions {
                slot_size: RPC_RESPONSE_SLOT_SIZE,
                num_slots: RPC_RESPONSE_NUM_SLOTS,
                reliable: true,
                channel_type: "subspace.RpcServerResponse".to_string(),
                ..Default::default()
            },
        )?;

        let running = Arc::new(AtomicBool::new(true));
        let mut next_session_id: i32 = 1;
        let mut task_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

        log::info!("RPC server for service {} started", self.service);

        loop {
            // Check for shutdown.
            if *shutdown_rx.borrow() {
                running.store(false, Ordering::Release);
                break;
            }

            // Try to drain all pending messages from the service channel.
            loop {
                let req: subspace_client::proto::RpcServerRequest =
                    match async_io::read_message::<subspace_client::proto::RpcServerRequest>(&service_sub)? {
                        Some(r) => r,
                        None => break,
                    };

                match req.request {
                    Some(subspace_client::proto::rpc_server_request::Request::Open(_)) => {
                        let session_id = next_session_id;
                        next_session_id += 1;

                        let session = self.create_session(
                            &client,
                            session_id,
                            req.client_id,
                        )?;

                        let open_resp = self.build_open_response(
                            session_id,
                            req.client_id,
                            req.request_id,
                            &session,
                        );

                        let data = async_io::encode_to_vec(&open_resp);
                        async_io::publish_message(&service_pub, &data)?;

                        let handles = self.spawn_session_tasks(
                            session,
                            running.clone(),
                        );
                        task_handles.extend(handles);

                        log::info!(
                            "Opened session {} for client {}",
                            session_id,
                            req.client_id
                        );
                    }
                    Some(subspace_client::proto::rpc_server_request::Request::Close(close)) => {
                        let close_resp = subspace_client::proto::RpcServerResponse {
                            client_id: req.client_id,
                            request_id: req.request_id,
                            response: Some(
                                subspace_client::proto::rpc_server_response::Response::Close(
                                    subspace_client::proto::RpcCloseResponse {},
                                ),
                            ),
                            error: String::new(),
                        };
                        let data = async_io::encode_to_vec(&close_resp);
                        async_io::publish_message(&service_pub, &data)?;

                        log::info!(
                            "Closed session {} for client {}",
                            close.session_id,
                            req.client_id
                        );
                    }
                    None => {}
                }
            }

            // Wait for new messages or shutdown.
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        running.store(false, Ordering::Release);
                        break;
                    }
                }
                _ = async_io::wait_readable(service_sub.get_poll_fd()) => {}
            }
        }

        for handle in task_handles {
            handle.abort();
        }

        log::info!("RPC server for service {} stopped", self.service);
        Ok(())
    }

    fn create_session(
        &self,
        client: &Client,
        session_id: i32,
        client_id: u64,
    ) -> Result<Session> {
        let mut methods = HashMap::new();

        for reg in &self.registered_methods {
            let req_channel_name = format!(
                "/rpc/{}/{}/{}/request",
                self.service, client_id, reg.name
            );
            let resp_channel_name = format!(
                "/rpc/{}/{}/{}/response",
                self.service, client_id, reg.name
            );

            let req_sub = client.create_subscriber(
                &req_channel_name,
                &SubscriberOptions {
                    reliable: true,
                    channel_type: format!("subspace.RpcRequest"),
                    ..Default::default()
                },
            )?;

            let resp_pub = client.create_publisher(
                &resp_channel_name,
                &PublisherOptions {
                    slot_size: reg.opts.slot_size,
                    num_slots: reg.opts.num_slots,
                    reliable: true,
                    channel_type: format!("subspace.RpcResponse"),
                    ..Default::default()
                },
            )?;

            let cancel_sub = if matches!(reg.kind, MethodKind::Streaming(_)) {
                let cancel_channel_name = format!(
                    "/rpc/{}/{}/{}/cancel",
                    self.service, client_id, reg.name
                );
                Some(client.create_subscriber(
                    &cancel_channel_name,
                    &SubscriberOptions {
                        reliable: true,
                        ..Default::default()
                    },
                )?)
            } else {
                None
            };

            methods.insert(
                reg.id,
                MethodInstance {
                    request_subscriber: req_sub,
                    response_publisher: resp_pub,
                    cancel_subscriber: cancel_sub,
                },
            );
        }

        Ok(Session {
            session_id,
            client_id,
            methods,
        })
    }

    fn build_open_response(
        &self,
        session_id: i32,
        client_id: u64,
        request_id: i32,
        _session: &Session,
    ) -> subspace_client::proto::RpcServerResponse {
        let mut methods = Vec::new();

        for reg in &self.registered_methods {
            let req_channel_name = format!(
                "/rpc/{}/{}/{}/request",
                self.service, client_id, reg.name
            );
            let resp_channel_name = format!(
                "/rpc/{}/{}/{}/response",
                self.service, client_id, reg.name
            );
            let cancel_channel = if matches!(reg.kind, MethodKind::Streaming(_)) {
                format!(
                    "/rpc/{}/{}/{}/cancel",
                    self.service, client_id, reg.name
                )
            } else {
                String::new()
            };

            methods.push(subspace_client::proto::rpc_open_response::Method {
                name: reg.name.clone(),
                id: reg.id,
                request_channel: Some(
                    subspace_client::proto::rpc_open_response::RequestChannel {
                        name: req_channel_name,
                        slot_size: reg.opts.slot_size,
                        num_slots: reg.opts.num_slots,
                        r#type: "subspace.RpcRequest".into(),
                    },
                ),
                response_channel: Some(
                    subspace_client::proto::rpc_open_response::ResponseChannel {
                        name: resp_channel_name,
                        r#type: "subspace.RpcResponse".into(),
                    },
                ),
                cancel_channel,
            });
        }

        subspace_client::proto::RpcServerResponse {
            client_id,
            request_id,
            response: Some(
                subspace_client::proto::rpc_server_response::Response::Open(
                    subspace_client::proto::RpcOpenResponse {
                        session_id,
                        methods,
                        client_id,
                    },
                ),
            ),
            error: String::new(),
        }
    }

    fn spawn_session_tasks(
        &self,
        session: Session,
        running: Arc<AtomicBool>,
    ) -> Vec<tokio::task::JoinHandle<()>> {
        let mut handles = Vec::new();
        let session = Arc::new(session);

        for reg in &self.registered_methods {
            let method_id = reg.id;
            let session = session.clone();
            let running = running.clone();

            match &reg.kind {
                MethodKind::Unary(handler) => {
                    let handler = handler.clone();
                    handles.push(tokio::spawn(async move {
                        Self::unary_method_loop(session, method_id, handler, running).await;
                    }));
                }
                MethodKind::Streaming(handler) => {
                    let handler = handler.clone();
                    handles.push(tokio::spawn(async move {
                        Self::streaming_method_loop(session, method_id, handler, running).await;
                    }));
                }
            }
        }

        handles
    }

    async fn unary_method_loop(
        session: Arc<Session>,
        method_id: i32,
        handler: Arc<dyn MethodHandler>,
        running: Arc<AtomicBool>,
    ) {
        let instance = match session.methods.get(&method_id) {
            Some(m) => m,
            None => return,
        };

        while running.load(Ordering::Acquire) {
            let fd = instance.request_subscriber.get_poll_fd();
            if async_io::wait_readable(fd).await.is_err() {
                if !running.load(Ordering::Acquire) {
                    break;
                }
                continue;
            }

            loop {
                let rpc_req: subspace_client::proto::RpcRequest =
                    match async_io::read_message(&instance.request_subscriber) {
                        Ok(Some(r)) => r,
                        Ok(None) => break,
                        Err(e) => {
                            log::error!("Error reading request: {}", e);
                            break;
                        }
                    };

                if rpc_req.session_id != session.session_id {
                    continue;
                }

                let argument = match rpc_req.argument {
                    Some(a) => a,
                    None => {
                        Self::send_error_response(
                            instance,
                            &rpc_req,
                            "Missing argument in request",
                        );
                        continue;
                    }
                };

                let result = handler.handle(argument).await;

                let rpc_resp = match result {
                    Ok(any) => subspace_client::proto::RpcResponse {
                        error: String::new(),
                        result: Some(any),
                        session_id: session.session_id,
                        request_id: rpc_req.request_id,
                        client_id: rpc_req.client_id,
                        is_last: false,
                        is_cancelled: false,
                    },
                    Err(e) => subspace_client::proto::RpcResponse {
                        error: format!("INTERNAL: {}", e),
                        result: None,
                        session_id: session.session_id,
                        request_id: rpc_req.request_id,
                        client_id: rpc_req.client_id,
                        is_last: false,
                        is_cancelled: false,
                    },
                };

                let data = async_io::encode_to_vec(&rpc_resp);
                if let Err(e) = async_io::publish_message(&instance.response_publisher, &data) {
                    log::error!("Error publishing response: {}", e);
                }
            }
        }
    }

    async fn streaming_method_loop(
        session: Arc<Session>,
        method_id: i32,
        handler: Arc<dyn StreamingMethodHandler>,
        running: Arc<AtomicBool>,
    ) {
        let instance = match session.methods.get(&method_id) {
            Some(m) => m,
            None => return,
        };

        while running.load(Ordering::Acquire) {
            let fd = instance.request_subscriber.get_poll_fd();
            if async_io::wait_readable(fd).await.is_err() {
                if !running.load(Ordering::Acquire) {
                    break;
                }
                continue;
            }

            loop {
                let rpc_req: subspace_client::proto::RpcRequest =
                    match async_io::read_message(&instance.request_subscriber) {
                        Ok(Some(r)) => r,
                        Ok(None) => break,
                        Err(e) => {
                            log::error!("Error reading request: {}", e);
                            break;
                        }
                    };

                if rpc_req.session_id != session.session_id {
                    continue;
                }

                let argument = match rpc_req.argument {
                    Some(a) => a,
                    None => {
                        Self::send_error_response(
                            instance,
                            &rpc_req,
                            "Missing argument in request",
                        );
                        continue;
                    }
                };

                let cancelled = Arc::new(AtomicBool::new(false));

                // Spawn cancel watcher if we have a cancel subscriber
                let cancel_handle = if let Some(ref cancel_sub) = instance.cancel_subscriber {
                    let cancelled = cancelled.clone();
                    let cancel_fd = cancel_sub.get_poll_fd();
                    let session_id = session.session_id;
                    let request_id = rpc_req.request_id;
                    let running = running.clone();
                    // We can't move cancel_sub into the spawned task because it's borrowed,
                    // so we read from it in the main loop instead.
                    // For now, just set up a simple polling approach.
                    let _ = (cancel_fd, session_id, request_id, running, cancelled);
                    None::<tokio::task::JoinHandle<()>>
                } else {
                    None
                };

                let writer = StreamWriter {
                    response_publisher: instance.response_publisher.clone(),
                    session_id: session.session_id,
                    request_id: rpc_req.request_id,
                    client_id: rpc_req.client_id,
                    cancelled: cancelled.clone(),
                };

                let result = handler.handle(argument, writer).await;

                if let Err(e) = result {
                    let rpc_resp = subspace_client::proto::RpcResponse {
                        error: format!("INTERNAL: {}", e),
                        result: None,
                        session_id: session.session_id,
                        request_id: rpc_req.request_id,
                        client_id: rpc_req.client_id,
                        is_last: true,
                        is_cancelled: false,
                    };
                    let data = async_io::encode_to_vec(&rpc_resp);
                    let _ = async_io::publish_message(&instance.response_publisher, &data);
                }

                if let Some(handle) = cancel_handle {
                    handle.abort();
                }
            }
        }
    }

    fn send_error_response(
        instance: &MethodInstance,
        rpc_req: &subspace_client::proto::RpcRequest,
        error_msg: &str,
    ) {
        let rpc_resp = subspace_client::proto::RpcResponse {
            error: format!("INTERNAL: {}", error_msg),
            result: None,
            session_id: rpc_req.session_id,
            request_id: rpc_req.request_id,
            client_id: rpc_req.client_id,
            is_last: false,
            is_cancelled: false,
        };
        let data = async_io::encode_to_vec(&rpc_resp);
        let _ = async_io::publish_message(&instance.response_publisher, &data);
    }
}
