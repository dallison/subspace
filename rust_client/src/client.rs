// Copyright 2024-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

use crate::channel::*;
use crate::checksum;
use crate::error::{Result, SubspaceError};
use crate::message::Message;
use crate::options::{PublisherOptions, SubscriberOptions};
use crate::proto;
use crate::publisher::{clear_trigger, PublisherImpl};
use crate::socket::SocketConnection;
use crate::subscriber::SubscriberImpl;
use crate::ReadMode;
use nix::sys::mman::ProtFlags;
use std::os::unix::io::RawFd;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

// ── Info / Stats types ──────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ChannelInfo {
    pub channel_name: String,
    pub num_publishers: i32,
    pub num_subscribers: i32,
    pub num_bridge_pubs: i32,
    pub num_bridge_subs: i32,
    pub channel_type: String,
    pub slot_size: u64,
    pub num_slots: i32,
    pub reliable: bool,
}

#[derive(Debug, Clone)]
pub struct ChannelStats {
    pub channel_name: String,
    pub total_bytes: u64,
    pub total_messages: u64,
    pub max_message_size: u64,
}

// ── ClientInner ─────────────────────────────────────────────────────────────

#[allow(dead_code)]
struct ClientInner {
    socket: SocketConnection,
    name: String,
    session_id: u64,
    scb_fd: RawFd,
    server_user_id: i32,
    server_group_id: i32,
    debug: bool,
}

// ── Publisher wrapper ───────────────────────────────────────────────────────

pub struct Publisher {
    inner: Arc<Mutex<ClientInner>>,
    pub(crate) imp: Arc<Mutex<PublisherImpl>>,
}

impl Publisher {
    pub fn name(&self) -> String {
        self.imp.lock().unwrap().channel.name.clone()
    }

    pub fn is_reliable(&self) -> bool {
        self.imp.lock().unwrap().options.reliable
    }

    pub fn is_fixed_size(&self) -> bool {
        self.imp.lock().unwrap().options.fixed_size
    }

    pub fn slot_size(&self) -> u64 {
        self.imp.lock().unwrap().channel.current_slot_size()
    }

    pub fn num_slots(&self) -> i32 {
        self.imp.lock().unwrap().channel.num_slots
    }

    /// Get a mutable pointer to the message buffer for writing.
    /// Returns None if no slot is available (reliable publisher).
    pub fn get_message_buffer(&self, max_size: i32) -> Result<Option<(*mut u8, usize)>> {
        let mut client = self.inner.lock().unwrap();
        let mut pub_impl = self.imp.lock().unwrap();

        if pub_impl.options.reliable {
            clear_trigger(pub_impl.poll_fd);
        }

        let slot_size = pub_impl.channel.current_slot_size() as i32;
        let mut span_size = slot_size as usize;

        if max_size != -1 && max_size > slot_size {
            let mut new_slot_size = slot_size;
            while new_slot_size <= slot_size || new_slot_size < max_size {
                new_slot_size = expand_slot_size(new_slot_size as u64) as i32;
            }
            span_size = new_slot_size as usize;

            if pub_impl.options.fixed_size {
                return Err(SubspaceError::Internal(format!(
                    "Channel {} is fixed size at {} bytes; can't increase to {} bytes",
                    pub_impl.channel.name, slot_size, new_slot_size
                )));
            }
            pub_impl.create_or_attach_buffers(aligned64(new_slot_size as i64) as u64)?;
            if let Some(si) = pub_impl.channel.slot {
                pub_impl.channel.set_slot_to_biggest_buffer(si);
            }
        }

        reload_subscribers_if_necessary(&mut *client, &mut pub_impl)?;

        if pub_impl.options.reliable && pub_impl.channel.slot.is_none() {
            if pub_impl
                .channel
                .num_subscribers(pub_impl.channel.vchan_id)
                == 0
            {
                return Ok(None);
            }
            let owner = pub_impl.publisher_id;
            let slot = pub_impl.find_free_slot_reliable(owner);
            if slot.is_none() {
                return Ok(None);
            }
        }

        let buffer = pub_impl.channel.get_current_buffer_address();
        if buffer.is_null() {
            return Err(SubspaceError::Internal(format!(
                "Channel {} has no buffer",
                pub_impl.channel.name
            )));
        }

        Ok(Some((buffer, span_size)))
    }

    /// Publish the message that was written into the buffer.
    pub fn publish_message(&self, message_size: i64) -> Result<Message> {
        let mut client = self.inner.lock().unwrap();
        let mut pub_impl = self.imp.lock().unwrap();

        reload_subscribers_if_necessary(&mut *client, &mut pub_impl)?;

        if message_size <= 0 {
            return Err(SubspaceError::InvalidArgument(
                "Message size must be > 0".into(),
            ));
        }

        let old_slot_id = pub_impl
            .channel
            .slot
            .map(|si| pub_impl.channel.slot_ref(si).id)
            .unwrap_or(-1);

        let slot_idx = pub_impl.channel.slot.unwrap();
        pub_impl.channel.slot_mut(slot_idx).message_size = message_size as u64;

        let owner = pub_impl.publisher_id;
        let reliable = pub_impl.options.reliable;
        let vchan_id = pub_impl.channel.vchan_id;

        let published = pub_impl.activate_slot_and_get_another(
            slot_idx, reliable, false, owner, false, false,
        );

        pub_impl.channel.slot = published.new_slot;
        pub_impl.trigger_subscribers();

        if published.new_slot.is_none() && !reliable {
            return Err(SubspaceError::Internal(format!(
                "Out of slots for channel {}",
                pub_impl.channel.name
            )));
        }

        Ok(Message {
            length: message_size as usize,
            buffer: std::ptr::null(),
            ordinal: published.ordinal,
            timestamp: published.timestamp,
            vchan_id,
            is_activation: false,
            slot_id: old_slot_id,
            checksum_error: false,
            active_message: None,
        })
    }

    /// Wait until a reliable publisher can try sending again.
    pub fn wait(&self, timeout_ms: Option<i64>) -> Result<()> {
        let pub_impl = self.imp.lock().unwrap();
        if !pub_impl.options.reliable {
            return Err(SubspaceError::Internal(
                "Unreliable publishers can't wait".into(),
            ));
        }
        let poll_fd = pub_impl.poll_fd;
        drop(pub_impl);

        let timeout = timeout_ms.map(|t| t as i32).unwrap_or(-1);
        let result = poll_fd_with_timeout(poll_fd, timeout)?;
        if result == 0 {
            return Err(SubspaceError::Timeout(
                "Timeout waiting for reliable publisher".into(),
            ));
        }
        Ok(())
    }

    pub fn get_poll_fd(&self) -> RawFd {
        self.imp.lock().unwrap().poll_fd
    }

    pub fn get_retirement_fd(&self) -> RawFd {
        self.imp.lock().unwrap().retirement_fd
    }

    pub fn virtual_channel_id(&self) -> i32 {
        self.imp.lock().unwrap().channel.vchan_id
    }

    pub fn num_subscribers(&self, vchan_id: i32) -> i32 {
        self.imp.lock().unwrap().channel.num_subscribers(vchan_id)
    }
}

impl Drop for Publisher {
    fn drop(&mut self) {
        let client = self.inner.lock().unwrap();
        let pub_impl = self.imp.lock().unwrap();

        let channel_name = pub_impl.channel.name.clone();
        let publisher_id = pub_impl.publisher_id;
        drop(pub_impl);

        let _ = remove_publisher_request(&client, &channel_name, publisher_id);
    }
}

// ── Subscriber wrapper ──────────────────────────────────────────────────────

pub struct Subscriber {
    inner: Arc<Mutex<ClientInner>>,
    pub(crate) imp: Arc<Mutex<SubscriberImpl>>,
}

impl Subscriber {
    pub fn name(&self) -> String {
        self.imp.lock().unwrap().channel.name.clone()
    }

    pub fn is_reliable(&self) -> bool {
        self.imp.lock().unwrap().options.reliable
    }

    pub fn is_placeholder(&self) -> bool {
        self.imp.lock().unwrap().channel.is_placeholder()
    }

    pub fn num_slots(&self) -> i32 {
        self.imp.lock().unwrap().channel.num_slots
    }

    pub fn current_ordinal(&self) -> i64 {
        let sub = self.imp.lock().unwrap();
        match sub.channel.slot {
            Some(si) => sub.channel.slot_ref(si).ordinal as i64,
            None => -1,
        }
    }

    pub fn timestamp(&self) -> u64 {
        let sub = self.imp.lock().unwrap();
        match sub.channel.slot {
            Some(si) => sub.channel.slot_ref(si).timestamp,
            None => 0,
        }
    }

    /// Read the next (or newest) message.
    pub fn read_message(&self, mode: ReadMode) -> Result<Message> {
        let mut client = self.inner.lock().unwrap();
        let mut sub_impl = self.imp.lock().unwrap();

        if sub_impl.channel.is_placeholder() {
            reload_subscriber(&mut *client, &mut sub_impl)?;
            if sub_impl.channel.is_placeholder() {
                sub_impl.clear_poll_fd();
                return Ok(Message::default());
            }
        }

        reload_reliable_publishers_if_necessary(&mut *client, &mut sub_impl)?;

        let pass_activation = sub_impl.options.pass_activation;
        read_message_internal(&mut *client, &mut sub_impl, mode, pass_activation, true)
    }

    /// Wait until there's a message available.
    pub fn wait(&self, timeout_ms: Option<i64>) -> Result<()> {
        let sub_impl = self.imp.lock().unwrap();
        let poll_fd = sub_impl.poll_fd;
        drop(sub_impl);

        let timeout = timeout_ms.map(|t| t as i32).unwrap_or(-1);
        let result = poll_fd_with_timeout(poll_fd, timeout)?;
        if result == 0 {
            return Err(SubspaceError::Timeout(
                "Timeout waiting for subscriber".into(),
            ));
        }
        Ok(())
    }

    pub fn get_poll_fd(&self) -> RawFd {
        self.imp.lock().unwrap().poll_fd
    }

    pub fn trigger(&self) {
        self.imp.lock().unwrap().trigger();
    }

    pub fn untrigger(&self) {
        self.imp.lock().unwrap().untrigger();
    }

    pub fn virtual_channel_id(&self) -> i32 {
        self.imp.lock().unwrap().channel.vchan_id
    }

    pub fn num_active_messages(&self) -> i32 {
        self.imp.lock().unwrap().num_active_messages()
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        let client = self.inner.lock().unwrap();
        let sub_impl = self.imp.lock().unwrap();

        let channel_name = sub_impl.channel.name.clone();
        let subscriber_id = sub_impl.subscriber_id;
        drop(sub_impl);

        let _ = remove_subscriber_request(&client, &channel_name, subscriber_id);
    }
}

// ── Client ──────────────────────────────────────────────────────────────────

pub struct Client {
    inner: Arc<Mutex<ClientInner>>,
}

impl Client {
    pub fn new(server_socket: &str, client_name: &str) -> Result<Self> {
        let socket = SocketConnection::connect(server_socket)?;

        // Send Init request.
        let req = proto::Request {
            request: Some(proto::request::Request::Init(proto::InitRequest {
                client_name: client_name.to_string(),
            })),
        };
        socket.send_request(&req)?;
        let (resp, fds) = socket.receive_response()?;

        let init = match resp.response {
            Some(proto::response::Response::Init(init)) => init,
            _ => return Err(SubspaceError::Internal("Unexpected response to Init".into())),
        };

        let scb_fd = fds[init.scb_fd_index as usize];

        Ok(Self {
            inner: Arc::new(Mutex::new(ClientInner {
                socket,
                name: client_name.to_string(),
                session_id: init.session_id as u64,
                scb_fd,
                server_user_id: init.user_id,
                server_group_id: init.group_id,
                debug: false,
            })),
        })
    }

    pub fn set_debug(&self, v: bool) {
        self.inner.lock().unwrap().debug = v;
    }

    pub fn create_publisher(
        &self,
        channel_name: &str,
        opts: &PublisherOptions,
    ) -> Result<Publisher> {
        let mut client = self.inner.lock().unwrap();

        let slot_size = aligned64(opts.slot_size as i64);

        let req = proto::Request {
            request: Some(proto::request::Request::CreatePublisher(
                proto::CreatePublisherRequest {
                    channel_name: channel_name.to_string(),
                    slot_size: slot_size as i32,
                    num_slots: opts.num_slots,
                    is_local: opts.local,
                    is_reliable: opts.reliable,
                    is_bridge: opts.bridge,
                    is_fixed_size: opts.fixed_size,
                    r#type: opts.channel_type.as_bytes().to_vec(),
                    mux: opts.mux.clone(),
                    vchan_id: opts.vchan_id,
                    notify_retirement: opts.notify_retirement,
                },
            )),
        };

        let (resp, fds) = send_request_receive_response(&mut client, &req)?;

        let pub_resp = match resp.response {
            Some(proto::response::Response::CreatePublisher(r)) => r,
            _ => {
                return Err(SubspaceError::Internal(
                    "Unexpected response to CreatePublisher".into(),
                ))
            }
        };

        if !pub_resp.error.is_empty() {
            return Err(SubspaceError::ServerError(pub_resp.error));
        }

        let mut pub_impl = PublisherImpl::new(
            channel_name.to_string(),
            opts.num_slots,
            pub_resp.channel_id,
            pub_resp.publisher_id,
            pub_resp.vchan_id,
            client.session_id,
            String::from_utf8_lossy(&pub_resp.r#type).to_string(),
            opts.clone(),
        );

        let prot = ProtFlags::PROT_READ | ProtFlags::PROT_WRITE;
        pub_impl.channel.map(
            client.scb_fd,
            fds[pub_resp.ccb_fd_index as usize],
            fds[pub_resp.bcb_fd_index as usize],
            prot,
        )?;

        pub_impl.create_or_attach_buffers(slot_size as u64)?;

        pub_impl.trigger_fd = fds[pub_resp.pub_trigger_fd_index as usize];
        pub_impl.poll_fd = fds[pub_resp.pub_poll_fd_index as usize];

        pub_impl.subscriber_trigger_fds.clear();
        for &idx in &pub_resp.sub_trigger_fd_indexes {
            pub_impl.subscriber_trigger_fds.push(fds[idx as usize]);
        }

        pub_impl.channel.num_updates = pub_resp.num_sub_updates as u16;

        if !opts.reliable {
            let owner = pub_impl.publisher_id;

            let slot = pub_impl.find_free_slot_unreliable(owner);
            if slot.is_none() {
                return Err(SubspaceError::Internal(
                    "No slot available for publisher".into(),
                ));
            }

            if !opts.bridge && opts.activate {
                activate_channel(&mut pub_impl)?;
            }
        } else if !opts.bridge {
            activate_reliable_channel(&mut pub_impl)?;
        }

        if pub_resp.retirement_fd_index != -1 {
            pub_impl.retirement_fd = fds[pub_resp.retirement_fd_index as usize];
        }
        pub_impl.retirement_trigger_fds.clear();
        for &idx in &pub_resp.retirement_fd_indexes {
            pub_impl.retirement_trigger_fds.push(fds[idx as usize]);
        }

        pub_impl.trigger_subscribers();

        let pub_arc = Arc::new(Mutex::new(pub_impl));
        Ok(Publisher {
            inner: self.inner.clone(),
            imp: pub_arc,
        })
    }

    pub fn create_subscriber(
        &self,
        channel_name: &str,
        opts: &SubscriberOptions,
    ) -> Result<Subscriber> {
        let mut client = self.inner.lock().unwrap();

        if opts.max_active_messages < 1 {
            return Err(SubspaceError::InvalidArgument(
                "MaxActiveMessages must be at least 1".into(),
            ));
        }

        let req = proto::Request {
            request: Some(proto::request::Request::CreateSubscriber(
                proto::CreateSubscriberRequest {
                    channel_name: channel_name.to_string(),
                    subscriber_id: -1,
                    is_reliable: opts.reliable,
                    is_bridge: opts.bridge,
                    r#type: opts.channel_type.as_bytes().to_vec(),
                    max_active_messages: opts.max_active_messages,
                    mux: opts.mux.clone(),
                    vchan_id: opts.vchan_id,
                },
            )),
        };

        let (resp, fds) = send_request_receive_response(&mut client, &req)?;

        let sub_resp = match resp.response {
            Some(proto::response::Response::CreateSubscriber(r)) => r,
            _ => {
                return Err(SubspaceError::Internal(
                    "Unexpected response to CreateSubscriber".into(),
                ))
            }
        };

        if !sub_resp.error.is_empty() {
            return Err(SubspaceError::ServerError(sub_resp.error));
        }

        let mut sub_impl = SubscriberImpl::new(
            channel_name.to_string(),
            sub_resp.num_slots,
            sub_resp.channel_id,
            sub_resp.subscriber_id,
            sub_resp.vchan_id,
            client.session_id,
            String::from_utf8_lossy(&sub_resp.r#type).to_string(),
            opts.clone(),
        );

        let prot = if opts.bridge || opts.read_write {
            ProtFlags::PROT_READ | ProtFlags::PROT_WRITE
        } else {
            ProtFlags::PROT_READ | ProtFlags::PROT_WRITE // CCB needs write for atomic ops
        };

        sub_impl.channel.num_slots = sub_resp.num_slots;
        sub_impl.channel.embargoed_slots.resize(sub_resp.num_slots as usize);

        sub_impl.channel.map(
            client.scb_fd,
            fds[sub_resp.ccb_fd_index as usize],
            fds[sub_resp.bcb_fd_index as usize],
            prot,
        )?;

        sub_impl.attach_buffers()?;

        sub_impl.trigger_fd = fds[sub_resp.trigger_fd_index as usize];
        sub_impl.poll_fd = fds[sub_resp.poll_fd_index as usize];

        {
            let mut pub_fds = sub_impl.reliable_publisher_fds.lock().unwrap();
            pub_fds.clear();
            for &idx in &sub_resp.reliable_pub_trigger_fd_indexes {
                pub_fds.push(fds[idx as usize]);
            }
        }

        sub_impl.retirement_trigger_fds.clear();
        for &idx in &sub_resp.retirement_fd_indexes {
            sub_impl.retirement_trigger_fds.push(fds[idx as usize]);
        }

        sub_impl.channel.num_updates = sub_resp.num_pub_updates as u16;
        sub_impl.trigger();

        let sub_arc = Arc::new(Mutex::new(sub_impl));
        {
            let weak = Arc::downgrade(&sub_arc);
            let mut sub = sub_arc.lock().unwrap();
            sub.set_self_ref(weak);
            sub.init_active_messages();
        }
        Ok(Subscriber {
            inner: self.inner.clone(),
            imp: sub_arc,
        })
    }

    pub fn get_channel_info(&self, channel_name: &str) -> Result<ChannelInfo> {
        let mut client = self.inner.lock().unwrap();
        let req = proto::Request {
            request: Some(proto::request::Request::GetChannelInfo(
                proto::GetChannelInfoRequest {
                    channel_name: channel_name.to_string(),
                },
            )),
        };

        let (resp, _fds) = send_request_receive_response(&mut client, &req)?;
        let info_resp = match resp.response {
            Some(proto::response::Response::GetChannelInfo(r)) => r,
            _ => {
                return Err(SubspaceError::Internal(
                    "Unexpected response to GetChannelInfo".into(),
                ))
            }
        };
        if !info_resp.error.is_empty() {
            return Err(SubspaceError::ServerError(info_resp.error));
        }
        if info_resp.channels.len() != 1 {
            return Err(SubspaceError::Internal(
                "Invalid response for GetChannelInfo".into(),
            ));
        }
        let info = &info_resp.channels[0];
        Ok(ChannelInfo {
            channel_name: info.name.clone(),
            num_publishers: info.num_pubs,
            num_subscribers: info.num_subs,
            num_bridge_pubs: info.num_bridge_pubs,
            num_bridge_subs: info.num_bridge_subs,
            channel_type: String::from_utf8_lossy(&info.r#type).to_string(),
            slot_size: info.slot_size as u64,
            num_slots: info.num_slots,
            reliable: info.is_reliable,
        })
    }

    pub fn get_all_channel_info(&self) -> Result<Vec<ChannelInfo>> {
        let mut client = self.inner.lock().unwrap();
        let req = proto::Request {
            request: Some(proto::request::Request::GetChannelInfo(
                proto::GetChannelInfoRequest {
                    channel_name: String::new(),
                },
            )),
        };

        let (resp, _fds) = send_request_receive_response(&mut client, &req)?;
        let info_resp = match resp.response {
            Some(proto::response::Response::GetChannelInfo(r)) => r,
            _ => {
                return Err(SubspaceError::Internal(
                    "Unexpected response to GetChannelInfo".into(),
                ))
            }
        };
        if !info_resp.error.is_empty() {
            return Err(SubspaceError::ServerError(info_resp.error));
        }
        Ok(info_resp
            .channels
            .iter()
            .map(|info| ChannelInfo {
                channel_name: info.name.clone(),
                num_publishers: info.num_pubs,
                num_subscribers: info.num_subs,
                num_bridge_pubs: info.num_bridge_pubs,
                num_bridge_subs: info.num_bridge_subs,
                channel_type: String::from_utf8_lossy(&info.r#type).to_string(),
                slot_size: info.slot_size as u64,
                num_slots: info.num_slots,
                reliable: info.is_reliable,
            })
            .collect())
    }

    pub fn get_channel_stats(&self, channel_name: &str) -> Result<ChannelStats> {
        let mut client = self.inner.lock().unwrap();
        let req = proto::Request {
            request: Some(proto::request::Request::GetChannelStats(
                proto::GetChannelStatsRequest {
                    channel_name: channel_name.to_string(),
                },
            )),
        };

        let (resp, _fds) = send_request_receive_response(&mut client, &req)?;
        let stats_resp = match resp.response {
            Some(proto::response::Response::GetChannelStats(r)) => r,
            _ => {
                return Err(SubspaceError::Internal(
                    "Unexpected response to GetChannelStats".into(),
                ))
            }
        };
        if !stats_resp.error.is_empty() {
            return Err(SubspaceError::ServerError(stats_resp.error));
        }
        if stats_resp.channels.len() != 1 {
            return Err(SubspaceError::Internal(
                "Invalid response for GetChannelStats".into(),
            ));
        }
        let s = &stats_resp.channels[0];
        Ok(ChannelStats {
            channel_name: s.channel_name.clone(),
            total_bytes: s.total_bytes as u64,
            total_messages: s.total_messages as u64,
            max_message_size: s.max_message_size as u64,
        })
    }
}

// ── Internal helper functions ───────────────────────────────────────────────

fn send_request_receive_response(
    client: &mut ClientInner,
    req: &proto::Request,
) -> Result<(proto::Response, Vec<RawFd>)> {
    client.socket.send_request(req)?;
    client.socket.receive_response()
}

fn read_message_internal(
    client: &mut ClientInner,
    sub: &mut SubscriberImpl,
    mode: ReadMode,
    pass_activation: bool,
    clear_trigger_flag: bool,
) -> Result<Message> {
    if clear_trigger_flag {
        sub.clear_poll_fd();
    }

    let old_slot = sub.channel.slot;
    let last_ordinal: i64 = match old_slot {
        Some(si) => sub.channel.slot_ref(si).ordinal as i64,
        None => -1,
    };

    let new_slot_idx = match mode {
        ReadMode::ReadNext => sub.next_slot(),
        ReadMode::ReadNewest => sub.last_slot(),
    };

    let new_idx = match new_slot_idx {
        Some(idx) => idx,
        None => {
            sub.trigger_reliable_publishers();
            return Ok(Message::default());
        }
    };

    sub.channel.slot = Some(new_idx);

    if mode == ReadMode::ReadNext && last_ordinal != -1 {
        let new_vchan_id = sub.channel.slot_ref(new_idx).vchan_id as i32;
        let drops = sub.detect_drops(new_vchan_id);
        if drops > 0 {
            if sub.options.log_dropped_messages {
                log::warn!(
                    "Dropped {} message{} on channel {}",
                    drops,
                    if drops == 1 { "" } else { "s" },
                    sub.channel.name
                );
            }
            sub.channel
                .ccb()
                .total_drops
                .fetch_add(drops as u32, Ordering::Relaxed);
        }
    }

    let prefix = sub.channel.get_prefix(new_idx);
    let mut is_activation = false;
    let mut checksum_error = false;

    if !prefix.is_null() {
        unsafe {
            let p = &*prefix;
            if p.has_checksum() && sub.options.checksum {
                let buffer = sub.channel.get_buffer_address(new_idx);
                let slot = sub.channel.slot_ref(new_idx);
                let data = checksum::get_message_checksum_data(
                    prefix,
                    buffer,
                    slot.message_size as usize,
                );
                let spans: Vec<&[u8]> = data.iter().copied().collect();
                checksum_error = !checksum::verify_checksum(&spans, p.checksum);
            }

            if (p.flags & MESSAGE_ACTIVATE) != 0 {
                is_activation = true;
                if !pass_activation {
                    sub.ignore_activation(new_idx);
                    if sub.options.reliable {
                        sub.trigger_reliable_publishers();
                    }
                    return read_message_internal(client, sub, mode, false, false);
                }
            }
        }
    }

    let slot = sub.channel.slot_ref(new_idx);
    if slot.message_size == 0 {
        return Ok(Message::default());
    }

    let buffer = sub.channel.get_buffer_address(new_idx);
    let msg_size = slot.message_size as usize;
    let ordinal = slot.ordinal;
    let timestamp = slot.timestamp;
    let vchan_id = slot.vchan_id as i32;
    let slot_id = slot.id;

    if !sub.add_active_message() {
        sub.unread_slot(new_idx);
        return Ok(Message::default());
    }

    sub.claim_slot(
        new_idx,
        sub.channel.vchan_id,
        mode == ReadMode::ReadNewest,
    );

    if checksum_error && !sub.options.pass_checksum_errors {
        return Err(SubspaceError::ChecksumError);
    }

    let am = &sub.active_messages[new_idx];
    am.inc_ref();

    Ok(Message {
        length: msg_size,
        buffer,
        ordinal,
        timestamp,
        vchan_id,
        is_activation,
        slot_id,
        checksum_error,
        active_message: Some(Arc::clone(am)),
    })
}

fn reload_subscriber(client: &mut ClientInner, sub: &mut SubscriberImpl) -> Result<()> {
    let scb = sub.channel.scb();
    let channel_id = sub.channel.channel_id as usize;
    let updates = scb.counters[channel_id].num_pub_updates;
    if sub.channel.num_updates == updates {
        return Ok(());
    }
    sub.channel.num_updates = updates;

    let req = proto::Request {
        request: Some(proto::request::Request::CreateSubscriber(
            proto::CreateSubscriberRequest {
                channel_name: sub.channel.name.clone(),
                subscriber_id: sub.subscriber_id,
                mux: sub.options.mux.clone(),
                ..Default::default()
            },
        )),
    };

    let (resp, fds) = send_request_receive_response(client, &req)?;
    let sub_resp = match resp.response {
        Some(proto::response::Response::CreateSubscriber(r)) => r,
        _ => return Err(SubspaceError::Internal("Unexpected response".into())),
    };
    if !sub_resp.error.is_empty() {
        return Err(SubspaceError::ServerError(sub_resp.error));
    }

    sub.channel.unmap();
    if !sub_resp.r#type.is_empty() {
        sub.channel.channel_type = String::from_utf8_lossy(&sub_resp.r#type).to_string();
    }
    sub.channel.num_slots = sub_resp.num_slots;
    sub.channel.embargoed_slots.resize(sub_resp.num_slots as usize);

    let prot = ProtFlags::PROT_READ | ProtFlags::PROT_WRITE;
    sub.channel.map(
        client.scb_fd,
        fds[sub_resp.ccb_fd_index as usize],
        fds[sub_resp.bcb_fd_index as usize],
        prot,
    )?;

    sub.attach_buffers()?;

    sub.trigger_fd = fds[sub_resp.trigger_fd_index as usize];
    sub.poll_fd = fds[sub_resp.poll_fd_index as usize];

    {
        let mut pub_fds = sub.reliable_publisher_fds.lock().unwrap();
        pub_fds.clear();
        for &idx in &sub_resp.reliable_pub_trigger_fd_indexes {
            pub_fds.push(fds[idx as usize]);
        }
    }

    sub.retirement_trigger_fds.clear();
    for &idx in &sub_resp.retirement_fd_indexes {
        sub.retirement_trigger_fds.push(fds[idx as usize]);
    }

    sub.init_active_messages();

    Ok(())
}

fn reload_subscribers_if_necessary(
    client: &mut ClientInner,
    publisher: &mut PublisherImpl,
) -> Result<()> {
    let scb = publisher.channel.scb();
    let channel_id = publisher.channel.channel_id as usize;
    let updates = scb.counters[channel_id].num_sub_updates;
    if publisher.channel.num_updates == updates {
        return Ok(());
    }
    publisher.channel.num_updates = updates;

    let req = proto::Request {
        request: Some(proto::request::Request::GetTriggers(
            proto::GetTriggersRequest {
                channel_name: publisher.channel.name.clone(),
            },
        )),
    };

    let (resp, fds) = send_request_receive_response(client, &req)?;
    let trig_resp = match resp.response {
        Some(proto::response::Response::GetTriggers(r)) => r,
        _ => return Err(SubspaceError::Internal("Unexpected response".into())),
    };

    publisher.subscriber_trigger_fds.clear();
    for &idx in &trig_resp.sub_trigger_fd_indexes {
        publisher.subscriber_trigger_fds.push(fds[idx as usize]);
    }

    publisher.retirement_trigger_fds.clear();
    for &idx in &trig_resp.retirement_fd_indexes {
        publisher.retirement_trigger_fds.push(fds[idx as usize]);
    }

    Ok(())
}

fn reload_reliable_publishers_if_necessary(
    client: &mut ClientInner,
    subscriber: &mut SubscriberImpl,
) -> Result<()> {
    let scb = subscriber.channel.scb();
    let channel_id = subscriber.channel.channel_id as usize;
    let updates = scb.counters[channel_id].num_pub_updates;
    if subscriber.channel.num_updates == updates {
        return Ok(());
    }
    subscriber.channel.num_updates = updates;

    let req = proto::Request {
        request: Some(proto::request::Request::GetTriggers(
            proto::GetTriggersRequest {
                channel_name: subscriber.channel.name.clone(),
            },
        )),
    };

    let (resp, fds) = send_request_receive_response(client, &req)?;
    let trig_resp = match resp.response {
        Some(proto::response::Response::GetTriggers(r)) => r,
        _ => return Err(SubspaceError::Internal("Unexpected response".into())),
    };

    {
        let mut pub_fds = subscriber.reliable_publisher_fds.lock().unwrap();
        pub_fds.clear();
        for &idx in &trig_resp.reliable_pub_trigger_fd_indexes {
            pub_fds.push(fds[idx as usize]);
        }
    }

    subscriber.retirement_trigger_fds.clear();
    for &idx in &trig_resp.retirement_fd_indexes {
        subscriber.retirement_trigger_fds.push(fds[idx as usize]);
    }

    Ok(())
}

fn activate_reliable_channel(publisher: &mut PublisherImpl) -> Result<()> {
    let owner = publisher.publisher_id;
    let slot = publisher.find_free_slot_reliable(owner);
    if slot.is_none() {
        return Err(SubspaceError::Internal(format!(
            "Channel {} has no free slots",
            publisher.channel.name
        )));
    }
    let si = slot.unwrap();

    let buffer = publisher.channel.get_buffer_address(si);
    if buffer.is_null() {
        return Err(SubspaceError::Internal(format!(
            "Channel {} has no buffer",
            publisher.channel.name
        )));
    }
    publisher.channel.slot_mut(si).message_size = 1;

    let owner = publisher.publisher_id;
    publisher.activate_slot_and_get_another(si, true, true, owner, false, false);
    publisher.channel.slot = None;
    publisher.trigger_subscribers();

    Ok(())
}

fn activate_channel(publisher: &mut PublisherImpl) -> Result<()> {
    if publisher
        .channel
        .is_activated(publisher.channel.vchan_id)
    {
        return Ok(());
    }

    let si = publisher.channel.slot.unwrap();
    let buffer = publisher.channel.get_buffer_address(si);
    if buffer.is_null() {
        return Err(SubspaceError::Internal(format!(
            "Channel {} has no buffer",
            publisher.channel.name
        )));
    }
    publisher.channel.slot_mut(si).message_size = 1;

    let owner = publisher.publisher_id;
    let published = publisher.activate_slot_and_get_another(si, false, true, owner, false, false);
    publisher.channel.slot = published.new_slot;
    publisher.trigger_subscribers();

    Ok(())
}

fn remove_publisher_request(
    client: &ClientInner,
    channel_name: &str,
    publisher_id: i32,
) -> Result<()> {
    let req = proto::Request {
        request: Some(proto::request::Request::RemovePublisher(
            proto::RemovePublisherRequest {
                channel_name: channel_name.to_string(),
                publisher_id,
            },
        )),
    };
    client.socket.send_request(&req)?;
    let (resp, _fds) = client.socket.receive_response()?;
    if let Some(proto::response::Response::RemovePublisher(r)) = resp.response {
        if !r.error.is_empty() {
            return Err(SubspaceError::ServerError(r.error));
        }
    }
    Ok(())
}

fn remove_subscriber_request(
    client: &ClientInner,
    channel_name: &str,
    subscriber_id: i32,
) -> Result<()> {
    let req = proto::Request {
        request: Some(proto::request::Request::RemoveSubscriber(
            proto::RemoveSubscriberRequest {
                channel_name: channel_name.to_string(),
                subscriber_id,
            },
        )),
    };
    client.socket.send_request(&req)?;
    let (resp, _fds) = client.socket.receive_response()?;
    if let Some(proto::response::Response::RemoveSubscriber(r)) = resp.response {
        if !r.error.is_empty() {
            return Err(SubspaceError::ServerError(r.error));
        }
    }
    Ok(())
}

fn poll_fd_with_timeout(fd: RawFd, timeout_ms: i32) -> Result<i32> {
    let mut pfd = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    let result = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
    if result < 0 {
        return Err(SubspaceError::Io(std::io::Error::last_os_error()));
    }
    Ok(result)
}

fn expand_slot_size(slot_size: u64) -> u64 {
    let multipliers = [2.0f64, 1.5, 1.25, 1.125, 1.0625, 1.03125];
    let size_ranges: [u64; 5] = [4096, 16384, 65536, 262144, 1048576];

    let mut i = 0;
    while i < size_ranges.len() {
        if slot_size <= size_ranges[i] {
            break;
        }
        i += 1;
    }
    aligned64((slot_size as f64 * multipliers[i]) as i64) as u64
}
