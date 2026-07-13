// Copyright 2024-2026 David Allison
// Rust client is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

use crate::split_buffer::{
    SplitBufferAllocateCallback, SplitBufferCallbacks, SplitBufferFreeCallback,
    SplitBufferMapCallback, SplitBufferUnmapCallback,
};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct PublisherOptions {
    pub slot_size: i32,
    pub num_slots: i32,
    pub subscriber_queue_size: i32,
    pub local: bool,
    pub reliable: bool,
    pub bridge: bool,
    pub for_tunnel: bool,
    pub fixed_size: bool,
    pub channel_type: String,
    pub activate: bool,
    pub mux: String,
    pub vchan_id: i32,
    pub notify_retirement: bool,
    pub checksum: bool,
    pub checksum_size: i32,
    pub metadata_size: i32,
    pub use_split_buffers: bool,
    pub split_buffers_over_bridge: bool,
    pub split_buffer_callbacks: SplitBufferCallbacks,
}

impl Default for PublisherOptions {
    fn default() -> Self {
        Self {
            slot_size: 0,
            num_slots: 0,
            subscriber_queue_size: 0,
            local: false,
            reliable: false,
            bridge: false,
            for_tunnel: false,
            fixed_size: false,
            channel_type: String::new(),
            activate: false,
            mux: String::new(),
            vchan_id: -1,
            notify_retirement: false,
            checksum: false,
            checksum_size: 4,
            metadata_size: 0,
            use_split_buffers: false,
            split_buffers_over_bridge: false,
            split_buffer_callbacks: SplitBufferCallbacks::default(),
        }
    }
}

impl PublisherOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_slot_size(mut self, size: i32) -> Self {
        self.slot_size = size;
        self
    }

    pub fn set_num_slots(mut self, num: i32) -> Self {
        self.num_slots = num;
        self
    }

    /// Set each subscriber's per-subscriber slot queue capacity.
    ///
    /// A value of 0 disables the queue and uses the available-slot bitset.
    /// Larger values allow subscribers to absorb more publisher/subscriber skew
    /// at the cost of shared memory in every subscriber queue.
    pub fn set_subscriber_queue_size(mut self, size: i32) -> Self {
        self.subscriber_queue_size = size;
        self
    }

    pub fn set_local(mut self, v: bool) -> Self {
        self.local = v;
        self
    }

    pub fn set_reliable(mut self, v: bool) -> Self {
        self.reliable = v;
        self
    }

    pub fn set_type(mut self, t: String) -> Self {
        self.channel_type = t;
        self
    }

    pub fn set_fixed_size(mut self, v: bool) -> Self {
        self.fixed_size = v;
        self
    }

    pub fn set_bridge(mut self, v: bool) -> Self {
        self.bridge = v;
        self
    }

    pub fn set_for_tunnel(mut self, v: bool) -> Self {
        self.for_tunnel = v;
        self
    }

    pub fn set_mux(mut self, m: String) -> Self {
        self.mux = m;
        self
    }

    pub fn set_vchan_id(mut self, id: i32) -> Self {
        self.vchan_id = id;
        self
    }

    pub fn set_activate(mut self, v: bool) -> Self {
        self.activate = v;
        self
    }

    pub fn set_notify_retirement(mut self, v: bool) -> Self {
        self.notify_retirement = v;
        self
    }

    pub fn set_checksum(mut self, v: bool) -> Self {
        self.checksum = v;
        self
    }

    pub fn set_checksum_size(mut self, size: i32) -> Self {
        self.checksum_size = size;
        self
    }

    pub fn set_metadata_size(mut self, size: i32) -> Self {
        self.metadata_size = size;
        self
    }

    pub fn set_use_split_buffers(mut self, v: bool) -> Self {
        self.use_split_buffers = v;
        self
    }

    pub fn set_split_buffers_over_bridge(mut self, v: bool) -> Self {
        self.split_buffers_over_bridge = v;
        self
    }

    pub fn set_split_buffer_callbacks(mut self, callbacks: SplitBufferCallbacks) -> Self {
        self.split_buffer_callbacks = callbacks;
        self
    }

    pub fn set_split_buffer_allocate_callback<F>(mut self, callback: F) -> Self
    where
        F: Fn(
                &crate::split_buffer::SplitBufferMetadata,
            ) -> crate::error::Result<crate::split_buffer::SplitBufferMapping>
            + Send
            + Sync
            + 'static,
    {
        self.split_buffer_callbacks.allocate =
            Some(Arc::new(callback) as SplitBufferAllocateCallback);
        self
    }

    pub fn set_split_buffer_free_callback<F>(mut self, callback: F) -> Self
    where
        F: Fn(
                &crate::split_buffer::SplitBufferMetadata,
                &crate::split_buffer::SplitBufferMapping,
            ) -> crate::error::Result<()>
            + Send
            + Sync
            + 'static,
    {
        self.split_buffer_callbacks.free = Some(Arc::new(callback) as SplitBufferFreeCallback);
        self
    }
}

#[derive(Debug, Clone)]
pub struct SubscriberOptions {
    pub reliable: bool,
    pub subscriber_queue_size: i32,
    pub bridge: bool,
    pub for_tunnel: bool,
    pub channel_type: String,
    pub max_active_messages: i32,
    pub log_dropped_messages: bool,
    pub detect_dropped_messages: bool,
    pub pass_activation: bool,
    pub read_write: bool,
    pub mux: String,
    pub vchan_id: i32,
    pub checksum: bool,
    pub pass_checksum_errors: bool,
    pub keep_active_message: bool,
    pub split_buffer_callbacks: SplitBufferCallbacks,
}

impl Default for SubscriberOptions {
    fn default() -> Self {
        Self {
            reliable: false,
            subscriber_queue_size: 0,
            bridge: false,
            for_tunnel: false,
            channel_type: String::new(),
            max_active_messages: 1,
            log_dropped_messages: true,
            detect_dropped_messages: true,
            pass_activation: false,
            read_write: false,
            mux: String::new(),
            vchan_id: -1,
            checksum: false,
            pass_checksum_errors: false,
            keep_active_message: false,
            split_buffer_callbacks: SplitBufferCallbacks::default(),
        }
    }
}

impl SubscriberOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_reliable(mut self, v: bool) -> Self {
        self.reliable = v;
        self
    }

    pub fn set_subscriber_queue_size(mut self, size: i32) -> Self {
        self.subscriber_queue_size = size;
        self
    }

    pub fn set_type(mut self, t: String) -> Self {
        self.channel_type = t;
        self
    }

    pub fn set_max_shared_ptrs(mut self, n: i32) -> Self {
        self.max_active_messages = n + 1;
        self
    }

    pub fn set_max_active_messages(mut self, n: i32) -> Self {
        self.max_active_messages = n;
        self
    }

    pub fn set_log_dropped_messages(mut self, v: bool) -> Self {
        self.log_dropped_messages = v;
        self
    }

    pub fn set_detect_dropped_messages(mut self, v: bool) -> Self {
        self.detect_dropped_messages = v;
        self
    }

    pub fn set_bridge(mut self, v: bool) -> Self {
        self.bridge = v;
        self
    }

    pub fn set_for_tunnel(mut self, v: bool) -> Self {
        self.for_tunnel = v;
        self
    }

    pub fn set_mux(mut self, m: String) -> Self {
        self.mux = m;
        self
    }

    pub fn set_vchan_id(mut self, id: i32) -> Self {
        self.vchan_id = id;
        self
    }

    pub fn set_pass_activation(mut self, v: bool) -> Self {
        self.pass_activation = v;
        self
    }

    pub fn set_read_write(mut self, v: bool) -> Self {
        self.read_write = v;
        self
    }

    pub fn set_checksum(mut self, v: bool) -> Self {
        self.checksum = v;
        self
    }

    pub fn set_pass_checksum_errors(mut self, v: bool) -> Self {
        self.pass_checksum_errors = v;
        self
    }

    pub fn set_keep_active_message(mut self, v: bool) -> Self {
        self.keep_active_message = v;
        self
    }

    pub fn set_split_buffer_callbacks(mut self, callbacks: SplitBufferCallbacks) -> Self {
        self.split_buffer_callbacks = callbacks;
        self
    }

    pub fn set_split_buffer_map_callback<F>(mut self, callback: F) -> Self
    where
        F: Fn(
                &crate::split_buffer::SplitBufferMetadata,
            ) -> crate::error::Result<crate::split_buffer::SplitBufferMapping>
            + Send
            + Sync
            + 'static,
    {
        self.split_buffer_callbacks.map = Some(Arc::new(callback) as SplitBufferMapCallback);
        self
    }

    pub fn set_split_buffer_unmap_callback<F>(mut self, callback: F) -> Self
    where
        F: Fn(
                &crate::split_buffer::SplitBufferMetadata,
                &crate::split_buffer::SplitBufferMapping,
            ) -> crate::error::Result<()>
            + Send
            + Sync
            + 'static,
    {
        self.split_buffer_callbacks.unmap = Some(Arc::new(callback) as SplitBufferUnmapCallback);
        self
    }
}
