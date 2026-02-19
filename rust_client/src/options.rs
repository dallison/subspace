// Copyright 2024-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#[derive(Debug, Clone, Default)]
pub struct PublisherOptions {
    pub slot_size: i32,
    pub num_slots: i32,
    pub local: bool,
    pub reliable: bool,
    pub bridge: bool,
    pub fixed_size: bool,
    pub channel_type: String,
    pub activate: bool,
    pub mux: String,
    pub vchan_id: i32,
    pub notify_retirement: bool,
    pub checksum: bool,
}

impl PublisherOptions {
    pub fn new() -> Self {
        Self {
            vchan_id: -1,
            ..Default::default()
        }
    }

    pub fn set_slot_size(mut self, size: i32) -> Self {
        self.slot_size = size;
        self
    }

    pub fn set_num_slots(mut self, num: i32) -> Self {
        self.num_slots = num;
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
}

#[derive(Debug, Clone)]
pub struct SubscriberOptions {
    pub reliable: bool,
    pub bridge: bool,
    pub channel_type: String,
    pub max_active_messages: i32,
    pub log_dropped_messages: bool,
    pub pass_activation: bool,
    pub read_write: bool,
    pub mux: String,
    pub vchan_id: i32,
    pub checksum: bool,
    pub pass_checksum_errors: bool,
    pub keep_active_message: bool,
}

impl Default for SubscriberOptions {
    fn default() -> Self {
        Self {
            reliable: false,
            bridge: false,
            channel_type: String::new(),
            max_active_messages: 1,
            log_dropped_messages: true,
            pass_activation: false,
            read_write: false,
            mux: String::new(),
            vchan_id: -1,
            checksum: false,
            pass_checksum_errors: false,
            keep_active_message: false,
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

    pub fn set_bridge(mut self, v: bool) -> Self {
        self.bridge = v;
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
}
