// Copyright 2024-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

use std::sync::atomic::{AtomicI32, Ordering};

/// Tracks the lifetime of a message slot reference.
/// When the ref count drops to zero, the slot is released.
pub struct ActiveMessage {
    pub length: usize,
    pub buffer: *const u8,
    pub ordinal: u64,
    pub timestamp: u64,
    pub vchan_id: i32,
    pub slot_index: usize,
    pub is_activation: bool,
    pub checksum_error: bool,
    pub refs: AtomicI32,

    /// Callback invoked when ref count reaches zero.
    /// Arguments: slot_index.
    release_fn: Option<Box<dyn Fn(usize) + Send + Sync>>,
}

unsafe impl Send for ActiveMessage {}
unsafe impl Sync for ActiveMessage {}

impl ActiveMessage {
    pub fn new(slot_index: usize, release_fn: Box<dyn Fn(usize) + Send + Sync>) -> Self {
        Self {
            length: 0,
            buffer: std::ptr::null(),
            ordinal: 0,
            timestamp: 0,
            vchan_id: -1,
            slot_index,
            is_activation: false,
            checksum_error: false,
            refs: AtomicI32::new(0),
            release_fn: Some(release_fn),
        }
    }

    pub fn set(
        &mut self,
        length: usize,
        buffer: *const u8,
        ordinal: u64,
        timestamp: u64,
        vchan_id: i32,
        is_activation: bool,
        checksum_error: bool,
    ) {
        self.length = length;
        self.buffer = buffer;
        self.ordinal = ordinal;
        self.timestamp = timestamp;
        self.vchan_id = vchan_id;
        self.is_activation = is_activation;
        self.checksum_error = checksum_error;
    }

    pub fn inc_ref(&self) {
        self.refs.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_ref(&self) {
        let old = self.refs.fetch_sub(1, Ordering::Relaxed);
        if old == 1 && self.length != 0 {
            if let Some(ref f) = self.release_fn {
                f(self.slot_index);
            }
        }
    }

    pub fn reset(&mut self) {
        self.length = 0;
        self.buffer = std::ptr::null();
        self.ordinal = u64::MAX;
        self.timestamp = 0;
        self.vchan_id = -1;
        self.is_activation = false;
        self.checksum_error = false;
    }

    pub fn ref_count(&self) -> i32 {
        self.refs.load(Ordering::Relaxed)
    }
}

/// A message read from a subscriber or returned by publish.
#[derive(Clone)]
pub struct Message {
    pub length: usize,
    pub buffer: *const u8,
    pub ordinal: u64,
    pub timestamp: u64,
    pub vchan_id: i32,
    pub is_activation: bool,
    pub slot_id: i32,
    pub checksum_error: bool,
}

unsafe impl Send for Message {}
unsafe impl Sync for Message {}

impl Default for Message {
    fn default() -> Self {
        Self {
            length: 0,
            buffer: std::ptr::null(),
            ordinal: 0,
            timestamp: 0,
            vchan_id: -1,
            is_activation: false,
            slot_id: -1,
            checksum_error: false,
        }
    }
}

impl Message {
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Get the message data as a byte slice.
    ///
    /// # Safety
    /// The buffer pointer must still be valid (the underlying shared memory
    /// must not have been unmapped).
    pub unsafe fn as_slice(&self) -> &[u8] {
        if self.buffer.is_null() || self.length == 0 {
            &[]
        } else {
            std::slice::from_raw_parts(self.buffer, self.length)
        }
    }
}
