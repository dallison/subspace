// Copyright 2024-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

/// Pre-allocated per-slot guard that releases a shared-memory message slot
/// when the last `Message` referencing it is dropped.
///
/// The subscriber keeps one `Arc<ActiveMessage>` per slot; reading a
/// message just clones the `Arc` (atomic increment, no heap allocation).
/// An internal `AtomicI32` ref count tracks how many `Message` objects
/// reference the slot; when it reaches zero the `release` callback fires,
/// calling `SubscriberImpl::remove_active_message`.
pub(crate) struct ActiveMessage {
    refs: AtomicI32,
    release: Box<dyn Fn() + Send + Sync>,
}

impl ActiveMessage {
    pub fn new(release: Box<dyn Fn() + Send + Sync>) -> Self {
        Self {
            refs: AtomicI32::new(0),
            release,
        }
    }

    pub fn inc_ref(&self) {
        self.refs.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_ref(&self) {
        let old = self.refs.fetch_sub(1, Ordering::Release);
        if old == 1 {
            std::sync::atomic::fence(Ordering::Acquire);
            (self.release)();
        }
    }
}

/// A message read from a subscriber or returned by publish.
///
/// Cloning a `Message` shares ownership of the underlying slot; the slot
/// is released only when the *last* clone is dropped.
pub struct Message {
    pub length: usize,
    pub buffer: *const u8,
    pub ordinal: u64,
    pub timestamp: u64,
    pub vchan_id: i32,
    pub is_activation: bool,
    pub slot_id: i32,
    pub checksum_error: bool,

    pub(crate) active_message: Option<Arc<ActiveMessage>>,
}

unsafe impl Send for Message {}
unsafe impl Sync for Message {}

impl Clone for Message {
    fn clone(&self) -> Self {
        if let Some(ref am) = self.active_message {
            am.inc_ref();
        }
        Self {
            length: self.length,
            buffer: self.buffer,
            ordinal: self.ordinal,
            timestamp: self.timestamp,
            vchan_id: self.vchan_id,
            is_activation: self.is_activation,
            slot_id: self.slot_id,
            checksum_error: self.checksum_error,
            active_message: self.active_message.clone(),
        }
    }
}

impl Drop for Message {
    fn drop(&mut self) {
        if let Some(ref am) = self.active_message {
            am.dec_ref();
        }
    }
}

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
            active_message: None,
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
