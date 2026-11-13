// Copyright 2024-2026 David Allison
// Rust client is Copyright 2026 Cruise LLC
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

/// `refs` value once the last reference has been dropped and the release has
/// not been claimed yet.  Nothing can take the message in this state, and the
/// release is performed by whoever claims it with `claim_release`.  The
/// subscriber claims it under its lock, so the release never runs while another
/// read or find is partway through taking the same slot.
const PENDING_RELEASE: i32 = 1 << 30;

impl ActiveMessage {
    pub fn new(release: Box<dyn Fn() + Send + Sync>) -> Self {
        Self {
            refs: AtomicI32::new(0),
            release,
        }
    }

    /// Adds a reference.  The caller already holds one.
    pub fn inc_ref(&self) {
        self.refs.fetch_add(1, Ordering::AcqRel);
    }

    /// Adds a reference if the message is held.  Returns false, without
    /// changing anything, when nothing holds it or its release is pending.
    pub fn try_hold(&self) -> bool {
        let mut held = self.refs.load(Ordering::Acquire);
        loop {
            if held <= 0 || held >= PENDING_RELEASE {
                return false;
            }
            match self.refs.compare_exchange_weak(
                held,
                held + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(current) => held = current,
            }
        }
    }

    /// True when nothing holds the message and no release is pending.
    pub fn is_free(&self) -> bool {
        self.refs.load(Ordering::Acquire) == 0
    }

    /// Drops one reference.  The last one invokes the release callback.
    pub fn dec_ref(&self) {
        if self.dec_ref_no_release() {
            (self.release)();
        }
    }

    /// Drops one reference without invoking the release callback.  Returns
    /// true if this dropped the last one, in which case the caller must
    /// release the message with `claim_release` and the release routine.
    pub fn dec_ref_no_release(&self) -> bool {
        let mut held = self.refs.load(Ordering::Acquire);
        loop {
            // A pending release is not a count, so it is never decremented.
            if held > 1 && held < PENDING_RELEASE {
                match self.refs.compare_exchange_weak(
                    held,
                    held - 1,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return false,
                    Err(current) => held = current,
                }
            } else if held == 1 {
                match self.refs.compare_exchange_weak(
                    held,
                    PENDING_RELEASE,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return true,
                    Err(current) => held = current,
                }
            } else {
                return false;
            }
        }
    }

    /// Claims a pending release.  Returns true for exactly one caller.
    pub fn claim_release(&self) -> bool {
        self.refs
            .compare_exchange(PENDING_RELEASE, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
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

impl std::fmt::Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Message")
            .field("length", &self.length)
            .field("buffer", &self.buffer)
            .field("ordinal", &self.ordinal)
            .field("timestamp", &self.timestamp)
            .field("vchan_id", &self.vchan_id)
            .field("is_activation", &self.is_activation)
            .field("slot_id", &self.slot_id)
            .field("checksum_error", &self.checksum_error)
            .finish()
    }
}

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
