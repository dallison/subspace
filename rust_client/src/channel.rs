// Copyright 2024-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

use crate::bitset::{
    bits_to_words, sizeof_atomic_bitset, AtomicBitSet, DynamicBitSet, InPlaceAtomicBitSet,
};
use nix::sys::mman::{mmap, munmap, MapFlags, ProtFlags};
use std::num::NonZeroUsize;
use std::os::fd::BorrowedFd;
use std::os::unix::io::RawFd;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, AtomicU64, Ordering};

// ── Flag constants ──────────────────────────────────────────────────────────

pub const MESSAGE_ACTIVATE: i64 = 1;
pub const MESSAGE_BRIDGED: i64 = 2;
pub const MESSAGE_HAS_CHECKSUM: i64 = 4;

pub const MESSAGE_SEEN: u32 = 1;
pub const MESSAGE_IS_ACTIVATION: u32 = 2;

pub const MAX_CHANNELS: usize = 1024;
pub const MAX_SLOT_OWNERS: usize = 1024;
pub const MAX_VCHAN_ID: usize = 1023;
pub const MAX_CHANNEL_NAME: usize = 64;
pub const MAX_BUFFERS: usize = 1024;

// ── Ref count bit field constants ───────────────────────────────────────────

pub const REF_COUNT_MASK: u64 = 0x3ff;
pub const REF_COUNT_SHIFT: u64 = 10;
pub const RELIABLE_REF_COUNT_SHIFT: u64 = 10;
pub const PUB_OWNED: u64 = 1u64 << 63;
pub const REFS_MASK: u64 = (1u64 << 20) - 1;
pub const RELIABLE_REFS_MASK: u64 = ((1u64 << 10) - 1) << RELIABLE_REF_COUNT_SHIFT;
pub const RETIRED_REFS_SIZE: u64 = 10;
pub const RETIRED_REFS_MASK: u64 = (1u64 << RETIRED_REFS_SIZE) - 1;
pub const RETIRED_REFS_SHIFT: u64 = 20;

pub const VCHAN_ID_SHIFT: u64 = RETIRED_REFS_SHIFT + RETIRED_REFS_SIZE;
pub const VCHAN_ID_SIZE: u64 = 10;
pub const VCHAN_ID_MASK: u64 = (1u64 << VCHAN_ID_SIZE) - 1;

pub const ORDINAL_SIZE: u64 = 23;
pub const ORDINAL_MASK: u64 = (1u64 << ORDINAL_SIZE) - 1;
pub const ORDINAL_SHIFT: u64 = 40;

// ── Bit field helpers ───────────────────────────────────────────────────────

pub fn build_refs_bit_field(ordinal: u64, vchan_id: i32, retired_refs: i32) -> u64 {
    ((ordinal & ORDINAL_MASK) << ORDINAL_SHIFT)
        | if ordinal == 0 {
            0
        } else {
            (vchan_id as u64 & VCHAN_ID_MASK) << VCHAN_ID_SHIFT
        }
        | ((retired_refs as u64 & RETIRED_REFS_MASK) << RETIRED_REFS_SHIFT)
}

pub fn aligned<const ALIGNMENT: i64>(v: i64) -> i64 {
    (v + (ALIGNMENT - 1)) & !(ALIGNMENT - 1)
}

pub fn aligned64(v: i64) -> i64 {
    aligned::<64>(v)
}

// ── MessagePrefix ───────────────────────────────────────────────────────────

#[repr(C)]
pub struct MessagePrefix {
    pub padding: i32,
    pub slot_id: i32,
    pub message_size: u64,
    pub ordinal: u64,
    pub timestamp: u64,
    pub flags: i64,
    pub vchan_id: i32,
    pub checksum: u32,
    pub padding2: [u8; 16],
}

const _: () = assert!(std::mem::size_of::<MessagePrefix>() == 64);

impl MessagePrefix {
    pub fn is_activation(&self) -> bool {
        (self.flags & MESSAGE_ACTIVATE) != 0
    }
    pub fn set_is_activation(&mut self) {
        self.flags |= MESSAGE_ACTIVATE;
    }
    pub fn has_checksum(&self) -> bool {
        (self.flags & MESSAGE_HAS_CHECKSUM) != 0
    }
    pub fn set_has_checksum(&mut self) {
        self.flags |= MESSAGE_HAS_CHECKSUM;
    }
}

// ── MessageSlot ─────────────────────────────────────────────────────────────

/// Number of 64-bit words needed for MAX_SLOT_OWNERS (1024) bits.
const SLOT_OWNER_WORDS: usize = bits_to_words(MAX_SLOT_OWNERS);

#[repr(C)]
pub struct MessageSlot {
    pub refs: AtomicU64,
    pub ordinal: u64,
    pub message_size: u64,
    pub id: i32,
    pub buffer_index: i16,
    pub vchan_id: i16,
    pub sub_owners: AtomicBitSet<SLOT_OWNER_WORDS>,
    pub timestamp: u64,
    pub flags: u32,
    pub bridged_slot_id: i32,
}

#[derive(Clone)]
pub struct ActiveSlot {
    pub slot_index: usize,
    pub ordinal: u64,
    pub timestamp: u64,
    pub vchan_id: i32,
}

// ── ChannelCounters ─────────────────────────────────────────────────────────

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct ChannelCounters {
    pub num_pub_updates: u16,
    pub num_sub_updates: u16,
    pub num_pubs: u16,
    pub num_reliable_pubs: u16,
    pub num_subs: u16,
    pub num_reliable_subs: u16,
    pub num_resizes: u16,
}

// ── SystemControlBlock ──────────────────────────────────────────────────────

#[repr(C)]
pub struct SystemControlBlock {
    pub counters: [ChannelCounters; MAX_CHANNELS],
}

// ── BufferControlBlock ──────────────────────────────────────────────────────

#[repr(C)]
pub struct BufferControlBlock {
    pub refs: [AtomicI32; MAX_BUFFERS],
    pub sizes: [AtomicU64; MAX_BUFFERS],
}

// ── OrdinalAccumulator ──────────────────────────────────────────────────────

#[repr(C)]
pub struct OrdinalAccumulator {
    ordinals: [AtomicU64; MAX_VCHAN_ID + 1],
}

impl OrdinalAccumulator {
    pub fn next(&self, vchan_id: i32) -> u64 {
        let idx = (vchan_id + 1) as usize;
        self.ordinals[idx].fetch_add(1, Ordering::Relaxed)
    }
}

// ── ActivationTracker ───────────────────────────────────────────────────────

const ACTIVATION_WORDS: usize = bits_to_words(MAX_VCHAN_ID + 1);

#[repr(C)]
pub struct ActivationTracker {
    activations: AtomicBitSet<ACTIVATION_WORDS>,
}

impl ActivationTracker {
    pub fn activate(&self, vchan_id: i32) {
        self.activations.set((vchan_id + 1) as usize);
    }

    pub fn is_activated(&self, vchan_id: i32) -> bool {
        self.activations.is_set((vchan_id + 1) as usize)
    }
}

// ── SubscriberCounter ───────────────────────────────────────────────────────

#[repr(C)]
pub struct SubscriberCounter {
    num_subs: [i32; MAX_VCHAN_ID + 1],
}

impl SubscriberCounter {
    pub fn add_subscriber(&mut self, vchan_id: i32) {
        self.num_subs[(vchan_id + 1) as usize] += 1;
    }

    pub fn remove_subscriber(&mut self, vchan_id: i32) {
        self.num_subs[(vchan_id + 1) as usize] -= 1;
    }

    pub fn num_subscribers(&self, vchan_id: i32) -> i32 {
        let n = self.num_subs[0];
        if vchan_id == -1 {
            n
        } else {
            n + self.num_subs[(vchan_id + 1) as usize]
        }
    }
}

// ── ChannelControlBlock ─────────────────────────────────────────────────────

/// The fixed part of the CCB. The variable-length slots array and trailing
/// bitsets are accessed via pointer arithmetic, matching the C++ layout.
#[repr(C)]
pub struct ChannelControlBlock {
    pub channel_name: [u8; MAX_CHANNEL_NAME],
    pub num_slots: i32,
    pub ordinals: OrdinalAccumulator,
    pub activation_tracker: ActivationTracker,
    pub buffer_index: i32,
    pub num_buffers: AtomicI32,
    pub subscribers: AtomicBitSet<SLOT_OWNER_WORDS>,
    pub sub_vchan_ids: [i16; MAX_SLOT_OWNERS],

    pub num_subs: SubscriberCounter,

    pub total_bytes: AtomicU64,
    pub total_messages: AtomicU64,
    pub max_message_size: AtomicU32,
    pub total_drops: AtomicU32,

    pub free_slots_exhausted: AtomicBool,
    // Followed by: slots[num_slots], then trailing bitsets.
    // Accessed via unsafe pointer arithmetic.
}

pub fn ccb_size(num_slots: i32) -> usize {
    let ns = num_slots as usize;
    let base = aligned64(
        (std::mem::size_of::<ChannelControlBlock>() + ns * std::mem::size_of::<MessageSlot>())
            as i64,
    ) as usize;
    base + aligned64(sizeof_atomic_bitset(ns) as i64) as usize * 2
        + sizeof_atomic_bitset(ns) * MAX_SLOT_OWNERS
}

// ── Channel: shared memory accessor ─────────────────────────────────────────

/// Provides safe access to a channel's shared memory regions.
pub struct Channel {
    pub name: String,
    pub num_slots: i32,
    pub channel_id: i32,
    pub channel_type: String,
    pub vchan_id: i32,
    pub session_id: u64,
    pub num_updates: u16,

    pub scb: *mut SystemControlBlock,
    pub ccb: *mut ChannelControlBlock,
    pub bcb: *mut BufferControlBlock,

    scb_size: usize,
    ccb_size: usize,
    bcb_size: usize,

    pub buffers: Vec<BufferSet>,

    pub active_slots: Vec<ActiveSlot>,
    pub embargoed_slots: DynamicBitSet,
    pub slot: Option<usize>, // Current slot index.
}

unsafe impl Send for Channel {}
unsafe impl Sync for Channel {}

pub struct BufferSet {
    pub full_size: u64,
    pub slot_size: u64,
    pub buffer: *mut u8,
}

unsafe impl Send for BufferSet {}
unsafe impl Sync for BufferSet {}

impl Channel {
    pub fn new(
        name: String,
        num_slots: i32,
        channel_id: i32,
        channel_type: String,
        vchan_id: i32,
        session_id: u64,
    ) -> Self {
        let ns = num_slots as usize;
        Self {
            name,
            num_slots,
            channel_id,
            channel_type,
            vchan_id,
            session_id,
            num_updates: 0,
            scb: std::ptr::null_mut(),
            ccb: std::ptr::null_mut(),
            bcb: std::ptr::null_mut(),
            scb_size: 0,
            ccb_size: 0,
            bcb_size: 0,
            buffers: Vec::new(),
            active_slots: Vec::with_capacity(ns),
            embargoed_slots: DynamicBitSet::new(ns),
            slot: None,
        }
    }

    pub fn map(
        &mut self,
        scb_fd: RawFd,
        ccb_fd: RawFd,
        bcb_fd: RawFd,
        prot: ProtFlags,
    ) -> crate::error::Result<()> {
        let scb_sz = std::mem::size_of::<SystemControlBlock>();
        let ccb_sz = ccb_size(self.num_slots);
        let bcb_sz = std::mem::size_of::<BufferControlBlock>();

        self.scb = map_memory(scb_fd, scb_sz, ProtFlags::PROT_READ | ProtFlags::PROT_WRITE)?
            as *mut SystemControlBlock;
        self.ccb = map_memory(ccb_fd, ccb_sz, prot)? as *mut ChannelControlBlock;
        self.bcb = map_memory(bcb_fd, bcb_sz, ProtFlags::PROT_READ | ProtFlags::PROT_WRITE)?
            as *mut BufferControlBlock;
        self.scb_size = scb_sz;
        self.ccb_size = ccb_sz;
        self.bcb_size = bcb_sz;
        Ok(())
    }

    pub fn unmap(&mut self) {
        if !self.scb.is_null() {
            unsafe {
                let _ = munmap(
                    NonNull::new_unchecked(self.scb as *mut _),
                    self.scb_size,
                );
            }
            self.scb = std::ptr::null_mut();
        }
        if !self.ccb.is_null() {
            unsafe {
                let _ = munmap(
                    NonNull::new_unchecked(self.ccb as *mut _),
                    self.ccb_size,
                );
            }
            self.ccb = std::ptr::null_mut();
        }
        if !self.bcb.is_null() {
            unsafe {
                let _ = munmap(
                    NonNull::new_unchecked(self.bcb as *mut _),
                    self.bcb_size,
                );
            }
            self.bcb = std::ptr::null_mut();
        }
    }

    pub fn is_placeholder(&self) -> bool {
        self.num_slots == 0
    }

    pub fn ccb(&self) -> &ChannelControlBlock {
        unsafe { &*self.ccb }
    }

    pub fn scb(&self) -> &SystemControlBlock {
        unsafe { &*self.scb }
    }

    pub fn bcb(&self) -> &BufferControlBlock {
        unsafe { &*self.bcb }
    }

    pub fn slot_ptr(&self, index: usize) -> *mut MessageSlot {
        unsafe {
            let slots_start =
                (self.ccb as *mut u8).add(std::mem::size_of::<ChannelControlBlock>());
            (slots_start as *mut MessageSlot).add(index)
        }
    }

    pub fn slot_ref(&self, index: usize) -> &MessageSlot {
        unsafe { &*self.slot_ptr(index) }
    }

    pub fn slot_mut(&self, index: usize) -> &mut MessageSlot {
        unsafe { &mut *self.slot_ptr(index) }
    }

    fn end_of_slots(&self) -> *mut u8 {
        unsafe {
            (self.ccb as *mut u8).add(aligned64(
                (std::mem::size_of::<ChannelControlBlock>()
                    + self.num_slots as usize * std::mem::size_of::<MessageSlot>())
                    as i64,
            ) as usize)
        }
    }

    fn end_of_retired_slots(&self) -> *mut u8 {
        unsafe {
            self.end_of_slots()
                .add(aligned64(sizeof_atomic_bitset(self.num_slots as usize) as i64) as usize)
        }
    }

    fn end_of_free_slots(&self) -> *mut u8 {
        unsafe {
            self.end_of_retired_slots()
                .add(aligned64(sizeof_atomic_bitset(self.num_slots as usize) as i64) as usize)
        }
    }

    pub fn retired_slots(&self) -> InPlaceAtomicBitSet {
        unsafe { InPlaceAtomicBitSet::new(self.end_of_slots()) }
    }

    pub fn free_slots(&self) -> InPlaceAtomicBitSet {
        unsafe { InPlaceAtomicBitSet::new(self.end_of_retired_slots()) }
    }

    pub fn get_available_slots(&self, sub_id: usize) -> InPlaceAtomicBitSet {
        unsafe {
            let base = self.end_of_free_slots();
            let offset = sizeof_atomic_bitset(self.num_slots as usize) * sub_id;
            InPlaceAtomicBitSet::new(base.add(offset))
        }
    }

    pub fn num_subscribers(&self, vchan_id: i32) -> i32 {
        self.ccb().num_subs.num_subscribers(vchan_id)
    }

    pub fn is_activated(&self, vchan_id: i32) -> bool {
        self.ccb().activation_tracker.is_activated(vchan_id)
    }

    pub fn get_sub_vchan_id(&self, sub_id: usize) -> i32 {
        self.ccb().sub_vchan_ids[sub_id] as i32
    }

    pub fn register_subscriber(&self, sub_id: usize, vchan_id: i32, is_new: bool) {
        let ccb = self.ccb();
        ccb.subscribers.set(sub_id);
        unsafe {
            let ccb_mut = &mut *self.ccb;
            ccb_mut.sub_vchan_ids[sub_id] = vchan_id as i16;
            if is_new {
                ccb_mut.num_subs.add_subscriber(vchan_id);
            }
        }
    }

    /// Atomically increment/decrement the ref count on a slot.
    /// Returns true if successful, false if the slot is pub-owned or ordinal mismatch.
    pub fn atomic_inc_ref_count<F: FnOnce()>(
        &self,
        slot_idx: usize,
        reliable: bool,
        inc: i32,
        ordinal: u64,
        vchan_id: i32,
        retire: bool,
        retire_callback: Option<F>,
    ) -> bool {
        let slot = self.slot_ref(slot_idx);
        let ordinal = ordinal & ORDINAL_MASK;

        loop {
            let r = slot.refs.load(Ordering::Relaxed);
            if (r & PUB_OWNED) != 0 {
                return false;
            }

            let ref_ord = (r >> ORDINAL_SHIFT) & ORDINAL_MASK;
            if ref_ord != 0 && ordinal != 0 && ref_ord != ordinal {
                return false;
            }
            let ref_vchan_id = {
                let v = ((r >> VCHAN_ID_SHIFT) & VCHAN_ID_MASK) as i32;
                if v == ((1 << VCHAN_ID_SIZE) - 1) as i32 {
                    -1
                } else {
                    v
                }
            };
            if ref_ord != 0 && ordinal != 0 && ref_vchan_id != vchan_id {
                return false;
            }

            let mut new_refs = (r & REF_COUNT_MASK) as i64;
            let mut new_reliable_refs =
                ((r >> RELIABLE_REF_COUNT_SHIFT) & REF_COUNT_MASK) as i64;

            if inc < 0 && new_refs == 0 {
                return true;
            }

            new_refs += inc as i64;
            if reliable {
                new_reliable_refs += inc as i64;
            }

            let mut retired_refs =
                ((r >> RETIRED_REFS_SHIFT) & RETIRED_REFS_MASK) as i32;
            if retire {
                retired_refs += 1;
            }

            let new_ref = build_refs_bit_field(ref_ord, ref_vchan_id, retired_refs)
                | ((new_reliable_refs as u64 & REF_COUNT_MASK) << RELIABLE_REF_COUNT_SHIFT)
                | (new_refs as u64 & REF_COUNT_MASK);

            if slot
                .refs
                .compare_exchange_weak(r, new_ref, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                if retire
                    && new_refs == 0
                    && new_reliable_refs == 0
                    && retired_refs >= self.num_subscribers(ref_vchan_id)
                {
                    self.retired_slots().set(slot.id as usize);
                    if let Some(cb) = retire_callback {
                        cb();
                    }
                }
                return true;
            }
        }
    }

    /// Get the buffer address for a slot, accounting for prefix.
    pub fn get_buffer_address(&self, slot_idx: usize) -> *mut u8 {
        let slot = self.slot_ref(slot_idx);
        let buf_idx = slot.buffer_index;
        if buf_idx < 0 || buf_idx as usize >= self.buffers.len() {
            return std::ptr::null_mut();
        }
        let buffer = &self.buffers[buf_idx as usize];
        let prefix_size = std::mem::size_of::<MessagePrefix>();
        let slot_size = aligned64(buffer.slot_size as i64) as usize;
        unsafe {
            buffer
                .buffer
                .add((prefix_size + slot_size) * slot_idx + prefix_size)
        }
    }

    pub fn get_prefix(&self, slot_idx: usize) -> *mut MessagePrefix {
        let slot = self.slot_ref(slot_idx);
        let buf_idx = slot.buffer_index;
        if buf_idx < 0 || buf_idx as usize >= self.buffers.len() {
            return std::ptr::null_mut();
        }
        let buffer = &self.buffers[buf_idx as usize];
        let prefix_size = std::mem::size_of::<MessagePrefix>();
        let slot_size = aligned64(buffer.slot_size as i64) as usize;
        unsafe { buffer.buffer.add((prefix_size + slot_size) * slot_idx) as *mut MessagePrefix }
    }

    pub fn get_current_buffer_address(&self) -> *mut u8 {
        match self.slot {
            Some(idx) => self.get_buffer_address(idx),
            None => std::ptr::null_mut(),
        }
    }

    pub fn slot_size_for_slot(&self, slot_idx: usize) -> u64 {
        let slot = self.slot_ref(slot_idx);
        let buf_idx = slot.buffer_index;
        if buf_idx < 0 || buf_idx as usize >= self.buffers.len() {
            return 0;
        }
        self.buffers[buf_idx as usize].slot_size
    }

    pub fn current_slot_size(&self) -> u64 {
        if self.buffers.is_empty() {
            return 0;
        }
        self.buffers.last().unwrap().slot_size
    }

    pub fn slot_size_to_buffer_size(&self, slot_size: u64) -> u64 {
        let ns = self.num_slots as u64;
        ns * (slot_size + std::mem::size_of::<MessagePrefix>() as u64)
    }

    pub fn buffer_size_to_slot_size(&self, buffer_size: u64) -> u64 {
        let ns = self.num_slots as u64;
        let prefix_total = ns * std::mem::size_of::<MessagePrefix>() as u64;
        if buffer_size < prefix_total {
            return 0;
        }
        (buffer_size - prefix_total) / ns
    }

    pub fn validate_slot_buffer(&self, slot_idx: usize) -> bool {
        let slot = self.slot_ref(slot_idx);
        let buf_idx = slot.buffer_index;
        if buf_idx < 0 {
            return true;
        }
        (buf_idx as usize) < self.buffers.len()
    }

    pub fn set_slot_to_biggest_buffer(&mut self, slot_idx: usize) {
        let slot = self.slot_mut(slot_idx);
        if slot.buffer_index != -1 {
            self.decrement_buffer_refs(slot.buffer_index as usize);
        }
        slot.buffer_index = (self.buffers.len() - 1) as i16;
        self.increment_buffer_refs(slot.buffer_index as usize);
    }

    pub fn decrement_buffer_refs(&self, buffer_index: usize) {
        unsafe {
            (*self.bcb).refs[buffer_index].fetch_sub(1, Ordering::Relaxed);
        }
    }

    pub fn increment_buffer_refs(&self, buffer_index: usize) {
        unsafe {
            (*self.bcb).refs[buffer_index].fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn cleanup_slots(&self, owner: i32, reliable: bool, is_pub: bool, vchan_id: i32) {
        if is_pub {
            for i in 0..self.num_slots as usize {
                let slot = self.slot_ref(i);
                let refs = slot.refs.load(Ordering::Relaxed);
                if refs == (PUB_OWNED | owner as u64) {
                    self.slot_mut(i).ordinal = 0;
                    slot.refs.store(0, Ordering::SeqCst);

                    let ccb = self.ccb();
                    ccb.subscribers.traverse(|sub_id| {
                        self.get_available_slots(sub_id).clear(slot.id as usize);
                    });
                    return;
                }
            }
        } else {
            let ccb = self.ccb();
            ccb.subscribers.clear(owner as usize);
            unsafe {
                (*self.ccb).num_subs.remove_subscriber(vchan_id);
            }

            for i in 0..self.num_slots as usize {
                let slot = self.slot_ref(i);
                if slot.sub_owners.is_set(owner as usize) {
                    slot.sub_owners.clear(owner as usize);
                    self.atomic_inc_ref_count::<fn()>(i, reliable, -1, 0, 0, true, None);
                }
            }
        }
    }

    pub fn buffer_shared_memory_name(&self, resolved_name: &str, buffer_index: usize) -> String {
        let sanitized = resolved_name.replace('/', ".");
        #[cfg(target_os = "linux")]
        {
            format!("subspace_{}_{}_{}",self.session_id, sanitized, buffer_index)
        }
        #[cfg(not(target_os = "linux"))]
        {
            format!("/tmp/subspace_{}_{}_{}", self.session_id, sanitized, buffer_index)
        }
    }
}

impl Drop for Channel {
    fn drop(&mut self) {
        self.unmap();
        for buf in &self.buffers {
            if !buf.buffer.is_null() {
                unsafe {
                    let _ = munmap(
                        NonNull::new_unchecked(buf.buffer as *mut _),
                        buf.full_size as usize,
                    );
                }
            }
        }
    }
}

// ── Memory mapping helpers ──────────────────────────────────────────────────

pub fn map_memory(fd: RawFd, size: usize, prot: ProtFlags) -> crate::error::Result<*mut u8> {
    let size = NonZeroUsize::new(size).ok_or_else(|| {
        crate::error::SubspaceError::InvalidArgument("Cannot map zero-size region".into())
    })?;
    unsafe {
        let borrowed = BorrowedFd::borrow_raw(fd);
        let ptr = mmap(None, size, prot, MapFlags::MAP_SHARED, borrowed, 0)?;
        Ok(ptr.as_ptr() as *mut u8)
    }
}

pub fn now_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe {
        libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts);
    }
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}
