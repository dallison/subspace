// Copyright 2024-2026 David Allison
// Rust client is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

use crate::channel::*;
use crate::checksum;
use crate::error::{Result, SubspaceError};
use crate::options::PublisherOptions;
use crate::split_buffer::{
    create_split_shared_memory_buffer, open_split_shared_memory_buffer, page_aligned_size,
    read_split_buffer_metadata_file, split_buffer_object_name, write_split_buffer_metadata_file,
    SplitBufferMetadata,
};
#[cfg(target_os = "linux")]
use crate::syscall_shim::shim_fstat;
use crate::syscall_shim::{shim_close, shim_ftruncate, shim_open, shim_read, shim_write};
use nix::fcntl::OFlag;
use nix::sys::mman::ProtFlags;
use nix::sys::stat::Mode;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicU32, Ordering};

pub type OnSendCallback = Box<dyn Fn(*mut u8, i64) -> Result<i64> + Send + Sync>;
pub type ResizeCallback = Box<dyn Fn(i32, i32) -> Result<()> + Send + Sync>;

pub struct PublishedMessage {
    pub new_slot: Option<usize>,
    pub ordinal: u64,
    pub timestamp: u64,
}

struct SubscriberQueuePublishGuard<'a> {
    channel: &'a Channel,
    publisher_id: usize,
    local_depth: &'a AtomicU32,
}

impl<'a> SubscriberQueuePublishGuard<'a> {
    fn new(channel: &'a Channel, publisher_id: usize, local_depth: &'a AtomicU32) -> Self {
        local_depth.fetch_add(1, Ordering::SeqCst);
        channel.begin_subscriber_queue_publish(publisher_id);
        Self {
            channel,
            publisher_id,
            local_depth,
        }
    }
}

impl Drop for SubscriberQueuePublishGuard<'_> {
    fn drop(&mut self) {
        self.channel
            .end_subscriber_queue_publish(self.publisher_id);
        self.local_depth.fetch_sub(1, Ordering::SeqCst);
    }
}

pub struct PublisherImpl {
    pub channel: Channel,
    pub publisher_id: i32,
    pub options: PublisherOptions,
    pub active_queue_publish_depth: AtomicU32,

    pub subscriber_trigger_fds: Vec<RawFd>,
    pub poll_fd: RawFd,
    pub trigger_fd: RawFd,
    pub retirement_fd: RawFd,
    pub retirement_trigger_fds: Vec<RawFd>,

    pub(crate) on_send_callback: Option<OnSendCallback>,
    pub(crate) resize_callback: Option<ResizeCallback>,
    pub(crate) checksum_callback: Option<checksum::ChecksumCallback>,
}

impl PublisherImpl {
    pub fn new(
        name: String,
        num_slots: i32,
        subscriber_queue_size: i32,
        subscriber_queue_arena_size: u64,
        channel_id: i32,
        publisher_id: i32,
        vchan_id: i32,
        session_id: u64,
        channel_type: String,
        options: PublisherOptions,
    ) -> Self {
        Self {
            channel: Channel::new(
                name,
                num_slots,
                subscriber_queue_size,
                subscriber_queue_arena_size,
                channel_id,
                channel_type,
                vchan_id,
                session_id,
            ),
            publisher_id,
            options,
            active_queue_publish_depth: AtomicU32::new(0),
            subscriber_trigger_fds: Vec::new(),
            poll_fd: -1,
            trigger_fd: -1,
            retirement_fd: -1,
            retirement_trigger_fds: Vec::new(),
            on_send_callback: None,
            resize_callback: None,
            checksum_callback: None,
        }
    }

    pub fn resolved_name(&self) -> &str {
        if !self.options.mux.is_empty() && self.channel.vchan_id != -1 {
            &self.options.mux
        } else {
            &self.channel.name
        }
    }

    pub fn prefix_size(&self) -> i32 {
        self.channel.prefix_size
    }

    pub fn checksum_size(&self) -> i32 {
        self.channel.checksum_size
    }

    pub fn metadata_size(&self) -> i32 {
        self.channel.metadata_size
    }

    /// Get a mutable slice over the user metadata area for the current slot.
    /// Returns an empty slice if metadata_size is 0 or no slot is active.
    pub fn get_metadata(&mut self) -> &mut [u8] {
        if self.channel.metadata_size == 0 {
            return &mut [];
        }
        if let Some(slot_idx) = self.channel.slot {
            let prefix = self.channel.get_prefix(slot_idx);
            if prefix.is_null() {
                return &mut [];
            }
            unsafe {
                checksum::get_metadata_slice(
                    prefix,
                    self.channel.checksum_size,
                    self.channel.metadata_size,
                )
            }
        } else {
            &mut []
        }
    }

    pub fn find_free_slot_unreliable(&mut self, owner: i32) -> Option<usize> {
        let max_retries = self.channel.num_slots as usize * 1000;
        let mut retries = max_retries;
        let mut slot_idx: Option<usize> = None;
        self.channel.embargoed_slots.clear_all();
        let max_cas_retries = 1000;
        let mut cas_retries = 0;
        let mut retired_slot: i32;
        let mut free_slot: i32;

        loop {
            free_slot = -1;
            retired_slot = -1;

            let free_exhausted = self
                .channel
                .ccb()
                .free_slots_exhausted
                .load(Ordering::Relaxed);

            if !free_exhausted {
                if let Some(fs) = self.channel.free_slots().find_first_set() {
                    free_slot = fs as i32;
                    slot_idx = Some(fs);
                    self.channel.free_slots().clear(fs);
                    if self.channel.free_slots().is_empty() {
                        self.channel
                            .ccb()
                            .free_slots_exhausted
                            .store(true, Ordering::Relaxed);
                    }
                }
            }

            if free_slot == -1 {
                if let Some(rs) = self.channel.retired_slots().find_first_set() {
                    if !self.channel.embargoed_slots.is_set(rs) {
                        retired_slot = rs as i32;
                        self.channel.retired_slots().clear(rs);
                        slot_idx = Some(rs);
                    } else {
                        continue;
                    }
                }
            }

            if free_slot == -1 && retired_slot == -1 {
                let mut earliest_timestamp = u64::MAX;
                slot_idx = None;
                for i in 0..self.channel.num_slots as usize {
                    if self.channel.embargoed_slots.is_set(i) {
                        continue;
                    }
                    let s = self.channel.slot_ref(i);
                    let refs = s.refs.load(Ordering::Relaxed);
                    if (refs & PUB_OWNED) != 0 {
                        continue;
                    }
                    if (refs & REFS_MASK) == 0 && s.timestamp() < earliest_timestamp {
                        slot_idx = Some(i);
                        earliest_timestamp = s.timestamp();
                    }
                }
            }

            if slot_idx.is_none() {
                if retries == 0 {
                    return None;
                }
                retries -= 1;
                continue;
            }
            let si = slot_idx.unwrap();

            let slot_ptr = self.channel.slot_ptr(si);
            unsafe {
                let old_refs = (*slot_ptr).refs.load(Ordering::Relaxed);
                let ref_val = PUB_OWNED | owner as u64;
                let expected = build_refs_bit_field(
                    (*slot_ptr).ordinal(),
                    ((old_refs >> VCHAN_ID_SHIFT) & VCHAN_ID_MASK) as i32,
                    ((old_refs >> RETIRED_REFS_SHIFT) & RETIRED_REFS_MASK) as i32,
                );

                if (*slot_ptr)
                    .refs
                    .compare_exchange_weak(expected, ref_val, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
                {
                    if !self.channel.validate_slot_buffer(si) {
                        self.channel.embargoed_slots.set(si);
                        (*slot_ptr).refs.store(old_refs, Ordering::Relaxed);
                        slot_idx = None;
                        continue;
                    }
                    break;
                }
            }
            cas_retries += 1;
            if cas_retries >= max_cas_retries {
                return None;
            }
            slot_idx = None;
        }

        let si = slot_idx.unwrap();
        let slot = self.channel.slot_ref(si);
        slot.set_ordinal(0);
        slot.set_timestamp(0);
        slot.set_vchan_id(self.channel.vchan_id as i16);
        self.channel.set_slot_to_biggest_buffer(si);

        let prefix = self.channel.get_prefix(si);
        if !prefix.is_null() {
            unsafe {
                (*prefix).flags = 0;
                (*prefix).vchan_id = self.channel.vchan_id;
            }
        }

        // Clear slot in all subscriber bitsets.
        let ccb = self.channel.ccb();
        let vchan_id = self.channel.vchan_id;
        ccb.subscribers.traverse(|sub_id| {
            let vid = self.channel.get_sub_vchan_id(sub_id);
            if vid != -1 && vchan_id != -1 && vid != vchan_id {
                return;
            }
            self.channel.get_available_slots(sub_id).clear(si);
        });

        if free_slot == -1 && retired_slot == -1 {
            self.trigger_retirement(si);
        }

        self.channel.slot = Some(si);
        Some(si)
    }

    pub fn find_free_slot_reliable(&mut self, owner: i32) -> Option<usize> {
        let mut slot_idx: Option<usize>;
        self.channel.embargoed_slots.clear_all();
        let mut retired_slot: i32;
        let mut free_slot: i32;

        loop {
            free_slot = -1;
            retired_slot = -1;
            self.channel.active_slots.clear();

            let free_exhausted = self
                .channel
                .ccb()
                .free_slots_exhausted
                .load(Ordering::Relaxed);

            if !free_exhausted {
                if let Some(fs) = self.channel.free_slots().find_first_set() {
                    free_slot = fs as i32;
                    self.channel.free_slots().clear(fs);
                    if self.channel.free_slots().is_empty() {
                        self.channel
                            .ccb()
                            .free_slots_exhausted
                            .store(true, Ordering::Relaxed);
                    }
                    let s = self.channel.slot_ref(fs);
                    self.channel.active_slots.push(ActiveSlot {
                        slot_index: fs,
                        ordinal: s.ordinal(),
                        timestamp: s.timestamp(),
                        vchan_id: s.vchan_id() as i32,
                    });
                }
            }

            if free_slot == -1 {
                if let Some(rs) = self.channel.retired_slots().find_first_set() {
                    if !self.channel.embargoed_slots.is_set(rs) {
                        retired_slot = rs as i32;
                        self.channel.retired_slots().clear(rs);
                        let s = self.channel.slot_ref(rs);
                        self.channel.active_slots.push(ActiveSlot {
                            slot_index: rs,
                            ordinal: s.ordinal(),
                            timestamp: s.timestamp(),
                            vchan_id: s.vchan_id() as i32,
                        });
                    } else {
                        continue;
                    }
                }
            }

            if free_slot == -1 && retired_slot == -1 {
                for i in 0..self.channel.num_slots as usize {
                    if self.channel.embargoed_slots.is_set(i) {
                        continue;
                    }
                    let s = self.channel.slot_ref(i);
                    let refs = s.refs.load(Ordering::Relaxed);
                    if (refs & PUB_OWNED) == 0 {
                        self.channel.active_slots.push(ActiveSlot {
                            slot_index: i,
                            ordinal: s.ordinal(),
                            timestamp: s.timestamp(),
                            vchan_id: s.vchan_id() as i32,
                        });
                    }
                }
            }

            self.channel.active_slots.sort_by_key(|s| s.timestamp);

            slot_idx = None;
            let require_reliable_seen =
                self.channel.scb().counters[self.channel.channel_id as usize].num_reliable_subs
                    != 0;
            for active in &self.channel.active_slots {
                let s = self.channel.slot_ref(active.slot_index);
                let refs = s.refs.load(Ordering::Relaxed);
                if ((refs >> RELIABLE_REF_COUNT_SHIFT) & REF_COUNT_MASK) != 0 {
                    break;
                }
                if require_reliable_seen
                    && active.ordinal != 0
                    && (s.flags() & MESSAGE_SEEN_BY_RELIABLE) == 0
                {
                    break;
                }
                if (refs & REFS_MASK) == 0 {
                    slot_idx = Some(active.slot_index);
                    break;
                }
            }

            if slot_idx.is_none() {
                return None;
            }
            let si = slot_idx.unwrap();

            let slot_ptr = self.channel.slot_ptr(si);
            unsafe {
                let old_refs = (*slot_ptr).refs.load(Ordering::Relaxed);
                let ref_val = PUB_OWNED | owner as u64;
                let expected = build_refs_bit_field(
                    (*slot_ptr).ordinal(),
                    ((old_refs >> VCHAN_ID_SHIFT) & VCHAN_ID_MASK) as i32,
                    ((old_refs >> RETIRED_REFS_SHIFT) & RETIRED_REFS_MASK) as i32,
                );

                if (*slot_ptr)
                    .refs
                    .compare_exchange_weak(expected, ref_val, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
                {
                    if !self.channel.validate_slot_buffer(si) {
                        self.channel.embargoed_slots.set(si);
                        (*slot_ptr).refs.store(old_refs, Ordering::Relaxed);
                        continue;
                    }
                    self.channel.retired_slots().clear(si as usize);
                    break;
                }
            }
        }

        let si = slot_idx.unwrap();
        let slot = self.channel.slot_ref(si);
        slot.set_ordinal(0);
        slot.set_timestamp(0);
        slot.set_vchan_id(self.channel.vchan_id as i16);
        self.channel.set_slot_to_biggest_buffer(si);

        let prefix = self.channel.get_prefix(si);
        if !prefix.is_null() {
            unsafe {
                (*prefix).flags = 0;
                (*prefix).vchan_id = self.channel.vchan_id;
            }
        }

        let ccb = self.channel.ccb();
        let vchan_id = self.channel.vchan_id;
        ccb.subscribers.traverse(|sub_id| {
            let vid = self.channel.get_sub_vchan_id(sub_id);
            if vid != -1 && vchan_id != -1 && vid != vchan_id {
                return;
            }
            self.channel.get_available_slots(sub_id).clear(si);
        });

        if free_slot == -1 && retired_slot == -1 {
            self.trigger_retirement(si);
        }

        self.channel.slot = Some(si);
        Some(si)
    }

    pub fn activate_slot_and_get_another(
        &mut self,
        slot_idx: usize,
        reliable: bool,
        is_activation: bool,
        owner: i32,
        omit_prefix: bool,
        use_prefix_slot_id: bool,
    ) -> PublishedMessage {
        let slot = self.channel.slot_ref(slot_idx);
        let vchan_id = self.channel.vchan_id;
        let slot_vchan_id = slot.vchan_id();

        let ordinal = self.channel.ccb().ordinals.next(slot_vchan_id as i32);
        slot.set_ordinal(ordinal);
        slot.set_timestamp(now_ns());
        slot.set_flags(0);

        let prefix = self.channel.get_prefix(slot_idx);
        if !prefix.is_null() {
            unsafe {
                let p = &mut *prefix;
                if omit_prefix {
                    slot.set_timestamp(p.timestamp);
                    slot.set_vchan_id(p.vchan_id as i16);
                    slot.set_bridged_slot_id(if use_prefix_slot_id {
                        p.slot_id
                    } else {
                        slot.id
                    });
                } else {
                    let message_size = slot.message_size();
                    let ordinal = slot.ordinal();
                    let timestamp = slot.timestamp();
                    let vchan_id_i16 = slot.vchan_id();
                    p.message_size = message_size;
                    p.ordinal = ordinal;
                    p.timestamp = timestamp;
                    p.vchan_id = vchan_id_i16 as i32;
                    p.checksum_size = self.channel.checksum_size as u16;
                    p.metadata_size = self.channel.metadata_size as u16;
                    p.flags = 0;
                    p.slot_id = slot.id;
                    slot.set_bridged_slot_id(slot.id);
                    if is_activation {
                        p.set_is_activation();
                        slot.set_flag(MESSAGE_IS_ACTIVATION);
                        self.channel
                            .ccb()
                            .activation_tracker
                            .activate(vchan_id);
                    }
                    if self.options.checksum {
                        p.set_has_checksum();
                        let buffer = self.channel.get_buffer_address(slot_idx);
                        let cs = self.channel.checksum_size;
                        let ms = self.channel.metadata_size;
                        let data = checksum::get_message_checksum_data(
                            prefix,
                            buffer,
                            message_size as usize,
                            cs,
                            ms,
                        );
                        let cksum = checksum::get_checksum_slice(prefix, cs);
                        cksum.fill(0);
                        if let Some(ref cb) = self.checksum_callback {
                            cb(&data, cksum);
                        } else {
                            checksum::calculate_crc32_checksum(&data, cksum);
                        }
                    }
                }
            }
        }

        // Release the slot: store refs with ordinal, no PUB_OWNED.
        let ordinal = slot.ordinal();
        slot.refs.store(
            build_refs_bit_field(ordinal, vchan_id, 0),
            Ordering::Release,
        );

        // Tell all subscribers the slot is available.
        let ccb = self.channel.ccb();
        {
            let _publish_guard =
                SubscriberQueuePublishGuard::new(
                    &self.channel,
                    owner as usize,
                    &self.active_queue_publish_depth,
                );
            let mut failed_queues: Vec<*const SlotQueueHeader> = Vec::new();
            ccb.subscribers.traverse_seq_cst(|sub_id| {
                if vchan_id != -1
                    && self.channel.get_sub_vchan_id(sub_id) != -1
                    && vchan_id != self.channel.get_sub_vchan_id(sub_id)
                {
                    return;
                }
                self.channel.get_available_slots(sub_id).set(slot_idx);
                let queue = self.channel.get_available_slot_queue(sub_id);
                if let Some(queue) = queue {
                    if !queue.push(
                        slot.id,
                        ordinal,
                        /* report_insertion_failure= */ false,
                    ) {
                        failed_queues.push(queue as *const SlotQueueHeader);
                    }
                }
            });
            ccb.total_messages.fetch_add(1, Ordering::SeqCst);
            for queue in failed_queues {
                unsafe { (&*queue).mark_insertion_failure() };
            }
        }

        if !is_activation {
            let message_size = slot.message_size();
            self.channel
                .ccb()
                .total_bytes
                .fetch_add(message_size, Ordering::Relaxed);
            let msg_size = message_size as u32;
            let mut old_max = self.channel.ccb().max_message_size.load(Ordering::Relaxed);
            while msg_size > old_max {
                match self.channel.ccb().max_message_size.compare_exchange_weak(
                    old_max,
                    msg_size,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(v) => old_max = v,
                }
            }
        }
        if reliable {
            return PublishedMessage {
                new_slot: None,
                ordinal: unsafe { (*prefix).ordinal },
                timestamp: unsafe { (*prefix).timestamp },
            };
        }

        let new_slot = self.find_free_slot_unreliable(owner);

        let (ordinal, timestamp) = if !prefix.is_null() {
            unsafe { ((*prefix).ordinal, (*prefix).timestamp) }
        } else {
            (0, 0)
        };

        PublishedMessage {
            new_slot,
            ordinal,
            timestamp,
        }
    }

    pub fn create_or_attach_buffers(&mut self, final_slot_size: u64) -> crate::error::Result<()> {
        if self.options.use_split_buffers {
            return self.create_or_attach_split_buffers(final_slot_size);
        }
        self.create_or_attach_shm_buffers(final_slot_size)
    }

    fn create_or_attach_shm_buffers(&mut self, final_slot_size: u64) -> crate::error::Result<()> {
        let final_slot_size = if final_slot_size == 0 {
            64
        } else {
            final_slot_size
        };
        let final_buffer_size = self.channel.slot_size_to_buffer_size(final_slot_size);
        let mut current_slot_size: u64 = 0;
        let mut num_buffers = self.channel.ccb().num_buffers.load(Ordering::Relaxed);

        loop {
            while current_slot_size < final_slot_size
                || self.channel.buffers.len() < num_buffers as usize
            {
                let buffer_index = self.channel.buffers.len();
                let shm_name = self
                    .channel
                    .buffer_shared_memory_name(self.resolved_name(), buffer_index);

                // Try to create the shared memory file.
                match create_shm(&shm_name, final_buffer_size as usize) {
                    Ok(fd) => {
                        unsafe {
                            (*self.channel.bcb).sizes[buffer_index]
                                .store(final_buffer_size, Ordering::Relaxed);
                        }
                        let addr = map_memory(
                            fd,
                            final_buffer_size as usize,
                            ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
                        )?;
                        shim_close(fd);
                        self.channel.buffers.push(BufferSet::shared(
                            final_buffer_size,
                            final_slot_size,
                            addr,
                        ));
                        current_slot_size = final_slot_size;
                    }
                    Err(_) => {
                        // File already exists, attach to it.
                        let fd = open_shm(&shm_name)?;
                        let size = get_shm_size(fd, &shm_name)?;
                        let cs = self.channel.buffer_size_to_slot_size(size as u64);
                        let addr = if cs > 0 {
                            map_memory(fd, size, ProtFlags::PROT_READ | ProtFlags::PROT_WRITE)?
                        } else {
                            std::ptr::null_mut()
                        };
                        shim_close(fd);
                        unsafe {
                            (*self.channel.bcb).sizes[buffer_index]
                                .store(final_buffer_size, Ordering::Relaxed);
                        }
                        self.channel
                            .buffers
                            .push(BufferSet::shared(size as u64, cs, addr));
                        current_slot_size = cs;
                    }
                }
            }

            let new_num_buffers = self.channel.buffers.len() as i32;
            if self
                .channel
                .ccb()
                .num_buffers
                .compare_exchange(
                    num_buffers,
                    new_num_buffers,
                    Ordering::Release,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                break;
            }
            num_buffers = self.channel.ccb().num_buffers.load(Ordering::Acquire);
        }
        Ok(())
    }

    fn create_or_attach_split_buffers(&mut self, final_slot_size: u64) -> crate::error::Result<()> {
        let final_slot_size = if final_slot_size == 0 {
            64
        } else {
            final_slot_size
        };
        let final_buffer_size = self.channel.slot_size_to_buffer_size(final_slot_size);
        let mut current_slot_size: u64 = 0;
        let mut num_buffers = self.channel.ccb().num_buffers.load(Ordering::Relaxed);

        loop {
            while current_slot_size < final_slot_size
                || self.channel.buffers.len() < num_buffers as usize
            {
                let buffer_index = self.channel.buffers.len();
                let split_buffer = self.create_or_open_split_buffer_set(
                    buffer_index,
                    final_buffer_size,
                    final_slot_size,
                )?;
                unsafe {
                    (*self.channel.bcb).sizes[buffer_index]
                        .store(split_buffer.full_size, Ordering::Relaxed);
                }
                current_slot_size = split_buffer.slot_size;
                self.channel.buffers.push(split_buffer);
            }

            let new_num_buffers = self.channel.buffers.len() as i32;
            if self
                .channel
                .ccb()
                .num_buffers
                .compare_exchange(
                    num_buffers,
                    new_num_buffers,
                    Ordering::Release,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                break;
            }
            num_buffers = self.channel.ccb().num_buffers.load(Ordering::Acquire);
        }
        Ok(())
    }

    fn create_or_open_split_buffer_set(
        &mut self,
        buffer_index: usize,
        full_size: u64,
        slot_size: u64,
    ) -> crate::error::Result<BufferSet> {
        match self.create_split_buffer_set(buffer_index, full_size, slot_size)? {
            Some(buffer) => Ok(buffer),
            None => open_split_buffer_set(
                &self.channel,
                self.resolved_name(),
                buffer_index,
                full_size,
                slot_size,
                true,
            ),
        }
    }

    fn create_split_buffer_set(
        &mut self,
        buffer_index: usize,
        full_size: u64,
        slot_size: u64,
    ) -> crate::error::Result<Option<BufferSet>> {
        let mut buffer = BufferSet::split(full_size, slot_size);
        buffer.owns_split_buffers = true;
        buffer.split_buffer_callbacks = self.options.split_buffer_callbacks.clone();
        buffer.uses_split_callbacks = self.options.split_buffer_callbacks.allocate.is_some();
        let payload_size = page_aligned_size(slot_size);

        let base = self
            .channel
            .buffer_shared_memory_name(self.resolved_name(), buffer_index);
        let metadata_base = if base.starts_with('/') {
            base
        } else {
            format!("/tmp/{base}")
        };
        let prefix_name = format!("{metadata_base}_prefix");
        let mut prefix_metadata = SplitBufferMetadata {
            channel_name: self.resolved_name().to_string(),
            session_id: self.channel.session_id,
            buffer_index: buffer_index as u32,
            slot_id: 0,
            is_prefix: true,
            full_size: page_aligned_size(
                self.channel.prefix_size as u64 * self.channel.num_slots as u64,
            ),
            allocation_size: page_aligned_size(
                self.channel.prefix_size as u64 * self.channel.num_slots as u64,
            ),
            handle: 0,
            shadow_file: prefix_name.clone(),
            object_name: split_buffer_object_name(&prefix_name),
        };

        let prefix_fd = match create_split_shared_memory_buffer(&prefix_metadata)? {
            Some(fd) => fd,
            None => return Ok(None),
        };
        let prefix_addr = map_memory(
            prefix_fd,
            prefix_metadata.allocation_size as usize,
            ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
        )?;
        prefix_metadata.handle = prefix_fd as u64;
        write_split_buffer_metadata_file(&prefix_metadata)?;
        self.channel
            .pending_client_buffer_registrations
            .push(prefix_metadata.to_proto("split_shm"));
        buffer.split_prefix_fd = prefix_fd;
        buffer.split_prefix_buffer = prefix_addr;
        buffer.split_prefix_buffer_size = prefix_metadata.allocation_size;
        buffer.split_prefix_metadata = Some(prefix_metadata);

        for slot in 0..self.channel.num_slots as usize {
            let shadow_file = format!("{metadata_base}_slot_{slot}");
            let mut metadata = SplitBufferMetadata {
                channel_name: self.resolved_name().to_string(),
                session_id: self.channel.session_id,
                buffer_index: buffer_index as u32,
                slot_id: slot as u32,
                is_prefix: false,
                full_size: payload_size,
                allocation_size: payload_size,
                handle: 0,
                shadow_file: shadow_file.clone(),
                object_name: split_buffer_object_name(&shadow_file),
            };
            let slot_fd;
            let slot_addr;
            let slot_handle;
            let slot_mapped_size;
            let slot_private_data;
            if let Some(allocate) = &self.options.split_buffer_callbacks.allocate {
                let mapping = allocate(&metadata)?;
                if mapping.address.is_null() {
                    return Err(SubspaceError::Internal(
                        "Split buffer allocation callback returned an empty mapping".into(),
                    ));
                }
                slot_fd = -1;
                slot_addr = mapping.address;
                slot_handle = mapping.handle;
                slot_mapped_size = if mapping.size == 0 {
                    payload_size
                } else {
                    mapping.size as u64
                };
                slot_private_data = mapping.private_data;
                metadata.handle = slot_handle;
                metadata.allocation_size = slot_mapped_size;
            } else {
                slot_fd = create_split_shared_memory_buffer(&metadata)?.ok_or_else(|| {
                    SubspaceError::Internal(format!(
                        "Split buffer slot already exists for channel {} slot {slot}",
                        self.resolved_name()
                    ))
                })?;
                slot_addr = map_memory(
                    slot_fd,
                    payload_size as usize,
                    ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
                )?;
                slot_handle = slot_fd as u64;
                slot_mapped_size = payload_size;
                slot_private_data = 0;
                metadata.handle = slot_handle;
            }
            write_split_buffer_metadata_file(&metadata)?;
            self.channel
                .pending_client_buffer_registrations
                .push(metadata.to_proto(if buffer.uses_split_callbacks {
                    "split_callback"
                } else {
                    "split_shm"
                }));
            buffer.split_handles.push(slot_handle);
            buffer.split_slot_buffers.push(slot_addr);
            buffer.split_slot_sizes.push(slot_mapped_size);
            buffer.split_private_data.push(slot_private_data);
            if slot_fd >= 0 {
                buffer.split_fds.push(slot_fd);
            }
            buffer.split_metadata.push(metadata);
        }

        Ok(Some(buffer))
    }

    pub fn trigger_subscribers(&self) {
        for &fd in &self.subscriber_trigger_fds {
            trigger_fd(fd);
        }
    }

    pub fn trigger_retirement(&self, slot_id: usize) {
        for &fd in &self.retirement_trigger_fds {
            let buf = (slot_id as i32).to_ne_bytes();
            shim_write(fd, buf.as_ptr() as *const libc::c_void, buf.len());
        }
    }

    pub fn clear_poll_fd(&self) {
        clear_trigger(self.poll_fd);
    }
}

// ── Shared memory helpers ───────────────────────────────────────────────────

#[cfg(target_os = "linux")]
fn create_shm(name: &str, size: usize) -> crate::error::Result<RawFd> {
    let path = format!("/dev/shm/{}", name);
    let fd = shim_open(
        path.as_str(),
        OFlag::O_RDWR | OFlag::O_CREAT | OFlag::O_EXCL,
        Mode::from_bits_truncate(0o666),
    )?;
    shim_ftruncate(fd, size as libc::off_t);
    Ok(fd)
}

#[cfg(not(target_os = "linux"))]
fn posix_shm_name(shadow_path: &str) -> crate::error::Result<String> {
    let stat = crate::syscall_shim::shim_stat(shadow_path)?;
    Ok(format!("subspace_{}", stat.st_ino))
}

#[cfg(not(target_os = "linux"))]
fn create_shm(name: &str, size: usize) -> crate::error::Result<RawFd> {
    let shadow_fd = shim_open(
        name,
        OFlag::O_RDWR | OFlag::O_CREAT,
        Mode::from_bits_truncate(0o666),
    )?;
    shim_ftruncate(shadow_fd, size as libc::off_t);
    shim_close(shadow_fd);

    // Use the shadow file's inode to build a short name that fits within
    // macOS's PSHMNAMELEN (31-char) limit.
    let shm_name = posix_shm_name(name)?;

    let raw_fd = crate::syscall_shim::shim_shm_open(
        shm_name.as_str(),
        OFlag::O_RDWR | OFlag::O_CREAT | OFlag::O_EXCL,
        Mode::from_bits_truncate(0o666),
    )?;
    shim_ftruncate(raw_fd, size as libc::off_t);
    Ok(raw_fd)
}

#[cfg(target_os = "linux")]
fn open_shm(name: &str) -> crate::error::Result<RawFd> {
    let path = format!("/dev/shm/{}", name);
    let fd = shim_open(path.as_str(), OFlag::O_RDWR, Mode::empty())?;
    Ok(fd)
}

#[cfg(not(target_os = "linux"))]
fn open_shm(name: &str) -> crate::error::Result<RawFd> {
    let shm_name = posix_shm_name(name)?;
    let fd = crate::syscall_shim::shim_shm_open(
        shm_name.as_str(),
        OFlag::O_RDWR,
        nix::sys::stat::Mode::empty(),
    )?;
    Ok(fd)
}

#[cfg(target_os = "linux")]
fn get_shm_size(fd: RawFd, _shadow_path: &str) -> crate::error::Result<usize> {
    let stat = shim_fstat(fd)?;
    Ok(stat.st_size as usize)
}

#[cfg(not(target_os = "linux"))]
fn get_shm_size(_fd: RawFd, shadow_path: &str) -> crate::error::Result<usize> {
    // macOS fstat on shm fds returns page-aligned sizes; read the shadow file
    // to get the real size (matches C++ GetBufferSize).
    let stat = crate::syscall_shim::shim_stat(shadow_path)?;
    Ok(stat.st_size as usize)
}

pub fn trigger_fd(fd: RawFd) {
    if fd < 0 {
        return;
    }
    let val: u64 = 1;
    shim_write(fd, val.to_ne_bytes().as_ptr() as *const libc::c_void, 8);
}

pub fn clear_trigger(fd: RawFd) {
    if fd < 0 {
        return;
    }
    let mut buf = [0u8; 8];
    loop {
        let n = shim_read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len());
        if n <= 0 {
            break;
        }
    }
}

pub fn attach_buffers(
    channel: &mut Channel,
    resolved_name: &str,
    read_write: bool,
) -> crate::error::Result<()> {
    if channel.use_split_buffers {
        return attach_split_buffers(channel, resolved_name, read_write);
    }
    attach_shm_buffers(channel, resolved_name, read_write)
}

fn attach_shm_buffers(
    channel: &mut Channel,
    resolved_name: &str,
    read_write: bool,
) -> crate::error::Result<()> {
    let num_buffers = channel.ccb().num_buffers.load(Ordering::Acquire) as usize;
    while channel.buffers.len() < num_buffers {
        let buffer_index = channel.buffers.len();
        let shm_name = channel.buffer_shared_memory_name(resolved_name, buffer_index);

        let fd = open_shm(&shm_name)?;
        let size = get_shm_size(fd, &shm_name)?;
        let cs = channel.buffer_size_to_slot_size(size as u64);
        let prot = if read_write {
            ProtFlags::PROT_READ | ProtFlags::PROT_WRITE
        } else {
            ProtFlags::PROT_READ
        };
        let addr = if cs > 0 {
            map_memory(fd, size, prot)?
        } else {
            std::ptr::null_mut()
        };
        shim_close(fd);
        channel
            .buffers
            .push(BufferSet::shared(size as u64, cs, addr));
    }
    Ok(())
}

fn attach_split_buffers(
    channel: &mut Channel,
    resolved_name: &str,
    read_write: bool,
) -> crate::error::Result<()> {
    let num_buffers = channel.ccb().num_buffers.load(Ordering::Acquire) as usize;
    while channel.buffers.len() < num_buffers {
        let buffer_index = channel.buffers.len();
        let full_size = channel.bcb().sizes[buffer_index].load(Ordering::Acquire);
        let slot_size = channel.buffer_size_to_slot_size(full_size);
        let buffer = open_split_buffer_set(
            channel,
            resolved_name,
            buffer_index,
            full_size,
            slot_size,
            read_write,
        )?;
        channel.buffers.push(buffer);
    }
    Ok(())
}

fn open_split_buffer_set(
    channel: &Channel,
    resolved_name: &str,
    buffer_index: usize,
    full_size: u64,
    slot_size: u64,
    read_write: bool,
) -> crate::error::Result<BufferSet> {
    let payload_size = page_aligned_size(slot_size);
    let base = channel.buffer_shared_memory_name(resolved_name, buffer_index);
    let metadata_base = if base.starts_with('/') {
        base
    } else {
        format!("/tmp/{base}")
    };
    let prefix_name = format!("{metadata_base}_prefix");
    let prefix_metadata = read_split_buffer_metadata_file(&prefix_name)?;
    let prefix_fd = open_split_shared_memory_buffer(&prefix_metadata)?;
    let prefix_addr = map_memory(
        prefix_fd,
        prefix_metadata.allocation_size as usize,
        ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
    )?;

    let mut buffer = BufferSet::split(full_size, slot_size);
    buffer.split_buffer_callbacks = channel.split_buffer_callbacks.clone();
    buffer.uses_split_callbacks = channel.split_buffer_callbacks.map.is_some();
    buffer.split_prefix_fd = prefix_fd;
    buffer.split_prefix_buffer = prefix_addr;
    buffer.split_prefix_buffer_size = prefix_metadata.allocation_size;
    buffer.split_prefix_metadata = Some(prefix_metadata);

    let prot = if read_write {
        ProtFlags::PROT_READ | ProtFlags::PROT_WRITE
    } else {
        ProtFlags::PROT_READ
    };
    for slot in 0..channel.num_slots as usize {
        let shadow_file = format!("{metadata_base}_slot_{slot}");
        let metadata = read_split_buffer_metadata_file(&shadow_file)?;
        let slot_fd;
        let slot_addr;
        let slot_handle;
        let slot_size;
        let slot_private_data;
        if let Some(map) = &channel.split_buffer_callbacks.map {
            let mapping = map(&metadata)?;
            if mapping.address.is_null() {
                return Err(SubspaceError::Internal(
                    "Split buffer map callback returned an empty mapping".into(),
                ));
            }
            slot_fd = -1;
            slot_addr = mapping.address;
            slot_handle = if mapping.handle == 0 {
                metadata.handle
            } else {
                mapping.handle
            };
            slot_size = if mapping.size == 0 {
                payload_size
            } else {
                mapping.size as u64
            };
            slot_private_data = mapping.private_data;
        } else {
            slot_fd = open_split_shared_memory_buffer(&metadata)?;
            slot_addr = map_memory(slot_fd, metadata.allocation_size as usize, prot)?;
            slot_handle = slot_fd as u64;
            slot_size = if metadata.allocation_size == 0 {
                payload_size
            } else {
                metadata.allocation_size
            };
            slot_private_data = 0;
        }
        buffer.split_handles.push(slot_handle);
        buffer.split_slot_buffers.push(slot_addr);
        buffer.split_slot_sizes.push(slot_size);
        buffer.split_private_data.push(slot_private_data);
        if slot_fd >= 0 {
            buffer.split_fds.push(slot_fd);
        }
        buffer.split_metadata.push(metadata);
    }

    Ok(buffer)
}
