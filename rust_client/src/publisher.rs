// Copyright 2024-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

use crate::channel::*;
use crate::checksum;
use crate::error::Result;
use crate::options::PublisherOptions;
use nix::fcntl::OFlag;
use nix::sys::mman::ProtFlags;
use nix::sys::stat::Mode;
use std::os::unix::io::RawFd;
use std::sync::atomic::Ordering;

pub type OnSendCallback = Box<dyn Fn(*mut u8, i64) -> Result<i64> + Send + Sync>;
pub type ResizeCallback = Box<dyn Fn(i32, i32) -> Result<()> + Send + Sync>;

pub struct PublishedMessage {
    pub new_slot: Option<usize>,
    pub ordinal: u64,
    pub timestamp: u64,
}

pub struct PublisherImpl {
    pub channel: Channel,
    pub publisher_id: i32,
    pub options: PublisherOptions,

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
                channel_id,
                channel_type,
                vchan_id,
                session_id,
            ),
            publisher_id,
            options,
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
                    if (refs & REFS_MASK) == 0 && s.timestamp < earliest_timestamp {
                        slot_idx = Some(i);
                        earliest_timestamp = s.timestamp;
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
                    (*slot_ptr).ordinal,
                    ((old_refs >> VCHAN_ID_SHIFT) & VCHAN_ID_MASK) as i32,
                    ((old_refs >> RETIRED_REFS_SHIFT) & RETIRED_REFS_MASK) as i32,
                );

                if (*slot_ptr)
                    .refs
                    .compare_exchange_weak(
                        expected,
                        ref_val,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
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
        let slot = self.channel.slot_mut(si);
        slot.ordinal = 0;
        slot.timestamp = 0;
        slot.vchan_id = self.channel.vchan_id as i16;
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
                        ordinal: s.ordinal,
                        timestamp: s.timestamp,
                        vchan_id: s.vchan_id as i32,
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
                            ordinal: s.ordinal,
                            timestamp: s.timestamp,
                            vchan_id: s.vchan_id as i32,
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
                            ordinal: s.ordinal,
                            timestamp: s.timestamp,
                            vchan_id: s.vchan_id as i32,
                        });
                    }
                }
            }

            self.channel
                .active_slots
                .sort_by_key(|s| s.timestamp);

            slot_idx = None;
            for active in &self.channel.active_slots {
                let s = self.channel.slot_ref(active.slot_index);
                let refs = s.refs.load(Ordering::Relaxed);
                if ((refs >> RELIABLE_REF_COUNT_SHIFT) & REF_COUNT_MASK) != 0 {
                    break;
                }
                if active.ordinal != 0 && (s.flags & MESSAGE_SEEN) == 0 {
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
                    (*slot_ptr).ordinal,
                    ((old_refs >> VCHAN_ID_SHIFT) & VCHAN_ID_MASK) as i32,
                    ((old_refs >> RETIRED_REFS_SHIFT) & RETIRED_REFS_MASK) as i32,
                );

                if (*slot_ptr)
                    .refs
                    .compare_exchange_weak(
                        expected,
                        ref_val,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
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
        let slot = self.channel.slot_mut(si);
        slot.ordinal = 0;
        slot.timestamp = 0;
        slot.vchan_id = self.channel.vchan_id as i16;
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
        let slot = self.channel.slot_mut(slot_idx);
        let vchan_id = self.channel.vchan_id;

        slot.ordinal = self.channel.ccb().ordinals.next(slot.vchan_id as i32);
        slot.timestamp = now_ns();
        slot.flags = 0;

        let prefix = self.channel.get_prefix(slot_idx);
        if !prefix.is_null() {
            unsafe {
                let p = &mut *prefix;
                if omit_prefix {
                    let slot = self.channel.slot_mut(slot_idx);
                    slot.timestamp = p.timestamp;
                    slot.vchan_id = p.vchan_id as i16;
                    slot.bridged_slot_id = if use_prefix_slot_id {
                        p.slot_id
                    } else {
                        slot.id
                    };
                } else {
                    let slot = self.channel.slot_ref(slot_idx);
                    p.message_size = slot.message_size;
                    p.ordinal = slot.ordinal;
                    p.timestamp = slot.timestamp;
                    p.vchan_id = slot.vchan_id as i32;
                    p.flags = 0;
                    p.slot_id = slot.id;
                    let slot = self.channel.slot_mut(slot_idx);
                    slot.bridged_slot_id = slot.id;
                    if is_activation {
                        p.set_is_activation();
                        slot.flags |= MESSAGE_IS_ACTIVATION;
                        self.channel.ccb().activation_tracker.activate(vchan_id);
                    }
                    if self.options.checksum {
                        p.set_has_checksum();
                        let buffer = self.channel.get_buffer_address(slot_idx);
                        let data = checksum::get_message_checksum_data(
                            prefix,
                            buffer,
                            slot.message_size as usize,
                        );
                        let spans: Vec<&[u8]> = data.iter().copied().collect();
                        p.checksum = if let Some(ref cb) = self.checksum_callback {
                            cb(&spans)
                        } else {
                            checksum::calculate_checksum(&spans)
                        };
                    }
                }
            }
        }

        let slot = self.channel.slot_ref(slot_idx);
        if !is_activation {
            self.channel
                .ccb()
                .total_messages
                .fetch_add(1, Ordering::Relaxed);
            self.channel
                .ccb()
                .total_bytes
                .fetch_add(slot.message_size, Ordering::Relaxed);

            let msg_size = slot.message_size as u32;
            let mut old_max = self
                .channel
                .ccb()
                .max_message_size
                .load(Ordering::Relaxed);
            while msg_size > old_max {
                match self
                    .channel
                    .ccb()
                    .max_message_size
                    .compare_exchange_weak(old_max, msg_size, Ordering::Relaxed, Ordering::Relaxed)
                {
                    Ok(_) => break,
                    Err(v) => old_max = v,
                }
            }
        }

        // Release the slot: store refs with ordinal, no PUB_OWNED.
        let slot = self.channel.slot_ref(slot_idx);
        slot.refs.store(
            build_refs_bit_field(slot.ordinal, vchan_id, 0),
            Ordering::Release,
        );

        // Tell all subscribers the slot is available.
        let ccb = self.channel.ccb();
        ccb.subscribers.traverse(|sub_id| {
            if vchan_id != -1
                && self.channel.get_sub_vchan_id(sub_id) != -1
                && vchan_id != self.channel.get_sub_vchan_id(sub_id)
            {
                return;
            }
            self.channel
                .get_available_slots(sub_id)
                .set(slot_idx);
        });

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

    pub fn create_or_attach_buffers(
        &mut self,
        final_slot_size: u64,
    ) -> crate::error::Result<()> {
        let final_slot_size = if final_slot_size == 0 {
            64
        } else {
            final_slot_size
        };
        let final_buffer_size = self.channel.slot_size_to_buffer_size(final_slot_size);
        let mut current_slot_size: u64 = 0;
        let mut num_buffers = self
            .channel
            .ccb()
            .num_buffers
            .load(Ordering::Relaxed);

        loop {
            while current_slot_size < final_slot_size
                || self.channel.buffers.len() < num_buffers as usize
            {
                let buffer_index = self.channel.buffers.len();
                let shm_name =
                    self.channel
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
                        unsafe { libc::close(fd); }
                        self.channel.buffers.push(BufferSet {
                            full_size: final_buffer_size,
                            slot_size: final_slot_size,
                            buffer: addr,
                        });
                        current_slot_size = final_slot_size;
                    }
                    Err(_) => {
                        // File already exists, attach to it.
                        let fd = open_shm(&shm_name)?;
                        let size = get_shm_size(fd)?;
                        let cs = self.channel.buffer_size_to_slot_size(size as u64);
                        let addr = if cs > 0 {
                            map_memory(
                                fd,
                                size,
                                ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
                            )?
                        } else {
                            std::ptr::null_mut()
                        };
                        unsafe { libc::close(fd); }
                        unsafe {
                            (*self.channel.bcb).sizes[buffer_index]
                                .store(final_buffer_size, Ordering::Relaxed);
                        }
                        self.channel.buffers.push(BufferSet {
                            full_size: size as u64,
                            slot_size: cs,
                            buffer: addr,
                        });
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
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                break;
            }
            num_buffers = self
                .channel
                .ccb()
                .num_buffers
                .load(Ordering::Relaxed);
        }
        Ok(())
    }

    pub fn trigger_subscribers(&self) {
        for &fd in &self.subscriber_trigger_fds {
            trigger_fd(fd);
        }
    }

    pub fn trigger_retirement(&self, slot_id: usize) {
        for &fd in &self.retirement_trigger_fds {
            let buf = (slot_id as i32).to_ne_bytes();
            unsafe {
                libc::write(fd, buf.as_ptr() as *const libc::c_void, buf.len());
            }
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
    let fd = nix::fcntl::open(
        path.as_str(),
        OFlag::O_RDWR | OFlag::O_CREAT | OFlag::O_EXCL,
        Mode::from_bits_truncate(0o666),
    )?;
    unsafe { libc::ftruncate(fd, size as libc::off_t); }
    Ok(fd)
}

#[cfg(not(target_os = "linux"))]
fn create_shm(name: &str, size: usize) -> crate::error::Result<RawFd> {
    use nix::sys::mman::shm_open;
    let fd = shm_open(
        name,
        OFlag::O_RDWR | OFlag::O_CREAT | OFlag::O_EXCL,
        Mode::from_bits_truncate(0o666),
    )?;
    unsafe { libc::ftruncate(fd, size as libc::off_t); }
    Ok(fd)
}

#[cfg(target_os = "linux")]
fn open_shm(name: &str) -> crate::error::Result<RawFd> {
    let path = format!("/dev/shm/{}", name);
    let fd = nix::fcntl::open(path.as_str(), OFlag::O_RDWR, Mode::empty())?;
    Ok(fd)
}

#[cfg(not(target_os = "linux"))]
fn open_shm(name: &str) -> crate::error::Result<RawFd> {
    use nix::sys::mman::shm_open;
    let fd = shm_open(name, OFlag::O_RDWR, Mode::empty())?;
    Ok(fd)
}

fn get_shm_size(fd: RawFd) -> crate::error::Result<usize> {
    let stat = nix::sys::stat::fstat(fd)?;
    Ok(stat.st_size as usize)
}

pub fn trigger_fd(fd: RawFd) {
    if fd < 0 {
        return;
    }
    let val: u64 = 1;
    unsafe {
        libc::write(fd, val.to_ne_bytes().as_ptr() as *const libc::c_void, 8);
    }
}

pub fn clear_trigger(fd: RawFd) {
    if fd < 0 {
        return;
    }
    let mut buf = [0u8; 8];
    loop {
        let n = unsafe {
            libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len())
        };
        if n <= 0 {
            break;
        }
    }
}

pub fn attach_buffers(channel: &mut Channel, read_write: bool) -> crate::error::Result<()> {
    let num_buffers = channel.ccb().num_buffers.load(Ordering::Relaxed) as usize;
    let resolved_name = channel.name.clone();
    while channel.buffers.len() < num_buffers {
        let buffer_index = channel.buffers.len();
        let shm_name = channel.buffer_shared_memory_name(&resolved_name, buffer_index);

        let fd = open_shm(&shm_name)?;
        let size = get_shm_size(fd)?;
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
        unsafe { libc::close(fd); }
        channel.buffers.push(BufferSet {
            full_size: size as u64,
            slot_size: cs,
            buffer: addr,
        });
    }
    Ok(())
}
