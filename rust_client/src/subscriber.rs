// Copyright 2024-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

use crate::channel::*;
use crate::checksum;
use crate::message::ActiveMessage;
use crate::options::SubscriberOptions;
use crate::publisher::{attach_buffers, clear_trigger, trigger_fd};
use std::collections::{HashMap, HashSet, VecDeque};
use std::os::unix::io::RawFd;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, Weak};

use crate::error::Result;
use crate::message::Message;

pub type DroppedMessageCallback = Box<dyn Fn(i64) + Send + Sync>;
pub type MessageCallback = Box<dyn Fn(Message) + Send + Sync>;
pub type OnReceiveCallback = Box<dyn Fn(*mut u8, i64) -> Result<i64> + Send + Sync>;

pub struct SubscriberImpl {
    pub channel: Channel,
    pub subscriber_id: i32,
    pub options: SubscriberOptions,

    pub poll_fd: RawFd,
    pub trigger_fd: RawFd,
    pub reliable_publisher_fds: Mutex<Vec<RawFd>>,
    pub retirement_trigger_fds: Vec<RawFd>,

    num_active_messages: std::sync::atomic::AtomicI32,

    ordinal_trackers: HashMap<i32, OrdinalTracker>,

    self_ref: Option<Weak<Mutex<SubscriberImpl>>>,
    pub(crate) active_messages: Vec<Arc<ActiveMessage>>,
    pub(crate) dropped_message_callback: Option<DroppedMessageCallback>,
    pub(crate) message_callback: Option<MessageCallback>,
    pub(crate) on_receive_callback: Option<OnReceiveCallback>,
    pub(crate) checksum_callback: Option<checksum::ChecksumCallback>,
}

struct OrdinalTracker {
    ring: FastRingBuffer,
    last_ordinal_seen: u64,
}

impl OrdinalTracker {
    fn new() -> Self {
        Self {
            ring: FastRingBuffer::new(MAX_SLOT_OWNERS),
            last_ordinal_seen: 0,
        }
    }
}

/// Ring buffer + set for tracking seen ordinals per vchan.
struct FastRingBuffer {
    buffer: VecDeque<OrdinalAndVchanId>,
    set: HashSet<OrdinalAndVchanId>,
    capacity: usize,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct OrdinalAndVchanId {
    ordinal: u64,
    vchan_id: i32,
}

impl FastRingBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            buffer: VecDeque::with_capacity(capacity),
            set: HashSet::with_capacity(capacity),
            capacity,
        }
    }

    fn insert(&mut self, value: OrdinalAndVchanId) {
        if self.buffer.len() == self.capacity {
            if let Some(old) = self.buffer.pop_front() {
                self.set.remove(&old);
            }
        }
        self.buffer.push_back(value);
        self.set.insert(value);
    }

    fn contains(&self, value: &OrdinalAndVchanId) -> bool {
        self.set.contains(value)
    }

    fn traverse<F: FnMut(&OrdinalAndVchanId)>(&self, mut func: F) {
        for v in &self.buffer {
            func(v);
        }
    }

    fn size(&self) -> usize {
        self.buffer.len()
    }
}

fn virtual_channel_id_match(slot_vchan_id: i16, subscriber_vchan_id: i32) -> bool {
    subscriber_vchan_id == -1
        || slot_vchan_id as i32 == -1
        || slot_vchan_id as i32 == subscriber_vchan_id
}

impl SubscriberImpl {
    pub fn new(
        name: String,
        num_slots: i32,
        channel_id: i32,
        subscriber_id: i32,
        vchan_id: i32,
        session_id: u64,
        channel_type: String,
        options: SubscriberOptions,
    ) -> Self {
        let mut s = Self {
            channel: Channel::new(
                name,
                num_slots,
                channel_id,
                channel_type,
                vchan_id,
                session_id,
            ),
            subscriber_id,
            options,
            poll_fd: -1,
            trigger_fd: -1,
            reliable_publisher_fds: Mutex::new(Vec::new()),
            retirement_trigger_fds: Vec::new(),
            num_active_messages: std::sync::atomic::AtomicI32::new(0),
            ordinal_trackers: HashMap::new(),
            self_ref: None,
            active_messages: Vec::new(),
            dropped_message_callback: None,
            message_callback: None,
            on_receive_callback: None,
            checksum_callback: None,
        };
        s.get_or_create_tracker(vchan_id);
        s
    }

    pub fn set_self_ref(&mut self, weak: Weak<Mutex<SubscriberImpl>>) {
        self.self_ref = Some(weak);
    }

    /// (Re)builds the per-slot active messages used by `Message` to release
    /// slots on drop.  Must be called after `set_self_ref` and whenever
    /// `num_slots` changes (e.g. after a reload).
    pub fn init_active_messages(&mut self) {
        let weak = self.self_ref.as_ref().expect("self_ref not set").clone();
        self.active_messages.clear();
        for i in 0..self.channel.num_slots as usize {
            let w = weak.clone();
            self.active_messages.push(Arc::new(ActiveMessage::new(Box::new(move || {
                if let Some(sub_arc) = w.upgrade() {
                    let sub = sub_arc.lock().unwrap();
                    sub.remove_active_message(i);
                }
            }))));
        }
    }

    pub fn resolved_name(&self) -> &str {
        if !self.options.mux.is_empty() && self.channel.vchan_id != -1 {
            &self.options.mux
        } else {
            &self.channel.name
        }
    }

    fn get_or_create_tracker(&mut self, vchan_id: i32) -> &mut OrdinalTracker {
        self.ordinal_trackers
            .entry(vchan_id)
            .or_insert_with(OrdinalTracker::new)
    }

    pub fn remember_ordinal(&mut self, ordinal: u64, vchan_id: i32) {
        let tracker = self.get_or_create_tracker(self.channel.vchan_id);
        tracker.ring.insert(OrdinalAndVchanId {
            ordinal,
            vchan_id,
        });
    }

    pub fn add_active_message(&self) -> bool {
        let old = self.num_active_messages.fetch_add(1, Ordering::Relaxed);
        if old >= self.options.max_active_messages {
            self.num_active_messages.fetch_sub(1, Ordering::Relaxed);
            return false;
        }
        true
    }

    pub fn remove_active_message(&self, slot_idx: usize) {
        let slot = self.channel.slot_ref(slot_idx);
        slot.sub_owners.clear(self.subscriber_id as usize);
        let ordinal = slot.ordinal;
        let vchan_id = slot.vchan_id as i32;
        let bridged_slot_id = slot.bridged_slot_id;
        let reliable = self.options.reliable;

        self.channel.atomic_inc_ref_count(
            slot_idx,
            reliable,
            -1,
            ordinal,
            vchan_id,
            true,
            Some(|| {
                self.trigger_retirement(bridged_slot_id as usize);
            }),
        );
        let new_count = self.num_active_messages.fetch_sub(1, Ordering::Relaxed) - 1;
        if new_count < self.options.max_active_messages {
            self.trigger();
            if reliable {
                self.trigger_reliable_publishers();
            }
        }
    }

    pub fn num_active_messages(&self) -> i32 {
        self.num_active_messages.load(Ordering::Relaxed)
    }

    pub fn populate_active_slots(&self, bits: &crate::bitset::InPlaceAtomicBitSet) {
        loop {
            let num_messages = self
                .channel
                .ccb()
                .total_messages
                .load(Ordering::Relaxed);
            bits.clear_all();

            for i in 0..self.channel.num_slots as usize {
                let s = self.channel.slot_ref(i);
                let refs = s.refs.load(Ordering::Relaxed);
                if virtual_channel_id_match(s.vchan_id, self.channel.vchan_id)
                    && s.ordinal != 0
                    && (refs & PUB_OWNED) == 0
                {
                    bits.set(i);
                }
            }

            if num_messages
                == self
                    .channel
                    .ccb()
                    .total_messages
                    .load(Ordering::Relaxed)
            {
                break;
            }
        }
    }

    pub fn collect_visible_slots(&mut self, bits: &crate::bitset::InPlaceAtomicBitSet) {
        loop {
            let num_messages = self
                .channel
                .ccb()
                .total_messages
                .load(Ordering::Relaxed);
            self.channel.active_slots.clear();

            bits.traverse(|i| {
                if self.channel.embargoed_slots.is_set(i) {
                    return;
                }
                let s = self.channel.slot_ref(i);
                if !virtual_channel_id_match(s.vchan_id, self.channel.vchan_id) {
                    return;
                }
                if s.buffer_index == -1 {
                    return;
                }
                self.channel.active_slots.push(ActiveSlot {
                    slot_index: i,
                    ordinal: s.ordinal,
                    timestamp: s.timestamp,
                    vchan_id: s.vchan_id as i32,
                });
            });

            if num_messages
                == self
                    .channel
                    .ccb()
                    .total_messages
                    .load(Ordering::Relaxed)
            {
                break;
            }
        }
    }

    fn find_unseen_ordinal(&self) -> Option<usize> {
        let tracker = self.ordinal_trackers.get(&self.channel.vchan_id)?;
        for (i, active) in self.channel.active_slots.iter().enumerate() {
            if active.ordinal != 0
                && !tracker.ring.contains(&OrdinalAndVchanId {
                    ordinal: active.ordinal,
                    vchan_id: active.vchan_id,
                })
            {
                return Some(i);
            }
        }
        None
    }

    pub fn next_slot(&mut self) -> Option<usize> {
        let bits = self
            .channel
            .get_available_slots(self.subscriber_id as usize);
        self.channel.embargoed_slots.clear_all();

        const MAX_RETRIES: usize = 1000;
        let mut retries = 0;

        while retries < MAX_RETRIES {
            retries += 1;

            if self.channel.slot.is_none() {
                self.populate_active_slots(&bits);
            }

            self.collect_visible_slots(&bits);

            self.channel
                .active_slots
                .sort_by_key(|s| s.timestamp);

            let unseen_idx = match self.find_unseen_ordinal() {
                Some(idx) => idx,
                None => return None,
            };

            let active = self.channel.active_slots[unseen_idx].clone();
            let reliable = self.options.reliable;

            if self.channel.atomic_inc_ref_count::<fn()>(
                active.slot_index,
                reliable,
                1,
                active.ordinal,
                active.vchan_id,
                false,
                None,
            ) {
                if !self.channel.validate_slot_buffer(active.slot_index)
                    || self.channel.slot_ref(active.slot_index).buffer_index == -1
                {
                    self.channel.embargoed_slots.set(active.slot_index);
                    self.channel.atomic_inc_ref_count::<fn()>(
                        active.slot_index,
                        reliable,
                        -1,
                        active.ordinal,
                        active.vchan_id,
                        false,
                        None,
                    );
                    continue;
                }
                return Some(active.slot_index);
            }
        }
        None
    }

    pub fn last_slot(&mut self) -> Option<usize> {
        let bits = self
            .channel
            .get_available_slots(self.subscriber_id as usize);
        self.channel.embargoed_slots.clear_all();

        loop {
            if self.channel.slot.is_none() {
                self.populate_active_slots(&bits);
            }

            self.collect_visible_slots(&bits);

            self.channel
                .active_slots
                .sort_by_key(|s| s.timestamp);

            let new_active = if let Some(last) = self.channel.active_slots.last() {
                if let Some(current_idx) = self.channel.slot {
                    if current_idx == last.slot_index {
                        None
                    } else {
                        Some(last.clone())
                    }
                } else {
                    Some(last.clone())
                }
            } else {
                None
            };

            let active = match new_active {
                Some(a) => a,
                None => return None,
            };

            let reliable = self.options.reliable;
            if self.channel.atomic_inc_ref_count::<fn()>(
                active.slot_index,
                reliable,
                1,
                active.ordinal,
                active.vchan_id,
                false,
                None,
            ) {
                if !self.channel.validate_slot_buffer(active.slot_index)
                    || self.channel.slot_ref(active.slot_index).buffer_index == -1
                {
                    self.channel.embargoed_slots.set(active.slot_index);
                    self.channel.atomic_inc_ref_count::<fn()>(
                        active.slot_index,
                        reliable,
                        -1,
                        active.ordinal,
                        active.vchan_id,
                        false,
                        None,
                    );
                    continue;
                }
                return Some(active.slot_index);
            }
        }
    }

    pub fn claim_slot(&mut self, slot_idx: usize, vchan_id: i32, was_newest: bool) {
        let slot = self.channel.slot_ref(slot_idx);
        slot.sub_owners.set(self.subscriber_id as usize);
        if was_newest {
            self.channel
                .get_available_slots(self.subscriber_id as usize)
                .clear_all();
        } else {
            self.channel
                .get_available_slots(self.subscriber_id as usize)
                .clear(slot_idx);
        }
        let ordinal = slot.ordinal;
        self.remember_ordinal(ordinal, vchan_id);
        self.channel.slot_mut(slot_idx).flags |= MESSAGE_SEEN;
    }

    pub fn unread_slot(&self, slot_idx: usize) {
        self.channel.slot_mut(slot_idx).flags &= !MESSAGE_SEEN;
        self.decrement_slot_ref(slot_idx, false);
    }

    pub fn ignore_activation(&mut self, slot_idx: usize) {
        let slot = self.channel.slot_ref(slot_idx);
        let ordinal = slot.ordinal;
        let vchan_id = slot.vchan_id as i32;
        self.remember_ordinal(ordinal, vchan_id);
        self.decrement_slot_ref(slot_idx, true);
        self.channel.slot_mut(slot_idx).flags |= MESSAGE_SEEN;
    }

    pub fn decrement_slot_ref(&self, slot_idx: usize, retire: bool) {
        let slot = self.channel.slot_ref(slot_idx);
        let ordinal = slot.ordinal & ORDINAL_MASK;
        let vchan_id = self.channel.vchan_id;
        let reliable = self.options.reliable;
        self.channel
            .atomic_inc_ref_count::<fn()>(slot_idx, reliable, -1, ordinal, vchan_id, retire, None);
    }

    pub fn detect_drops(&mut self, vchan_id: i32) -> i32 {
        let tracker_vchan = self.channel.vchan_id;
        let tracker = match self.ordinal_trackers.get(&tracker_vchan) {
            Some(t) => t,
            None => return 0,
        };

        let mut ordinals: Vec<OrdinalAndVchanId> = Vec::with_capacity(tracker.ring.size());
        let last_seen = tracker.last_ordinal_seen;
        tracker.ring.traverse(|o| {
            if o.vchan_id == vchan_id && o.ordinal >= last_seen {
                ordinals.push(*o);
            }
        });

        if ordinals.is_empty() {
            return 0;
        }

        ordinals.sort_by(|a, b| {
            a.vchan_id
                .cmp(&b.vchan_id)
                .then(a.ordinal.cmp(&b.ordinal))
        });

        let last_ordinal = ordinals.last().unwrap().ordinal;

        let mut drops: i32 = 0;
        for i in 1..ordinals.len() {
            if ordinals[i].vchan_id != vchan_id {
                continue;
            }
            let gap = ordinals[i].ordinal.wrapping_sub(ordinals[i - 1].ordinal);
            if gap > 1 {
                drops += (gap - 1) as i32;
            }
        }

        // Update last_ordinal_seen.
        let tracker = self.ordinal_trackers.get_mut(&tracker_vchan).unwrap();
        tracker.last_ordinal_seen = last_ordinal;

        drops
    }

    pub fn find_active_slot_by_timestamp(
        &mut self,
        timestamp: u64,
    ) -> Option<usize> {
        self.channel.embargoed_slots.clear_all();
        loop {
            let mut buffer: Vec<ActiveSlot> = Vec::with_capacity(self.channel.num_slots as usize);

            for i in 0..self.channel.num_slots as usize {
                if self.channel.embargoed_slots.is_set(i) {
                    continue;
                }
                let s = self.channel.slot_ref(i);
                let refs = s.refs.load(Ordering::Relaxed);
                if s.ordinal != 0 && (refs & PUB_OWNED) == 0 {
                    let prefix = self.channel.get_prefix(i);
                    let ts = if !prefix.is_null() {
                        unsafe { (*prefix).timestamp }
                    } else {
                        s.timestamp
                    };
                    buffer.push(ActiveSlot {
                        slot_index: i,
                        ordinal: 0,
                        timestamp: ts,
                        vchan_id: s.vchan_id as i32,
                    });
                }
            }

            buffer.sort_by_key(|s| s.timestamp);

            if buffer.is_empty() || timestamp < buffer.first().unwrap().timestamp {
                return None;
            }

            let pos = buffer.partition_point(|s| s.timestamp < timestamp);
            if pos >= buffer.len() {
                return None;
            }

            let active = &buffer[pos];
            let reliable = self.options.reliable;
            if self.channel.atomic_inc_ref_count::<fn()>(
                active.slot_index,
                reliable,
                1,
                active.ordinal,
                active.vchan_id,
                false,
                None,
            ) {
                if !self.channel.validate_slot_buffer(active.slot_index)
                    || self.channel.slot_ref(active.slot_index).buffer_index == -1
                {
                    self.channel.embargoed_slots.set(active.slot_index);
                    self.channel.atomic_inc_ref_count::<fn()>(
                        active.slot_index,
                        reliable,
                        -1,
                        active.ordinal,
                        active.vchan_id,
                        false,
                        None,
                    );
                    continue;
                }
                let slot = self.channel.slot_mut(active.slot_index);
                slot.flags |= MESSAGE_SEEN;
                slot.sub_owners.set(self.subscriber_id as usize);
                return Some(active.slot_index);
            }
        }
    }

    pub fn trigger(&self) {
        trigger_fd(self.trigger_fd);
    }

    pub fn untrigger(&self) {
        clear_trigger(self.poll_fd);
    }

    pub fn clear_poll_fd(&self) {
        clear_trigger(self.poll_fd);
    }

    pub fn trigger_reliable_publishers(&self) {
        let fds = self.reliable_publisher_fds.lock().unwrap();
        for &fd in fds.iter() {
            trigger_fd(fd);
        }
    }

    fn trigger_retirement(&self, slot_id: usize) {
        for &fd in &self.retirement_trigger_fds {
            let buf = (slot_id as i32).to_ne_bytes();
            unsafe {
                libc::write(fd, buf.as_ptr() as *const libc::c_void, buf.len());
            }
        }
    }

    pub fn attach_buffers(&mut self) -> crate::error::Result<()> {
        let read_write = self.options.bridge || self.options.read_write;
        attach_buffers(&mut self.channel, read_write)
    }
}
