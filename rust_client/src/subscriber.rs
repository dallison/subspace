// Copyright 2024-2026 David Allison
// Rust client is Copyright 2026 Cruise LLC
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
    pub subscriber_queue_size: i32,
    pub options: SubscriberOptions,

    pub poll_fd: RawFd,
    pub trigger_fd: RawFd,
    pub reliable_publisher_fds: Mutex<Vec<RawFd>>,
    pub retirement_trigger_fds: Vec<RawFd>,

    num_active_messages: std::sync::atomic::AtomicI32,

    ordinal_trackers: HashMap<i32, OrdinalTracker>,

    self_ref: Option<Weak<Mutex<SubscriberImpl>>>,
    pub(crate) active_messages: Vec<Arc<ActiveMessage>>,
    pub(crate) kept_active_message: Option<(usize, Arc<ActiveMessage>)>,
    pub(crate) dropped_message_callback: Option<DroppedMessageCallback>,
    pub(crate) message_callback: Option<MessageCallback>,
    pub(crate) on_receive_callback: Option<OnReceiveCallback>,
    pub(crate) checksum_callback: Option<checksum::ChecksumCallback>,
    pub checksum_tmp: Vec<u8>,
    pub poll_drain_pending: bool,
    pub(crate) poll_drain_exhausted: bool,
    pub(crate) queue_drain_tail: Option<u64>,
    queue_bitset_fallback: bool,
    pub(crate) poll_snapshot_valid: bool,
    poll_snapshot_total: u64,
    poll_snapshot: Vec<ActiveSlot>,
    newest_snapshot: Option<Vec<ActiveSlot>>,
    pub(crate) pending_queue_drops: i32,
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
        default_subscriber_queue_size: i32,
        subscriber_queue_arena_size: u64,
        subscriber_queue_size: i32,
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
                default_subscriber_queue_size,
                subscriber_queue_arena_size,
                channel_id,
                channel_type,
                vchan_id,
                session_id,
            ),
            subscriber_id,
            subscriber_queue_size,
            options,
            poll_fd: -1,
            trigger_fd: -1,
            reliable_publisher_fds: Mutex::new(Vec::new()),
            retirement_trigger_fds: Vec::new(),
            num_active_messages: std::sync::atomic::AtomicI32::new(0),
            ordinal_trackers: HashMap::new(),
            self_ref: None,
            active_messages: Vec::new(),
            kept_active_message: None,
            dropped_message_callback: None,
            message_callback: None,
            on_receive_callback: None,
            checksum_callback: None,
            checksum_tmp: vec![0u8; 4],
            poll_drain_pending: false,
            poll_drain_exhausted: false,
            queue_drain_tail: None,
            queue_bitset_fallback: false,
            poll_snapshot_valid: false,
            poll_snapshot_total: 0,
            poll_snapshot: Vec::new(),
            newest_snapshot: None,
            pending_queue_drops: 0,
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

    pub fn reset_delivery_state(&mut self) {
        self.channel.active_slots.clear();
        self.channel.embargoed_slots.clear_all();
        self.channel.slot = None;
        self.poll_snapshot_valid = false;
        self.poll_snapshot_total = 0;
        self.poll_snapshot.clear();
        self.poll_drain_exhausted = false;
        self.queue_drain_tail = None;
        self.queue_bitset_fallback = false;
        self.pending_queue_drops = 0;
        self.newest_snapshot = None;
        self.ordinal_trackers.clear();
        self.get_or_create_tracker(self.channel.vchan_id);
    }

    pub fn total_messages(&self) -> u64 {
        self.channel.ccb().total_messages.load(Ordering::SeqCst)
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

    /// Get a const slice over the user metadata area for the current slot.
    /// Returns an empty slice if metadata_size is 0 or no slot is active.
    pub fn get_metadata(&self) -> &[u8] {
        if self.channel.metadata_size == 0 {
            return &[];
        }
        if let Some(slot_idx) = self.channel.slot {
            let prefix = self.channel.get_prefix(slot_idx);
            if prefix.is_null() {
                return &[];
            }
            unsafe {
                checksum::get_metadata_slice_const(
                    prefix,
                    self.channel.checksum_size,
                    self.channel.metadata_size,
                )
            }
        } else {
            &[]
        }
    }

    fn get_or_create_tracker(&mut self, vchan_id: i32) -> &mut OrdinalTracker {
        self.ordinal_trackers
            .entry(vchan_id)
            .or_insert_with(OrdinalTracker::new)
    }

    pub fn remember_ordinal(&mut self, ordinal: u64, vchan_id: i32) {
        let tracker = self.get_or_create_tracker(vchan_id);
        if ordinal > tracker.last_ordinal_seen {
            tracker.last_ordinal_seen = ordinal;
        }
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
        let ordinal = slot.ordinal();
        let vchan_id = slot.vchan_id() as i32;
        let bridged_slot_id = slot.bridged_slot_id();
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
            let total = self.total_messages();
            bits.clear_all();

            for i in 0..self.channel.num_slots as usize {
                let s = self.channel.slot_ref(i);
                let refs = s.refs.load(Ordering::Acquire);
                if virtual_channel_id_match(s.vchan_id(), self.channel.vchan_id)
                    && s.ordinal() != 0
                    && (refs & PUB_OWNED) == 0
                {
                    bits.set(i);
                }
            }

            if total == self.total_messages() {
                break;
            }
        }
    }

    pub fn collect_visible_slots(
        &mut self,
        bits: &crate::bitset::InPlaceAtomicBitSet,
    ) -> u64 {
        loop {
            let total = self.total_messages();
            self.channel.active_slots.clear();

            bits.traverse(|i| {
                if self.channel.embargoed_slots.is_set(i) {
                    return;
                }
                let s = self.channel.slot_ref(i);
                if !virtual_channel_id_match(s.vchan_id(), self.channel.vchan_id) {
                    return;
                }
                if s.buffer_index() == -1 {
                    return;
                }
                self.channel.active_slots.push(ActiveSlot {
                    slot_index: i,
                    ordinal: s.ordinal(),
                    timestamp: s.timestamp(),
                    vchan_id: s.vchan_id() as i32,
                });
            });

            if total == self.total_messages() {
                return total;
            }
        }
    }

    fn find_unseen_ordinal(&self) -> Option<usize> {
        for (i, active) in self.channel.active_slots.iter().enumerate() {
            let seen = self.ordinal_trackers.get(&active.vchan_id).is_some_and(
                |tracker| {
                    active.ordinal <= tracker.last_ordinal_seen
                        || tracker.ring.contains(&OrdinalAndVchanId {
                            ordinal: active.ordinal,
                            vchan_id: active.vchan_id,
                        })
                },
            );
            if active.ordinal != 0 && !seen {
                return Some(i);
            }
        }
        None
    }

    fn has_visible_ordinal_before(
        &self,
        vchan_id: i32,
        last_ordinal_seen: u64,
        max_ordinal: u64,
    ) -> bool {
        let bits = self
            .channel
            .get_available_slots(self.subscriber_id as usize);
        let mut found = false;
        bits.traverse(|slot_idx| {
            if found {
                return;
            }
            let slot = self.channel.slot_ref(slot_idx);
            let ordinal = slot.ordinal();
            if ordinal > last_ordinal_seen
                && ordinal <= max_ordinal
                && (slot.refs.load(Ordering::Acquire) & PUB_OWNED) == 0
                && slot.vchan_id() as i32 == vchan_id
                && slot.buffer_index() != -1
            {
                found = true;
            }
        });
        found
    }

    fn next_queued_slot(
        &mut self,
        max_queue_position: u64,
        overflow_baseline: u32,
    ) -> Option<usize> {
        if self.options.reliable {
            return None;
        }

        if self
            .channel
            .get_available_slot_queue(self.subscriber_id as usize)
            .is_none()
        {
            return None;
        }
        loop {
            let queue_at_boundary = match self
                .channel
                .get_available_slot_queue(self.subscriber_id as usize)
            {
                Some(queue) => queue.head() >= max_queue_position,
                None => true,
            };
            if queue_at_boundary {
                break;
            }
            let Some((slot_id, ordinal)) = self
                .channel
                .get_available_slot_queue(self.subscriber_id as usize)
                .and_then(|queue| queue.try_pop())
            else {
                break;
            };
            if slot_id < 0 || slot_id as usize >= self.channel.num_slots as usize {
                continue;
            }
            let slot_idx = slot_id as usize;
            let slot = self.channel.slot_ref(slot_idx);
            let refs = slot.refs.load(Ordering::Acquire);
            if (refs & PUB_OWNED) != 0 {
                continue;
            }
            let slot_ordinal = slot.ordinal();
            if slot_ordinal != ordinal || slot_ordinal == 0 {
                continue;
            }
            if !virtual_channel_id_match(slot.vchan_id(), self.channel.vchan_id) {
                continue;
            }

            let vchan_id = slot.vchan_id() as i32;
            let last_ordinal_seen = self
                .ordinal_trackers
                .get(&vchan_id)
                .map_or(0, |tracker| tracker.last_ordinal_seen);
            if ordinal <= last_ordinal_seen {
                continue;
            }
            if self.options.subscriber_queue_size == 0
                && last_ordinal_seen != 0
                && ordinal > last_ordinal_seen + 1
                && self.has_visible_ordinal_before(
                    vchan_id,
                    last_ordinal_seen,
                    ordinal - 1,
                )
            {
                self.queue_bitset_fallback = true;
                self.poll_snapshot_valid = false;
                return None;
            }
            if self.channel.atomic_inc_ref_count::<fn()>(
                slot_idx,
                false,
                1,
                ordinal,
                vchan_id,
                false,
                None,
            ) {
                if self.channel.slot_ref(slot_idx).ordinal() != ordinal
                    || self.channel.slot_ref(slot_idx).vchan_id() as i32 != vchan_id
                {
                    self.channel.atomic_inc_ref_count::<fn()>(
                        slot_idx,
                        false,
                        -1,
                        ordinal,
                        vchan_id,
                        false,
                        None,
                    );
                    continue;
                }
                if let Some(queue) = self
                    .channel
                    .get_available_slot_queue(self.subscriber_id as usize)
                {
                    if queue.insertion_failed() {
                        self.channel.atomic_inc_ref_count::<fn()>(
                            slot_idx,
                            false,
                            -1,
                            ordinal,
                            vchan_id,
                            false,
                            None,
                        );
                        queue.consume_insertion_failure();
                        self.queue_bitset_fallback = true;
                        self.poll_snapshot_valid = false;
                        return None;
                    }
                }
                if self.options.subscriber_queue_size == 0 {
                    if let Some(queue) = self
                        .channel
                        .get_available_slot_queue(self.subscriber_id as usize)
                    {
                        if queue.overflow_count() != overflow_baseline {
                            self.channel.atomic_inc_ref_count::<fn()>(
                                slot_idx,
                                false,
                                -1,
                                ordinal,
                                vchan_id,
                                false,
                                None,
                            );
                            self.queue_bitset_fallback = true;
                            self.poll_snapshot_valid = false;
                            return None;
                        }
                    }
                }
                if !self.channel.validate_slot_buffer(slot_idx)
                    || self.channel.slot_ref(slot_idx).buffer_index() == -1
                {
                    if self.channel.buffers_changed() {
                        self.channel.atomic_inc_ref_count::<fn()>(
                            slot_idx,
                            false,
                            -1,
                            ordinal,
                            vchan_id,
                            false,
                            None,
                        );
                        self.reload_buffers_if_necessary();
                        continue;
                    }
                    self.channel.atomic_inc_ref_count::<fn()>(
                        slot_idx,
                        false,
                        -1,
                        ordinal,
                        vchan_id,
                        false,
                        None,
                    );
                    continue;
                }
                if self.options.subscriber_queue_size != 0 {
                    let concurrent_drops = self
                        .channel
                        .get_available_slot_queue(self.subscriber_id as usize)
                        .map(|queue| queue.consume_overflow())
                        .unwrap_or(0);
                    if self.options.detect_dropped_messages {
                        self.pending_queue_drops = self
                            .pending_queue_drops
                            .saturating_add(concurrent_drops as i32);
                    }
                }
                return Some(slot_idx);
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

            self.reload_buffers_if_necessary();

            if self.poll_drain_pending && self.poll_drain_exhausted {
                if self.total_messages() != self.poll_snapshot_total {
                    self.poll_drain_exhausted = false;
                    self.queue_drain_tail = None;
                    self.poll_snapshot_valid = false;
                } else {
                    return None;
                }
            }

            if self.poll_drain_pending && !self.poll_snapshot_valid {
                self.poll_snapshot_total = self.collect_visible_slots(&bits);
                self.channel
                    .active_slots
                    .sort_by_key(|slot| (slot.timestamp, slot.ordinal));
                self.poll_snapshot.clone_from(&self.channel.active_slots);
                self.poll_snapshot_valid = true;
                self.queue_drain_tail = self
                    .channel
                    .get_available_slot_queue(self.subscriber_id as usize)
                    .map(|queue| queue.tail());
            }
            let mut queue_overflow_baseline = 0;
            let queue_status = self
                .channel
                .get_available_slot_queue(self.subscriber_id as usize)
                .map(|queue| {
                    let queue_drops = queue.consume_overflow();
                    let insertion_failed = queue.consume_insertion_failure();
                    let overflow_after_consume = queue.overflow_count();
                    (
                        queue_drops,
                        insertion_failed,
                        overflow_after_consume,
                    )
                });
            if let Some((
                queue_drops,
                insertion_failed,
                overflow_after_consume,
            )) = queue_status
            {
                queue_overflow_baseline = overflow_after_consume;
                let recover_overflow = self.options.subscriber_queue_size == 0
                    && (queue_drops != 0 || overflow_after_consume != 0);
                if self.options.detect_dropped_messages && !recover_overflow {
                    self.pending_queue_drops = self
                        .pending_queue_drops
                        .saturating_add(queue_drops as i32);
                }
                if recover_overflow || insertion_failed {
                    self.queue_bitset_fallback = true;
                }
            }
            let max_queue_position = self.queue_drain_tail.unwrap_or(u64::MAX);
            if !self.queue_bitset_fallback {
                if let Some(slot_idx) =
                    self.next_queued_slot(max_queue_position, queue_overflow_baseline)
                {
                    return Some(slot_idx);
                }
            }

            if self.channel.slot.is_none() {
                self.populate_active_slots(&bits);
            }

            if self.poll_drain_pending && self.poll_snapshot_valid {
                self.channel
                    .active_slots
                    .clone_from(&self.poll_snapshot);
            } else {
                self.collect_visible_slots(&bits);
            }

            if self.queue_bitset_fallback {
                self.channel.active_slots.sort_by_key(|s| s.ordinal);
            } else {
                self.channel
                    .active_slots
                    .sort_by_key(|s| (s.timestamp, s.ordinal));
            }

            let unseen_idx = match self.find_unseen_ordinal() {
                Some(idx) => idx,
                None => {
                    if self.queue_bitset_fallback {
                        if let Some(queue) = self
                            .channel
                            .get_available_slot_queue(self.subscriber_id as usize)
                        {
                            queue.discard_all();
                        }
                        self.queue_bitset_fallback = false;
                    }
                    break;
                }
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
                    || self.channel.slot_ref(active.slot_index).buffer_index() == -1
                {
                    if self.channel.buffers_changed() {
                        self.channel.atomic_inc_ref_count::<fn()>(
                            active.slot_index,
                            reliable,
                            -1,
                            active.ordinal,
                            active.vchan_id,
                            false,
                            None,
                        );
                        self.reload_buffers_if_necessary();
                        continue;
                    }
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
        if self.poll_drain_pending && self.total_messages() != self.poll_snapshot_total {
            self.trigger();
        }
        if self.poll_drain_pending {
            self.poll_drain_exhausted = true;
        }
        None
    }

    pub fn last_slot(&mut self) -> Option<usize> {
        let bits = self
            .channel
            .get_available_slots(self.subscriber_id as usize);
        self.channel.embargoed_slots.clear_all();
        self.newest_snapshot = None;
        if let Some(queue) = self
            .channel
            .get_available_slot_queue(self.subscriber_id as usize)
        {
            let queue_drops = queue.consume_overflow();
            if self.options.detect_dropped_messages {
                self.pending_queue_drops = self
                    .pending_queue_drops
                    .saturating_add(queue_drops as i32);
            }
            queue.consume_insertion_failure();
        }

        loop {
            self.reload_buffers_if_necessary();

            if self.channel.slot.is_none() {
                self.populate_active_slots(&bits);
            }

            self.collect_visible_slots(&bits);

            self.channel
                .active_slots
                .sort_by_key(|s| (s.timestamp, s.ordinal));

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

            self.newest_snapshot = Some(self.channel.active_slots.clone());

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
                    || self.channel.slot_ref(active.slot_index).buffer_index() == -1
                {
                    if self.channel.buffers_changed() {
                        self.channel.atomic_inc_ref_count::<fn()>(
                            active.slot_index,
                            reliable,
                            -1,
                            active.ordinal,
                            active.vchan_id,
                            false,
                            None,
                        );
                        self.reload_buffers_if_necessary();
                        continue;
                    }
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
        let ordinal = self.channel.slot_ref(slot_idx).ordinal();
        self.channel
            .slot_ref(slot_idx)
            .sub_owners
            .set(self.subscriber_id as usize);
        let bits = self
            .channel
            .get_available_slots(self.subscriber_id as usize);
        if was_newest {
            let mut skipped = Vec::new();
            if let Some(snapshot) = self.newest_snapshot.take() {
                for active in snapshot {
                    let pinned = active.slot_index == slot_idx
                        || self.channel.atomic_inc_ref_count::<fn()>(
                            active.slot_index,
                            self.options.reliable,
                            1,
                            active.ordinal,
                            active.vchan_id,
                            false,
                            None,
                        );
                    if pinned {
                        bits.clear(active.slot_index);
                        skipped.push((active.ordinal, active.vchan_id));
                        if active.slot_index != slot_idx {
                            self.channel.atomic_inc_ref_count::<fn()>(
                                active.slot_index,
                                self.options.reliable,
                                -1,
                                active.ordinal,
                                active.vchan_id,
                                false,
                                None,
                            );
                        }
                    }
                }
            } else {
                bits.clear(slot_idx);
            }
            for (skipped_ordinal, skipped_vchan_id) in skipped {
                self.remember_ordinal(skipped_ordinal, skipped_vchan_id);
            }
        } else {
            bits.clear(slot_idx);
        }
        self.remember_ordinal(ordinal, vchan_id);
        let slot = self.channel.slot_ref(slot_idx);
        slot.set_flag(MESSAGE_SEEN);
        if self.options.reliable {
            slot.set_flag(MESSAGE_SEEN_BY_RELIABLE);
        }
    }

    pub fn unread_slot(&mut self, slot_idx: usize, ordinal: u64, vchan_id: i32) {
        self.decrement_slot_ref(slot_idx, ordinal, vchan_id, false);
        if self
            .channel
            .get_available_slot_queue(self.subscriber_id as usize)
            .is_some()
        {
            // The queue entry was already popped. Recover the rejected ordinal
            // from the authoritative bitset before accepting newer hints.
            self.queue_bitset_fallback = true;
        }
        self.poll_snapshot_valid = false;
        self.newest_snapshot = None;
    }

    pub fn ignore_activation(&mut self, slot_idx: usize) {
        let ordinal = self.channel.slot_ref(slot_idx).ordinal();
        let vchan_id = self.channel.slot_ref(slot_idx).vchan_id() as i32;
        self.remember_ordinal(ordinal, vchan_id);
        self.decrement_slot_ref(slot_idx, ordinal, vchan_id, true);
        self.channel.slot_ref(slot_idx).set_flag(MESSAGE_SEEN);
        if self.options.reliable {
            self.channel.slot_ref(slot_idx).set_flag(MESSAGE_SEEN_BY_RELIABLE);
        }
    }

    pub fn decrement_slot_ref(
        &self,
        slot_idx: usize,
        ordinal: u64,
        vchan_id: i32,
        retire: bool,
    ) {
        let reliable = self.options.reliable;
        self.channel.atomic_inc_ref_count::<fn()>(
            slot_idx,
            reliable,
            -1,
            ordinal & ORDINAL_MASK,
            vchan_id,
            retire,
            None,
        );
    }

    pub fn release_unclaimed_slot(
        &self,
        slot_idx: usize,
        ordinal: u64,
        vchan_id: i32,
    ) {
        self.decrement_slot_ref(slot_idx, ordinal, vchan_id, false);
    }

    pub fn detect_drops(&mut self, vchan_id: i32) -> i32 {
        let Some(slot_idx) = self.channel.slot else {
            return 0;
        };
        let ordinal = self.channel.slot_ref(slot_idx).ordinal();
        let tracker = self.get_or_create_tracker(vchan_id);
        if ordinal == 0 || ordinal <= tracker.last_ordinal_seen {
            return 0;
        }
        let last_seen = tracker.last_ordinal_seen;
        tracker.last_ordinal_seen = ordinal;
        if last_seen == 0 || ordinal == last_seen + 1 {
            0
        } else {
            (ordinal - last_seen - 1) as i32
        }
    }

    pub fn find_active_slot_by_timestamp(
        &mut self,
        timestamp: u64,
    ) -> Option<usize> {
        self.channel.embargoed_slots.clear_all();
        loop {
            self.reload_buffers_if_necessary();

            let mut buffer: Vec<ActiveSlot> = Vec::with_capacity(self.channel.num_slots as usize);

            for i in 0..self.channel.num_slots as usize {
                if self.channel.embargoed_slots.is_set(i) {
                    continue;
                }
                let s = self.channel.slot_ref(i);
                let refs = s.refs.load(Ordering::Acquire);
                let ordinal = s.ordinal();
                if ordinal != 0 && (refs & PUB_OWNED) == 0 {
                    let prefix = self.channel.get_prefix(i);
                    let ts = if !prefix.is_null() {
                        unsafe { (*prefix).timestamp }
                    } else {
                        s.timestamp()
                    };
                    buffer.push(ActiveSlot {
                        slot_index: i,
                        ordinal,
                        timestamp: ts,
                        vchan_id: s.vchan_id() as i32,
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
                    || self.channel.slot_ref(active.slot_index).buffer_index() == -1
                {
                    if self.channel.buffers_changed() {
                        self.channel.atomic_inc_ref_count::<fn()>(
                            active.slot_index,
                            reliable,
                            -1,
                            active.ordinal,
                            active.vchan_id,
                            false,
                            None,
                        );
                        self.reload_buffers_if_necessary();
                        continue;
                    }
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
                let slot = self.channel.slot_ref(active.slot_index);
                slot.set_flag(MESSAGE_SEEN);
                if reliable {
                    slot.set_flag(MESSAGE_SEEN_BY_RELIABLE);
                }
                slot.sub_owners.set(self.subscriber_id as usize);
                return Some(active.slot_index);
            }
        }
    }

    fn reload_buffers_if_necessary(&mut self) {
        if self.channel.buffers_changed() {
            if let Err(e) = self.attach_buffers() {
                log::error!("Failed to attach new buffers: {}", e);
            }
        }
    }

    pub fn clear_active_message(&mut self) {
        if !self.options.keep_active_message {
            return;
        }
        if let Some((slot_idx, am)) = self.kept_active_message.take() {
            if am.dec_ref_no_release() {
                self.remove_active_message(slot_idx);
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
            crate::syscall_shim::shim_write(fd, buf.as_ptr() as *const libc::c_void, buf.len());
        }
    }

    pub fn attach_buffers(&mut self) -> crate::error::Result<()> {
        let read_write = self.options.bridge || self.options.read_write;
        let resolved_name = self.resolved_name().to_string();
        attach_buffers(&mut self.channel, &resolved_name, read_write)
    }
}
