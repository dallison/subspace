// Copyright 2024-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

use std::sync::atomic::{AtomicU64, Ordering};

pub const fn bits_to_words(bits: usize) -> usize {
    if bits == 0 {
        0
    } else {
        (bits - 1) / 64 + 1
    }
}

pub fn sizeof_atomic_bitset(num_bits: usize) -> usize {
    std::mem::size_of::<usize>() + std::mem::size_of::<AtomicU64>() * bits_to_words(num_bits)
}

/// Fixed-size atomic bitset. Binary-compatible with C++ `AtomicBitSet<N>`.
///
/// WORDS is `bits_to_words(N)` where N is the number of bits.
#[repr(C)]
pub struct AtomicBitSet<const WORDS: usize> {
    num_bits: usize,
    bits: [AtomicU64; WORDS],
}

impl<const WORDS: usize> AtomicBitSet<WORDS> {
    pub fn set(&self, bit: usize) {
        let word = bit / 64;
        let offset = bit % 64;
        self.bits[word].fetch_or(1u64 << offset, Ordering::Relaxed);
    }

    pub fn clear(&self, bit: usize) {
        let word = bit / 64;
        let offset = bit % 64;
        self.bits[word].fetch_and(!(1u64 << offset), Ordering::Relaxed);
    }

    pub fn is_set(&self, bit: usize) -> bool {
        let word = bit / 64;
        let offset = bit % 64;
        self.bits[word].load(Ordering::Relaxed) & (1u64 << offset) != 0
    }

    pub fn clear_all(&self) {
        for w in &self.bits {
            w.store(0, Ordering::Relaxed);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.bits.iter().all(|w| w.load(Ordering::Relaxed) == 0)
    }

    pub fn find_first_set(&self) -> Option<usize> {
        for (i, w) in self.bits.iter().enumerate() {
            let val = w.load(Ordering::Relaxed);
            if val != 0 {
                return Some(i * 64 + val.trailing_zeros() as usize);
            }
        }
        None
    }

    pub fn traverse<F: FnMut(usize)>(&self, mut func: F) {
        let num_bits = self.num_bits;
        for i in 0..WORDS {
            let mut shift = 0usize;
            let mut bit = i * 64;
            while bit < num_bits && shift < 64 {
                let word = self.bits[i].load(Ordering::Relaxed) >> shift;
                let n = ffs64(word);
                if n == 0 {
                    break;
                }
                bit += n;
                func(bit - 1);
                shift += n;
            }
        }
    }
}

/// In-place atomic bitset accessor for shared memory.
///
/// This provides safe access to `AtomicBitSet<0>` instances in shared memory
/// where the bits follow the `num_bits` field in memory layout.
pub struct InPlaceAtomicBitSet {
    ptr: *mut u8,
}

impl InPlaceAtomicBitSet {
    /// # Safety
    /// `ptr` must point to a valid in-place atomic bitset in shared memory
    /// with enough space for `num_bits` field (usize) followed by the bits array.
    pub unsafe fn new(ptr: *mut u8) -> Self {
        Self { ptr }
    }

    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr
    }

    fn num_bits(&self) -> usize {
        unsafe { *(self.ptr as *const usize) }
    }

    fn num_words(&self) -> usize {
        bits_to_words(self.num_bits())
    }

    fn word_ptr(&self, index: usize) -> *const AtomicU64 {
        unsafe {
            (self.ptr as *const u8).add(std::mem::size_of::<usize>()) as *const AtomicU64
        }
        .wrapping_add(index)
    }

    fn word(&self, index: usize) -> &AtomicU64 {
        unsafe { &*self.word_ptr(index) }
    }

    pub fn init(&self, num_bits: usize) {
        unsafe {
            *(self.ptr as *mut usize) = num_bits;
        }
        for i in 0..bits_to_words(num_bits) {
            self.word(i).store(0, Ordering::Relaxed);
        }
    }

    pub fn set(&self, bit: usize) {
        let word_idx = bit / 64;
        let offset = bit % 64;
        self.word(word_idx)
            .fetch_or(1u64 << offset, Ordering::Relaxed);
    }

    pub fn clear(&self, bit: usize) {
        let word_idx = bit / 64;
        let offset = bit % 64;
        self.word(word_idx)
            .fetch_and(!(1u64 << offset), Ordering::Relaxed);
    }

    pub fn is_set(&self, bit: usize) -> bool {
        let word_idx = bit / 64;
        let offset = bit % 64;
        self.word(word_idx).load(Ordering::Relaxed) & (1u64 << offset) != 0
    }

    pub fn clear_all(&self) {
        for i in 0..self.num_words() {
            self.word(i).store(0, Ordering::Relaxed);
        }
    }

    pub fn is_empty(&self) -> bool {
        for i in 0..self.num_words() {
            if self.word(i).load(Ordering::Relaxed) != 0 {
                return false;
            }
        }
        true
    }

    pub fn find_first_set(&self) -> Option<usize> {
        for i in 0..self.num_words() {
            let val = self.word(i).load(Ordering::Relaxed);
            if val != 0 {
                return Some(i * 64 + val.trailing_zeros() as usize);
            }
        }
        None
    }

    pub fn traverse<F: FnMut(usize)>(&self, mut func: F) {
        let num_bits = self.num_bits();
        let num_words = self.num_words();
        for i in 0..num_words {
            let mut shift = 0usize;
            let mut bit = i * 64;
            while bit < num_bits && shift < 64 {
                let word = self.word(i).load(Ordering::Relaxed) >> shift;
                let n = ffs64(word);
                if n == 0 {
                    break;
                }
                bit += n;
                func(bit - 1);
                shift += n;
            }
        }
    }
}

unsafe impl Send for InPlaceAtomicBitSet {}
unsafe impl Sync for InPlaceAtomicBitSet {}

/// Non-atomic dynamic bitset for local (non-shared-memory) use.
pub struct DynamicBitSet {
    num_bits: usize,
    bits: Vec<u64>,
}

impl DynamicBitSet {
    pub fn new(num_bits: usize) -> Self {
        Self {
            num_bits,
            bits: vec![0u64; bits_to_words(num_bits)],
        }
    }

    pub fn resize(&mut self, num_bits: usize) {
        self.num_bits = num_bits;
        self.bits.resize(bits_to_words(num_bits), 0);
    }

    pub fn set(&mut self, bit: usize) {
        let word = bit / 64;
        let offset = bit % 64;
        self.bits[word] |= 1u64 << offset;
    }

    pub fn clear(&mut self, bit: usize) {
        let word = bit / 64;
        let offset = bit % 64;
        self.bits[word] &= !(1u64 << offset);
    }

    pub fn is_set(&self, bit: usize) -> bool {
        let word = bit / 64;
        let offset = bit % 64;
        self.bits[word] & (1u64 << offset) != 0
    }

    pub fn clear_all(&mut self) {
        for w in &mut self.bits {
            *w = 0;
        }
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.bits.iter().all(|&w| w == 0)
    }
}

/// Equivalent of C `ffsll`: returns 1-indexed position of first set bit, or 0 if none.
fn ffs64(val: u64) -> usize {
    if val == 0 {
        0
    } else {
        (val.trailing_zeros() + 1) as usize
    }
}
