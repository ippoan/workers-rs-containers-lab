//! メモリの目安を 2 通りで取る。
//!
//! - 線形メモリ (`core::arch::wasm32::memory_size`): Worker の 128 MB に効く実物。ただし**伸びたら縮まない**ので、
//!   同じ isolate で先に大きい処理が走っていれば `before` から大きく、`grown` (後 − 前) は 0 になる (isolate の高水位)
//! - ヒープの最大 (`heap_peak`): グローバルアロケータで数えた、計測中に同時に生きていた確保の合計の最大。
//!   isolate の前の処理に左右されないので、処理どうし (/zip の copy と stream) の比較はこちらで見る
//!   (アロケータの断片化や線形メモリの伸ばし方の分は入らないので、線形メモリより小さく出る)

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use serde::Serialize;

const PAGE: usize = 64 * 1024;

pub fn linear_memory_bytes() -> usize {
    core::arch::wasm32::memory_size(0) * PAGE
}

/// System に委ね、生きている確保の合計 (`LIVE`) とその最大 (`PEAK`) を数える
struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn add(n: usize) {
    let live = LIVE.fetch_add(n, Ordering::Relaxed) + n;
    PEAK.fetch_max(live, Ordering::Relaxed);
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc(layout);
        if !p.is_null() {
            add(layout.size());
        }
        p
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc_zeroed(layout);
        if !p.is_null() {
            add(layout.size());
        }
        p
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = System.realloc(ptr, layout, new_size);
        if !p.is_null() {
            if new_size >= layout.size() {
                add(new_size - layout.size());
            } else {
                LIVE.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        p
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

/// 計測の区間。`start` で線形メモリを控え、ヒープの最大を今の値に戻す
pub struct Span {
    before: usize,
    heap_before: usize,
}

impl Span {
    pub fn start() -> Self {
        let live = LIVE.load(Ordering::Relaxed);
        PEAK.store(live, Ordering::Relaxed);
        Span {
            before: linear_memory_bytes(),
            heap_before: live,
        }
    }

    pub fn finish(&self) -> Memory {
        let after = linear_memory_bytes();
        Memory {
            before: self.before,
            after,
            grown: after - self.before,
            heap_before: self.heap_before,
            heap_peak: PEAK.load(Ordering::Relaxed),
        }
    }
}

/// 区間の前後の線形メモリ・伸びた量と、区間中のヒープの最大 (バイト)
#[derive(Serialize, Clone, Copy)]
pub struct Memory {
    pub before: usize,
    pub after: usize,
    pub grown: usize,
    pub heap_before: usize,
    pub heap_peak: usize,
}
