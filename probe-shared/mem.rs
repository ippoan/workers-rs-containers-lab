//! wasm の線形メモリのサイズ (ピークメモリの目安)。
//!
//! 線形メモリは伸びるだけで縮まないので、処理の後の値は「その isolate がそこまでに使った最大」に近い。
//! 同じ isolate で先に大きい処理が走っていれば、前の値からもう大きい (応答の `before` で分かる)。

use serde::Serialize;

const PAGE: usize = 64 * 1024;

pub fn linear_memory_bytes() -> usize {
    core::arch::wasm32::memory_size(0) * PAGE
}

/// 処理の前後の線形メモリ (バイト)
#[derive(Serialize, Clone, Copy)]
pub struct Memory {
    pub before: usize,
    pub after: usize,
}
