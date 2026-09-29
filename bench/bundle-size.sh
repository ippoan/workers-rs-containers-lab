#!/usr/bin/env bash
# worker の wasm の raw / gzip -9 のバイト数を出す。Workers の上限はスクリプトの圧縮前 64 MiB
# (圧縮後の上限は無い。ほかに起動 1 秒・メモリ 128 MB。https://developers.cloudflare.com/workers/platform/limits/)。
# gzip は参考値として出すだけで判定しない。
# GITHUB_STEP_SUMMARY があれば追記する。
#
#   bash bench/bundle-size.sh <label> <index_bg.wasm>
#   例: bash bench/bundle-size.sh lab-unknown worker-unknown/build/index_bg.wasm
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: $0 <label> <wasm>" >&2
  exit 2
fi
label="$1" wasm="$2"

raw=$(stat -c%s "$wasm")
gz=$(gzip -9c "$wasm" | wc -c)
line="${label} wasm: raw=${raw} B / gzip=${gz} B"
echo "$line"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  echo "$line" >>"$GITHUB_STEP_SUMMARY"
fi
limit=$((64 * 1024 * 1024))
[ "$raw" -lt "$limit" ] || { echo "::error::${label}: 圧縮前 ${raw} B が 64 MiB を超えた"; exit 1; }
