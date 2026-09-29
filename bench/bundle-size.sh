#!/usr/bin/env bash
# worker の wasm の raw / gzip -9 のバイト数を出す (Workers の上限 10MB は gzip 後)。
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
[ "$gz" -lt 10000000 ] || { echo "::error::${label}: gzip 後 ${gz} B が 10MB を超えた"; exit 1; }
