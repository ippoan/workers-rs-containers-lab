#!/usr/bin/env bash
# check-exposure.sh の陰性対照。wrangler.toml を 1 か所ずつ崩したコピーで exit 1 になること、
# 元のままなら exit 0 になること、複数渡したうち 1 つでも崩れていれば exit 1 になることを確かめる。
# CI で check-exposure.sh の直後に走る。
#
#   bash scripts/check-exposure-test.sh [wrangler.toml]              (既定 worker-unknown/wrangler.toml)
#   bash scripts/check-exposure-test.sh --private lab-db/wrangler.toml  (どの env も公開しないスクリプト)
set -euo pipefail
cd "$(dirname "$0")/.."

PRIV=()
if [ "${1:-}" = "--private" ]; then
  PRIV=(--private)
  shift
fi
BASE="${1:-worker-unknown/wrangler.toml}"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
fail=0

expect() { # $1 = 期待する exit, $2 = label, $3.. = wrangler.toml
  local want="$1" label="$2"
  shift 2
  if bash scripts/check-exposure.sh "$@" >"$tmp/out" 2>&1; then got=0; else got=1; fi
  if [ "$got" = "$want" ]; then
    echo "ok   ${label} (exit ${got})"
  else
    echo "FAIL ${label}: exit ${got}, want ${want}"; sed 's/^/     /' "$tmp/out"; fail=1
  fi
}

mutate() { # $1 = label, $2 = python の置換式 (s を書き換える)
  python3 - "$BASE" "$tmp/w.toml" "$2" <<'PY'
import re
import sys
s = open(sys.argv[1]).read()
before = s
exec(sys.argv[3])
assert s != before, "mutation did not change wrangler.toml"
open(sys.argv[2], "w").write(s)
PY
  expect 1 "$1" "${PRIV[@]}" "$tmp/w.toml"
}

expect 0 "${BASE} そのまま" "${PRIV[@]}" "$BASE"
mutate "staging 以外の env で workers_dev = true" \
  's += "\n[env.dev]\nname = \"lab-dev\"\nworkers_dev = true\n"'
mutate "staging 以外の env で preview_urls = true" \
  's += "\n[env.preview]\nname = \"lab-preview\"\npreview_urls = true\n"'
mutate "staging 以外の env がトップレベルの true を継承" \
  's = re.sub(r"^workers_dev = false$", "workers_dev = true", s, count=1, flags=re.M) + "\n[env.other]\nname = \"x\"\n"'
mutate "トップレベルの vars に ALLOW_INSECURE_DB" \
  's = s.replace("[build]", "[vars]\nALLOW_INSECURE_DB = \"1\"\n\n[build]", 1)'
mutate "staging の vars に ALLOW_INSECURE_DB" \
  's += "\n[env.staging.vars]\nALLOW_INSECURE_DB = \"1\"\n"'
mutate "トップレベルの workers_dev を true に" \
  's = re.sub(r"^workers_dev = false$", "workers_dev = true", s, count=1, flags=re.M)'
mutate "トップレベルの preview_urls を消す" \
  's = re.sub(r"^preview_urls = false\n", "", s, count=1, flags=re.M)'
mutate "トップレベルに routes を足す" \
  's = s.replace("[build]", "routes = [{ pattern = \"lab.example.com\", custom_domain = true }]\n\n[build]", 1)'
mutate "staging に route を足す" \
  's = s.replace("[env.staging.observability]", "route = \"lab.example.com/*\"\n\n[env.staging.observability]", 1)'
# 複数渡したとき、後ろの 1 つだけ崩れていても見逃さない (c-2 で worker-emscripten を足す形)
expect 1 "正常 + 崩れた 1 つを並べて渡す" "${PRIV[@]}" "$BASE" "${PRIV[@]}" "$tmp/w.toml"
if [ "${#PRIV[@]}" -gt 0 ]; then
  # --private では env.staging の workers_dev / preview_urls = true も許さない
  mutate "--private で env.staging の workers_dev = true" \
    's = re.sub(r"(\[env\.staging\]\n(?:.*\n)*?)workers_dev = false", r"\1workers_dev = true", s, count=1)'
  mutate "--private で env.staging の preview_urls = true" \
    's = re.sub(r"(\[env\.staging\]\n(?:.*\n)*?)preview_urls = false", r"\1preview_urls = true", s, count=1)'
  # 同じ崩し方でも --private を付けなければ通る (= --private が効いていることの対照)
  python3 - "$BASE" "$tmp/w.toml" <<'PY'
import re
import sys
s = open(sys.argv[1]).read()
s = re.sub(r"(\[env\.staging\]\n(?:.*\n)*?)workers_dev = false", r"\1workers_dev = true", s, count=1)
open(sys.argv[2], "w").write(s)
PY
  expect 0 "--private を外すと env.staging の workers_dev = true は通る" "$tmp/w.toml"
fi
if bash scripts/check-exposure.sh >/dev/null 2>&1; then
  echo "FAIL 引数なし: exit 0, want 非 0"; fail=1
else
  echo "ok   引数なし (非 0)"
fi

exit "$fail"
