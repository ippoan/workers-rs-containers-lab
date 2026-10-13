#!/usr/bin/env bash
# DO が起動のたびに流す SQL を、正本の ippoan/alc-migrations (public) から版を固定して取り出し、
# 1 個の JSON (.alc-migrations/bundle.json) にまとめる (src/index.ts が import して bundle に同梱する)。
# 取り方は alc-vein-worker の scripts/fetch-migrations.sh と同じ。写しをこの repo に commit しない。
#
#   - 版は ALC_MIGRATIONS_REV の 1 行 (commit の SHA)。**rev を書くのはそのファイルだけ**
#   - 持ち込むのは scripts/init_local_db.sql・scripts/local_app_grants.sql・migrations/*.sql だけ
#   - 既に同じ rev が在れば何もしない
# wrangler.toml の [build] command がこれを通すので、`wrangler deploy` (--dry-run も) の前に手で打たなくてよい。
set -euo pipefail
cd "$(dirname "$0")/.."

REPO_URL="https://github.com/ippoan/alc-migrations"
dest=".alc-migrations"
rev="$(tr -d '[:space:]' <ALC_MIGRATIONS_REV)"
if ! [[ "$rev" =~ ^[0-9a-f]{40}$ ]]; then
  echo "ALC_MIGRATIONS_REV が 40 桁の commit SHA ではない" >&2
  exit 1
fi

if [ -f "$dest/.rev" ] && [ "$(cat "$dest/.rev")" = "$rev" ]; then
  echo "alc-migrations ${rev}: 取得済み (${dest})"
  exit 0
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
git init -q "$tmp/src"
git -C "$tmp/src" fetch -q --depth 1 "$REPO_URL" "$rev"
git -C "$tmp/src" checkout -q FETCH_HEAD
got="$(git -C "$tmp/src" rev-parse HEAD)"
if [ "$got" != "$rev" ]; then
  echo "取り出した commit (${got}) が ALC_MIGRATIONS_REV (${rev}) と違う" >&2
  exit 1
fi

mkdir -p "$tmp/out"
node scripts/bundle-migrations.mjs "$tmp/src" "$rev" "$tmp/out/bundle.json"
# .rev は最後に書く (途中で落ちたら次回は取り直す)
echo "$rev" >"$tmp/out/.rev"
rm -rf "$dest"
mv "$tmp/out" "$dest"
echo "alc-migrations ${rev}: ${dest}/bundle.json にまとめた"
