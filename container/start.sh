#!/bin/bash
# lab 用 DB の起動。ディスクは揮発なので毎回空の DB から立ち上げる:
#   initdb → role bench + 表 items (1000 行) → PgBouncer (6432) を前面で起動
# PgBouncer は最後に起動するので、Worker (LabDb) から 6432 に繋がった時点で DB は準備済み。
set -euo pipefail

# initdb は root で動かない。Container の実行ユーザーが image の USER と違っても動くよう、
# root で起動されたら postgres に降りて自分を実行し直す
if [ "$(id -u)" = 0 ]; then
  mkdir -p "$PGDATA" && chown postgres:postgres "$PGDATA"
  exec gosu postgres "$0" "$@"
fi
trap 'echo "[lab-db] failed at line $LINENO (exit $?)" >&2' ERR

t0=$(date +%s%3N)
log() { echo "[lab-db +$(($(date +%s%3N) - t0))ms] $*"; }

# Container がどこで動いているか (Workers Logs で DO の colo と見比べる)。colo と loc だけを出す
# (trace の他の行は出さない)。cold start の計測に足さないよう裏で走らせ、取れなくても起動は止めない
{
  where=$(curl -s -m 5 https://cloudflare.com/cdn-cgi/trace 2>/dev/null | grep -E '^(colo|loc)=' | tr '\n' ' ' || true)
  log "where: ${where:-unknown}"
} &

initdb -D "$PGDATA" -U postgres --auth=trust --no-sync >/dev/null
# Cloudflare Containers には /var/run/postgresql (既定の unix socket の置き場) も /dev/shm も無い
pg_ctl -D "$PGDATA" -w -s -o "-c listen_addresses=127.0.0.1 -c unix_socket_directories=/tmp -c fsync=off" start
log "postgres started"

psql() { PGOPTIONS="-c client_min_messages=warning" command psql -q -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -U postgres -d postgres "$@"; }

# worker は superuser ではない bench で繋ぐ。認証は trust
psql <<'SQL'
CREATE ROLE bench LOGIN;
CREATE TABLE items (id serial PRIMARY KEY, v text NOT NULL);
INSERT INTO items (v) SELECT 'item-' || g FROM generate_series(1, 1000) AS g;
GRANT SELECT, INSERT ON items TO bench;
-- serial の INSERT には sequence の USAGE も要る
GRANT USAGE ON SEQUENCE items_id_seq TO bench;
SQL
log "role bench + items (1000 rows) ready"

echo '"bench" ""' >/tmp/pgbouncer-userlist.txt
log "starting pgbouncer on :6432 (pool_mode=transaction)"
exec pgbouncer /etc/pgbouncer/pgbouncer.ini
