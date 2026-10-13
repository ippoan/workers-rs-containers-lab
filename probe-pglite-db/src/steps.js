// DO が起動のたびに流す SQL の順 (alc-vein-worker の container/start.sh と同じ):
//   search_path → init_local_db.sql → 空の _sqlx_migrations → migrations (1 ファイル 1 トランザクション)
//   → local_app_grants.sql → テナント漏れ検査の種 (2 テナント)
// DO (src/index.ts) と build 時の scripts/gen-prebuilt-wasm.mjs が同じものを流す。

/// 本番の実行用ロール (表の所有者ではないので、FORCE の無い表でも RLS が掛かる)。migration 158 が作る
export const RUNTIME_ROLE = "alc_api_rt";

/// テナント漏れ検査の種 (alc-vein-worker の tests/seed.sql と container/start.sh の名前・乗務員の数と同じ)。
/// ID は起動のたびに gen_random_uuid() で作り、接続側 (probe-hyperdrive/src/pglite.rs) は名前で引く
export const SEED_TENANTS = [
  { name: "leak-a", employees: 7 },
  { name: "leak-b", employees: 13 },
];

/// pglite.wasm の線形メモリの初期値。パッケージの pglite.wasm は最小 2048 pages (128 MiB) を宣言していて
/// isolate の上限 (128 MB) と同じになるので、build 時に宣言を書き換え (scripts/gen-prebuilt-wasm.mjs)、この大きさで始める。
/// 足りなければ wasm が自分で伸ばす (ローカルの実測で migration 全量の後 約 38 MiB)
export const PGLITE_INITIAL_MIB = 32;

/// build 時に datadir を作る initdb の引数 (scripts/gen-prebuilt-wasm.mjs)。WAL のセグメントを 16 → 1 MiB にし、initdb が
/// 中で動かす postgres にも小さい設定を渡す。DO は出来た datadir を loadDataDir で読むだけで、実行時に initdb を走らせない
/// (initdb を DO で動かすと、起動中に initdb.wasm の 64 MiB と既定の shared_buffers のぶんが重なって isolate の上限を超えた)
export const INITDB_PARAMS = ["--wal-segsize=1", "-c", "shared_buffers=8MB", "-c", "max_connections=5"];

/// postgres の設定。既定の shared_buffers (128MB) だと起動だけで wasm のメモリが 約 190 MiB まで伸びる (ローカルの node の実測)
export const POSTGRESQL_CONF = [
  "shared_buffers=8MB",
  "work_mem=1MB",
  "maintenance_work_mem=4MB",
  "temp_buffers=1MB",
  "wal_buffers=1MB",
  "max_connections=5",
];

/// sqlx ではなく SQL で流すので表だけ作る (migration 160 の migration_status() が参照する。中身は空)
const SQLX_MIGRATIONS =
  "CREATE TABLE IF NOT EXISTS alc_api._sqlx_migrations (version BIGINT PRIMARY KEY, description TEXT NOT NULL, installed_on TIMESTAMPTZ NOT NULL DEFAULT now(), success BOOLEAN NOT NULL, checksum BYTEA NOT NULL, execution_time BIGINT NOT NULL)";

/// tests/seed.sql を psql の `-v` 無しで流せる形にしたもの (DB は毎回空から作るので DELETE は要らない)
function seedSql(t) {
  return `
GRANT SELECT ON alc_api.employees TO alc_api_app;
INSERT INTO alc_api.tenants (id, name) VALUES (gen_random_uuid(), '${t.name}');
INSERT INTO alc_api.employees (id, tenant_id, name)
SELECT gen_random_uuid(), t.id, t.name || '-' || g
FROM alc_api.tenants AS t, generate_series(1, ${t.employees}) AS g
WHERE t.name = '${t.name}';`;
}

/**
 * @param {{ init: string; grants: string; migrations: { name: string; sql: string }[] }} bundle
 * @returns {{ name: string; sql: string; migration: boolean }[]}
 */
export function setupSteps(bundle) {
  return [
    // init_local_db.sql の ALTER DATABASE … SET search_path は、既に開いているこの 1 本の接続には効かない
    { name: "search_path", sql: "SET search_path TO alc_api, public;", migration: false },
    { name: "init_local_db.sql", sql: bundle.init, migration: false },
    { name: "_sqlx_migrations", sql: SQLX_MIGRATIONS, migration: false },
    ...bundle.migrations.map((m) => ({ name: m.name, sql: `BEGIN;\n${m.sql}\nCOMMIT;`, migration: true })),
    { name: "local_app_grants.sql", sql: bundle.grants, migration: false },
    ...SEED_TENANTS.map((t) => ({ name: `seed ${t.name}`, sql: seedSql(t), migration: false })),
  ];
}
