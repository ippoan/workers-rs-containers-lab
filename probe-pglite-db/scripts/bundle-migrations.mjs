// fetch-migrations.sh が取り出した alc-migrations の checkout から、DO が流す SQL を 1 個の JSON にまとめる。
//   node scripts/bundle-migrations.mjs <checkout> <rev> <out.json>
// migrations は名前の順 (alc-vein-worker の container/start.sh の glob と同じ)。中身は改変しない。
import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const [src, rev, out] = process.argv.slice(2);
const read = (p) => readFileSync(join(src, p), "utf8");
const migrations = readdirSync(join(src, "migrations"))
  .filter((f) => f.endsWith(".sql"))
  .sort()
  .map((name) => ({ name, sql: read(join("migrations", name)) }));
writeFileSync(
  out,
  JSON.stringify({
    rev,
    init: read("scripts/init_local_db.sql"),
    grants: read("scripts/local_app_grants.sql"),
    migrations,
  }),
);
console.log(`${migrations.length} 個の migration`);
