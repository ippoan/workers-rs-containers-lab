#!/usr/bin/env bash
# worker が外から素で届かないことを wrangler.toml で検査する。引数で渡した wrangler.toml を全部見る。
#   - トップレベル: workers_dev = false と preview_urls = false が明示されている
#   - どこにも (トップレベル・env 配下とも) route / routes が無い (custom_domain も routes の中に書く)
#   - workers_dev / preview_urls が true になってよいのは env.staging だけ
#     (staging の workers.dev は Cloudflare Access で保護する前提。README 参照)
#   - `--private` を前に付けた wrangler.toml は、env.staging も含めてどこでも workers_dev / preview_urls が
#     true であってはならない (外から叩く口が要らないスクリプト。lab-db)
#   - どの vars にも ALLOW_INSECURE_DB (DB への平文接続を許すローカル専用フラグ) が無い
# 1 つでも違えば exit 1。CI で毎回走らせる。陰性対照は scripts/check-exposure-test.sh。
#
#   bash scripts/check-exposure.sh worker-unknown/wrangler.toml [worker-*/wrangler.toml ...] [--private lab-db/wrangler.toml]
set -euo pipefail

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <wrangler.toml> [<wrangler.toml> ...]" >&2
  exit 2
fi

python3 - "$@" <<'PY'
import sys
import tomllib

failed = False

# (path, private) の組にする。--private はすぐ後ろの 1 つだけに掛かる
targets = []
args = sys.argv[1:]
i = 0
while i < len(args):
    if args[i] == "--private":
        if i + 1 >= len(args):
            print("::error::--private の後に wrangler.toml が無い")
            sys.exit(2)
        targets.append((args[i + 1], True))
        i += 2
    else:
        targets.append((args[i], False))
        i += 1

for path, private in targets:
    with open(path, "rb") as f:
        cfg = tomllib.load(f)

    errors = []

    def err(msg):
        errors.append(msg)
        print(f"::error file={path}::{msg}")

    # トップレベル: 明示的に false
    for key in ("workers_dev", "preview_urls"):
        if cfg.get(key) is not False:
            err(f"トップレベルに {key} = false がない (外から直接届く)")

    envs = cfg.get("env", {})
    scopes = [("トップレベル", cfg)] + [(f"env.{n}", e) for n, e in envs.items()]

    for scope, table in scopes:
        for key in ("route", "routes"):
            if key in table:
                err(f"{scope} に {key} がある (外から直接届く)")

    # 公開してよいのは Access で守る env.staging だけ (workers_dev / preview_urls は env へ継承される)
    for name, e in envs.items():
        for key in ("workers_dev", "preview_urls"):
            if e.get(key, cfg.get(key)) is True and (private or name != "staging"):
                if private:
                    err(f"env.{name} の {key} が true (--private のスクリプトはどの env も公開しない)")
                else:
                    err(f"env.{name} の {key} が true (公開してよいのは Access で守る env.staging だけ)")

    # 平文の DB 接続を許すフラグはローカル (wrangler dev --var / .dev.vars) にだけ置く
    for scope, table in scopes:
        if "ALLOW_INSECURE_DB" in table.get("vars", {}):
            err(f"{scope} の vars に ALLOW_INSECURE_DB がある (DB 接続が平文に落ちうる。ローカル専用)")

    if errors:
        failed = True
    else:
        if private:
            print(f"OK: {path} はどの env も workers_dev / preview_urls = false・route 無し (--private)")
        else:
            print(f"OK: {path} はトップレベルが workers_dev / preview_urls = false・route 無しで、公開する env は staging だけ")

sys.exit(1 if failed else 0)
PY
