// wrangler の module rules で取り込むもの (wrangler.toml の [[rules]])
declare module "*.wasm" {
  const module: WebAssembly.Module;
  export default module;
}
declare module "*.tar.gz" {
  const data: ArrayBuffer;
  export default data;
}
declare module "*.data" {
  const data: ArrayBuffer;
  export default data;
}
// scripts/gen-prebuilt-wasm.mjs が作る (bytes の鍵 → 同梱の module)
declare module "*/.prebuilt/index.js" {
  const prebuilt: Map<string, WebAssembly.Module>;
  export default prebuilt;
}
// scripts/fetch-migrations.sh が作る。typecheck は取得前でも通るよう、形だけここで宣言する
declare module "*/.alc-migrations/bundle.json" {
  const bundle: {
    rev: string;
    init: string;
    grants: string;
    migrations: { name: string; sql: string }[];
  };
  export default bundle;
}
