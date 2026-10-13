// PGlite (Emscripten) が実行時に bytes からコンパイルする wasm (addFunction の小さな module・拡張の side module) を、
// build 時に作って同梱したものへ差し替える (Workers は実行時の wasm のコンパイルを許さない。scripts/gen-prebuilt-wasm.mjs)。
// 同梱に無い bytes はそのまま元の口へ流す (= Workers では CompileError になり、どこで止まったかがログに出る)。
import prebuilt from "../.prebuilt/index.js";
import { wasmKey } from "./wasm-key.js";

function lookup(src: unknown): WebAssembly.Module | undefined {
  if (src instanceof WebAssembly.Module) return undefined;
  if (ArrayBuffer.isView(src)) return prebuilt.get(wasmKey(new Uint8Array(src.buffer, src.byteOffset, src.byteLength)));
  if (src instanceof ArrayBuffer) return prebuilt.get(wasmKey(new Uint8Array(src)));
  return undefined;
}

let installed = false;

export function installWasmShim(): void {
  if (installed) return;
  installed = true;
  const OriginalModule = WebAssembly.Module;
  WebAssembly.Module = new Proxy(OriginalModule, {
    construct(target, args: unknown[]) {
      return lookup(args[0]) ?? Reflect.construct(target, args);
    },
  });
  const instantiate = WebAssembly.instantiate.bind(WebAssembly) as (
    src: unknown,
    imports?: WebAssembly.Imports,
  ) => Promise<unknown>;
  // bytes を渡されたら { module, instance }、module を渡されたら instance を返す (WebAssembly.instantiate と同じ)
  (WebAssembly as { instantiate: unknown }).instantiate = async (src: unknown, imports?: WebAssembly.Imports) => {
    const module = lookup(src);
    if (!module) return instantiate(src, imports);
    return { module, instance: await instantiate(module, imports) };
  };
}
