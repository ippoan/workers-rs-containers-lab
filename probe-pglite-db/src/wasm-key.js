// wasm の bytes の鍵 (長さ + FNV-1a 32bit)。build 時 (scripts/gen-prebuilt-wasm.mjs) と実行時 (src/wasm-shim.ts) で同じものを使う
/** @param {Uint8Array} bytes */
export function wasmKey(bytes) {
  let h = 0x811c9dc5;
  for (let i = 0; i < bytes.length; i++) {
    h ^= bytes[i];
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return `${bytes.length}-${h.toString(16)}`;
}
