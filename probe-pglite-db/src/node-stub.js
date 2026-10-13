// PGlite が node で動くときだけ動的 import する組み込み (fs・zlib 等) の代わり (wrangler.toml の [alias])。
// Workers では node の経路を通らない (nodejs_compat を付けないので `process` が無い) ので、中身は要らない
export default {};
