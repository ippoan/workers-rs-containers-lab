//! `GET /sign`: LINE / LINE WORKS 向けの RS256 と、保存 secret の AES-256-GCM を worker の中で回す。
//!
//! - (a) `jwt_ring`: `jsonwebtoken` (ring バックエンド) の RS256。rust-alc-api
//!   `crates/alc-notify/src/clients/lineworks.rs:343-350` と同じく `EncodingKey::from_rsa_pem` + `encode`
//! - (b) `jwt_rsa`: pure Rust の `rsa` crate の RS256 (PKCS#1 v1.5 + SHA-256)
//! - (c) `aes_gcm_ring`: `ring` の AES-256-GCM。rust-alc-api `crates/alc-core/src/auth_lineworks.rs` の
//!   `encrypt_secret` / `decrypt_secret` の写し
//!
//! RSA 鍵は repo に置かない: isolate の初回に `rsa` crate で 2048 bit を生成してメモリに持つ
//! (`key.keygen_ms` はその isolate で生成したときだけの値。`key.cached` が true なら前の要求で作った鍵)。
//! ring を使う (a) と (c) は feature `ring` を切るとコンパイルから外れ、`{"unsupported": "<理由>"}` を返す。

use std::cell::RefCell;
use std::rc::Rc;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use rsa::pkcs1v15::{Signature, SigningKey, VerifyingKey};
use rsa::signature::{SignatureEncoding, Signer, Verifier};
use rsa::RsaPrivateKey;
use serde::Serialize;
use serde_json::{json, Value};
use sha2::Sha256;

use crate::mem::{Memory, Span};
use crate::now_ms;

const BITS: usize = 2048;
/// 1 回は ms の分解能を下回るので、各操作を ITER 回まわした平均を返す
const ITER: u32 = 20;

struct Keys {
    private: RsaPrivateKey,
    /// jsonwebtoken に渡す PKCS#1 PEM (本番は secret に入った PEM を from_rsa_pem で読む)
    #[cfg(feature = "ring")]
    private_pem: String,
    #[cfg(feature = "ring")]
    public_pem: String,
}

thread_local! {
    static KEYS: RefCell<Option<Rc<Keys>>> = const { RefCell::new(None) };
}

/// (鍵, 生成にかかった ms。前の要求で作った鍵なら None)
fn keys() -> Result<(Rc<Keys>, Option<u64>), String> {
    if let Some(k) = KEYS.with(|k| k.borrow().clone()) {
        return Ok((k, None));
    }
    let t0 = now_ms();
    let private =
        RsaPrivateKey::new(&mut rsa::rand_core::OsRng, BITS).map_err(|e| format!("keygen: {e}"))?;
    let keygen_ms = now_ms() - t0;
    #[cfg(feature = "ring")]
    let (private_pem, public_pem) = {
        use rsa::pkcs1::{EncodeRsaPrivateKey, EncodeRsaPublicKey, LineEnding};
        let private_pem = private
            .to_pkcs1_pem(LineEnding::LF)
            .map_err(|e| format!("pem: {e}"))?
            .to_string();
        let public_pem = private
            .to_public_key()
            .to_pkcs1_pem(LineEnding::LF)
            .map_err(|e| format!("pem: {e}"))?;
        (private_pem, public_pem)
    };
    let k = Rc::new(Keys {
        private,
        #[cfg(feature = "ring")]
        private_pem,
        #[cfg(feature = "ring")]
        public_pem,
    });
    KEYS.with(|slot| *slot.borrow_mut() = Some(k.clone()));
    Ok((k, Some(keygen_ms)))
}

/// LINE WORKS の service account 用 JWT の claims と同じ形
#[derive(Serialize, serde::Deserialize)]
struct Claims {
    iss: String,
    sub: String,
    iat: u64,
    exp: u64,
}

fn claims() -> Claims {
    let now = now_ms() / 1000;
    Claims {
        iss: "probe-client-id".into(),
        sub: "probe@example.invalid".into(),
        iat: now,
        exp: now + 60,
    }
}

fn avg(total_ms: u64) -> f64 {
    total_ms as f64 / f64::from(ITER)
}

// ===== (a) jsonwebtoken (ring) =====

#[cfg(feature = "ring")]
fn jwt_ring(keys: &Keys) -> Result<(Value, String), String> {
    use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};

    let claims = claims();
    let t0 = now_ms();
    let mut token = String::new();
    for _ in 0..ITER {
        let key = EncodingKey::from_rsa_pem(keys.private_pem.as_bytes())
            .map_err(|e| format!("from_rsa_pem: {e}"))?;
        token = encode(&Header::new(Algorithm::RS256), &claims, &key)
            .map_err(|e| format!("encode: {e}"))?;
    }
    let sign_ms = now_ms() - t0;

    let t1 = now_ms();
    let mut ok = true;
    for _ in 0..ITER {
        let key = DecodingKey::from_rsa_pem(keys.public_pem.as_bytes())
            .map_err(|e| format!("DecodingKey: {e}"))?;
        let mut v = Validation::new(Algorithm::RS256);
        v.set_required_spec_claims(&["exp"]);
        ok &= decode::<Claims>(&token, &key, &v).is_ok();
    }
    let verify_ms = now_ms() - t1;
    Ok((
        json!({ "ok": ok, "sign_ms": avg(sign_ms), "verify_ms": avg(verify_ms) }),
        token,
    ))
}

#[cfg(not(feature = "ring"))]
fn jwt_ring(_keys: &Keys) -> Result<(Value, String), String> {
    Ok((
        json!({ "unsupported": crate::RING_UNSUPPORTED }),
        String::new(),
    ))
}

// ===== (b) rsa crate =====

/// `header.payload` を RS256 で署名する (jsonwebtoken の encode と同じ形の JWT を作る)
fn jwt_rsa(keys: &Keys, ring_token: &str) -> Result<Value, String> {
    let header = URL_SAFE_NO_PAD.encode(br#"{"typ":"JWT","alg":"RS256"}"#);
    let payload =
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims()).map_err(|e| format!("claims: {e}"))?);
    let message = format!("{header}.{payload}");

    let t0 = now_ms();
    let mut token = String::new();
    for _ in 0..ITER {
        let key = SigningKey::<Sha256>::new(keys.private.clone());
        let sig = key.sign(message.as_bytes());
        token = format!("{message}.{}", URL_SAFE_NO_PAD.encode(sig.to_bytes()));
    }
    let sign_ms = now_ms() - t0;

    let verifying = VerifyingKey::<Sha256>::new(keys.private.to_public_key());
    let t1 = now_ms();
    let mut ok = true;
    for _ in 0..ITER {
        ok &= verify(&verifying, &token);
    }
    let verify_ms = now_ms() - t1;

    // (a) の JWT を rsa crate で検証できるか (= 同じ RS256 か)。(a) が無いターゲットでは null
    let verifies_ring_token = (!ring_token.is_empty()).then(|| verify(&verifying, ring_token));
    Ok(json!({
        "ok": ok,
        "sign_ms": avg(sign_ms),
        "verify_ms": avg(verify_ms),
        "verifies_ring_token": verifies_ring_token,
    }))
}

fn verify(key: &VerifyingKey<Sha256>, token: &str) -> bool {
    let Some((message, sig)) = token.rsplit_once('.') else {
        return false;
    };
    let Ok(sig) = URL_SAFE_NO_PAD.decode(sig) else {
        return false;
    };
    let Ok(sig) = Signature::try_from(sig.as_slice()) else {
        return false;
    };
    key.verify(message.as_bytes(), &sig).is_ok()
}

// ===== (c) ring AES-256-GCM (rust-alc-api crates/alc-core/src/auth_lineworks.rs の写し) =====

#[cfg(feature = "ring")]
mod aes {
    use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
    use ring::aead::{self, Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM};
    use sha2::{Digest, Sha256};

    /// Encrypt plaintext with AES-256-GCM. Key is SHA-256 hash of key_material.
    /// Output: base64(nonce[12] + ciphertext + tag[16])
    pub fn encrypt_secret(plaintext: &str, key_material: &str) -> Result<String, String> {
        use ring::rand::{SecureRandom, SystemRandom};

        let mut key_bytes = [0u8; 32];
        let hash = Sha256::digest(key_material.as_bytes());
        key_bytes.copy_from_slice(&hash);

        let unbound_key =
            UnboundKey::new(&AES_256_GCM, &key_bytes).map_err(|e| format!("Key error: {e}"))?;
        let key = LessSafeKey::new(unbound_key);

        let rng = SystemRandom::new();
        let mut nonce_bytes = [0u8; 12];
        rng.fill(&mut nonce_bytes)
            .map_err(|e| format!("RNG error: {e}"))?;
        let nonce = Nonce::assume_unique_for_key(nonce_bytes);

        let mut in_out = plaintext.as_bytes().to_vec();
        let tag_len = aead::AES_256_GCM.tag_len();
        in_out.extend(vec![0u8; tag_len]);

        key.seal_in_place_separate_tag(nonce, Aad::empty(), &mut in_out[..plaintext.len()])
            .map(|tag| {
                in_out[plaintext.len()..].copy_from_slice(tag.as_ref());
            })
            .map_err(|e| format!("Encryption error: {e}"))?;

        let mut result = Vec::with_capacity(12 + in_out.len());
        result.extend_from_slice(&nonce_bytes);
        result.extend_from_slice(&in_out);

        Ok(BASE64.encode(&result))
    }

    /// Decrypt client_secret stored as AES-256-GCM(base64(nonce + ciphertext + tag))
    pub fn decrypt_secret(ciphertext_b64: &str, key_material: &str) -> Result<String, String> {
        let mut key_bytes = [0u8; 32];
        let hash = Sha256::digest(key_material.as_bytes());
        key_bytes.copy_from_slice(&hash);

        let unbound_key =
            UnboundKey::new(&AES_256_GCM, &key_bytes).map_err(|e| format!("Key error: {e}"))?;
        let key = LessSafeKey::new(unbound_key);

        let data = BASE64
            .decode(ciphertext_b64)
            .map_err(|e| format!("Base64 decode error: {e}"))?;

        if data.len() < 12 + aead::AES_256_GCM.tag_len() {
            return Err("Ciphertext too short".to_string());
        }

        let (nonce_bytes, ciphertext_and_tag) = data.split_at(12);
        let nonce = Nonce::assume_unique_for_key(
            nonce_bytes
                .try_into()
                .map_err(|_| "nonce length".to_string())?,
        );

        let mut in_out = ciphertext_and_tag.to_vec();
        let plaintext = key
            .open_in_place(nonce, Aad::empty(), &mut in_out)
            .map_err(|e| format!("Decryption error: {e}"))?;

        String::from_utf8(plaintext.to_vec()).map_err(|e| format!("UTF-8 error: {e}"))
    }
}

#[cfg(feature = "ring")]
fn aes_gcm_ring() -> Result<Value, String> {
    // 平文は LINE WORKS の bot secret 程度の長さ。鍵の素は固定の合成値 (本番は SSO_ENCRYPTION_KEY)
    const PLAIN: &str = "probe-bot-secret-0123456789abcdef";
    const KEY_MATERIAL: &str = "probe-key-material";
    let t0 = now_ms();
    let mut sealed = String::new();
    for _ in 0..ITER {
        sealed = aes::encrypt_secret(PLAIN, KEY_MATERIAL)?;
    }
    let encrypt_ms = now_ms() - t0;
    let t1 = now_ms();
    let mut ok = true;
    for _ in 0..ITER {
        ok &= aes::decrypt_secret(&sealed, KEY_MATERIAL)? == PLAIN;
    }
    let decrypt_ms = now_ms() - t1;
    Ok(json!({ "ok": ok, "encrypt_ms": avg(encrypt_ms), "decrypt_ms": avg(decrypt_ms) }))
}

#[cfg(not(feature = "ring"))]
fn aes_gcm_ring() -> Result<Value, String> {
    Ok(json!({ "unsupported": crate::RING_UNSUPPORTED }))
}

// ===== 口 =====

#[derive(Serialize)]
pub struct KeyInfo {
    pub bits: usize,
    pub cached: bool,
    pub keygen_ms: Option<u64>,
}

#[derive(Serialize)]
pub struct SignReport {
    pub key: KeyInfo,
    pub iterations: u32,
    pub jwt_ring: Value,
    pub jwt_rsa: Value,
    pub aes_gcm_ring: Value,
    pub memory: Memory,
}

pub fn run() -> Result<SignReport, String> {
    let span = Span::start();
    let (keys, keygen_ms) = keys()?;
    // 1 つの方式が実行時に失敗しても、ほかの方式の結果は返す
    let (jwt_ring, ring_token) =
        jwt_ring(&keys).unwrap_or_else(|e| (json!({ "error": e }), String::new()));
    let jwt_rsa = jwt_rsa(&keys, &ring_token).unwrap_or_else(|e| json!({ "error": e }));
    let aes_gcm_ring = aes_gcm_ring().unwrap_or_else(|e| json!({ "error": e }));
    Ok(SignReport {
        key: KeyInfo {
            bits: BITS,
            cached: keygen_ms.is_none(),
            keygen_ms,
        },
        iterations: ITER,
        jwt_ring,
        jwt_rsa,
        aes_gcm_ring,
        memory: span.finish(),
    })
}
