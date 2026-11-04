//! Kalshi request signing: `timestamp_ms + METHOD + path` signed with the
//! account's private key, Ed25519 or RSA-PSS-SHA256 depending on the key.

use std::path::Path;

use anyhow::Context;
use base64::Engine as _;
use ed25519_dalek::pkcs8::DecodePrivateKey as _;
use rsa::pkcs1::DecodeRsaPrivateKey as _;
use rsa::pkcs8::DecodePrivateKey as _;
use rsa::signature::{RandomizedSigner as _, SignatureEncoding as _};

pub const HEADER_KEY: &str = "KALSHI-ACCESS-KEY";
pub const HEADER_TIMESTAMP: &str = "KALSHI-ACCESS-TIMESTAMP";
pub const HEADER_SIGNATURE: &str = "KALSHI-ACCESS-SIGNATURE";

enum Signer {
    Ed25519(ed25519_dalek::SigningKey),
    Rsa(rsa::pss::SigningKey<rsa::sha2::Sha256>),
}

pub struct Auth {
    key_id: String,
    signer: Signer,
}

impl Auth {
    pub fn load(key_id: impl Into<String>, pem_path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = pem_path.as_ref();
        let pem = std::fs::read_to_string(path)
            .with_context(|| format!("reading private key {}", path.display()))?;
        Self::from_pem(key_id, &pem)
    }

    /// Accepts PKCS#8 (`BEGIN PRIVATE KEY`, Ed25519 or RSA) and PKCS#1
    /// (`BEGIN RSA PRIVATE KEY`). The algorithm is chosen from the parsed key,
    /// not from the PEM label, as Kalshi's docs recommend.
    pub fn from_pem(key_id: impl Into<String>, pem: &str) -> anyhow::Result<Self> {
        let pem = pem.trim();
        let signer = if let Ok(k) = ed25519_dalek::SigningKey::from_pkcs8_pem(pem) {
            Signer::Ed25519(k)
        } else if let Ok(k) = rsa::RsaPrivateKey::from_pkcs8_pem(pem) {
            Signer::Rsa(rsa::pss::SigningKey::new(k))
        } else if let Ok(k) = rsa::RsaPrivateKey::from_pkcs1_pem(pem) {
            Signer::Rsa(rsa::pss::SigningKey::new(k))
        } else {
            anyhow::bail!("private key is neither Ed25519 nor RSA (PKCS#8 or PKCS#1 PEM expected)");
        };
        Ok(Self {
            key_id: key_id.into(),
            signer,
        })
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    pub fn algorithm(&self) -> &'static str {
        match self.signer {
            Signer::Ed25519(_) => "ed25519",
            Signer::Rsa(_) => "rsa-pss-sha256",
        }
    }

    pub fn sign(&self, message: &[u8]) -> Vec<u8> {
        match &self.signer {
            Signer::Ed25519(k) => {
                use ed25519_dalek::Signer as _;
                k.sign(message).to_bytes().to_vec()
            }
            Signer::Rsa(k) => k
                .sign_with_rng(&mut rand::rngs::OsRng, message)
                .to_bytes()
                .to_vec(),
        }
    }

    /// The three auth headers for `method` on `path` (path without query
    /// string, including the `/trade-api/...` prefix), stamped now.
    pub fn headers(&self, method: &str, path: &str) -> [(&'static str, String); 3] {
        let ts = chrono::Utc::now().timestamp_millis().to_string();
        let path = path.split('?').next().unwrap_or(path);
        let msg = format!("{ts}{method}{path}");
        let sig = base64::engine::general_purpose::STANDARD.encode(self.sign(msg.as_bytes()));
        [
            (HEADER_KEY, self.key_id.clone()),
            (HEADER_TIMESTAMP, ts),
            (HEADER_SIGNATURE, sig),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ED25519_PEM: &str = "-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEIKhVMA1kVgvWLsq3ndDaHd9jXj3Z8qzWbfnoVDfHMk3G\n-----END PRIVATE KEY-----\n";

    #[test]
    fn ed25519_headers_verify() {
        use ed25519_dalek::Verifier as _;
        let auth = Auth::from_pem("kid", ED25519_PEM).unwrap();
        assert_eq!(auth.algorithm(), "ed25519");
        let [(_, kid), (_, ts), (_, sig)] = auth.headers("GET", "/trade-api/ws/v2?x=1");
        assert_eq!(kid, "kid");
        let sig = base64::engine::general_purpose::STANDARD.decode(sig).unwrap();
        let sig = ed25519_dalek::Signature::from_slice(&sig).unwrap();
        let key = ed25519_dalek::SigningKey::from_pkcs8_pem(ED25519_PEM).unwrap();
        key.verifying_key()
            .verify(format!("{ts}GET/trade-api/ws/v2").as_bytes(), &sig)
            .unwrap();
    }

    #[test]
    fn garbage_is_rejected() {
        assert!(Auth::from_pem("kid", "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----").is_err());
    }
}
