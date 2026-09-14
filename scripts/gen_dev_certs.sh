#!/usr/bin/env bash
# Generates a self-signed TLS cert for Envoy's HTTP/3 (QUIC) listener --
# QUIC mandates TLS, so even a local demo needs a certificate somewhere.
# Self-signed and localhost-only; regenerate any time by deleting
# envoy/certs/ and re-running. Never committed (see .gitignore).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CERT_DIR="$ROOT/envoy/certs"
mkdir -p "$CERT_DIR"

if [ -f "$CERT_DIR/dev.crt" ] && [ -f "$CERT_DIR/dev.key" ]; then
  echo "Dev TLS cert already exists at $CERT_DIR -- delete it first to regenerate."
  exit 0
fi

openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "$CERT_DIR/dev.key" -out "$CERT_DIR/dev.crt" \
  -days 365 -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"

# openssl defaults the key to 600 (owner-only). The Envoy container runs
# as a different UID than the host user that generated this file, so it
# needs to be readable -- fine for a self-signed, local-only, non-secret
# dev cert (real certificates would never get this treatment).
chmod 644 "$CERT_DIR/dev.key"

echo "Generated dev TLS cert: $CERT_DIR/dev.crt / dev.key"
