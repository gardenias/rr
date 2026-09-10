#!/usr/bin/env bash
#
# Generate a self-signed CA + per-node server/client certs for the rr
# perf-lab (deploy/perf). Companion to scripts/gen-dev-certs.sh — that one
# stays byte-identical (frozen R-7/R-8 baseline asset) and its SANs (rr-N,
# nodeN.rr.local) do not match the perf-lab hostnames (kv-N), so the perf-lab
# gets its own generator.
#
# DO NOT USE THESE IN PRODUCTION. Keys are written to disk unencrypted and
# world-readable inside the repo host (container uid 10001 must read them).
#
# Usage:
#   deploy/perf/gen-certs.sh [OUT_DIR] [NODE_COUNT]
#
# Defaults:
#   OUT_DIR    = <script dir>/certs
#   NODE_COUNT = 3
#
# Output (per node N in 1..=NODE_COUNT):
#   $OUT_DIR/ca.pem      — CA cert (shared by all nodes and clients)
#   $OUT_DIR/ca.key      — CA private key (KEEP SECRET)
#   $OUT_DIR/nodeN.pem   — node N server + client cert (SAN: kv-N, localhost)
#   $OUT_DIR/nodeN.key   — matching private key
#   $OUT_DIR/client.pem  — client identity for bench/probe containers
#   $OUT_DIR/client.key  — matching private key
#
# Node N serves/dials under SANs: kv-N, localhost, 127.0.0.1, ::1. Peer
# dialing uses the roster hostname (kv-N), so no --tls-domain override is
# needed in the perf-lab.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="${1:-${SCRIPT_DIR}/certs}"
NODE_COUNT="${2:-3}"

command -v openssl >/dev/null || {
  echo "error: openssl not in PATH" >&2
  exit 1
}

mkdir -p "$OUT_DIR"
cd "$OUT_DIR"

if [[ ! -f ca.key ]]; then
  echo "==> Generating CA"
  openssl req -x509 -newkey rsa:4096 -sha256 -days 3650 -nodes \
    -keyout ca.key -out ca.pem \
    -subj "/CN=rr-perf-ca" 2>/dev/null
else
  echo "==> Reusing existing CA (ca.pem, ca.key)"
fi

gen_cert() {
  local name="$1" cn="$2" dns1="$3"
  if [[ -f "${name}.pem" && -f "${name}.key" ]]; then
    echo "==> ${name}: already exists, skipping"
    return 0
  fi

  echo "==> Generating cert for ${name} (CN=${cn}, SAN=${dns1},localhost,127.0.0.1,::1)"
  openssl req -newkey rsa:2048 -sha256 -nodes \
    -keyout "${name}.key" -out "${name}.csr" \
    -subj "/CN=${cn}" 2>/dev/null

  cat > "${name}.ext" <<EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = ${dns1}
DNS.2 = localhost
IP.1  = 127.0.0.1
IP.2  = ::1
EOF

  openssl x509 -req -in "${name}.csr" \
    -CA ca.pem -CAkey ca.key -CAcreateserial \
    -out "${name}.pem" -days 365 -sha256 \
    -extfile "${name}.ext" 2>/dev/null

  rm -f "${name}.csr" "${name}.ext"
}

for i in $(seq 1 "$NODE_COUNT"); do
  gen_cert "node${i}" "kv-${i}" "kv-${i}"
done

# Client identity for the load/probe containers (mTLS clientAuth). Peers are
# servers; this cert is never presented as a server identity.
gen_cert "client" "rr-perf-client" "bench-1"

rm -f ca.srl

# Container processes run as uid 10001 (rr) and bind-mount this directory
# read-only, so keys must be world-readable. LOCAL PERF LAB ONLY.
chmod 644 ./*.pem ./*.key

echo
echo "Done. Certs in $(pwd):"
ls -1 ./*.pem ./*.key
