#!/usr/bin/env bash
#
# Generate a self-signed CA + per-node server/client certs for local
# development and integration tests.
#
# DO NOT USE THESE IN PRODUCTION. Keys are written to disk unencrypted.
#
# Usage:
#   ./scripts/gen-dev-certs.sh [OUT_DIR] [NODE_COUNT]
#
# Defaults:
#   OUT_DIR      = ./dev-certs
#   NODE_COUNT   = 3
#
# Output (per node N in 1..=NODE_COUNT):
#   $OUT_DIR/ca.pem           — CA cert (shared by all nodes)
#   $OUT_DIR/ca.key           — CA private key (KEEP SECRET)
#   $OUT_DIR/nodeN.pem        — server + client cert
#   $OUT_DIR/nodeN.key        — matching private key
#
# Each node cert has SANs for:
#   - localhost, 127.0.0.1, ::1
#   - rr-N (for use with docker-compose hostnames)
#   - nodeN.rr.local (for kubernetes-style DNS)

set -euo pipefail

OUT_DIR="${1:-./dev-certs}"
NODE_COUNT="${2:-3}"

command -v openssl >/dev/null || {
  echo "error: openssl not in PATH" >&2
  exit 1
}

mkdir -p "$OUT_DIR"
cd "$OUT_DIR"

if [[ ! -f ca.key ]]; then
  echo "==> Generating CA"
  # basicConstraints CA:TRUE (critical) — without it the "CA" cert is not a CA
  # at all for strict verifiers (webpki tolerates it, many others don't).
  openssl req -x509 -newkey rsa:4096 -sha256 -days 3650 -nodes \
    -keyout ca.key -out ca.pem \
    -subj "/CN=rr-dev-ca" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null
else
  echo "==> Reusing existing CA (ca.pem, ca.key)"
fi

for i in $(seq 1 "$NODE_COUNT"); do
  NODE="node${i}"
  if [[ -f "${NODE}.pem" ]] && [[ -f "${NODE}.key" ]]; then
    echo "==> ${NODE}: already exists, skipping"
    continue
  fi

  echo "==> Generating cert for ${NODE}"
  openssl req -newkey rsa:2048 -sha256 -nodes \
    -keyout "${NODE}.key" -out "${NODE}.csr" \
    -subj "/CN=${NODE}.rr.local" 2>/dev/null

  cat > "${NODE}.ext" <<EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = rr-${i}
DNS.3 = ${NODE}.rr.local
IP.1  = 127.0.0.1
IP.2  = ::1
EOF

  openssl x509 -req -in "${NODE}.csr" \
    -CA ca.pem -CAkey ca.key -CAcreateserial \
    -out "${NODE}.pem" -days 365 -sha256 \
    -extfile "${NODE}.ext" 2>/dev/null

  rm -f "${NODE}.csr" "${NODE}.ext"
done

rm -f ca.srl

# nullglob + count guards: a bare unmatched glob is passed literally to
# chmod/ls and fails under set -e (no keys on disk, e.g. NODE_COUNT=0 with
# a pem-only reused dir). The expansions stay inside the `if`s because an
# empty-array expansion is itself an unbound-variable error under set -u
# on bash < 4.4.
shopt -s nullglob
keys=(./*.key)
pems=(./*.pem)
shopt -u nullglob
if ((${#keys[@]} > 0)); then
  chmod 600 "${keys[@]}"
fi
if ((${#pems[@]} > 0)); then
  chmod 644 "${pems[@]}"
fi

echo
echo "Done. Certs in $(pwd):"
if ((${#pems[@]} > 0)); then
  ls -1 "${pems[@]}"
fi
if ((${#keys[@]} > 0)); then
  ls -1 "${keys[@]}"
fi
echo
echo "Example rr-server invocation (node 1):"
echo "  rr-server serve \\"
echo "    --node-id 1 --listen-addr 127.0.0.1:51001 \\"
echo "    --tls-cert ${OUT_DIR}/node1.pem \\"
echo "    --tls-key ${OUT_DIR}/node1.key \\"
echo "    --tls-ca ${OUT_DIR}/ca.pem \\"
echo "    --tls-domain node1.rr.local \\"
echo "    --tls-require-client-cert \\"
echo "    --bootstrap"
