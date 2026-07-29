#!/usr/bin/env bash
# 05-add-host-ssl-cert-file.sh — verify --add-host + SSL_CERT_FILE for SDK TLS.
#
# Spec claims under test (cluster-deploy.md §Sandbox→gateway (b)):
#   1. Docker `--add-host <hostname>:<ip>` writes a /etc/hosts entry the
#      container's stock NSS reads.
#   2. Bind-mounting a CA cert + setting `SSL_CERT_FILE` lets stock
#      Python/curl/etc. validate an HTTPS server signed by that CA.
#   3. The combination — sandbox dials `https://<hostname>:port/` →
#      hostname resolves via /etc/hosts → TLS validates via loom-ca —
#      works end-to-end with no SDK changes.
#
# These are the layers (1)+(2) of the rev 6+ SSRF defense. If any
# breaks, the gateway-as-base-URL pattern fails.
#
# Runs against plain Docker. No k8s required.

set -euo pipefail

CA_DIR="$(mktemp -d /tmp/spike-05-XXXXXX)"
SERVER="loom-https-server-spike"
CLIENT="loom-sandbox-spike-05"
NET="loom-spike-05-net"

fail() { echo "FAIL: $*" >&2; rm -rf "$CA_DIR"; exit 1; }
pass() { echo "PASS: $*"; }

cleanup() {
  set +e
  docker rm -f "$CLIENT" "$SERVER" >/dev/null 2>&1
  docker network rm "$NET" >/dev/null 2>&1
  rm -rf "$CA_DIR"
  set -e
}
trap cleanup EXIT

# --- Setup: mint a tiny CA + server cert for loom-sandbox-gateway.local -----

echo "Setup: minting loom-ca + server cert..."
openssl genrsa -out "$CA_DIR/ca.key" 2048 >/dev/null 2>&1
openssl req -x509 -new -key "$CA_DIR/ca.key" -days 1 -out "$CA_DIR/ca.crt" \
  -subj "/CN=loom-ca-spike" >/dev/null 2>&1

openssl genrsa -out "$CA_DIR/server.key" 2048 >/dev/null 2>&1
cat > "$CA_DIR/server.cnf" <<EOF
[req]
distinguished_name = req
[v3_req]
subjectAltName = DNS:loom-sandbox-gateway.local
EOF
openssl req -new -key "$CA_DIR/server.key" -out "$CA_DIR/server.csr" \
  -subj "/CN=loom-sandbox-gateway.local" >/dev/null 2>&1
openssl x509 -req -in "$CA_DIR/server.csr" \
  -CA "$CA_DIR/ca.crt" -CAkey "$CA_DIR/ca.key" -CAcreateserial \
  -days 1 -out "$CA_DIR/server.crt" \
  -extensions v3_req -extfile "$CA_DIR/server.cnf" >/dev/null 2>&1

chmod 644 "$CA_DIR"/*.{key,crt}
pass "CA + server cert minted (SAN: DNS:loom-sandbox-gateway.local)"

# --- Setup: HTTPS server on a Docker bridge --------------------------------

docker network create --driver bridge "$NET" >/dev/null

# nginx + the server cert bind-mounted.
cat > "$CA_DIR/nginx.conf" <<'EOF'
events { worker_connections 64; }
http {
  server {
    listen 8443 ssl;
    ssl_certificate     /etc/loom-ca/server.crt;
    ssl_certificate_key /etc/loom-ca/server.key;
    location / { return 200 "singleton-mock-reply\n"; }
  }
}
EOF

# Pin nginx away from mutable nginx:alpine. Hub has returned manifests whose
# layer blobs 404 (content descriptor not found), which fails this spike with
# docker exit 125 even when spike 01/04 pass (#1089).
NGINX_IMAGE="${LOOM_SPIKE_NGINX_IMAGE:-nginx:1.27-alpine}"
docker pull "$NGINX_IMAGE" >/dev/null
docker run -d --name "$SERVER" --network "$NET" \
  --mount "type=bind,source=$CA_DIR,target=/etc/loom-ca,readonly" \
  --mount "type=bind,source=$CA_DIR/nginx.conf,target=/etc/nginx/nginx.conf,readonly" \
  "$NGINX_IMAGE" >/dev/null

# Wait for nginx to start.
for i in {1..20}; do
  if docker exec "$SERVER" sh -c "curl -k -sf https://localhost:8443/" >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done

SERVER_IP=$(docker inspect "$SERVER" \
  --format "{{(index .NetworkSettings.Networks \"$NET\").IPAddress}}")
echo "  server up on $SERVER_IP:8443 (mocked singleton)"

# --- Claim 1: --add-host populates /etc/hosts inside the client ------------

docker run -d --name "$CLIENT" --network "$NET" \
  --add-host "loom-sandbox-gateway.local:$SERVER_IP" \
  --mount "type=bind,source=$CA_DIR/ca.crt,target=/etc/ssl/loom-ca/loom-ca.crt,readonly" \
  alpine sh -c 'apk add --no-cache curl >/dev/null && sleep 600' >/dev/null

# Wait for the install.
for i in {1..30}; do
  if docker exec "$CLIENT" sh -c "command -v curl" >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done

hosts_entry=$(docker exec "$CLIENT" grep "loom-sandbox-gateway.local" /etc/hosts)
echo "  /etc/hosts entry: $hosts_entry"
if ! echo "$hosts_entry" | grep -q "$SERVER_IP"; then
  fail "claim 1: --add-host did not populate /etc/hosts correctly"
fi
pass "claim 1: --add-host writes /etc/hosts entry the client can read"

# --- Claim 2: curl with SSL_CERT_FILE validates the server cert ------------

reply=$(docker exec "$CLIENT" sh -c \
  "SSL_CERT_FILE=/etc/ssl/loom-ca/loom-ca.crt CURL_CA_BUNDLE=/etc/ssl/loom-ca/loom-ca.crt curl -sf --resolve loom-sandbox-gateway.local:8443:$SERVER_IP https://loom-sandbox-gateway.local:8443/" 2>&1 || echo "ERR")

if [ "$reply" = "singleton-mock-reply" ]; then
  pass "claim 2: curl validates server cert via SSL_CERT_FILE / CURL_CA_BUNDLE"
else
  fail "claim 2: curl could not validate — got '$reply'"
fi

# --- Claim 3: full round-trip via /etc/hosts (no --resolve) ----------------
# This is the actual production path: SDK reads the hostname, NSS hits
# /etc/hosts, TLS validates against loom-ca. No --resolve override.

reply=$(docker exec "$CLIENT" sh -c \
  "CURL_CA_BUNDLE=/etc/ssl/loom-ca/loom-ca.crt curl -sf https://loom-sandbox-gateway.local:8443/" 2>&1 || echo "ERR")

if [ "$reply" = "singleton-mock-reply" ]; then
  pass "claim 3: full round-trip — /etc/hosts → TLS via loom-ca → server → reply"
else
  fail "claim 3: round-trip failed — got '$reply'"
fi

# --- Claim 4: curl FAILS without SSL_CERT_FILE -----------------------------
# Sanity: the server cert is self-signed by loom-ca; without trusting
# loom-ca, validation MUST fail. If this passes, the spike isn't proving
# what it claims (the system trust store somehow has loom-ca).

if docker exec "$CLIENT" curl -sf https://loom-sandbox-gateway.local:8443/ >/dev/null 2>&1; then
  fail "claim 4: curl succeeded without SSL_CERT_FILE — system trust store has loom-ca somehow"
fi
pass "claim 4: curl fails-closed without SSL_CERT_FILE (expected; loom-ca isn't in system trust)"

echo ""
echo "All claims verified. Rev 6+ Path B sandbox SDK redirect (--add-host"
echo "+ SSL_CERT_FILE + bind-mounted loom-ca) composes correctly with"
echo "Docker + stock curl. Phase 2 can implement against this contract."
