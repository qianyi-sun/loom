#!/usr/bin/env sh
set -eu

config_path="${LOOM_FRONTEND_CONFIG_PATH:-/usr/share/nginx/html/loom-frontend-config.json}"
index_template_path="${LOOM_FRONTEND_INDEX_TEMPLATE_PATH:-/usr/share/nginx/html/index.html.template}"
index_path="${LOOM_FRONTEND_INDEX_PATH:-/usr/share/nginx/html/index.html}"

environment="${LOOM_FRONTEND_ENVIRONMENT:-local}"
label="${LOOM_FRONTEND_ENVIRONMENT_LABEL:-Local development}"
route_path="${LOOM_FRONTEND_ROUTE_PATH:-}"
api_base="${LOOM_FRONTEND_API_BASE:-${route_path}}"
public_origin="${LOOM_FRONTEND_PUBLIC_ORIGIN:-}"

case "${environment}" in
  local|development|staging|production) ;;
  *)
    echo "LOOM_FRONTEND_ENVIRONMENT must be local, development, staging, or production" >&2
    exit 1
    ;;
esac

case "${route_path}" in
  ""|"/"|"/prod"|"/dev") ;;
  *)
    echo "LOOM_FRONTEND_ROUTE_PATH must be empty, /, /prod, or /dev" >&2
    exit 1
    ;;
esac

case "${api_base}" in
  ""|"/"|"/prod"|"/dev") ;;
  *)
    echo "LOOM_FRONTEND_API_BASE must be empty, /, /prod, or /dev" >&2
    exit 1
    ;;
esac

if [ "${route_path}" = "/" ]; then
  route_path=""
fi
if [ "${api_base}" = "/" ]; then
  api_base=""
fi
if [ "${api_base}" != "${route_path}" ]; then
  echo "LOOM_FRONTEND_API_BASE must match LOOM_FRONTEND_ROUTE_PATH" >&2
  exit 1
fi
if [ -n "${public_origin}" ] && ! printf '%s' "${public_origin}" | grep -Eq '^https://[^/]+$'; then
  echo "LOOM_FRONTEND_PUBLIC_ORIGIN must be an https origin without a path" >&2
  exit 1
fi
if [ "${environment}" = "production" ] && [ "${route_path}" != "/prod" ]; then
  echo "production frontend must use LOOM_FRONTEND_ROUTE_PATH=/prod" >&2
  exit 1
fi
if [ "${environment}" != "production" ] && [ "${route_path}" = "/prod" ]; then
  echo "non-production frontend must not use LOOM_FRONTEND_ROUTE_PATH=/prod" >&2
  exit 1
fi
if [ "${environment}" = "production" ] && printf '%s' "${label}" | grep -Eiq 'beta'; then
  echo "production frontend label must not contain beta wording" >&2
  exit 1
fi

if [ ! -f "${index_template_path}" ]; then
  echo "frontend index template not found: ${index_template_path}" >&2
  exit 1
fi

tmp_index_path="${index_path}.tmp"
if [ -n "${route_path}" ]; then
  sed \
    -e "s|src=\"\./assets/|src=\"${route_path}/assets/|g" \
    -e "s|href=\"\./assets/|href=\"${route_path}/assets/|g" \
    "${index_template_path}" > "${tmp_index_path}"
  if grep -Eq '(src|href)="\./assets/' "${tmp_index_path}"; then
    echo "frontend shell retained a relative build asset" >&2
    rm -f "${tmp_index_path}"
    exit 1
  fi
  if grep -Eq '/(dev|prod)/(dev|prod)/assets/' "${tmp_index_path}"; then
    echo "frontend shell contains a double or stale route prefix" >&2
    rm -f "${tmp_index_path}"
    exit 1
  fi
else
  cp "${index_template_path}" "${tmp_index_path}"
fi
mv "${tmp_index_path}" "${index_path}"

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

api_route_base="${public_origin}${api_base}/api"

tmp_path="${config_path}.tmp"
cat > "${tmp_path}" <<EOF
{
  "environment": "$(json_escape "${environment}")",
  "environmentLabel": "$(json_escape "${label}")",
  "routePath": "$(json_escape "${route_path}")",
  "apiBase": "$(json_escape "${api_base}")",
  "apiRouteBase": "$(json_escape "${api_route_base}")"
}
EOF
mv "${tmp_path}" "${config_path}"
