const REDACTED = "[REDACTED]";

const SECRET_TOKEN_RE =
  /\b(?:loom_(?:admin|api|invite|team|w|session|csrf|login)_[A-Za-z0-9._-]{12,}|sk-(?:proj-)?[A-Za-z0-9][A-Za-z0-9_-]{20,})\b/g;
const BEARER_RE = /\bBearer\s+(?!\[REDACTED\])[A-Za-z0-9._~+/=-]{12,}/gi;
const SECRET_REF_RE = /\b(?:loom|k8s-secret):\/\/[^\s"']+/gi;
const SIGNED_URL_PARAM_RE =
  /([?&](?:X-Amz-(?:Algorithm|Credential|Date|Expires|Security-Token|Signature|SignedHeaders)|AWSAccessKeyId|Expires|Signature)=)[^&\s"']+/gi;
const INTERNAL_URL_RE =
  /\bhttps?:\/\/(?:loom-control-plane|loom-llm-gateway|loom-worker|control-plane|llm-gateway|minio)(?:[.:][^/\s"']*)?/gi;

const SENSITIVE_KEYS = new Set([
  "authorization",
  "cookie",
  "set-cookie",
  "x-loom-csrf",
  "csrf",
  "csrf_token",
  "api_key",
  "apikey",
  "secret",
  "secret_key",
  "token",
  "access_token",
  "refresh_token",
  "invite_code",
  "password",
]);

export function redactText(value: string): string {
  return value
    .replace(BEARER_RE, `Bearer ${REDACTED}`)
    .replace(SECRET_TOKEN_RE, REDACTED)
    .replace(SECRET_REF_RE, REDACTED)
    .replace(SIGNED_URL_PARAM_RE, `$1${REDACTED}`)
    .replace(INTERNAL_URL_RE, REDACTED);
}

function shouldRedactKey(key: string): boolean {
  const normalized = key.trim().toLowerCase().replaceAll("-", "_");
  return (
    SENSITIVE_KEYS.has(normalized) ||
    normalized.endsWith("_token") ||
    normalized.endsWith("_secret") ||
    normalized.endsWith("_key")
  );
}

export function redactValue(value: unknown): unknown {
  if (typeof value === "string") return redactText(value);
  if (Array.isArray(value)) return value.map((item) => redactValue(item));
  if (!value || typeof value !== "object") return value;

  const out: Record<string, unknown> = {};
  for (const [key, inner] of Object.entries(value)) {
    out[key] = shouldRedactKey(key) ? REDACTED : redactValue(inner);
  }
  return out;
}
