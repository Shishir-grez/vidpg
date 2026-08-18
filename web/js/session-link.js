const SECRET_PATTERN = /^[0-9a-f]{64}$/;
const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function createSessionLink(baseUrl, sessionId, side, secret) {
  const normalizedSession = normalizeSessionId(sessionId);
  const normalizedSide = normalizeSide(side);
  const normalizedSecret = normalizeSecret(secret);
  const url = new URL(baseUrl, fallbackBaseUrl());
  url.searchParams.delete("session");
  url.searchParams.delete("side");
  url.searchParams.delete("token");
  url.searchParams.set("session", normalizedSession);
  url.searchParams.set("side", normalizedSide);
  url.hash = `token=${normalizedSecret}`;
  return url.toString();
}

export function parseSessionLink(value) {
  const url = new URL(value, fallbackBaseUrl());
  if (url.searchParams.has("token")) {
    throw new TypeError("session secret must be in the URL fragment");
  }
  const tokenParams = new URLSearchParams(url.hash.startsWith("#") ? url.hash.slice(1) : "");
  const token = tokenParams.get("token");
  if (token === null) {
    throw new TypeError("session link is missing its token fragment");
  }

  const sessionId = normalizeSessionId(url.searchParams.get("session"));
  const side = normalizeSide(url.searchParams.get("side"));
  const secret = normalizeSecret(token);
  const base = new URL(url.toString());
  base.searchParams.delete("session");
  base.searchParams.delete("side");
  base.searchParams.delete("token");
  base.hash = "";

  return {
    baseUrl: base.toString(),
    sessionId,
    side,
    secret,
    peerSide: side === "a" ? "b" : "a",
    currentLink: url.toString(),
  };
}

export async function copyPeerLink(currentLink) {
  const parsed = parseSessionLink(currentLink);
  const peerLink = createSessionLink(
    parsed.baseUrl,
    parsed.sessionId,
    parsed.peerSide,
    parsed.secret,
  );
  if (!globalThis.navigator?.clipboard?.writeText) {
    throw new Error("Clipboard API is unavailable");
  }
  await globalThis.navigator.clipboard.writeText(peerLink);
  return peerLink;
}

export function generateSessionId() {
  const cryptoApi = globalThis.crypto;
  if (typeof cryptoApi?.randomUUID === "function") {
    return cryptoApi.randomUUID();
  }
  if (typeof cryptoApi?.getRandomValues !== "function") {
    throw new Error("Secure random generation is unavailable");
  }
  const bytes = new Uint8Array(16);
  cryptoApi.getRandomValues(bytes);
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = Array.from(bytes, (value) => value.toString(16).padStart(2, "0"));
  return [
    hex.slice(0, 4).join(""),
    hex.slice(4, 6).join(""),
    hex.slice(6, 8).join(""),
    hex.slice(8, 10).join(""),
    hex.slice(10, 16).join(""),
  ].join("-");
}

export function generateSecret() {
  if (!globalThis.crypto?.getRandomValues) {
    throw new Error("Secure random generation is unavailable");
  }
  const bytes = new Uint8Array(32);
  globalThis.crypto.getRandomValues(bytes);
  return Array.from(bytes, (value) => value.toString(16).padStart(2, "0")).join("");
}

export function normalizeSide(side) {
  if (side !== "a" && side !== "b") {
    throw new TypeError("side must be a or b");
  }
  return side;
}

export function normalizeSecret(secret) {
  if (typeof secret !== "string" || !SECRET_PATTERN.test(secret)) {
    throw new TypeError("secret must be 64 lowercase hexadecimal characters");
  }
  return secret;
}

export function normalizeSessionId(sessionId) {
  if (typeof sessionId !== "string" || !UUID_PATTERN.test(sessionId)) {
    throw new TypeError("session must be a UUID");
  }
  return sessionId.toLowerCase();
}

function fallbackBaseUrl() {
  return globalThis.location?.href || "http://localhost/";
}
