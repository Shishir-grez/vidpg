const DEFAULTS = Object.freeze({
  width: 1280,
  height: 720,
  targetFps: 30,
  jpegQuality: 0.65,
  maxFrameBytes: 524288,
  wsBufferThresholdBytes: 524288,
});

export { DEFAULTS };

export function getBrowserConfig(locationLike = globalThis.location) {
  const href = locationLike?.href || "http://localhost/";
  const pageUrl = new URL(href);
  const params = pageUrl.searchParams;
  const relayOrigin =
    params.get("relay") || globalThis.__VIDPG_RELAY_ORIGIN__ || pageUrl.origin;

  return {
    ...DEFAULTS,
    relayOrigin,
  };
}

export function buildRelayWebSocketUrl(
  sessionId,
  side,
  relayOrigin = getBrowserConfig().relayOrigin,
) {
  const url = new URL(relayOrigin);
  if (url.protocol === "http:") {
    url.protocol = "ws:";
  } else if (url.protocol === "https:") {
    url.protocol = "wss:";
  } else if (url.protocol !== "ws:" && url.protocol !== "wss:") {
    throw new TypeError("relay origin must use http, https, ws, or wss");
  }
  url.pathname = "/ws";
  url.search = "";
  url.hash = "";
  url.searchParams.set("session", String(sessionId));
  url.searchParams.set("side", String(side));
  return url.toString();
}
