const defaultMetrics = createMetrics();

export function createMetrics() {
  return {
    captureAttempts: 0,
    captured: 0,
    encodedBytes: 0,
    sent: 0,
    sentBytes: 0,
    rendered: 0,
    renderedBytes: 0,
    skipped: 0,
    skippedByReason: {},
    lastSentSequence: null,
    lastRenderedSequence: null,
    lastCaptureAtMs: null,
    lastRenderedAtMs: null,
    listeners: new Set(),
  };
}

export function recordAttempt(metrics = defaultMetrics) {
  metrics.captureAttempts += 1;
  notify(metrics);
}

export function recordCapture(encoded, metrics = defaultMetrics) {
  metrics.captured += 1;
  metrics.encodedBytes += encoded?.bytes ?? encoded?.payload?.byteLength ?? 0;
  metrics.lastCaptureAtMs = now();
  notify(metrics);
}

export function recordSent(frame, metrics = defaultMetrics) {
  metrics.sent += 1;
  metrics.sentBytes += frame?.bytesWritten ?? frame?.bytes ?? frame?.payload?.byteLength ?? 0;
  metrics.lastSentSequence = String(frame.sequence);
  notify(metrics);
}

export function recordSkipped(reason, metrics = defaultMetrics) {
  const selectedReason = String(reason || "UNKNOWN");
  metrics.skipped += 1;
  metrics.skippedByReason[selectedReason] =
    (metrics.skippedByReason[selectedReason] || 0) + 1;
  notify(metrics);
}

export function recordRendered(frame, metrics = defaultMetrics) {
  metrics.rendered += 1;
  metrics.renderedBytes += frame?.payloadLength ?? frame?.payload?.byteLength ?? 0;
  metrics.lastRenderedSequence = String(frame.sequence);
  metrics.lastRenderedAtMs = now();
  notify(metrics);
}

export function snapshot(metrics = defaultMetrics) {
  return {
    captureAttempts: metrics.captureAttempts,
    captured: metrics.captured,
    encodedBytes: metrics.encodedBytes,
    sent: metrics.sent,
    sentBytes: metrics.sentBytes,
    rendered: metrics.rendered,
    renderedBytes: metrics.renderedBytes,
    skipped: metrics.skipped,
    skippedByReason: { ...metrics.skippedByReason },
    lastSentSequence: metrics.lastSentSequence,
    lastRenderedSequence: metrics.lastRenderedSequence,
    lastCaptureAtMs: metrics.lastCaptureAtMs,
    lastRenderedAtMs: metrics.lastRenderedAtMs,
  };
}

export function subscribe(listener, metrics = defaultMetrics) {
  metrics.listeners.add(listener);
  return () => metrics.listeners.delete(listener);
}

export function reset(metrics = defaultMetrics) {
  const replacement = createMetrics();
  for (const key of Object.keys(replacement)) {
    if (key !== "listeners") {
      metrics[key] = replacement[key];
    }
  }
  notify(metrics);
}

function notify(metrics) {
  for (const listener of metrics.listeners) {
    listener(snapshot(metrics));
  }
}

function now() {
  return typeof globalThis.performance?.now === "function"
    ? globalThis.performance.now()
    : Date.now();
}
