import { isSocketBackpressured, sendFrame } from "./websocket-client.js";
import { encodeFrameHeader } from "./protocol.js";
import { isEncodeBusy, sampleAndEncode } from "./encoder.js";
import {
  recordAttempt,
  recordCapture,
  recordSent,
  recordSkipped,
} from "./metrics.js";

export const SKIP_REASONS = Object.freeze({
  RATE_GATE: "RATE_GATE",
  ENCODE_BUSY: "ENCODE_BUSY",
  OVERSIZE_PAYLOAD: "OVERSIZE_PAYLOAD",
  SOCKET_NOT_OPEN: "SOCKET_NOT_OPEN",
  BUFFERED_AMOUNT_HIGH: "BUFFERED_AMOUNT_HIGH",
  ENCODE_ERROR: "ENCODE_ERROR",
  SEND_ERROR: "SEND_ERROR",
});

export function startCaptureLoop(state) {
  if (state.running) {
    return;
  }
  state.running = true;
  state.sequence = state.sequence === undefined ? 0n : BigInt(state.sequence);
  state.lastCaptureStart = null;
  const schedule = () => {
    if (!state.running) {
      return;
    }
    const video = state.video;
    if (typeof video?.requestVideoFrameCallback === "function") {
      state.videoFrameCallbackHandle = video.requestVideoFrameCallback((timestamp) => {
        void captureOne(state, Number(timestamp) || monotonicMilliseconds());
        schedule();
      });
      return;
    }
    state.captureTimer = setTimeout(() => {
      void captureOne(state, monotonicMilliseconds());
      schedule();
    }, framePeriod(state));
  };
  state.captureSchedule = schedule;
  schedule();
}

export function stopCaptureLoop(state) {
  state.running = false;
  if (state.captureTimer !== undefined) {
    clearTimeout(state.captureTimer);
    state.captureTimer = undefined;
  }
  const cancel = state.video?.cancelVideoFrameCallback;
  if (typeof cancel === "function" && state.videoFrameCallbackHandle !== undefined) {
    try {
      cancel.call(state.video, state.videoFrameCallbackHandle);
    } catch {
      // A callback that already fired cannot be cancelled; its running flag is enough.
    }
  }
  state.videoFrameCallbackHandle = undefined;
}

export function shouldSkipFrame(state, encoded) {
  const bytes = encoded?.bytes ?? encoded?.payload?.byteLength ?? 0;
  if (bytes <= 0 || bytes > state.maxFrameBytes) {
    return SKIP_REASONS.OVERSIZE_PAYLOAD;
  }
  if (!isSocketOpen(state.relay)) {
    return SKIP_REASONS.SOCKET_NOT_OPEN;
  }
  if (isSocketBackpressured(state.relay, state.wsBufferThresholdBytes)) {
    return SKIP_REASONS.BUFFERED_AMOUNT_HIGH;
  }
  return null;
}

export function nextSequence(state) {
  const current = state.sequence === undefined ? 0n : BigInt(state.sequence);
  const next = current + 1n;
  if (next > 0xffffffffffffffffn) {
    throw new Error("frame sequence exhausted");
  }
  state.sequence = next;
  return next;
}

async function captureOne(state, now) {
  if (!state.running) {
    return;
  }
  recordAttempt(state.metrics);
  const period = framePeriod(state);
  if (state.lastCaptureStart !== null && now < state.lastCaptureStart + period) {
    recordSkipped(SKIP_REASONS.RATE_GATE, state.metrics);
    return;
  }
  state.lastCaptureStart = now;
  if (isEncodeBusy(state.encoder)) {
    recordSkipped(SKIP_REASONS.ENCODE_BUSY, state.metrics);
    return;
  }

  let encoded;
  try {
    encoded = await sampleAndEncode(state.video, state.encoder);
  } catch (error) {
    state.lastError = error;
    recordSkipped(SKIP_REASONS.ENCODE_ERROR, state.metrics);
    stopCaptureLoop(state);
    return;
  }
  recordCapture(encoded, state.metrics);

  const skipReason = shouldSkipFrame(state, encoded);
  if (skipReason !== null) {
    recordSkipped(skipReason, state.metrics);
    return;
  }

  const sequence = nextSequence(state);
  const payload = encoded.payload;
  const meta = {
    streamId: state.uploadStream,
    sequence,
    capturedWallUs: encoded.capturedWallUs,
    width: encoded.width,
    height: encoded.height,
    payloadLength: payload.byteLength,
  };
  try {
    const decision = sendFrame(
      state.relay,
      { meta, payload },
      state.wsBufferThresholdBytes,
    );
    if (decision.sent) {
      recordSent({ ...encoded, sequence, bytesWritten: decision.bytesWritten }, state.metrics);
    } else {
      recordSkipped(decision.reason || SKIP_REASONS.SEND_ERROR, state.metrics);
    }
  } catch (error) {
    state.lastError = error;
    recordSkipped(SKIP_REASONS.SEND_ERROR, state.metrics);
  }
}

function isSocketOpen(relay) {
  const socket = relay?.socket ?? relay;
  return socket?.readyState === 1;
}

function framePeriod(state) {
  const fps = Number.isFinite(state.targetFps) && state.targetFps > 0
    ? state.targetFps
    : 30;
  return 1000 / fps;
}

function monotonicMilliseconds() {
  return typeof globalThis.performance?.now === "function"
    ? globalThis.performance.now()
    : Date.now();
}

export function buildFrameHeader(meta) {
  return encodeFrameHeader(meta);
}
