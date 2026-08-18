import { getBrowserConfig, buildRelayWebSocketUrl } from "./js/config.js";
import {
  copyPeerLink,
  createSessionLink,
  generateSecret,
  generateSessionId,
  parseSessionLink,
} from "./js/session-link.js";
import { connectRelay, sendJoin } from "./js/websocket-client.js";
import {
  attachPreview,
  defaultCameraConstraints,
  readTrackSettings,
  requestCamera,
  stopCamera,
} from "./js/camera.js";
import { createEncoder, disposeEncoder } from "./js/encoder.js";
import { startCaptureLoop, stopCaptureLoop } from "./js/uploader.js";
import { handleRemoteFrame, releasePreviousImage } from "./js/receiver.js";
import { createMetrics, snapshot, subscribe } from "./js/metrics.js";

const elements = {
  status: document.querySelector("#status"),
  error: document.querySelector("#error"),
  sessionLink: document.querySelector("#session-link"),
  peerLink: document.querySelector("#peer-link"),
  createSession: document.querySelector("#create-session"),
  copyPeer: document.querySelector("#copy-peer"),
  connect: document.querySelector("#connect"),
  disconnect: document.querySelector("#disconnect"),
  startCamera: document.querySelector("#start-camera"),
  stopCamera: document.querySelector("#stop-camera"),
  localVideo: document.querySelector("#local-video"),
  remoteImage: document.querySelector("#remote-image"),
  remoteImageBuffer: document.querySelector("#remote-image-buffer"),
  captureCanvas: document.querySelector("#capture-canvas"),
  cameraSettings: document.querySelector("#camera-settings"),
  metrics: document.querySelector("#metrics"),
};

const state = {
  config: getBrowserConfig(),
  metrics: createMetrics(),
  sessionId: null,
  side: null,
  secret: null,
  uploadSequence: 0n,
  uploadSequenceKey: null,
  currentLink: "",
  peerLink: "",
  relay: null,
  ready: null,
  cameraStream: null,
  encoder: null,
  capture: null,
  remote: {
    expectedStream: null,
    imageElement: elements.remoteImage,
    bufferImage: elements.remoteImageBuffer,
    activeImage: elements.remoteImage,
    imageUrls: new Map([
      [elements.remoteImage, null],
      [elements.remoteImageBuffer, null],
    ]),
    metrics: null,
    lastPaintedSequence: 0n,
    currentUrl: null,
    pendingUrl: null,
    pendingFrame: null,
    pendingRender: null,
  },
};
state.remote.metrics = state.metrics;

initialize();

function initialize() {
  elements.createSession.addEventListener("click", createSession);
  elements.copyPeer.addEventListener("click", copyPeer);
  elements.connect.addEventListener("click", connect);
  elements.disconnect.addEventListener("click", disconnect);
  elements.startCamera.addEventListener("click", startCamera);
  elements.stopCamera.addEventListener("click", stopCurrentCamera);
  subscribe(updateMetrics, state.metrics);
  setInterval(updateMetrics, 1000);

  try {
    const parsed = parseSessionLink(globalThis.location.href);
    applySession(parsed);
    setStatus("Session link loaded. Connect when both peers are ready.");
  } catch {
    setStatus("Create a session or open a peer link to begin.");
  }
  updateControls();
  updateMetrics();
  globalThis.__VIDPG_APP__ = state;
  globalThis.addEventListener?.("beforeunload", () => {
    stopCurrentCamera();
    state.relay?.close();
  });
}

function createSession() {
  try {
    const sessionId = generateSessionId();
    const secret = generateSecret();
    const baseUrl = cleanPageUrl();
    applySession({
      baseUrl,
      sessionId,
      side: "a",
      secret,
      peerSide: "b",
    });
    history.replaceState(null, "", state.currentLink);
    updateControls();
    setStatus("Session created. Copy the peer link to the other browser.");
  } catch (error) {
    showError(error);
  }
}

async function copyPeer() {
  if (!state.currentLink) {
    setStatus("Create or open a session before copying a peer link.");
    return;
  }
  try {
    state.peerLink = await copyPeerLink(state.currentLink);
    elements.peerLink.value = state.peerLink;
    setStatus("Peer link copied. Open it in the other browser.");
  } catch (error) {
    showError(error);
  }
}

async function connect() {
  if (!state.sessionId || !state.side || !state.secret) {
    setStatus("Create or open a session link first.");
    return;
  }
  if (state.relay) {
    return;
  }
  clearError();
  setStatus("Connecting to relay...");
  try {
    const url = buildRelayWebSocketUrl(
      state.sessionId,
      state.side,
      state.config.relayOrigin,
    );
    const relay = connectRelay(url);
    state.relay = relay;
    relay.onFrame = (frame) => handleRemoteFrame(frame, state.remote);
    relay.onError = (error) => {
      state.remote.lastError = error;
      showError(error);
    };
    relay.onControl = (message) => {
      if (message?.type === "close") {
        if (state.relay === relay) {
          setStatus("Relay connection closed.");
          stopCurrentCamera();
          releasePreviousImage(state.remote);
          state.relay = null;
          state.ready = null;
          updateControls();
        }
      }
    };
    const ready = await sendJoin(relay, state.secret);
    state.ready = ready;
    state.remote.expectedStream = ready.incoming_stream;
    state.remote.lastPaintedSequence = 0n;
    setStatus(`Connected as side ${state.side.toUpperCase()}.`);
  } catch (error) {
    state.relay?.close();
    state.relay = null;
    state.ready = null;
    showError(error);
  }
  updateControls();
}

function disconnect() {
  stopCurrentCamera();
  releasePreviousImage(state.remote);
  state.relay?.close();
  state.relay = null;
  state.ready = null;
  setStatus("Disconnected.");
  updateControls();
}

async function startCamera() {
  if (!state.ready || !state.relay) {
    setStatus("Connect to the relay before starting the camera.");
    return;
  }
  if (state.cameraStream) {
    return;
  }
  clearError();
  try {
    const stream = await requestCamera(
      defaultCameraConstraints(
        state.ready.target_fps || state.config.targetFps,
        state.config.width,
        state.config.height,
      ),
    );
    state.cameraStream = stream;
    attachPreview(stream, elements.localVideo);
    const settings = readTrackSettings(stream);
    elements.cameraSettings.textContent = formatCameraSettings(settings);
    state.encoder = createEncoder(
      elements.captureCanvas,
      state.config.jpegQuality,
      state.config.width,
      state.config.height,
    );
    state.capture = {
      video: elements.localVideo,
      encoder: state.encoder,
      relay: state.relay,
      metrics: state.metrics,
      uploadStream: state.ready.upload_stream,
      targetFps: state.ready.target_fps || state.config.targetFps,
      maxFrameBytes: state.ready.max_frame_bytes || state.config.maxFrameBytes,
      wsBufferThresholdBytes:
        state.ready.ws_buffer_threshold_bytes || state.config.wsBufferThresholdBytes,
      sequence: state.uploadSequence,
      running: false,
    };
    startCaptureLoop(state.capture);
    setStatus("Camera active. Sending newest JPEG frames.");
  } catch (error) {
    showError(error);
  }
  updateControls();
}

function stopCurrentCamera() {
  if (state.capture) {
    stopCaptureLoop(state.capture);
    state.uploadSequence = state.capture.sequence;
    writeUploadSequence(state.uploadSequenceKey, state.uploadSequence);
    state.capture = null;
  }
  if (state.encoder) {
    disposeEncoder(state.encoder);
    state.encoder = null;
  }
  if (state.cameraStream) {
    stopCamera(state.cameraStream, elements.localVideo);
    state.cameraStream = null;
  }
  updateControls();
}

function applySession(parsed) {
  const sessionChanged = state.sessionId !== parsed.sessionId || state.side !== parsed.side;
  state.sessionId = parsed.sessionId;
  state.side = parsed.side;
  state.secret = parsed.secret;
  state.uploadSequenceKey = `vidpg.sequence.${state.sessionId}.${state.side}`;
  if (sessionChanged) {
    state.uploadSequence = readUploadSequence(state.uploadSequenceKey);
  }
  state.currentLink = parsed.currentLink || createSessionLink(
    parsed.baseUrl,
    parsed.sessionId,
    parsed.side,
    parsed.secret,
  );
  state.peerLink = createSessionLink(
    parsed.baseUrl,
    parsed.sessionId,
    parsed.peerSide || (parsed.side === "a" ? "b" : "a"),
    parsed.secret,
  );
  elements.sessionLink.value = state.currentLink;
  elements.peerLink.value = state.peerLink;
}

function cleanPageUrl() {
  const url = new URL(globalThis.location.href);
  url.searchParams.delete("session");
  url.searchParams.delete("side");
  url.searchParams.delete("token");
  url.hash = "";
  return url.toString();
}

function setStatus(message) {
  elements.status.textContent = message;
}

function showError(error) {
  const message = error?.message || String(error);
  const stateLabel = error?.state ? `${error.state}: ` : "";
  elements.error.textContent = `${stateLabel}${message}`;
  elements.error.hidden = false;
  setStatus("Action needs attention.");
}

function clearError() {
  elements.error.textContent = "";
  elements.error.hidden = true;
}

function updateControls() {
  const hasSession = Boolean(state.sessionId && state.side && state.secret);
  const connected = Boolean(state.ready && state.relay);
  const cameraActive = Boolean(state.cameraStream);
  elements.connect.disabled = !hasSession || connected;
  elements.disconnect.disabled = !connected;
  elements.startCamera.disabled = !connected || cameraActive;
  elements.stopCamera.disabled = !cameraActive;
  elements.copyPeer.disabled = !hasSession;
}

function readUploadSequence(key) {
  try {
    const value = globalThis.sessionStorage?.getItem(key);
    if (value === null) {
      return 0n;
    }
    const sequence = BigInt(value);
    return sequence >= 0n ? sequence : 0n;
  } catch {
    return 0n;
  }
}

function writeUploadSequence(key, sequence) {
  if (!key) {
    return;
  }
  try {
    globalThis.sessionStorage?.setItem(key, String(sequence));
  } catch {
    // Session storage can be unavailable in privacy-restricted contexts.
  }
}

function updateMetrics() {
  const values = snapshot(state.metrics);
  const skipped = Object.entries(values.skippedByReason)
    .map(([reason, count]) => `${reason}: ${count}`)
    .join(", ");
  elements.metrics.textContent = [
    `encoded ${values.captured}`,
    `sent ${values.sent}`,
    `rendered ${values.rendered}`,
    `skipped ${values.skipped}`,
    `last sent ${values.lastSentSequence || "-"}`,
    `last rendered ${values.lastRenderedSequence || "-"}`,
    skipped ? `reasons ${skipped}` : "",
  ].filter(Boolean).join(" | ");
}

function formatCameraSettings(settings) {
  if (!settings.width || !settings.height) {
    return "Camera settings unavailable";
  }
  const fps = settings.frameRate ? ` at ${Math.round(settings.frameRate)} FPS` : "";
  return `${settings.width} x ${settings.height}${fps}`;
}
