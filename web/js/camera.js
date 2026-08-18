const DEFAULT_CONSTRAINTS = Object.freeze({
  video: { width: 1280, height: 720, frameRate: 30 },
  audio: false,
});

export function defaultCameraConstraints(targetFps = 30, width = 1280, height = 720) {
  return {
    video: { width, height, frameRate: targetFps },
    audio: false,
  };
}

export async function requestCamera(constraints = DEFAULT_CONSTRAINTS) {
  if (globalThis.isSecureContext === false) {
    throw new CameraError(
      "insecure-context",
      "Camera access requires HTTPS or localhost. Open this LAN address over HTTPS or enable the browser's local insecure-origin setting.",
    );
  }
  const mediaDevices = globalThis.navigator?.mediaDevices;
  if (!mediaDevices || typeof mediaDevices.getUserMedia !== "function") {
    throw new CameraError("unsupported-browser", "This browser cannot access a camera.");
  }
  try {
    return await mediaDevices.getUserMedia(constraints);
  } catch (error) {
    const state = cameraFailureState(error);
    throw new CameraError(state, cameraFailureMessage(state), error);
  }
}

export function attachPreview(stream, videoElement) {
  if (!videoElement) {
    throw new TypeError("videoElement is required");
  }
  videoElement.srcObject = stream;
  videoElement.muted = true;
  videoElement.playsInline = true;
  const playResult = videoElement.play?.();
  if (playResult && typeof playResult.catch === "function") {
    playResult.catch(() => undefined);
  }
}

export function stopCamera(stream, videoElement = null) {
  for (const track of stream?.getTracks?.() ?? []) {
    track.stop();
  }
  if (videoElement && videoElement.srcObject === stream) {
    videoElement.srcObject = null;
  }
}

export function readTrackSettings(stream) {
  const track = stream?.getVideoTracks?.()[0];
  if (!track || typeof track.getSettings !== "function") {
    return {};
  }
  const settings = track.getSettings();
  return {
    width: settings.width ?? null,
    height: settings.height ?? null,
    frameRate: settings.frameRate ?? null,
  };
}

export class CameraError extends Error {
  constructor(state, message, cause = undefined) {
    super(message, cause === undefined ? undefined : { cause });
    this.name = "CameraError";
    this.state = state;
  }
}

function cameraFailureState(error) {
  if (error?.name === "NotAllowedError" || error?.name === "SecurityError") {
    return "permission-denied";
  }
  if (error?.name === "NotFoundError" || error?.name === "OverconstrainedError") {
    return "no-camera";
  }
  return "camera-ended";
}

function cameraFailureMessage(state) {
  switch (state) {
    case "permission-denied":
      return "Camera permission was denied. Allow access and try again.";
    case "no-camera":
      return "No compatible camera was found.";
    default:
      return "The camera stopped or could not be started.";
  }
}
