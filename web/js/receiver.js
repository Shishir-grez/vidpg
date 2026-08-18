import { decodeFrameMessage, validateIncomingFrame } from "./protocol.js";
import { recordRendered, recordSkipped } from "./metrics.js";

export function handleRemoteFrame(frame, state) {
  let decoded = frame;
  try {
    if (frame instanceof ArrayBuffer || ArrayBuffer.isView(frame)) {
      decoded = decodeFrameMessage(frame);
    }
    const validation = validateIncomingFrame(decoded, state.expectedStream);
    if (!validation.ok) {
      recordSkipped(`REMOTE_${validation.code}`, state.metrics);
      return false;
    }
  } catch (error) {
    recordSkipped("REMOTE_INVALID", state.metrics);
    state.lastError = error;
    return false;
  }

  if (!paintIfNewest(decoded, state)) {
    recordSkipped("STALE_REMOTE", state.metrics);
    return false;
  }

  const urlApi = state.urlApi || globalThis.URL;
  if (!urlApi?.createObjectURL) {
    recordSkipped("RENDER_UNAVAILABLE", state.metrics);
    return false;
  }
  const url = urlApi.createObjectURL(
    new Blob([decoded.payload], { type: "image/jpeg" }),
  );
  const visibleImage = imageElement(state);
  const renderImage = inactiveImage(state);
  const pendingImage = renderImage || createPendingImage(visibleImage);
  if (!pendingImage) {
    urlApi.revokeObjectURL?.(url);
    recordSkipped("RENDER_UNAVAILABLE", state.metrics);
    return false;
  }
  const previousImageUrl = state.imageUrls?.get(renderImage);
  if (previousImageUrl && previousImageUrl !== state.currentUrl) {
    urlApi.revokeObjectURL?.(previousImageUrl);
  }
  const pending = {
    frame: decoded,
    url,
    image: pendingImage,
    renderImage,
  };
  state.imageUrls?.set(renderImage, url);
  state.pendingFrame = decoded;
  state.pendingUrl = url;
  state.pendingRender = pending;

  pendingImage.onload = () => {
    const decode = pendingImage.decode?.();
    if (decode && typeof decode.then === "function") {
      decode.then(
        () => requestFrame(state, () => commitPaint(pending, state, visibleImage)),
        () => failPending(pending, state),
      );
      return;
    }
    requestFrame(state, () => commitPaint(pending, state, visibleImage));
  };
  pendingImage.onerror = () => {
    failPending(pending, state);
  };
  pendingImage.src = url;
  return true;
}

export function paintIfNewest(frame, state) {
  const sequence = BigInt(frame.sequence);
  const painted = BigInt(state.lastPaintedSequence ?? 0n);
  return !state.pendingRender && sequence > painted;
}

export function releasePreviousImage(state) {
  const urlApi = state.urlApi || globalThis.URL;
  const pendingImage = state.pendingRender?.image;
  if (pendingImage) {
    pendingImage.onload = null;
    pendingImage.onerror = null;
  }
  const urls = new Set([
    state.currentUrl,
    state.pendingUrl,
    ...(state.imageUrls?.values?.() || []),
  ]);
  for (const url of urls) {
    if (url) {
      urlApi?.revokeObjectURL?.(url);
    }
  }
  state.currentUrl = null;
  state.pendingUrl = null;
  state.pendingFrame = null;
  state.pendingRender = null;
  for (const image of renderImages(state)) {
    image.onload = null;
    image.onerror = null;
    image.removeAttribute?.("src");
    setImageVisible(image, image === (state.imageElement || state.remoteImage));
  }
  state.activeImage = state.imageElement || state.remoteImage || null;
  state.imageUrls?.clear?.();
}

function commitPaint(pending, state, image) {
  if (state.pendingRender !== pending) {
    return false;
  }
  const sequence = BigInt(pending.frame.sequence);
  const urlApi = state.urlApi || globalThis.URL;
  if (sequence <= BigInt(state.lastPaintedSequence ?? 0n)) {
    urlApi?.revokeObjectURL?.(pending.url);
    return false;
  }
  const oldUrl = state.currentUrl;
  const oldImage = imageElement(state);
  const nextImage = pending.renderImage || image;
  if (pending.renderImage) {
    setImageVisible(nextImage, true);
    setImageVisible(oldImage, false);
    state.activeImage = nextImage;
  } else if (nextImage && nextImage.src !== pending.url) {
    nextImage.src = pending.url;
  }
  state.currentUrl = pending.url;
  state.pendingUrl = null;
  state.pendingFrame = null;
  state.pendingRender = null;
  state.lastPaintedSequence = sequence;
  if (oldUrl && oldUrl !== pending.url && !pending.renderImage) {
    urlApi?.revokeObjectURL?.(oldUrl);
  }
  if (pending.image) {
    pending.image.onload = null;
    pending.image.onerror = null;
  }
  recordRendered(pending.frame, state.metrics);
  return true;
}

function imageElement(state) {
  return state.activeImage || state.imageElement || state.remoteImage || null;
}

function renderImages(state) {
  const primary = state.imageElement || state.remoteImage || null;
  const buffer = state.bufferImage || state.remoteImageBuffer || null;
  return buffer && buffer !== primary ? [primary, buffer] : primary ? [primary] : [];
}

function inactiveImage(state) {
  const active = imageElement(state);
  return renderImages(state).find((image) => image !== active) || null;
}

function createPendingImage(visibleImage) {
  if (typeof globalThis.Image === "function") {
    const image = new globalThis.Image();
    image.decoding = "async";
    return image;
  }
  return visibleImage;
}

function failPending(pending, state) {
  if (state.pendingRender !== pending) {
    return;
  }
  const urlApi = state.urlApi || globalThis.URL;
  urlApi.revokeObjectURL?.(pending.url);
  state.imageUrls?.delete?.(pending.renderImage);
  state.pendingRender = null;
  state.pendingFrame = null;
  state.pendingUrl = null;
  recordSkipped("REMOTE_DECODE_ERROR", state.metrics);
}

function setImageVisible(image, visible) {
  if (!image) {
    return;
  }
  image.classList?.toggle?.("is-visible", visible);
  if (image.style) {
    image.style.opacity = visible ? "1" : "0";
  }
}

function requestFrame(state, callback) {
  const request = state.requestAnimationFrame || globalThis.requestAnimationFrame;
  if (typeof request === "function") {
    request(callback);
  } else {
    setTimeout(callback, 0);
  }
}
