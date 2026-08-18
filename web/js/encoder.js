export function createEncoder(canvas, quality = 0.65, width = 1280, height = 720) {
  if (!canvas) {
    throw new TypeError("canvas is required");
  }
  if (!Number.isFinite(quality) || quality < 0.3 || quality > 0.9) {
    throw new RangeError("JPEG quality must be between 0.3 and 0.9");
  }
  if (!Number.isInteger(width) || width < 160 || width > 1280) {
    throw new RangeError("encoder width must be between 160 and 1280");
  }
  if (!Number.isInteger(height) || height < 120 || height > 720) {
    throw new RangeError("encoder height must be between 120 and 720");
  }
  canvas.width = width;
  canvas.height = height;
  const context = canvas.getContext?.("2d");
  if (!context) {
    throw new Error("2D canvas context is unavailable");
  }
  return {
    canvas,
    context,
    quality,
    width,
    height,
    busy: false,
    disposed: false,
  };
}

export async function sampleAndEncode(video, encoder) {
  if (!encoder || encoder.disposed) {
    throw new Error("encoder is disposed");
  }
  if (encoder.busy) {
    throw new Error("encoder is already busy");
  }
  encoder.busy = true;
  const startedAt = monotonicMilliseconds();
  const capturedWallUs = BigInt(Date.now()) * 1000n;
  const capturedMonotonicNs = BigInt(Math.max(0, Math.floor(startedAt * 1_000_000)));
  try {
    encoder.context.drawImage(video, 0, 0, encoder.width, encoder.height);
    const blob = await canvasToBlob(encoder.canvas, encoder.quality);
    const payload = new Uint8Array(await blob.arrayBuffer());
    return {
      payload,
      bytes: payload.byteLength,
      width: encoder.width,
      height: encoder.height,
      codec: "jpeg",
      capturedWallUs,
      capturedMonotonicNs,
      encodeDurationMs: monotonicMilliseconds() - startedAt,
    };
  } finally {
    encoder.busy = false;
  }
}

export function isEncodeBusy(encoder) {
  return Boolean(encoder?.busy);
}

export function disposeEncoder(encoder) {
  if (!encoder) {
    return;
  }
  encoder.disposed = true;
  encoder.busy = false;
}

function canvasToBlob(canvas, quality) {
  return new Promise((resolve, reject) => {
    try {
      canvas.toBlob((blob) => {
        if (!blob) {
          reject(new Error("JPEG encoding returned no Blob"));
          return;
        }
        resolve(blob);
      }, "image/jpeg", quality);
    } catch (error) {
      reject(error);
    }
  });
}

function monotonicMilliseconds() {
  return typeof globalThis.performance?.now === "function"
    ? globalThis.performance.now()
    : Date.now();
}
