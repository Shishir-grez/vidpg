const HEADER_SIZE = 48;
const HEADER_FIELDS_SIZE = 44;
const PROTOCOL_VERSION = 0x01;
const MESSAGE_TYPE_VIDEO_FRAME = 0x01;
const CODEC_JPEG = 0x01;
const FLAGS_RESERVED = 0;
const MAX_UINT32 = 0xffffffffn;
const MAX_UINT64 = 0xffffffffffffffffn;
const MIN_INT64 = -0x8000000000000000n;
const MAX_INT64 = 0x7fffffffffffffffn;
const MAX_FRAME_BYTES = 524288;

const CRC_TABLE = createCrcTable();

export {
  CODEC_JPEG,
  FLAGS_RESERVED,
  HEADER_FIELDS_SIZE,
  HEADER_SIZE,
  MESSAGE_TYPE_VIDEO_FRAME,
  PROTOCOL_VERSION,
};

export function encodeFrameHeader(meta) {
  const normalized = normalizeMeta(meta);
  const bytes = new Uint8Array(HEADER_SIZE);
  const view = new DataView(bytes.buffer);
  view.setUint8(0, PROTOCOL_VERSION);
  view.setUint8(1, MESSAGE_TYPE_VIDEO_FRAME);
  view.setUint8(2, CODEC_JPEG);
  view.setUint8(3, FLAGS_RESERVED);
  bytes.set(uuidToBytes(normalized.streamId), 4);
  setUint64(view, 20, normalized.sequence);
  setInt64(view, 28, normalized.capturedWallUs);
  view.setUint16(36, normalized.width, false);
  view.setUint16(38, normalized.height, false);
  view.setUint32(40, normalized.payloadLength, false);
  view.setUint32(44, crc32(bytes.subarray(0, HEADER_FIELDS_SIZE)), false);
  return bytes.buffer;
}

export function decodeFrameMessage(buffer) {
  const bytes = asBytes(buffer);
  if (bytes.byteLength < HEADER_SIZE) {
    throw new ProtocolError("TRUNCATED_HEADER", "frame is shorter than 48 bytes");
  }
  const header = bytes.subarray(0, HEADER_SIZE);
  const view = new DataView(header.buffer, header.byteOffset, header.byteLength);
  if (view.getUint8(0) !== PROTOCOL_VERSION) {
    throw new ProtocolError("BAD_VERSION", "unsupported protocol version");
  }
  if (view.getUint8(1) !== MESSAGE_TYPE_VIDEO_FRAME) {
    throw new ProtocolError("BAD_MESSAGE_TYPE", "unsupported message type");
  }
  if (view.getUint8(2) !== CODEC_JPEG) {
    throw new ProtocolError("BAD_CODEC", "only JPEG frames are accepted");
  }
  if (view.getUint8(3) !== FLAGS_RESERVED) {
    throw new ProtocolError("BAD_FLAGS", "V1 flags must be zero");
  }
  const expectedCrc = view.getUint32(44, false);
  const actualCrc = crc32(header.subarray(0, HEADER_FIELDS_SIZE));
  if (expectedCrc !== actualCrc) {
    throw new ProtocolError("BAD_HEADER_CRC", "header CRC32 does not match");
  }

  const payloadLength = view.getUint32(40, false);
  const width = view.getUint16(36, false);
  const height = view.getUint16(38, false);
  if (width < 160 || width > 1280 || height < 120 || height > 720) {
    throw new ProtocolError("BAD_DIMENSION", "frame dimensions are outside V1 limits");
  }
  if (payloadLength < 1) {
    throw new ProtocolError("BAD_LENGTH", "payload must not be empty");
  }
  if (payloadLength > MAX_FRAME_BYTES) {
    throw new ProtocolError("OVERSIZE_PAYLOAD", "payload exceeds the V1 limit");
  }
  const expectedLength = HEADER_SIZE + payloadLength;
  if (bytes.byteLength !== expectedLength) {
    throw new ProtocolError("BAD_LENGTH", "frame length does not match header");
  }
  const payload = bytes.slice(HEADER_SIZE);
  if (payload.byteLength < 4 || payload[0] !== 0xff || payload[1] !== 0xd8 ||
      payload[payload.byteLength - 2] !== 0xff || payload[payload.byteLength - 1] !== 0xd9) {
    throw new ProtocolError("BAD_JPEG_MARKER", "JPEG SOI/EOI markers are required");
  }

  return {
    version: PROTOCOL_VERSION,
    messageType: MESSAGE_TYPE_VIDEO_FRAME,
    codec: "jpeg",
    flags: FLAGS_RESERVED,
    streamId: bytesToUuid(header.subarray(4, 20)),
    sequence: getUint64(view, 20),
    capturedWallUs: getInt64(view, 28),
    width,
    height,
    payloadLength,
    payload,
  };
}

export function validateIncomingFrame(frame, expectedStream = null) {
  if (!frame || typeof frame !== "object") {
    return invalid("BAD_TYPE", "frame must be an object");
  }
  try {
    if (expectedStream !== null && normalizeUuid(frame.streamId) !== normalizeUuid(expectedStream)) {
      return invalid("UNAUTHORIZED_STREAM", "frame stream does not match the peer stream");
    }
  } catch {
    return invalid("BAD_STREAM", "frame stream must be a UUID");
  }
  if (frame.codec !== "jpeg") {
    return invalid("BAD_CODEC", "only JPEG frames are accepted");
  }
  let sequence;
  try {
    sequence = toUint64(frame.sequence, "sequence");
  } catch (error) {
    return invalid("BAD_SEQUENCE", error.message);
  }
  if (sequence < 1n) {
    return invalid("BAD_SEQUENCE", "sequence must start at one");
  }
  if (!Number.isInteger(frame.width) || !Number.isInteger(frame.height) ||
      frame.width < 160 || frame.width > 1280 || frame.height < 120 || frame.height > 720) {
    return invalid("BAD_DIMENSION", "frame dimensions are outside V1 limits");
  }
  let payload;
  try {
    payload = asBytes(frame.payload);
  } catch {
    return invalid("BAD_TYPE", "payload must be binary data");
  }
  if (payload.byteLength !== frame.payloadLength) {
    return invalid("BAD_LENGTH", "payload length does not match bytes");
  }
  if (payload.byteLength > MAX_FRAME_BYTES) {
    return invalid("OVERSIZE_PAYLOAD", "payload exceeds the V1 limit");
  }
  if (payload.byteLength === 0 || payload[0] !== 0xff || payload[1] !== 0xd8 ||
      payload[payload.byteLength - 2] !== 0xff || payload[payload.byteLength - 1] !== 0xd9) {
    return invalid("BAD_JPEG_MARKER", "JPEG SOI/EOI markers are required");
  }
  return { ok: true, sequence };
}

export function packFrameMessage(meta, payload) {
  const rawPayload = asBytes(payload);
  const normalized = { ...meta, payloadLength: rawPayload.byteLength };
  const header = new Uint8Array(encodeFrameHeader(normalized));
  const result = new Uint8Array(HEADER_SIZE + rawPayload.byteLength);
  result.set(header, 0);
  result.set(rawPayload, HEADER_SIZE);
  return result.buffer;
}

export function uuidToBytes(value) {
  const normalized = normalizeUuid(value).replaceAll("-", "");
  const bytes = new Uint8Array(16);
  for (let index = 0; index < 16; index += 1) {
    bytes[index] = Number.parseInt(normalized.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}

export function bytesToUuid(bytes) {
  const raw = asBytes(bytes);
  if (raw.byteLength !== 16) {
    throw new ProtocolError("BAD_STREAM", "stream UUID must contain 16 bytes");
  }
  const hex = Array.from(raw, (value) => value.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

export class ProtocolError extends Error {
  constructor(code, message) {
    super(`${code}: ${message}`);
    this.name = "ProtocolError";
    this.code = code;
  }
}

function normalizeMeta(meta) {
  if (!meta || typeof meta !== "object") {
    throw new ProtocolError("BAD_TYPE", "frame metadata must be an object");
  }
  const streamId = normalizeUuid(meta.streamId ?? meta.stream_id);
  const sequence = toUint64(meta.sequence, "sequence");
  const capturedWallUs = toInt64(meta.capturedWallUs ?? meta.captured_wall_us ?? 0, "capturedWallUs");
  const width = integerInRange(meta.width, 0, 0xffff, "width");
  const height = integerInRange(meta.height, 0, 0xffff, "height");
  const payloadLength = integerInRange(
    meta.payloadLength ?? meta.payload_length,
    1,
    Number(MAX_UINT32),
    "payloadLength",
  );
  if (meta.codec !== undefined && meta.codec !== "jpeg") {
    throw new ProtocolError("BAD_CODEC", "only JPEG frames are accepted");
  }
  if (sequence < 1n) {
    throw new ProtocolError("BAD_SEQUENCE", "sequence must start at one");
  }
  if (width < 160 || width > 1280 || height < 120 || height > 720) {
    throw new ProtocolError("BAD_DIMENSION", "frame dimensions are outside V1 limits");
  }
  if (payloadLength > MAX_FRAME_BYTES) {
    throw new ProtocolError("OVERSIZE_PAYLOAD", "payload exceeds the V1 limit");
  }
  return { streamId, sequence, capturedWallUs, width, height, payloadLength };
}

function normalizeUuid(value) {
  if (typeof value !== "string" || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value)) {
    throw new ProtocolError("BAD_STREAM", "stream_id must be a UUID");
  }
  return value.toLowerCase();
}

function integerInRange(value, minimum, maximum, name) {
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    const code = name === "width" || name === "height"
      ? "BAD_DIMENSION"
      : name === "payloadLength"
        ? "BAD_LENGTH"
        : "BAD_TYPE";
    throw new ProtocolError(code, `${name} is outside its wire range`);
  }
  return value;
}

function toUint64(value, name) {
  let result;
  try {
    result = typeof value === "bigint" ? value : BigInt(value);
  } catch {
    throw new ProtocolError("BAD_" + name.toUpperCase(), `${name} must be an integer`);
  }
  if (result < 0n || result > MAX_UINT64) {
    throw new ProtocolError("BAD_" + name.toUpperCase(), `${name} is outside uint64`);
  }
  return result;
}

function toInt64(value, name) {
  let result;
  try {
    result = typeof value === "bigint" ? value : BigInt(value);
  } catch {
    throw new ProtocolError("BAD_TIMESTAMP", `${name} must be an integer`);
  }
  if (result < MIN_INT64 || result > MAX_INT64) {
    throw new ProtocolError("BAD_TIMESTAMP", `${name} is outside int64`);
  }
  return result;
}

function setUint64(view, offset, value) {
  let remaining = value;
  for (let index = 7; index >= 0; index -= 1) {
    view.setUint8(offset + index, Number(remaining & 0xffn));
    remaining >>= 8n;
  }
}

function setInt64(view, offset, value) {
  setUint64(view, offset, value < 0n ? value + (1n << 64n) : value);
}

function getUint64(view, offset) {
  let result = 0n;
  for (let index = 0; index < 8; index += 1) {
    result = (result << 8n) | BigInt(view.getUint8(offset + index));
  }
  return result;
}

function getInt64(view, offset) {
  const raw = getUint64(view, offset);
  return raw >= (1n << 63n) ? raw - (1n << 64n) : raw;
}

function asBytes(value) {
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  throw new ProtocolError("BAD_TYPE", "binary data must be an ArrayBuffer or view");
}

function invalid(code, message) {
  return { ok: false, code, message };
}

function createCrcTable() {
  const table = new Uint32Array(256);
  for (let index = 0; index < 256; index += 1) {
    let value = index;
    for (let bit = 0; bit < 8; bit += 1) {
      value = (value & 1) ? 0xedb88320 ^ (value >>> 1) : value >>> 1;
    }
    table[index] = value >>> 0;
  }
  return table;
}

function crc32(bytes) {
  let value = 0xffffffff;
  for (const byte of bytes) {
    value = CRC_TABLE[(value ^ byte) & 0xff] ^ (value >>> 8);
  }
  return (value ^ 0xffffffff) >>> 0;
}
